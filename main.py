import argparse
import logging
import math
import os
import re
import json
from datetime import datetime
from time import sleep
from typing import Any, Dict, List

from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import BulkWriteError, ServerSelectionTimeoutError

from src.api import ECourtsScraper
from src.parser import batch_process_judgments, case_details_parser, extract_years_data
from src.utils import get_all_dates_in_year

from config import *

load_dotenv()

DISPLAY_CASE = 100
BATCH_SIZE = 10
STATE_CODE = CALCUTTA_HIGH_COURT
COURT_NAME = "CALCUTTA_HIGH_COURT"


# Configure logging
def setup_logger(name: str = "main", level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(level)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    os.makedirs("logs", exist_ok=True)
    file_handler = logging.FileHandler(f'logs/main_{__import__("time").strftime("%Y%m%d")}.log')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


logger = setup_logger()


def save_state(state, server_no):
    """Save the current state to a local JSON file"""
    state_dir = "state_files"
    os.makedirs(state_dir, exist_ok=True)

    filename = f"{state_dir}/scraper_state_{state['state_code']}_{server_no}.json"
    with open(filename, "w") as f:
        json.dump(state, f, indent=2)
    logger.info(f"State saved to {filename}")


def load_state(state_code, server_no):
    """Load state from a local JSON file if it exists"""
    filename = f"state_files/scraper_state_{state_code}_{server_no}.json"
    if os.path.exists(filename):
        with open(filename, "r") as f:
            state = json.load(f)
        logger.info(f"Loaded existing state from {filename}")
        return state
    return None


def upload_to_azure_and_delete_local(
    file_path: str, blob_service_client: BlobServiceClient, container_name: str
) -> str:
    """Upload PDF to Azure Blob Storage and delete local file"""
    try:
        blob_name = os.path.basename(file_path)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        logger.info(f"Uploading {file_path} to Azure Blob Storage")
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

        # Delete local file
        os.remove(file_path)
        logger.info(f"Deleted local file: {file_path}")

        # Return Azure Blob URL
        blob_url = f"https://{blob_service_client.account_name}.blob.core.windows.net/{container_name}/{blob_name}"
        logger.info(f"Successfully uploaded to: {blob_url}")
        return blob_url

    except Exception as e:
        logger.error(f"Error uploading to Azure/deleting local file: {str(e)}", exc_info=True)
        return ""


def process_case_batch(
    batch: List[List[Any]],
    scraper: ECourtsScraper,
    blob_service_client: BlobServiceClient,
    container_name: str,
    mongo_collection,
) -> None:
    """Process a batch of cases: parse, download, upload to Azure, and store in MongoDB"""

    def process_single_case(case: List[Any]) -> Dict[str, Any]:
        try:
            case_detail = case_details_parser(case[1])
            if not case_detail.get("url"):
                logger.warning(f"No URL found for case: {case_detail.get('title', 'Unknown')}")
                return case_detail

            case_detail["court"] = COURT_NAME.replace("_", " ").lower()

            # Download PDF
            local_path = scraper.download_judgment(case_detail["url"])
            if local_path:
                # Upload to Azure and get URL
                azure_url = upload_to_azure_and_delete_local(local_path, blob_service_client, container_name)
                case_detail["url"] = azure_url
                # case_detail["local_url"] = local_path
            else:
                logger.warning(f"Failed to download PDF for case: {case_detail.get('title', 'Unknown')}")
                # case_detail["azure_url"] = ""
                # case_detail["local_url"] = ""

            return case_detail

        except Exception as e:
            logger.error(f"Error processing case: {str(e)}", exc_info=True)
            return {"_error": str(e), "_processing_failed": True}

    logger.info(f"Processing batch of {len(batch)} cases")
    processed_cases = batch_process_judgments(batch, process_single_case)

    # Store in MongoDB
    try:
        if processed_cases:
            mongo_collection.insert_many(processed_cases, ordered=False)
            logger.info(f"Stored {len(processed_cases)} cases in MongoDB")
    except BulkWriteError as bwe:
        logger.error(f"Error writing to MongoDB: {str(bwe)}", exc_info=True)
    except ServerSelectionTimeoutError as sste:
        logger.error(f"MongoDB connection error: {str(sste)}", exc_info=True)


def divide_data(data, n):
    """
    Divides the input data (year: count) into n parts with approximately equal counts.

    Args:
        data (dict): A dictionary where keys are years (strings) and values are counts (strings).
        n (int): The number of parts to divide the data into.

    Returns:
        list: A list of n dictionaries, where each dictionary represents a part of the data.
              Each part contains a subset of the original data with approximately equal counts.
    """

    print("years", data)
    print("count", n)

    # Convert counts to integers
    data = {year: int(count) for year, count in data.items()}

    # Sort the data by year
    sorted_data = sorted(data.items())

    # Calculate the total count
    total_count = sum(data.values())

    # Calculate the target count for each part
    target_count = total_count / n

    # Initialize the result list
    result = [{} for _ in range(n)]

    # Initialize the current part index
    part_index = 0

    # Initialize the current part count
    current_count = 0

    # Iterate over the sorted data
    for year, count in sorted_data:
        # If adding the current data point exceeds the target count,
        # split the data point into the current part and the next part
        if current_count + count > target_count:
            # Calculate the amount of count to add to the current part
            add_to_current = round(target_count - current_count, 6)

            # Add the amount to the current part
            result[part_index][year] = int(add_to_current)

            # Increment the part index
            part_index += 1

            # If we have reached the end of the result list, break the loop
            if part_index >= n:
                break

            # Calculate the amount of count to add to the next part
            add_to_next = round(count - add_to_current, 6)

            # Initialize the current count with the amount to add to the next part
            current_count = add_to_next

            # Add the amount to the next part
            result[part_index][year] = int(add_to_next)
        else:
            # Add the data point to the current part
            result[part_index][year] = count

            # Increment the current count
            current_count += count

        # If the current count equals the target count,
        # move to the next part
        if abs(current_count - target_count) < 1e-6:
            part_index += 1
            current_count = 0

        # If we have reached the end of the result list, break the loop
        if part_index >= n:
            break

    # If there are any remaining data points, add them to the last part
    if part_index < n and sorted_data[0][0] not in result[-1]:
        remaining_data = dict(sorted_data[len(result[0]) :])
        result[-1].update(remaining_data)

    # Convert counts back to strings and sort keys
    result = [{year: str(count) for year, count in sorted(part.items())} for part in result]

    return result


def main():
    parser = argparse.ArgumentParser(description="ECourts Scraper with data division")
    parser.add_argument("-c", "--server_count", type=int, help="Number of servers to divide data into", required=True)
    parser.add_argument("-s", "--server_no", type=int, help="Server number to process", required=True)
    args = parser.parse_args()

    server_count = args.server_count
    server_no = args.server_no

    # Initialize Azure Blob Storage
    azure_connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    container_name = os.getenv("AZURE_CONTAINER_NAME", "")
    if not azure_connection_string:
        logger.error("Missing AZURE_STORAGE_CONNECTION_STRING")
        raise ValueError("Azure Storage connection string not configured")

    blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)

    # Initialize MongoDB
    mongodb_uri = os.getenv("MONGODB_URI", "")
    mongodb_uri = re.sub(r"\\x3a", ":", mongodb_uri)
    database_name = os.getenv("MONGODB_DATABASE", "")
    collection_name = os.getenv("MONGODB_COLLECTION", "")

    try:
        mongo_client = MongoClient(mongodb_uri)
        db = mongo_client[database_name]
        collection = db[collection_name]
        logger.info("Connected to MongoDB")
    except ServerSelectionTimeoutError as e:
        logger.error(f"Failed to connect to MongoDB: {str(e)}")
        raise

    try:
        # Get or initialize state from local file
        state = load_state(STATE_CODE, server_no)

        if not state:
            # Initialize new state if not exists
            state = {
                "state_code": STATE_CODE,
                "current_year_index": 0,
                "current_date_index": 0,
                "current_request": 0,
                "current_batch": 0,
                "completed": False,
                "years": {},
                "last_updated": datetime.now().isoformat(),
            }
            save_state(state, server_no)
            logger.info(f"Initialized new state for {STATE_CODE}")
        else:
            logger.info(f"Resuming from existing state for {STATE_CODE}")

        scraper = ECourtsScraper(STATE_CODE)

        # Initializing session
        _ = scraper.search_cases()

        # Only fetch years data if we haven't already
        if not state["years"]:
            # Fetching year data available for given court
            response = scraper.get_highcourt_year_data()
            if not response or not response.get("year_dtls"):
                logger.warning(f"No results for years data")
                return

            years = extract_years_data(response["year_dtls"])
            logger.info(f"Total years data available for given court: {len(years)}")

            # Ensure years is stored as a dictionary in state
            if isinstance(years, dict):
                state["years"] = years
            else:
                # If years is returned as a list, convert to dict (assuming format)
                state["years"] = {str(year): "0" for year in years} if isinstance(years, list) else {}

            state["last_updated"] = datetime.now().isoformat()
            save_state(state, server_no)
        else:
            years = state["years"]
            logger.info(f"Using years from saved state: {years}")

        del scraper

        # Divide the years data based on server count
        divided_years = divide_data(years, server_count)

        # Get the years data for the current server
        if 1 <= server_no <= len(divided_years):
            years = divided_years[server_no - 1]
            logger.info(f"Processing data for server {server_no} with years: {years}")
        else:
            logger.error(f"Invalid server number: {server_no}. Must be between 1 and {len(divided_years)}")
            return

        # Convert years dict to a sorted list of year keys for iteration
        years_list = sorted(years.keys(), reverse=True)  # Sort years in descending order
        logger.info(f"Processing years: {years_list}")

        # Start processing from where we left off
        current_year_index = state["current_year_index"]
        for year_idx in range(current_year_index, len(years_list)):
            year = years_list[year_idx]
            year_count = years[year]  # Get the count for this year

            # Update the current year index in state
            state["current_year_index"] = year_idx
            state["last_updated"] = datetime.now().isoformat()
            save_state(state, server_no)

            # Debug logging
            logger.info(f"Processing year {year} with {year_count} records (index {year_idx})")

            # Generate date ranges for this year using the count from the dictionary
            date_ranges_in_a_year = get_all_dates_in_year(int(year), int(year_count))
            logger.info(f"Generated {len(date_ranges_in_a_year)} dates for year {year}")

            # If we're continuing from a previous run, start from the saved date index
            start_date_idx = state["current_date_index"] if year_idx == current_year_index else 0

            for date_idx in range(start_date_idx, len(date_ranges_in_a_year)):
                start_date, end_date = date_ranges_in_a_year[date_idx]

                # Update the current date index in state
                state["current_date_index"] = date_idx
                state["current_request"] = 0  # Reset request counter for new date
                state["current_batch"] = 0  # Reset batch counter for new date
                state["last_updated"] = datetime.now().isoformat()
                save_state(state, server_no)

                logger.info(f"Processing dates: {start_date}-{end_date}")

                scraper = ECourtsScraper(STATE_CODE)
                _ = scraper.search_cases()

                search_results = scraper.search_cases(
                    from_date=start_date, to_date=end_date, display_length=DISPLAY_CASE
                )
                if not search_results or not search_results.get("reportrow"):
                    logger.error(f"No search results returned for dates: {start_date}-{end_date}")
                    continue

                total_records = int(search_results["reportrow"]["iTotalRecords"])
                total_requests = math.ceil(total_records / DISPLAY_CASE)
                logger.info(f"Total records: {total_records}, Total requests: {total_requests}")

                # Start from the saved request number or from 0
                start_req = (
                    state["current_request"]
                    if (year_idx == current_year_index and date_idx == state["current_date_index"])
                    else 0
                )

                for req_no in range(start_req, total_requests):
                    # Update current request in state
                    state["current_request"] = req_no
                    state["current_batch"] = 0  # Reset batch counter for new request
                    state["last_updated"] = datetime.now().isoformat()
                    save_state(state, server_no)

                    logger.info(f"Processing request {req_no + 1}/{total_requests}")

                    start_from = req_no * DISPLAY_CASE
                    logger.info(f"Fetching results from index {start_from} with length {DISPLAY_CASE}")

                    search_results = scraper.search_cases(
                        from_date=start_date, to_date=end_date, start_from=start_from, display_length=DISPLAY_CASE
                    )

                    if not search_results or not search_results.get("reportrow"):
                        logger.warning(f"No results for batch starting at {start_from}")
                        continue

                    data = search_results["reportrow"]["aaData"]
                    logger.info(f"Retrieved {len(data)} results starting at index {start_from}")

                    if data is None or scraper is None:
                        continue

                    # Process in batches with state tracking
                    start_batch = state["current_batch"] if req_no == state["current_request"] else 0
                    batch_count = (len(data) + BATCH_SIZE - 1) // BATCH_SIZE  # Calculate total batches

                    for batch_idx in range(start_batch, batch_count):
                        # Update batch index in state
                        state["current_batch"] = batch_idx
                        state["last_updated"] = datetime.now().isoformat()
                        save_state(state, server_no)

                        start_pos = batch_idx * BATCH_SIZE
                        end_pos = min(start_pos + BATCH_SIZE, len(data))
                        batch = data[start_pos:end_pos]

                        logger.info(
                            f"Processing batch {batch_idx + 1}/{batch_count}, items {start_pos} to {end_pos-1}"
                        )
                        process_case_batch(batch, scraper, blob_service_client, container_name, collection)

                    sleep(1)

                logger.info(f"Processed data of court {STATE_CODE} from {start_date} to {end_date}")

            # Reset date index when moving to a new year
            state["current_date_index"] = 0
            state["last_updated"] = datetime.now().isoformat()
            save_state(state, server_no)

            logger.info(f"Processed data of court {STATE_CODE} of {year}")

        # Mark as completed
        state["completed"] = True
        state["last_updated"] = datetime.now().isoformat()
        save_state(state, server_no)

        logger.info("Processing completed.")

    except Exception as e:
        logger.error(f"Error in main processing loop: {str(e)}", exc_info=True)
        # Log more details about the error
        logger.error(f"Error type: {type(e)}")
        logger.error(f"Error trace:", exc_info=True)
    finally:
        mongo_client.close()
        logger.info("MongoDB connection closed")


if __name__ == "__main__":
    main()

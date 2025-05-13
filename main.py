import logging
import math
import os
import re
from time import sleep
from typing import Any, Dict, List

from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import BulkWriteError, ServerSelectionTimeoutError

from src.api import ECourtsScraper
from src.parser import batch_process_judgments, case_details_parser

load_dotenv()

DISPLAY_CASE = 1000
BATCH_SIZE = 25
STATE_CODE = 7
COURT_CODE = 2


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

            # Download PDF
            local_path = scraper.download_judgment(case_detail["url"])
            if local_path:
                # Upload to Azure and get URL
                # azure_url = upload_to_azure_and_delete_local(local_path, blob_service_client, container_name)
                # case_detail["azure_url"] = azure_url
                case_detail["local_url"] = local_path
            else:
                logger.warning(f"Failed to download PDF for case: {case_detail.get('title', 'Unknown')}")
                case_detail["azure_url"] = ""
                case_detail["local_url"] = ""

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


def main():

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

        scraper = ECourtsScraper()

        # Initial search to get total records
        search_results = scraper.search_cases(
            state_code=str(STATE_CODE),  # Delhi state code
            court_type=str(COURT_CODE),  # High Court
        )

        if not search_results or not search_results.get("reportrow"):
            logger.error("No search results returned")
            return

        total_records = int(search_results["reportrow"]["iTotalRecords"])
        total_requests = math.ceil(total_records / DISPLAY_CASE)
        logger.info(f"Total records: {total_records}, Total requests: {total_requests}")

        for req_no in range(total_requests):
            logger.info(f"Processing request {req_no + 1}/{total_requests}")

            start_from = req_no * DISPLAY_CASE
            logger.info(f"Fetching results from index {start_from} with length {DISPLAY_CASE}")

            search_results = scraper.search_cases(
                state_code=STATE_CODE, court_type=COURT_CODE, start_from=start_from, display_length=DISPLAY_CASE
            )

            if not search_results or not search_results.get("reportrow"):
                logger.warning(f"No results for batch starting at {start_from}")
                return None, None

            data = search_results["reportrow"]["aaData"]
            logger.info(f"Retrieved {len(data)} results starting at index {start_from}")

            if data is None or scraper is None:
                continue

            # Process in batches
            for i in range(0, len(data), BATCH_SIZE):
                batch = data[i : i + BATCH_SIZE]
                process_case_batch(batch, scraper, blob_service_client, container_name, collection)

            sleep(3)

        logger.info("Processing complete")

    except Exception as e:
        logger.error(f"Error in main processing loop: {str(e)}", exc_info=True)
    finally:
        mongo_client.close()
        logger.info("MongoDB connection closed")


if __name__ == "__main__":
    main()

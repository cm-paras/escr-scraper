import logging
import re
from typing import Dict, Optional, Any, List, Union
from bs4 import BeautifulSoup, Tag, NavigableString


# Configure logging
def setup_logger(name: str = "parser", level: int = logging.INFO) -> logging.Logger:
    """Set up and return a configured logger instance"""
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(level)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    try:
        import os

        os.makedirs("logs", exist_ok=True)
        file_handler = logging.FileHandler(f'logs/parser_{__import__("time").strftime("%Y%m%d")}.log')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except Exception as e:
        logger.warning(f"Failed to create file handler: {e}")

    return logger


logger = setup_logger()


def extract_pdf_path_from_button(onclick_attr: str) -> Optional[str]:
    """Extract the PDF path from the button's onclick attribute"""
    if not onclick_attr:
        logger.warning("Empty onclick attribute provided")
        return None

    patterns = [
        r"open_pdf\s*\(\s*['\"]?(\d+)['\"]?\s*,\s*['\"]([^'\"]*)['\"]?\s*,\s*['\"]([^'\"]+)['\"]",
        r"open_pdf\('(\d+)',\s*'',\s*'([^']+)'",
        r"open_pdf\(.*?['\"]([^'\"]+\.pdf[^'\"]*)['\"]",
        r"(https?://[^'\"]+\.pdf[^'\"]*)",
    ]

    for pattern in patterns:
        match = re.search(pattern, onclick_attr)
        if match:
            path = match.group(3 if pattern == patterns[0] else 2 if pattern == patterns[1] else 1).strip()
            logger.debug(f"Extracted PDF path: {path[:50]}...")
            return path

    logger.warning(f"Failed to extract PDF path from onclick: {onclick_attr[:100]}...")
    return None


def safe_text_extraction(element: Union[Tag, NavigableString, None]) -> str:
    """Safely extract text from a BeautifulSoup element"""
    if element is None:
        return ""
    try:
        return element.text.strip()
    except Exception as e:
        logger.warning(f"Error extracting text: {e}")
        return ""


def normalize_text(text: str) -> str:
    """Normalize text by removing extra whitespace and converting to lowercase"""
    if not text:
        return ""
    return re.sub(r"\s+", " ", text.strip()).lower()


def case_details_parser(res: str) -> Dict[str, Any]:
    """Parse case details from HTML string"""
    data = {}
    logger.info("Parsing case details")

    if not res or not isinstance(res, str):
        logger.error("Invalid input: empty or non-string input")
        return data

    try:
        soup = BeautifulSoup(res, "html.parser")

        # Extract URL
        pdf_path = extract_pdf_path_from_button(res)
        if pdf_path:
            data["url"] = pdf_path.replace("&search=%20", "")

        # Parse button for case details
        button = soup.find("button")
        if button:
            heading_text = safe_text_extraction(button)

            if "of" in heading_text:
                parts = heading_text.split("of", 1)
                if len(parts) > 1:
                    data["title"] = normalize_text(parts[1])

                if parts[0].strip():
                    case_details = parts[0].strip()
                    details_parts = case_details.split("/")
                    if len(details_parts) >= 3:
                        data["case_type"], data["case_number"], data["year"] = map(normalize_text, details_parts[:3])
                    elif len(details_parts) == 2:
                        data["case_type"] = normalize_text(details_parts[0])
                        number_year = details_parts[1].strip()
                        year_match = re.search(r"(\d{4})", number_year)
                        if year_match:
                            data["year"] = year_match.group(1)
                            data["case_number"] = normalize_text(number_year[: year_match.start()])
                        else:
                            data["case_number"] = normalize_text(number_year)
                    else:
                        case_match = re.search(r"([a-zA-Z\s]+)\s+(\d+)(?:\s+of\s+|\s+)(\d{4})", case_details)
                        if case_match:
                            data["case_type"], data["case_number"], data["year"] = map(
                                normalize_text, case_match.groups()
                            )
                        else:
                            data["raw_details"] = normalize_text(case_details)
            else:
                data["raw_heading"] = normalize_text(heading_text)

        # Extract judge name
        judge_element = soup.find("strong")
        if judge_element:
            judge_text = safe_text_extraction(judge_element)
            judge_match = re.search(r"(?:Judge|Hon\'ble|Justice)[:\s]+([^:]+)", judge_text, re.IGNORECASE)
            data["judge"] = normalize_text(judge_match.group(1) if judge_match else judge_text.replace("Judge :", ""))

        # Extract other case details
        case_details_elem = soup.find("strong", class_="caseDetailsTD")
        if case_details_elem:
            fields = case_details_elem.find_all("span")
            values = case_details_elem.find_all("font")

            for field, value in zip(fields, values):
                field_name = normalize_text(safe_text_extraction(field).replace("|", "").replace(":", ""))
                if field_name:
                    data[field_name] = normalize_text(safe_text_extraction(value))

            if not fields or not values:
                raw_details = safe_text_extraction(case_details_elem)
                data["raw_case_details"] = normalize_text(raw_details)
                for field, value in re.findall(r"([^:|]+):\s*([^|]+)", raw_details):
                    data[normalize_text(field)] = normalize_text(value)

        # Add metadata
        data["_metadata"] = {
            "parser_version": "2.1",
            "raw_html_length": len(res),
            "fields_extracted": len(data),
            "timestamp": __import__("time").time(),
            "is_complete": not bool(
                [f for f in ["url", "title", "case_type", "case_number", "year"] if f not in data]
            ),
        }

        logger.info(f"Parsed case details with {len(data)} fields")
        return data

    except Exception as e:
        logger.error(f"Error parsing case details: {str(e)}", exc_info=True)
        data["_error"] = str(e)
        return data


def extract_judgment_metadata(html_row: str) -> Dict[str, Any]:
    """Extract metadata from judgment HTML row"""
    metadata = {}
    logger.info("Extracting judgment metadata")

    if not html_row or not isinstance(html_row, str):
        logger.error("Invalid input: empty or non-string HTML row")
        return metadata

    try:
        soup = BeautifulSoup(html_row, "html.parser")

        button = soup.find("button")
        if button and "onclick" in button.attrs:
            if pdf_path := extract_pdf_path_from_button(button["onclick"]):
                metadata["pdf_url"] = pdf_path

        if id_element := soup.find(attrs={"data-judgment-id": True}):
            metadata["judgment_id"] = id_element["data-judgment-id"]

        if button:
            button_text = safe_text_extraction(button)
            if "of" in button_text:
                parts = button_text.split("of", 1)
                if len(parts) > 1:
                    metadata["title"] = normalize_text(parts[1])
                if parts[0].strip():
                    case_details = parts[0].strip().split("/")
                    if len(case_details) >= 3:
                        metadata["case_type"], metadata["case_number"], metadata["year"] = map(
                            normalize_text, case_details[:3]
                        )

        if date_elem := soup.find(text=re.compile(r"\d{2}-\d{2}-\d{4}")):
            if date_match := re.search(r"(\d{2}-\d{2}-\d{4})", str(date_elem)):
                metadata["judgment_date"] = date_match.group(1)

        if judge_elem := soup.find("strong"):
            judge_text = safe_text_extraction(judge_elem)
            if "Judge :" in judge_text:
                metadata["judge"] = normalize_text(judge_text.replace("Judge :", ""))

        logger.info(f"Extracted metadata with {len(metadata)} fields")
        return metadata

    except Exception as e:
        logger.error(f"Error extracting metadata: {str(e)}", exc_info=True)
        return {"_error": str(e)}


def parse_search_results(search_results: Dict[str, Any]) -> Dict[str, Any]:
    """Parse search results into structured judgment data"""
    parsed_results = []
    logger.info("Parsing search results")

    if not search_results or not isinstance(search_results, dict):
        logger.error("Invalid search results: not a dictionary")
        return {"judgments": [], "metadata": {"error": "Invalid input"}}

    try:
        rows = search_results.get("reportrow", [])
        logger.info(f"Found {len(rows)} judgment rows")

        for i, row in enumerate(rows):
            try:
                if not row:
                    logger.warning(f"Empty row at index {i}")
                    continue

                judgment_data = case_details_parser(row[0])
                judgment_data["_row_index"] = i
                if len(row[0]) < 1000:
                    judgment_data["_raw_html"] = row[0]

                parsed_results.append(judgment_data)

                if (i + 1) % 20 == 0 or i + 1 == len(rows):
                    logger.info(f"Progress: {i+1}/{len(rows)} rows parsed")

            except Exception as e:
                logger.error(f"Error parsing row {i+1}: {str(e)}", exc_info=True)
                parsed_results.append({"_row_index": i, "_error": str(e), "_parsing_failed": True})

        metadata = {
            "total_rows": len(rows),
            "successful_parses": sum(1 for r in parsed_results if not r.get("_parsing_failed", False)),
            "failed_parses": sum(1 for r in parsed_results if r.get("_parsing_failed", False)),
            "parser_version": "2.1",
            "timestamp": __import__("time").time(),
        }

        logger.info(f"Parsed {len(parsed_results)}/{len(rows)} rows")
        return {"judgments": parsed_results, "metadata": metadata}

    except Exception as e:
        logger.error(f"Error parsing search results: {str(e)}", exc_info=True)
        return {"judgments": [], "metadata": {"error": str(e)}}


def batch_process_judgments(judgments: List[Dict[str, Any]], processor_func: callable) -> List[Dict[str, Any]]:
    """Process a batch of judgments with a custom processor function"""
    results = []
    logger.info(f"Batch processing {len(judgments)} judgments")

    for i, judgment in enumerate(judgments):
        try:
            results.append(processor_func(judgment))
            if (i + 1) % 10 == 0 or i + 1 == len(judgments):
                logger.info(f"Progress: {i+1}/{len(judgments)} judgments processed")
        except Exception as e:
            logger.error(f"Error processing judgment {i+1}: {str(e)}", exc_info=True)
            judgment["_processing_error"] = str(e)
            results.append(judgment)

    return results

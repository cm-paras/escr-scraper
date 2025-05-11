import requests
import json
import time
import os
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin
import uuid
import logging
from PIL import Image
from io import BytesIO
from azure.cognitiveservices.vision.computervision import ComputerVisionClient
from azure.cognitiveservices.vision.computervision.models import OperationStatusCodes
from msrest.authentication import CognitiveServicesCredentials


# Configure logging
def setup_logger(name: str = "scraper", level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(level)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    os.makedirs("logs", exist_ok=True)
    file_handler = logging.FileHandler(f'logs/scraper_{time.strftime("%Y%m%d")}.log')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


class ECourtsScraper:
    """Class to scrape judgments from the eCourts Judgments search website"""

    BASE_URL = "https://judgments.ecourts.gov.in/pdfsearch/"
    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-US,en;q=0.9",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://judgments.ecourts.gov.in",
        "Referer": "https://judgments.ecourts.gov.in/",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }

    def __init__(self):
        """Initialize with a session object to maintain cookies"""
        self.logger = setup_logger()
        self.session = requests.Session()
        self.app_token = None
        self.verified = False

        # Get Azure credentials from environment variables
        subscription_key = os.getenv("COMPUTER_VISION_CLIENT_SUBSCRIPTION_KEY")
        endpoint = os.getenv("COMPUTER_VISION_CLIENT_ENDPOINT")

        if not subscription_key or not endpoint:
            self.logger.error(
                "Missing Azure credentials. Please set COMPUTER_VISION_CLIENT_SUBSCRIPTION_KEY and COMPUTER_VISION_CLIENT_ENDPOINT environment variables."
            )
            raise ValueError("Azure credentials not configured properly")

        self.client = ComputerVisionClient(endpoint, CognitiveServicesCredentials(subscription_key))
        self.max_captcha_attempts = 5
        self.initialize_session()

    def initialize_session(self):
        """Initialize the session and get the first app_token"""
        try:
            self.logger.info("Initializing session")
            response = self.session.get(self.BASE_URL)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "html.parser")
            app_token_tag = soup.find("input", {"id": "app_token"})

            if app_token_tag and app_token_tag.get("value"):
                self.app_token = app_token_tag.get("value")
                self.logger.info(f"Initial app_token acquired")
            else:
                script_tags = soup.find_all("script")
                for script in script_tags:
                    if script.string and "app_token" in str(script.string):
                        token_match = re.search(r"app_token=([a-f0-9]+)", str(script.string))
                        if token_match:
                            self.app_token = token_match.group(1)
                            self.logger.info(f"Found app_token in script")
                            break

            if not self.app_token:
                self.logger.error("Failed to extract initial app_token")
                # raise ValueError("Could not initialize session: No app_token found")

            self.logger.debug(f"Current cookies: {self.session.cookies.get_dict()}")

        except Exception as e:
            self.logger.error(f"Error initializing session: {str(e)}", exc_info=True)
            raise

    def verify(self):
        for attempt in range(self.max_captcha_attempts):
            self.logger.info(f"Verification attempt {attempt + 1}/{self.max_captcha_attempts}")
            if self.get_captcha():
                captcha_code = self.solve_captcha()
                if self.verify_captcha(captcha_code):
                    self.verified = True
                    self.logger.info("CAPTCHA verification successful")
                    return
            self.logger.warning(f"CAPTCHA attempt {attempt + 1} failed")
        self.logger.error("Failed to verify CAPTCHA after maximum attempts")
        raise ValueError("CAPTCHA verification failed")

    def update_app_token(self, response_data):
        """Update app_token from response"""
        if isinstance(response_data, dict) and "app_token" in response_data:
            self.app_token = response_data["app_token"]
            self.logger.debug(f"Updated app_token")

    def get_captcha(self, save_path="captcha.png"):
        """Get CAPTCHA image and save it to a file"""
        try:
            captcha_url = urljoin(self.BASE_URL, f"vendor/securimage/securimage_show.php?{int(time.time() * 1000)}")
            self.logger.info(f"Fetching CAPTCHA from {captcha_url}")

            img_response = self.session.get(
                captcha_url,
                stream=True,
                headers={"Referer": self.BASE_URL, "User-Agent": self.headers["User-Agent"]},
            )
            img_response.raise_for_status()

            with open(save_path, "wb") as f:
                for chunk in img_response.iter_content(1024):
                    f.write(chunk)

            self.logger.info(f"CAPTCHA image saved as {save_path}")
            return True

        except Exception as e:
            self.logger.error(f"Error getting CAPTCHA: {str(e)}", exc_info=True)
            return False

    def solve_expression(self, text):
        """Solve a mathematical expression extracted from CAPTCHA."""
        try:
            self.logger.debug(f"Solving expression: {text}")
            text = text.strip().replace(" ", "")

            operators = {
                "+": lambda x, y: x + y,
                "-": lambda x, y: x - y,
                "*": lambda x, y: x * y,
                "x": lambda x, y: x * y,
                "/": lambda x, y: x / y,
            }

            for op, func in operators.items():
                if op in text:
                    operands = [int("".join(filter(str.isdigit, part))) for part in text.split(op)]
                    if len(operands) == 2:
                        result = func(operands[0], operands[1])
                        self.logger.info(f"Solved: {operands[0]}{op}{operands[1]}={result}")
                        return result
                    raise ValueError(f"Invalid expression format: {text}")

            digits_only = "".join(filter(str.isdigit, text))
            if digits_only:
                self.logger.warning(f"No operator found, returning digits: {digits_only}")
                return int(digits_only)
            raise ValueError(f"No valid operator found in expression: {text}")

        except Exception as e:
            self.logger.error(f"Error evaluating expression '{text}': {str(e)}")
            return f"Error evaluating expression: {str(e)}"

    def solve_captcha(self, image_path="captcha.png"):
        """Extract text from a CAPTCHA image using Azure OCR."""
        try:
            self.logger.info(f"Processing CAPTCHA image: {image_path}")
            with open(image_path, "rb") as image_data:
                read_response = self.client.read_in_stream(image_data, raw=True)

            operation_location = read_response.headers["Operation-Location"]
            operation_id = operation_location.split("/")[-1]

            for _ in range(10):  # Max 10 seconds wait
                get_text_results = self.client.get_read_result(operation_id)
                if get_text_results.status not in [OperationStatusCodes.running, OperationStatusCodes.not_started]:
                    break
                time.sleep(1)

            if get_text_results.status == OperationStatusCodes.succeeded:
                text = "\n".join(
                    line.text
                    for page_result in get_text_results.analyze_result.read_results
                    for line in page_result.lines
                )
                self.logger.info(f"Extracted CAPTCHA text: {text}")
                return self.solve_expression(text.strip())

            self.logger.error(f"Text extraction failed with status: {get_text_results.status}")
            return "Failed to read text from image"

        except Exception as e:
            self.logger.error(f"Error extracting text from image: {str(e)}", exc_info=True)
            return "Failed to read text from image"

    def verify_captcha(self, captcha_solution):
        """Verify CAPTCHA solution"""
        try:
            url = f"{self.BASE_URL}?p=pdf_search/checkCaptcha"
            data = {
                "captcha": captcha_solution,
                "search_text": "",
                "search_opt": "PHRASE",
                "escr_flag": "",
                "proximity": "",
                "sel_lang": "",
                "ajax_req": "true",
                "app_token": self.app_token,
            }

            self.logger.info("Verifying CAPTCHA")
            response = self.session.post(url, headers=self.headers, data=data)
            response.raise_for_status()

            result = response.json()
            self.update_app_token(result)

            if result.get("captcha_status") == "Y":
                self.logger.info("CAPTCHA verified successfully")
                return True

            self.logger.warning("CAPTCHA verification failed")
            return False

        except Exception as e:
            self.logger.error(f"Error verifying CAPTCHA: {str(e)}", exc_info=True)
            return False

    def search_cases(
        self,
        search_text="",
        captcha_solution="",
        state_code="",
        dist_code="",
        court_type="2",
        display_length=1000,
        start_from=0,
        search_opt="PHRASE",
    ):
        """Search for court cases"""
        if not self.verified:
            self.verify()

        if not self.verified:
            self.logger.error("Cannot proceed with search: CAPTCHA verification failed")
            return None

        data = {
            "sEcho": "1",
            "iColumns": "2",
            "sColumns": ",",
            "iDisplayStart": str(start_from),
            "iDisplayLength": str(display_length),
            "mDataProp_0": "0",
            "sSearch_0": "",
            "bRegex_0": "false",
            "bSearchable_0": "true",
            "bSortable_0": "true",
            "mDataProp_1": "1",
            "sSearch_1": "",
            "bRegex_1": "false",
            "bSearchable_1": "true",
            "bSortable_1": "true",
            "sSearch": "",
            "bRegex": "false",
            "iSortCol_0": "0",
            "sSortDir_0": "asc",
            "iSortingCols": "1",
            "search_txt1": search_text,
            "search_txt2": "",
            "search_txt3": "",
            "search_txt4": "",
            "search_txt5": "",
            "pet_res": "",
            "state_code": state_code,
            "state_code_li": "",
            "dist_code": dist_code,
            "case_no": "",
            "case_year": "",
            "from_date": "",
            "to_date": "",
            "judge_name": "",
            "reg_year": "",
            "fulltext_case_type": "",
            "sel_search_by": "",
            "sections": "",
            "judge_txt": "",
            "act_txt": "",
            "section_txt": "",
            "judge_val": "",
            "act_val": "",
            "year_val": "",
            "judge_arr": "",
            "flag": "",
            "captcha": captcha_solution,
            "disp_nature": "",
            "search_opt": search_opt,
            "date_val": "ALL",
            "fcourt_type": court_type,
            "citation_yr": "",
            "citation_vol": "",
            "citation_supl": "",
            "citation_page": "",
            "case_no1": "",
            "case_year1": "",
            "pet_res1": "",
            "fulltext_case_type1": "",
            "citation_keyword": "",
            "proximity": "",
            "sel_lang": "",
            "neu_cit_year": "",
            "neu_no": "",
            "ajax_req": "true",
            "app_token": self.app_token,
        }

        try:
            search_url = f"{self.BASE_URL}?p=pdf_search/home"
            self.logger.info(f"Executing search with text: {search_text}")
            response = self.session.post(search_url, headers=self.headers, data=data)
            response.raise_for_status()

            result = response.json()
            self.update_app_token(result)

            if "reportrow" in result:
                self.logger.info(f"Search returned {len(result['reportrow'])} results")
                return result

            self.logger.warning("No search results found")
            return None

        except Exception as e:
            self.logger.error(f"Error during search: {str(e)}", exc_info=True)
            return None

    def download_judgment(self, path, val="0", citation_year="", output_dir="judgments"):
        """Download a judgment PDF file"""
        try:
            os.makedirs(output_dir, exist_ok=True)
            filename = os.path.basename(path.split("#")[0])
            if not filename.lower().endswith(".pdf"):
                filename += ".pdf"
            output_path = os.path.join(output_dir, filename)

            pdf_info_url = "https://judgments.ecourts.gov.in/pdfsearch/?p=pdf_search/openpdfcaptcha"
            path_with_params = (
                f"{path}#page=&search=+&citation_year=&fcourt_type=2&file_type=undefined&nc_display=undefined"
            )

            data = {
                "val": val,
                "lang_flg": "undefined",
                "path": path_with_params,
                "ajax_req": "true",
                "app_token": self.app_token,
            }

            pdf_headers = {
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Accept-Encoding": "gzip, deflate, br, zstd",
                "Accept-Language": "en-US,en;q=0.9",
                "Connection": "keep-alive",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "Origin": "https://judgments.ecourts.gov.in",
                "Referer": "https://judgments.ecourts.gov.in/",
                "sec-ch-ua": '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Linux"',
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
                "X-Requested-With": "XMLHttpRequest",
            }

            if "__session:0.8998627207867523:" not in self.session.cookies:
                self.session.cookies.set("__session:0.8998627207867523:", "https:")

            self.logger.info(f"Requesting PDF: {path}")
            pdf_info_response = self.session.post(pdf_info_url, headers=pdf_headers, data=data)
            pdf_info_response.raise_for_status()

            try:
                pdf_info_result = pdf_info_response.json()
                self.update_app_token(pdf_info_result)

                if "outputfile" in pdf_info_result:
                    pdf_url = urljoin("https://judgments.ecourts.gov.in/", pdf_info_result["outputfile"])
                    self.logger.info(f"Downloading PDF from: {pdf_url}")

                    pdf_download_headers = {
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                        "Accept-Encoding": "gzip, deflate, br, zstd",
                        "Accept-Language": "en-US,en;q=0.9",
                        "Connection": "keep-alive",
                        "Referer": "https://judgments.ecourts.gov.in/",
                        "sec-ch-ua": '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
                        "sec-ch-ua-mobile": "?0",
                        "sec-ch-ua-platform": '"Linux"',
                        "Sec-Fetch-Dest": "document",
                        "Sec-Fetch-Mode": "navigate",
                        "Sec-Fetch-Site": "same-origin",
                        "Upgrade-Insecure-Requests": "1",
                        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
                    }

                    pdf_response = self.session.get(pdf_url, stream=True, headers=pdf_download_headers)
                    pdf_response.raise_for_status()

                    with open(output_path, "wb") as f:
                        for chunk in pdf_response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)

                    self.logger.info(f"Successfully downloaded PDF to {output_path}")
                    return output_path

                self.logger.error(
                    f"PDF download failed: {pdf_info_result.get('errormsg', 'No error message provided')}"
                )
                return None

            except json.JSONDecodeError:
                self.logger.error(f"Invalid JSON response: {pdf_info_response.text[:200]}")
                return None

        except Exception as e:
            self.logger.error(f"Error downloading judgment: {str(e)}", exc_info=True)
            return None

    def get_district_data(self, state_code):
        """Get district data for a state"""
        try:
            url = f"{self.BASE_URL}?p=pdf_search/get_distData"
            data = {"state_code": state_code, "ajax_req": "true", "app_token": self.app_token}

            self.logger.info(f"Fetching district data for state: {state_code}")
            response = self.session.post(url, headers=self.headers, data=data)
            response.raise_for_status()

            result = response.json()
            self.update_app_token(result)
            self.logger.info(f"Successfully retrieved district data")
            return result

        except Exception as e:
            self.logger.error(f"Error getting district data: {str(e)}", exc_info=True)
            return None

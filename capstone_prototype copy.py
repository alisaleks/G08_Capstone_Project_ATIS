from selenium.common.exceptions import StaleElementReferenceException
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException  # Add this import
from webdriver_manager.chrome import ChromeDriverManager
import time

def initialize_browser():
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run in headless mode
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)
    print("Browser initialized successfully.")
    return driver

def handle_cookie_banner(browser):
    try:
        print("Attempting to handle cookies banner...")
        accept_button = WebDriverWait(browser, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button[data-full-consent='true']"))
        )
        accept_button.click()
        print("Cookies banner accepted.")
    except Exception:
        print("No cookies banner found or already handled.")

def extract_table_data(browser):
    """
    Extracts data from the table's <tbody> and returns it as a list of dictionaries.
    Retries if elements become stale.
    """
    rows = WebDriverWait(browser, 10).until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR, "tbody > tr"))
    )
    results = []
    for row in rows:
        try:
            # Re-locate cells to avoid stale references
            cells = row.find_elements(By.TAG_NAME, "td")
            entry = {
                "Publication Date": cells[0].text.strip(),
                "Deadline": cells[1].text.strip(),
                "Description": cells[2].text.strip(),
                "Type": cells[3].text.strip(),
                "Publisher": cells[4].text.strip(),
            }
            results.append(entry)
        except (IndexError, StaleElementReferenceException):
            print("Stale or mismatched row detected. Skipping row.")
    return results

def paginate_and_scrape(browser):
    """
    Handles pagination and extracts data from all pages dynamically.
    """
    all_results = []
    page = 1

    while True:
        print(f"Scraping page {page}...")
        
        # Extract data from the current page
        try:
            all_results.extend(extract_table_data(browser))
        except StaleElementReferenceException as e:
            print(f"Stale element encountered on page {page}: {e}. Retrying...")
            continue

        # Check if the "Next" button is present and clickable
        try:
            next_button = WebDriverWait(browser, 10).until(
                EC.element_to_be_clickable((By.ID, "nextPage"))
            )
            next_button.click()
            print(f"Moving to page {page + 1}...")

            # Wait for the table content to update (check staleness or new rows)
            WebDriverWait(browser, 10).until(
                EC.staleness_of(browser.find_element(By.CSS_SELECTOR, "tbody"))
            )
            WebDriverWait(browser, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "tbody > tr"))
            )

            # Proceed to the next page
            page += 1
        except TimeoutException:
            print("No more pages to scrape.")
            break

    return all_results

def perform_search(browser, keyword):
    try:
        print(f"Searching for keyword: {keyword}")
        search_input = WebDriverWait(browser, 60).until(
            EC.presence_of_element_located((By.ID, "searchText"))
        )
        search_input.clear()
        search_input.send_keys(keyword)
        print(f"Keyword '{keyword}' entered.")

        search_button = WebDriverWait(browser, 60).until(
            EC.element_to_be_clickable((By.ID, "searchStart"))
        )
        search_button.click()
        print("Search button clicked.")

        # Wait for results to load
        WebDriverWait(browser, 60).until(
            EC.presence_of_element_located((By.CLASS_NAME, "browsePages"))
        )
        print(f"Results loaded for keyword: {keyword}")
    except Exception as e:
        print(f"Error during search for keyword '{keyword}': {e}")

def scrape_dtvp(browser, url, keywords):
    print(f"Accessing {url}...")
    browser.get(url)
    handle_cookie_banner(browser)
    for keyword in keywords:
        perform_search(browser, keyword)
        results = paginate_and_scrape(browser)
        print(f"Results for '{keyword}': {len(results)} entries.")
        for result in results:
            print(result)

if __name__ == "__main__":
    dtvp_url = "https://www.dtvp.de/Center/company/announcements/categoryOverview.do?method=showCategoryOverview"
    keywords = ["Erlebnis", "Freizeit", "Destination", "Tourismus"]
    browser = initialize_browser()
    try:
        scrape_dtvp(browser, dtvp_url, keywords)
    finally:
        browser.quit()

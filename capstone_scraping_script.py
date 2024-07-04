import concurrent.futures
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementNotInteractableException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys 
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from dateutil import parser
from urllib.parse import urljoin
import time
import re
import pandas as pd
import datetime
import os

# Mapping cities to states
city_to_state = {
    "Berlin": "Berlin",
    "Hamburg": "Hamburg",
    "Munich": "Bavaria",
    "Cologne": "North Rhine-Westphalia",
    "Frankfurt": "Hesse",
    "Stuttgart": "Baden-Württemberg",
    "Düsseldorf": "North Rhine-Westphalia",
    "Dortmund": "North Rhine-Westphalia",
    "Essen": "North Rhine-Westphalia",
    "Leipzig": "Saxony",
    "Bremen": "Bremen",
    "Dresden": "Saxony",
    "Hanover": "Lower Saxony",
    "Nuremberg": "Bavaria",
    "Duisburg": "North Rhine-Westphalia",
    "Bochum": "North Rhine-Westphalia",
    "Wuppertal": "North Rhine-Westphalia",
    "Bielefeld": "North Rhine-Westphalia",
    "Bonn": "North Rhine-Westphalia",
    "Münster": "North Rhine-Westphalia",
    "Karlsruhe": "Baden-Württemberg",
    "Mannheim": "Baden-Württemberg",
    "Augsburg": "Bavaria",
    "Wiesbaden": "Hesse",
    "Gelsenkirchen": "North Rhine-Westphalia",
    "Mönchengladbach": "North Rhine-Westphalia",
    "Braunschweig": "Lower Saxony",
    "Chemnitz": "Saxony",
    "Kiel": "Schleswig-Holstein",
    "Aachen": "North Rhine-Westphalia",
    "Halle": "Saxony-Anhalt",
    "Magdeburg": "Saxony-Anhalt",
    "Freiburg": "Baden-Württemberg",
    "Krefeld": "North Rhine-Westphalia",
    "Lübeck": "Schleswig-Holstein",
    "Oberhausen": "North Rhine-Westphalia",
    "Erfurt": "Thuringia",
    "Mainz": "Rhineland-Palatinate",
    "Rostock": "Mecklenburg-Vorpommern",
    "Kassel": "Hesse",
    "Hagen": "North Rhine-Westphalia",
    "Hamm": "North Rhine-Westphalia",
    "Saarbrücken": "Saarland",
    "Mülheim": "North Rhine-Westphalia",
    "Potsdam": "Brandenburg",
    "Ludwigshafen": "Rhineland-Palatinate",
    "Oldenburg": "Lower Saxony",
    "Leverkusen": "North Rhine-Westphalia",
    "Osnabrück": "Lower Saxony",
    "Solingen": "North Rhine-Westphalia",
    "Herne": "North Rhine-Westphalia",
    "Neuss": "North Rhine-Westphalia",
    "Heidelberg": "Baden-Württemberg",
    "Darmstadt": "Hesse",
    "Paderborn": "North Rhine-Westphalia",
    "Regensburg": "Bavaria",
    "Ingolstadt": "Bavaria",
    "Würzburg": "Bavaria",
    "Fürth": "Bavaria",
    "Wolfsburg": "Lower Saxony",
    "Offenbach": "Hesse",
    "Ulm": "Baden-Württemberg",
    "Heilbronn": "Baden-Württemberg",
    "Pforzheim": "Baden-Württemberg",
    "Göttingen": "Lower Saxony",
    "Bottrop": "North Rhine-Westphalia",
    "Trier": "Rhineland-Palatinate",
    "Recklinghausen": "North Rhine-Westphalia",
    "Reutlingen": "Baden-Württemberg",
    "Bremerhaven": "Bremen",
    "Koblenz": "Rhineland-Palatinate",
    "Bergisch Gladbach": "North Rhine-Westphalia",
    "Jena": "Thuringia",
    "Remscheid": "North Rhine-Westphalia",
    "Erlangen": "Bavaria",
    "Moers": "North Rhine-Westphalia",
    "Siegen": "North Rhine-Westphalia",
    "Hildesheim": "Lower Saxony",
    "Salzgitter": "Lower Saxony",
    # Add other city-to-state mappings here as needed
}

def get_state_from_location(location):
    for city, state in city_to_state.items():
        if city in location:
            return state
    return "not specified"

# Initialize the WebDriver
def initialize_browser(browser_type="chrome"):
    print("Initializing browser...")
    if (browser_type.lower() == "chrome"):
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # run headless if desired
        browser = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)
    else:
        raise ValueError("Unsupported browser type: use 'chrome'")
    return browser

def format_date(date_string):
    try:
        date_obj = parser.parse(date_string, dayfirst=True)
        formatted_date = date_obj.strftime("%d.%m.%y")
        return formatted_date
    except (parser.ParserError, ValueError):
        return "not specified"

def parse_application_period(period_string):
    try:
        start_date_str, end_date_str = period_string.split(" until ")
        start_date = format_date(start_date_str)
        end_date = format_date(end_date_str)
        return start_date, end_date
    except ValueError:
        return "not specified", "not specified"

def extract_tender_code(tender_name, source_url):
    if "myorder.rib.de" in source_url:
        match = re.search(r'\(([^)]+)\)', tender_name)
        tender_code = tender_name.split()[0] if match is None else match.group(1)
    else:
        tender_code = tender_name.split()[0]
    return tender_code

def extract_tender_deadline(block):
    deadline_labels = ["Application deadline", "Expiration time"]
    for label in deadline_labels:
        info_tag = block.find('div', class_='info-label', string=lambda text: text and label in text)
        if info_tag:
            value = info_tag.find_next('div').text.strip()
            return format_date(value)
    return "not specified"

def scrape_bayern_selenium(browser, url, keywords, source_url):
    print(f"Scraping dynamic content from {url}...")
    tenders = []
    browser.get(url)
    
    # Scroll to the bottom of the page to ensure all dynamic content is loaded
    last_height = browser.execute_script("return document.body.scrollHeight")
    while True:
        # Scroll down to the bottom of the page
        browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        
        # Wait for new content to load
        time.sleep(2)
        
        # Calculate new scroll height and compare with last scroll height
        new_height = browser.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

    # Now that the page is fully loaded, proceed with scraping
    html = browser.page_source
    soup = BeautifulSoup(html, 'html.parser')
    
    tender_blocks = soup.find_all('div', class_='item')
    print(f"Found {len(tender_blocks)} tender blocks on the current page.")

    for block in tender_blocks:
        title_div = block.find('div', style=lambda value: value and 'overflow: hidden' in value)
        title_tag = title_div.find('strong') if title_div else None

        if title_tag:
            title = title_tag.get_text(strip=True)
            found_keywords = [keyword for keyword in keywords if keyword.lower() in title.lower()]
            if not found_keywords:
                continue  # Skip this tender if no keywords are found in the title

            description_tag = block.find('div', class_='text-muted')
            description = description_tag.get_text(strip=True) if description_tag else "No Description"
            
            tender_authority = description.split(' by ')[-1]
            tender_code = extract_tender_code(title, source_url)
            
            tender_details = {
                'tender_name': title,
                'tender_authority': tender_authority,
                'tender_code': tender_code,
                'source_url': source_url,
                'found_keywords': ', '.join(found_keywords)
            }

            info_dict = {
                'Application period': 'application_period',
                'Period': 'period',
                'Execution place': 'tender_location'
            }

            for info_label, info_key in info_dict.items():
                info_tag = block.find('div', class_='info-label', string=lambda text: text and info_label in text)
                if info_tag:
                    value = info_tag.find_next('div').text.strip() if info_tag.find_next('div') else "not specified"
                    if info_key == 'application_period':
                        start_date, end_date = parse_application_period(value)
                        tender_details['application_start_date'] = start_date
                        tender_details['application_deadline'] = end_date
                    else:
                        tender_details[info_key] = value

            tender_details['tender_deadline'] = extract_tender_deadline(block)
            
            # Extract publication date
            date_div = block.find('div', class_='item-right meta')
            if date_div:
                day = date_div.find('div', class_='date').text.strip()
                month_year = date_div.find('div', class_='month').text.strip()
                publication_date_str = f"{day} {month_year}"
                publication_date = format_date(publication_date_str)
                tender_details['date_published'] = publication_date
            else:
                tender_details['date_published'] = "not specified"
            
            # Add the state column
            tender_details['state'] = get_state_from_location(tender_details.get('tender_location', ''))

            tenders.append(tender_details)

    # Handling pagination
    while True:
        next_page = soup.find('a', {'aria-label': 'Next'})
        if next_page and 'disabled' not in next_page.get('class', []):
            next_url = urljoin(url, next_page['href'])
            print(f"Navigating to the next page: {next_url}")
            browser.get(next_url)
            time.sleep(2)
            
            html = browser.page_source
            soup = BeautifulSoup(html, 'html.parser')
            tender_blocks = soup.find_all('div', class_='item')
            print(f"Found {len(tender_blocks)} tender blocks on the next page.")

            for block in tender_blocks:
                title_div = block.find('div', style=lambda value: value and 'overflow: hidden' in value)
                title_tag = title_div.find('strong') if title_div else None

                if title_tag:
                    title = title_tag.get_text(strip=True)
                    found_keywords = [keyword for keyword in keywords if keyword.lower() in title.lower()]
                    if not found_keywords:
                        continue  # Skip this tender if no keywords are found in the title

                    description_tag = block.find('div', class_='text-muted')
                    description = description_tag.get_text(strip=True) if description_tag else "No Description"
                    
                    tender_authority = description.split(' by ')[-1]
                    tender_code = extract_tender_code(title, source_url)
                    
                    tender_details = {
                        'tender_name': title,
                        'tender_authority': tender_authority,
                        'tender_code': tender_code,
                        'source_url': source_url,
                        'found_keywords': ', '.join(found_keywords)
                    }

                    info_dict = {
                        'Application period': 'application_period',
                        'Period': 'period',
                        'Execution place': 'tender_location'
                    }

                    for info_label, info_key in info_dict.items():
                        info_tag = block.find('div', class_='info-label', string=lambda text: text and info_label in text)
                        if info_tag:
                            value = info_tag.find_next('div').text.strip() if info_tag.find_next('div') else "not specified"
                            if info_key == 'application_period':
                                start_date, end_date = parse_application_period(value)
                                tender_details['application_start_date'] = start_date
                                tender_details['application_deadline'] = end_date
                            else:
                                tender_details[info_key] = value

                    tender_details['tender_deadline'] = extract_tender_deadline(block)
                    
                    # Extract publication date
                    date_div = block.find('div', class_='item-right meta')
                    if date_div:
                        day = date_div.find('div', class_='date').text.strip()
                        month_year = date_div.find('div', class_='month').text.strip()
                        publication_date_str = f"{day} {month_year}"
                        publication_date = format_date(publication_date_str)
                        tender_details['date_published'] = publication_date
                    else:
                        tender_details['date_published'] = "not specified"
                    
                    # Add the state column
                    tender_details['state'] = get_state_from_location(tender_details.get('tender_location', ''))

                    tenders.append(tender_details)
        else:
            break
    
    print(f"Total tenders found: {len(tenders)}")
    return tenders

def scrape_muenchen(html, keywords, source_url):
    print(f"Scraping static content from {source_url}...")
    tenders = []
    soup = BeautifulSoup(html, 'html.parser')

    tender_rows = soup.find_all('tr', class_='tableRow clickable-row publicationDetail')
    print(f"Found {len(tender_rows)} tender rows.")

    for row in tender_rows:
        date_published = row.find('td').text.strip()
        tender_name = row.find('td', class_='tender').text.strip()
        tender_authority = row.find('td', class_='tenderAuthority').text.strip()
        tender_type = row.find('td', class_='tenderType').text.strip()
        tender_deadline = row.find('td', class_='tenderDeadline').text.strip()

        # Remove the hour from the tender deadline
        tender_deadline_date = tender_deadline.split(' ')[0]  # This assumes the format is "date time"
        formatted_deadline = format_date(tender_deadline_date)  # Format the date if needed

        # Reformat date published to dd.mm.yy
        try:
            date_obj = parser.parse(date_published, dayfirst=True)
            formatted_date_published = date_obj.strftime("%d.%m.%y")
        except (parser.ParserError, ValueError):
            formatted_date_published = "not specified"

        found_keywords = [keyword for keyword in keywords if keyword.lower() in tender_name.lower()]
        if found_keywords:
            tenders.append({
                'date_published': formatted_date_published,  # Use the formatted date
                'tender_name': tender_name,
                'tender_authority': tender_authority,
                'tender_type': tender_type,
                'tender_deadline': formatted_deadline,  # Use the formatted deadline
                'source_url': source_url,
                'found_keywords': ', '.join(found_keywords)
            })
        
    return tenders


def scrape_vmstart(browser, url, keywords, source_url):
    print(f"Scraping dynamic content from {url}...")
    all_tenders = {}
    browser.get(url)

    for keyword in keywords:
        try:
            print(f"Searching for keyword: {keyword}")
            
            # Check if we are on the initial page or subsequent searches
            if "searchVisible" in browser.page_source:
                search_input = WebDriverWait(browser, 30).until(
                    EC.presence_of_element_located((By.ID, "searchVisible"))
                )
            else:
                search_input = WebDriverWait(browser, 30).until(
                    EC.presence_of_element_located((By.ID, "inputSearchKey"))
                )

            search_input.clear()
            search_input.send_keys(keyword)
            
            if "searchVisible" in browser.page_source:
                search_submit = WebDriverWait(browser, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "button.btn-mainSearch"))
                )
            else:
                search_submit = WebDriverWait(browser, 5).until(
                    EC.element_to_be_clickable((By.ID, "btnSearchSubmit"))
                )
            browser.execute_script("arguments[0].click();", search_submit)

            # Wait for results to load
            try:
                WebDriverWait(browser, 30).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.border.col-lg-12"))
                )
                time.sleep(5)  # Adjust the sleep duration as needed
            except TimeoutException:
                print(f"Timeout while waiting for search results for keyword: {keyword}")
                continue

            # Parse results
            html = browser.page_source
            soup = BeautifulSoup(html, 'html.parser')

            # Check if no results were found
            no_results_text = soup.find('h3', class_='color-main')
            if no_results_text and "0 gefundene Ausschreibung" in no_results_text.text:
                print(f"No tenders found for keyword: {keyword}")
                continue

            # Check if results were found
            results_text = soup.find('h3', class_='color-main')
            if results_text and "gefundene Ausschreibung" in results_text.text:
                num_results = int(results_text.text.split()[0])
                print(f"{num_results} tenders found for keyword: {keyword}")

            # Find all tender rows
            tender_blocks = soup.select("tbody.tableLeftHeaderBlock[tabindex='0']")

            for tbody in tender_blocks:
                rows = tbody.find_all('tr', class_='tableRowLeft')
                tender_details = {
                    'tender_name': 'not specified',
                    'tender_authority': 'not specified',
                    'tender_type': 'not specified',
                    'tender_law': 'not specified',
                    'tender_deadline': 'not specified'
                }
                for row in rows:
                    try:
                        cells = row.find_all('td')
                        if len(cells) < 2:
                            continue  # Skip rows with insufficient data

                        header_text = cells[0].get_text(strip=True)
                        data_text = cells[1].get_text(strip=True) if cells[1].get_text(strip=True) else "not specified"

                        if 'Ausschreibung' in header_text:
                            tender_details['tender_name'] = data_text
                        elif 'Vergabestelle' in header_text:
                            tender_details['tender_authority'] = data_text
                        elif 'Verfahrensart' in header_text:
                            tender_details['tender_type'] = data_text
                        elif 'Rechtsrahmen' in header_text:
                            tender_details['tender_law'] = data_text
                        elif 'Abgabefrist' in header_text:
                            tender_details['tender_deadline'] = format_date(data_text.split()[0]) if data_text != "not specified" else "not specified"

                    except IndexError:
                        print(f"IndexError: Skipping a row due to insufficient columns: {[cell.get_text(strip=True) for cell in cells]}")
                    except Exception as e:
                        print(f"An error occurred while parsing row: {e}")

                if tender_details['tender_name'] != 'not specified':
                    tender_name = tender_details['tender_name']
                    tender_details['source_url'] = source_url
                    tender_details['found_keywords'] = keyword

                    if tender_name in all_tenders:
                        if keyword not in all_tenders[tender_name]['found_keywords']:
                            all_tenders[tender_name]['found_keywords'] += f", {keyword}"
                    else:
                        all_tenders[tender_name] = tender_details

        except TimeoutException:
            print(f"Timeout while searching for keyword: {keyword}")
        except NoSuchElementException:
            print(f"No element found for keyword: {keyword}")
        except ElementNotInteractableException:
            print(f"Element not interactable for keyword: {keyword}")
        except Exception as e:
            print(f"An error occurred while searching for keyword: {keyword} - {str(e)}")

    tenders = list(all_tenders.values())
    total_tenders = len(tenders)
    print(f"Total tenders found: {total_tenders}")
    return tenders


def Rheinland(browser, url, keywords, source_url):
    print(f"Scraping dynamic content from {url}...")
    all_tenders = {}
    browser.get(url)

    for keyword in keywords:
        try:
            print(f"Searching for keyword: {keyword}")
            # Perform search
            search_input = WebDriverWait(browser, 20).until(
                EC.presence_of_element_located((By.ID, "searchText"))
            )
            search_input.clear()
            search_input.send_keys(keyword)
            
            # Click the search button
            search_button = browser.find_element(By.ID, "searchStart")
            search_button.click()

            while True:
                # Wait for results to load
                try:
                    WebDriverWait(browser, 20).until(
                        EC.presence_of_element_located((By.XPATH, "//div[@id='listTemplate']"))
                    )
                    print("Search processed, results loaded")
                    # Adding delay to ensure the page is fully loaded
                    time.sleep(5)  # Adjust the sleep duration as needed
                except TimeoutException:
                    print("Timeout while waiting for search results")
                    break

                # Parse results
                html = browser.page_source
                soup = BeautifulSoup(html, 'html.parser')

                # Find all tender rows
                tender_rows = soup.select("div#listTemplate tbody tr")

                if len(tender_rows) == 0:
                    print(f"No tenders found for keyword: {keyword}")
                    break

                for row in tender_rows:
                    try:
                        # Extract and strip text from each relevant cell
                        cells = row.find_all('td')
                        cell_texts = [cell.text.strip() for cell in cells]
                        if len(cells) < 5:
                            print(f"No tender found for keyword: {keyword}")
                            continue  # Skip rows without sufficient data

                        date_published = cells[0].find('abbr').text.strip() if cells[0].find('abbr') else "not specified"
                        tender_deadline = cells[1].find('abbr').text.strip() if cells[1].find('abbr') else "not specified"
                        tender_name = cells[2].text.strip() if cells[2] else "not specified"
                        tender_type = cells[3].text.strip() if cells[3] else "not specified"
                        tender_authority = cells[4].text.strip() if cells[4] else "not specified"

                        # Format dates
                        formatted_deadline = format_date(tender_deadline) if tender_deadline != "nv" else "not specified"
                        formatted_date_published = format_date(date_published)

                        # Create or update tender details
                        if tender_name in all_tenders:
                            if keyword not in all_tenders[tender_name]['found_keywords']:
                                all_tenders[tender_name]['found_keywords'] += f", {keyword}"
                        else:
                            tender_details = {
                                'date_published': formatted_date_published,
                                'tender_name': tender_name,
                                'tender_authority': tender_authority,
                                'tender_type': tender_type,
                                'tender_deadline': formatted_deadline,
                                'source_url': source_url,
                                'found_keywords': keyword  # Initialize with the searched keyword
                            }
                            all_tenders[tender_name] = tender_details
                    except Exception as e:
                        print(f"An error occurred while parsing row: {e}")

                # Check for next page
                next_page = soup.find('a', {'title': 'Nächste Seite'})
                if next_page and 'disabled' not in next_page.get('class', []):
                    next_url = urljoin(url, next_page['href'])
                    print(f"Navigating to the next page: {next_url}")
                    browser.get(next_url)
                else:
                    break
        except TimeoutException:
            print(f"Timeout while searching for keyword: {keyword}")
        except NoSuchElementException:
            print(f"No element found for keyword: {keyword}")
        except ElementNotInteractableException:
            print(f"Element not interactable for keyword: {keyword}")
        except Exception as e:
            print(f"An error occurred while searching for keyword: {keyword} - {str(e)}")

    tenders = list(all_tenders.values())
    total_tenders = len(tenders)
    print(f"Total tenders found for Rheinland: {total_tenders}")
    return all_tenders

def scrape_nrw(browser, url, keywords, source_url):
    print(f"Scraping dynamic content from {url}...")
    all_tenders = {}
    browser.get(url)

    for keyword in keywords:
        try:
            print(f"Searching for keyword: {keyword}")
            # Perform search
            search_input = WebDriverWait(browser, 20).until(
                EC.presence_of_element_located((By.ID, "searchText"))
            )
            search_input.clear()
            search_input.send_keys(keyword)
            
            # Click the search button
            search_button = browser.find_element(By.ID, "searchStart")
            search_button.click()

            while True:
                # Wait for results to load
                try:
                    WebDriverWait(browser, 20).until(
                        EC.presence_of_element_located((By.XPATH, "//div[@id='listTemplate']"))
                    )
                    # Adding delay to ensure the page is fully loaded
                    time.sleep(5)  # Adjust the sleep duration as needed
                except TimeoutException:
                    print("Timeout while waiting for search results")
                    break

                # Parse results
                html = browser.page_source
                soup = BeautifulSoup(html, 'html.parser')

                # Find all tender rows
                tender_rows = soup.select("div#listTemplate tbody tr")
                print(f"Found {len(tender_rows)} tender rows for keyword: {keyword}")

                for row in tender_rows:
                    try:
                        # Extract and strip text from each relevant cell
                        cells = row.find_all('td')
                        cell_texts = [cell.text.strip() for cell in cells]

                        if len(cells) < 5 or "Es wurden keine passenden Bekanntmachungen gefunden." in cell_texts[0]:
                            continue  # Skip rows without sufficient data or with no matching tenders

                        date_published = cells[0].find('abbr').text.strip() if cells[0].find('abbr') else "not specified"
                        tender_deadline = cells[1].find('abbr').text.strip() if cells[1].find('abbr') else "not specified"
                        tender_name = cells[2].text.strip() if cells[2] else "not specified"
                        tender_type = cells[3].text.strip() if cells[3] else "not specified"
                        tender_authority = cells[4].text.strip() if cells[4] else "not specified"

                        # Format dates
                        formatted_deadline = format_date(tender_deadline) if tender_deadline != "nv" else "not specified"
                        formatted_date_published = format_date(date_published)

                        # Create or update tender details
                        if tender_name in all_tenders:
                            if keyword not in all_tenders[tender_name]['found_keywords']:
                                all_tenders[tender_name]['found_keywords'] += f", {keyword}"
                        else:
                            tender_details = {
                                'date_published': formatted_date_published,
                                'tender_name': tender_name,
                                'tender_authority': tender_authority,
                                'tender_type': tender_type,
                                'tender_deadline': formatted_deadline,
                                'source_url': source_url,
                                'found_keywords': keyword  # Initialize with the searched keyword
                            }
                            all_tenders[tender_name] = tender_details

                    except Exception as e:
                        print(f"An error occurred while parsing row: {e}")

                # Check for next page
                next_page = soup.find('a', {'title': 'Nächste Seite'})
                if next_page and 'disabled' not in next_page.get('class', []):
                    next_url = urljoin(url, next_page['href'])
                    print(f"Navigating to the next page: {next_url}")
                    browser.get(next_url)
                else:
                    break
        except TimeoutException:
            print(f"Timeout while searching for keyword: {keyword}")
        except NoSuchElementException:
            print(f"No element found for keyword: {keyword}")
        except ElementNotInteractableException:
            print(f"Element not interactable for keyword: {keyword}")
        except Exception as e:
            print(f"An error occurred while searching for keyword: {keyword} - {str(e)}")

    tenders = list(all_tenders.values())
    total_tenders = len(tenders)
    print(f"Total tenders found for NRW: {total_tenders}")
    return all_tenders

def scrape_brandenburg(browser, url, keywords, source_url):
    print(f"Scraping dynamic content from {url}...")
    all_tenders = {}
    browser.get(url)

    for keyword in keywords:
        try:
            print(f"Searching for keyword: {keyword}")
            # Perform search
            search_input = WebDriverWait(browser, 20).until(
                EC.presence_of_element_located((By.ID, "searchText"))
            )
            search_input.clear()
            search_input.send_keys(keyword)
            
            # Click the search button
            search_button = browser.find_element(By.ID, "searchStart")
            search_button.click()

            while True:
                # Wait for results to load
                try:
                    WebDriverWait(browser, 20).until(
                        EC.presence_of_element_located((By.XPATH, "//div[@id='listTemplate']"))
                    )
                    print("Search processed, results loaded")
                    # Adding delay to ensure the page is fully loaded
                    time.sleep(5)  # Adjust the sleep duration as needed
                except TimeoutException:
                    print("Timeout while waiting for search results")
                    break

                # Parse results
                html = browser.page_source
                soup = BeautifulSoup(html, 'html.parser')

                # Find all tender rows
                tender_rows = soup.select("div#listTemplate tbody tr")
                if len(tender_rows) == 0:
                    print(f"No tenders found for keyword: {keyword}")
                    break

                for row in tender_rows:
                    try:
                        # Extract and strip text from each relevant cell
                        cells = row.find_all('td')
                        cell_texts = [cell.text.strip() for cell in cells]

                        if len(cells) < 5 or "Es wurden keine passenden Bekanntmachungen gefunden." in cell_texts[0]:
                            print(f"No tender found for keyword: {keyword}")
                            continue  # Skip rows without sufficient data or with no matching tenders

                        date_published = cells[0].find('abbr').text.strip() if cells[0].find('abbr') else "not specified"
                        tender_deadline = cells[1].find('abbr').text.strip() if cells[1].find('abbr') else "not specified"
                        tender_name = cells[2].text.strip() if cells[2] else "not specified"
                        tender_type = cells[3].text.strip() if cells[3] else "not specified"
                        tender_authority = cells[4].text.strip() if cells[4] else "not specified"

                        # Format dates
                        formatted_deadline = format_date(tender_deadline) if tender_deadline != "nv" else "not specified"
                        formatted_date_published = format_date(date_published)

                        # Create or update tender details
                        if tender_name in all_tenders:
                            if keyword not in all_tenders[tender_name]['found_keywords']:
                                all_tenders[tender_name]['found_keywords'] += f", {keyword}"
                        else:
                            tender_details = {
                                'date_published': formatted_date_published,
                                'tender_name': tender_name,
                                'tender_authority': tender_authority,
                                'tender_type': tender_type,
                                'tender_deadline': formatted_deadline,
                                'source_url': source_url,
                                'found_keywords': keyword  # Initialize with the searched keyword
                            }
                            all_tenders[tender_name] = tender_details

                    except Exception as e:
                        print(f"An error occurred while parsing row: {e}")

                # Check for next page
                next_page = soup.find('a', {'title': 'Nächste Seite'})
                if next_page and 'disabled' not in next_page.get('class', []):
                    next_url = urljoin(url, next_page['href'])
                    print(f"Navigating to the next page: {next_url}")
                    browser.get(next_url)
                else:
                    break
        except TimeoutException:
            print(f"Timeout while searching for keyword: {keyword}")
        except NoSuchElementException:
            print(f"No element found for keyword: {keyword}")
        except ElementNotInteractableException:
            print(f"Element not interactable for keyword: {keyword}")
        except Exception as e:
            print(f"An error occurred while searching for keyword: {keyword} - {str(e)}")

    tenders = list(all_tenders.values())
    total_tenders = len(tenders)
    print(f"Total tenders found for Brandenburg: {total_tenders}")
    return all_tenders

def scrape_niedersachsen(browser, url, keywords, source_url):
    print(f"Scraping dynamic content from {url}...")
    all_tenders = {}
    browser.get(url)

    for keyword in keywords:
        try:
            print(f"Searching for keyword: {keyword}")
            # Perform search
            search_input = WebDriverWait(browser, 20).until(
                EC.presence_of_element_located((By.ID, "searchText"))
            )
            search_input.clear()
            search_input.send_keys(keyword)
            
            # Click the search button
            search_button = browser.find_element(By.ID, "searchStart")
            search_button.click()

            while True:
                # Wait for results to load
                try:
                    WebDriverWait(browser, 20).until(
                        EC.presence_of_element_located((By.XPATH, "//div[@id='listTemplate']"))
                    )
                    print("Search processed, results loaded")
                    # Adding delay to ensure the page is fully loaded
                    time.sleep(5)  # Adjust the sleep duration as needed
                except TimeoutException:
                    print("Timeout while waiting for search results")
                    break

                # Parse results
                html = browser.page_source
                soup = BeautifulSoup(html, 'html.parser')

                # Find all tender rows
                tender_rows = soup.select("div#listTemplate tbody tr")
                if len(tender_rows) == 0:
                    print(f"No tenders found for keyword: {keyword}")
                    break

                for row in tender_rows:
                    try:
                        # Extract and strip text from each relevant cell
                        cells = row.find_all('td')
                        cell_texts = [cell.text.strip() for cell in cells]

                        if len(cells) < 5 or "Es wurden keine passenden Bekanntmachungen gefunden." in cell_texts[0]:
                            continue  # Skip rows without sufficient data or with no matching tenders

                        date_published = cells[0].find('abbr').text.strip() if cells[0].find('abbr') else "not specified"
                        tender_deadline = cells[1].find('abbr').text.strip() if cells[1].find('abbr') else "not specified"
                        tender_name = cells[2].text.strip() if cells[2] else "not specified"
                        tender_type = cells[3].text.strip() if cells[3] else "not specified"
                        tender_authority = cells[4].text.strip() if cells[4] else "not specified"

                        # Format dates
                        formatted_deadline = format_date(tender_deadline) if tender_deadline != "nv" else "not specified"
                        formatted_date_published = format_date(date_published)

                        # Create or update tender details
                        if tender_name in all_tenders:
                            if keyword not in all_tenders[tender_name]['found_keywords']:
                                all_tenders[tender_name]['found_keywords'] += f", {keyword}"
                        else:
                            tender_details = {
                                'date_published': formatted_date_published,
                                'tender_name': tender_name,
                                'tender_authority': tender_authority,
                                'tender_type': tender_type,
                                'tender_deadline': formatted_deadline,
                                'source_url': source_url,
                                'found_keywords': keyword  # Initialize with the searched keyword
                            }
                            all_tenders[tender_name] = tender_details

                    except Exception as e:
                        print(f"An error occurred while parsing row: {e}")

                # Check for next page
                next_page = soup.find('a', {'title': 'Nächste Seite'})
                if next_page and 'disabled' not in next_page.get('class', []):
                    next_url = urljoin(url, next_page['href'])
                    print(f"Navigating to the next page: {next_url}")
                    browser.get(next_url)
                else:
                    break
        except TimeoutException:
            print(f"Timeout while searching for keyword: {keyword}")
        except NoSuchElementException:
            print(f"No element found for keyword: {keyword}")
        except ElementNotInteractableException:
            print(f"Element not interactable for keyword: {keyword}")
        except Exception as e:
            print(f"An error occurred while searching for keyword: {keyword} - {str(e)}")

    tenders = list(all_tenders.values())
    total_tenders = len(tenders)
    print(f"Total tenders found for Niedersachsen: {total_tenders}")
    return all_tenders

def scrape_metropoleruhr(browser, url, keywords, source_url):
    print(f"Scraping dynamic content from {url}...")
    all_tenders = {}
    browser.get(url)

    for keyword in keywords:
        try:
            print(f"Searching for keyword: {keyword}")
            # Perform search
            search_input = WebDriverWait(browser, 20).until(
                EC.presence_of_element_located((By.ID, "searchText"))
            )
            search_input.clear()
            search_input.send_keys(keyword)
            
            # Click the search button
            search_button = browser.find_element(By.ID, "searchStart")
            search_button.click()

            while True:
                # Wait for results to load
                try:
                    WebDriverWait(browser, 20).until(
                        EC.presence_of_element_located((By.XPATH, "//div[@id='listTemplate']"))
                    )
                    print("Search processed, results loaded")
                    # Adding delay to ensure the page is fully loaded
                    time.sleep(5)  # Adjust the sleep duration as needed
                except TimeoutException:
                    print("Timeout while waiting for search results")
                    break

                # Parse results
                html = browser.page_source
                soup = BeautifulSoup(html, 'html.parser')

                # Find all tender rows
                tender_rows = soup.select("div#listTemplate tbody tr")
                if len(tender_rows) == 0:
                    print(f"No tenders found for keyword: {keyword}")
                    break

                for row in tender_rows:
                    try:
                        # Extract and strip text from each relevant cell
                        cells = row.find_all('td')
                        cell_texts = [cell.text.strip() for cell in cells]

                        if len(cells) < 5 or "Es wurden keine passenden Bekanntmachungen gefunden." in cell_texts[0]:
                            print(f"No tender found for keyword: {keyword}")
                            continue  # Skip rows without sufficient data or with no matching tenders

                        date_published = cells[0].find('abbr').text.strip() if cells[0].find('abbr') else "not specified"
                        tender_deadline = cells[1].find('abbr').text.strip() if cells[1].find('abbr') else "not specified"
                        tender_name = cells[2].text.strip() if cells[2] else "not specified"
                        tender_type = cells[3].text.strip() if cells[3] else "not specified"
                        tender_authority = cells[4].text.strip() if cells[4] else "not specified"

                        # Format dates
                        formatted_deadline = format_date(tender_deadline) if tender_deadline != "nv" else "not specified"
                        formatted_date_published = format_date(date_published)

                        # Create or update tender details
                        if tender_name in all_tenders:
                            if keyword not in all_tenders[tender_name]['found_keywords']:
                                all_tenders[tender_name]['found_keywords'] += f", {keyword}"
                        else:
                            tender_details = {
                                'date_published': formatted_date_published,
                                'tender_name': tender_name,
                                'tender_authority': tender_authority,
                                'tender_type': tender_type,
                                'tender_deadline': formatted_deadline,
                                'source_url': source_url,
                                'found_keywords': keyword  # Initialize with the searched keyword
                            }
                            all_tenders[tender_name] = tender_details

                    except Exception as e:
                        print(f"An error occurred while parsing row: {e}")

                # Check for next page
                next_page = soup.find('a', {'title': 'Nächste Seite'})
                if next_page and 'disabled' not in next_page.get('class', []):
                    next_url = urljoin(url, next_page['href'])
                    print(f"Navigating to the next page: {next_url}")
                    browser.get(next_url)
                else:
                    break
        except TimeoutException:
            print(f"Timeout while searching for keyword: {keyword}")
        except NoSuchElementException:
            print(f"No element found for keyword: {keyword}")
        except ElementNotInteractableException:
            print(f"Element not interactable for keyword: {keyword}")
        except Exception as e:
            print(f"An error occurred while searching for keyword: {keyword} - {str(e)}")

    tenders = list(all_tenders.values())
    total_tenders = len(tenders)
    print(f"Total tenders found for Metropoleruhr: {total_tenders}")
    return all_tenders


def scrape_saarvpsl(browser, url, keywords, source_url):
    print(f"Scraping dynamic content from {url}...")
    all_tenders = {}
    browser.get(url)

    for keyword in keywords:
        try:
            print(f"Searching for keyword: {keyword}")
            
            # Check if we are on the initial page or subsequent searches
            if "searchVisible" in browser.page_source:
                search_input = WebDriverWait(browser, 20).until(
                    EC.presence_of_element_located((By.ID, "searchVisible"))
                )
            else:
                search_input = WebDriverWait(browser, 20).until(
                    EC.presence_of_element_located((By.ID, "inputSearchKey"))
                )

            search_input.clear()
            search_input.send_keys(keyword)
            
            if "searchVisible" in browser.page_source:
                search_submit = WebDriverWait(browser, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "button.btn-mainSearch"))
                )
            else:
                search_submit = WebDriverWait(browser, 10).until(
                    EC.element_to_be_clickable((By.ID, "btnSearchSubmit"))
                )

            browser.execute_script("arguments[0].click();", search_submit)

            while True:
                # Wait for results to load
                try:
                    WebDriverWait(browser, 20).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "div.border.col-lg-12"))
                    )
                    time.sleep(5)  # Adjust the sleep duration as needed
                except TimeoutException:
                    print(f"Timeout while waiting for search results for keyword: {keyword}")
                    break

                # Parse results
                html = browser.page_source
                soup = BeautifulSoup(html, 'html.parser')

                # Check if no results were found
                no_results_tag = soup.find('td', colspan="6", string="There were no matching notices found.")
                if no_results_tag:
                    print(f"No tenders found for keyword: {keyword}")
                    break

                # Check if results were found
                results_text = soup.find('h3', class_='color-main')
                if results_text and "gefundene Ausschreibung" in results_text.text:
                    num_results = int(results_text.text.split()[0])

                # Find all tender rows
                tender_blocks = soup.select("tbody.tableLeftHeaderBlock[tabindex='0']")
                print(f"Found {len(tender_blocks)} tender blocks for keyword: {keyword}")

                for tbody in tender_blocks:
                    rows = tbody.find_all('tr', class_='tableRowLeft')
                    tender_details = {}
                    for row in rows:
                        try:
                            cells = row.find_all('td')
                            if len(cells) < 2:
                                continue  # Skip rows with insufficient data

                            header_text = cells[0].get_text(strip=True)
                            data_text = cells[1].get_text(strip=True)

                            if 'Ausschreibung' in header_text:
                                tender_details['tender_name'] = data_text or "not specified"
                            elif 'Vergabestelle' in header_text:
                                tender_details['tender_authority'] = data_text or "not specified"
                            elif 'Verfahrensart' in header_text:
                                tender_details['tender_type'] = data_text or "not specified"
                            elif 'Rechtsrahmen' in header_text:
                                tender_details['tender_law'] = data_text or "not specified"
                            elif 'Abgabefrist' in header_text:
                                tender_details['tender_deadline'] = format_date(data_text.split()[0]) if data_text else "not specified"
                            elif 'Erschienen am' in header_text:
                                tender_details['date_published'] = format_date(data_text) if data_text else "not specified"

                        except Exception as e:
                            print(f"An error occurred while parsing row: {e}")

                    if tender_details:
                        tender_name = tender_details.get('tender_name', 'unknown')
                        tender_details['source_url'] = source_url
                        tender_details['found_keywords'] = keyword

                        if tender_name in all_tenders:
                            if keyword not in all_tenders[tender_name]['found_keywords']:
                                all_tenders[tender_name]['found_keywords'] += f", {keyword}"
                        else:
                            all_tenders[tender_name] = tender_details

                # Check for next page
                next_page = soup.find('a', {'title': 'Nächste Seite'})
                if next_page and 'disabled' not in next_page.get('class', []):
                    next_url = urljoin(url, next_page['href'])
                    print(f"Navigating to the next page: {next_url}")
                    browser.get(next_url)
                else:
                    break

        except TimeoutException:
            print(f"Timeout while searching for keyword: {keyword}")
        except NoSuchElementException:
            print(f"No element found for keyword: {keyword}")
        except ElementNotInteractableException:
            print(f"Element not interactable for keyword: {keyword}")
        except Exception as e:
            print(f"An error occurred while searching for keyword: {keyword} - {str(e)}")

    tenders = list(all_tenders.values())
    total_tenders = len(tenders)
    print(f"Total tenders found for Saarvpsl: {total_tenders}")
    return tenders


def scrape_e_vergabe_sh(browser, url, keywords, source_url):
    print(f"Scraping dynamic content from {url}...")
    all_tenders = {}
    browser.get(url)

    for keyword in keywords:
        try:
            print(f"Searching for keyword: {keyword}")

            # Perform search
            search_input = WebDriverWait(browser, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input.search[type='text'][name='tx_ncevergabe_pi2[searchDemand][searchTerm]']"))
            )
            search_input.clear()
            search_input.send_keys(keyword)

            # Click the search button
            search_button = browser.find_element(By.CSS_SELECTOR, "input.btn[type='submit'][value='Suchen']")
            browser.execute_script("arguments[0].click();", search_button)

            # Wait for results to load
            WebDriverWait(browser, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.bek_list_scroll"))
            )
            time.sleep(5)  # Adjust the sleep duration as needed

            # Parse results
            html = browser.page_source
            soup = BeautifulSoup(html, 'html.parser')

            # Check if no results were found
            no_results_tag = soup.find('div', style="text-align:center; margin-top:50px;", string="Es wurden keine Vergabeinformationen zu Ihren Suchkriterien gefunden!")
            if no_results_tag:
                print(f"No tenders found for keyword: {keyword}")
                continue

            # Find all tender rows
            tender_blocks = soup.select("div.bek_list_item_w_hover.js-list-detaillink")
            print(f"Found {len(tender_blocks)} tender blocks for keyword: {keyword}")

            for block in tender_blocks:
                try:
                    tender_name = block.find('div', class_='bek_list_item_headline').get_text(strip=True) or "not specified"
                    tender_authority = block.find('div', class_='bek_list_item_info').get_text(strip=True).replace('Beauftragtes Unternehmen: ', '') or "not specified"
                    tender_code = block.find('div', class_='bek_list_item_left').contents[0].strip() or "not specified"
                    tender_date = block.find('div', class_='bek-date').get_text(strip=True).replace('Datum: ', '') or "not specified"
                    tender_date = format_date(tender_date)

                    tender_details = {
                        'tender_name': tender_name,
                        'tender_authority': tender_authority,
                        'tender_code': tender_code,
                        'date_published': tender_date,
                        'source_url': source_url,
                        'found_keywords': keyword
                    }

                    if tender_name in all_tenders:
                        if keyword not in all_tenders[tender_name]['found_keywords']:
                            all_tenders[tender_name]['found_keywords'] += f", {keyword}"
                    else:
                        all_tenders[tender_name] = tender_details

                except Exception as e:
                    print(f"An error occurred while parsing block: {e}")

        except TimeoutException:
            print(f"Timeout while searching for keyword: {keyword}")
        except NoSuchElementException:
            print(f"No element found for keyword: {keyword}")
        except ElementNotInteractableException:
            print(f"Element not interactable for keyword: {keyword}")
        except Exception as e:
            print(f"An error occurred while searching for keyword: {keyword} - {str(e)}")

    tenders = list(all_tenders.values())
    total_tenders = len(tenders)
    print(f"Total tenders found for Schleswig-Holstein: {total_tenders}")
    return tenders


def scrape_website(url):
    print(f"Fetching content from {url}...")
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raises an HTTPError if the HTTP request returned an unsuccessful status code
        return response.text
    except requests.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"An error occurred: {err}")
    return None

def scrape_site(site_info):
    url, scrape_func, source_url = site_info
    keywords = ["Erlebnis", "Freizeit", "Destination", "Tourismus", "Tourismusförderung", "Tourismuskonzept",
                "Tourismuskonzeption", "Tourismusservice", "Besucher", "Museum", "Markenwelt", "Ausstellung",
                "Ideenskizze", "Konzept", "Nutzungsidee", "Masterplan", "Machbarkeit", "Beratung", "Studie",
                "Analyse", "Machbarkeitsanalyse", "Marktforschung", "Plausibilisierung", "Investitionskostenschätzung",
                "Machbarkeitsstudie", "Besucherzentrum", "Informationszentrum", "Gartenschau", "Grünanlage",
                "Besucherinformationszentrum", "Gutachten"]
    
    if "myorder.rib.de" in source_url:
        tenders = scrape_bayern_selenium(initialize_browser(), url, keywords, source_url)
    elif "vmstart" in source_url or "saarvpsl.vmstart.de" in source_url:
        tenders = globals()[scrape_func](initialize_browser(), url, keywords, source_url)
    elif "vergabe.rlp.de" in source_url or "evergabe.nrw.de" in source_url or "vergabe.metropoleruhr.de" in source_url or "vergabe.niedersachsen.de" in source_url or "vergabemarktplatz.brandenburg.de" in source_url or "e-vergabe-sh.de" in source_url:
        tenders_dict = globals()[scrape_func](initialize_browser(), url, keywords, source_url)
        tenders = [tender for tender in tenders_dict.values()]
    else:
        html = scrape_website(url)
        if html:
            tenders = globals()[scrape_func](html, keywords, source_url)
    return tenders

def scrape_all():
    results = []
    browser = initialize_browser("chrome")
    
    websites = {
        "https://vergabe.muenchen.de/NetServer/PublicationSearchControllerServlet?function=SearchPublications&Gesetzesgrundlage=All&Category=InvitationToTender&thContext=publications": ("scrape_muenchen", "https://vergabe.muenchen.de"),
        "https://vergabe.vmstart.de/NetServer/PublicationSearchControllerServlet?function=SearchPublications&Gesetzesgrundlage=All&Category=InvitationToTender&thContext=publications": ("scrape_vmstart", "https://vergabe.vmstart.de"),
        "https://www.myorder.rib.de/public/publications": ("scrape_bayern_selenium", "https://www.myorder.rib.de"),
        "https://vergabe.rlp.de/VMPCenter/company/announcements/categoryOverview.do?method=show": ("Rheinland", "https://vergabe.rlp.de"),
        "https://www.evergabe.nrw.de/VMPCenter/company/announcements/categoryOverview.do?method=show": ("scrape_nrw", "https://www.evergabe.nrw.de"),
        "https://www.vergabe.metropoleruhr.de/VMPSatellite/company/announcements/categoryOverview.do?method=show": ("scrape_metropoleruhr", "https://www.vergabe.metropoleruhr.de"),
        "https://vergabe.niedersachsen.de/Satellite/company/announcements/categoryOverview.do?method=show": ("scrape_niedersachsen", "https://vergabe.niedersachsen.de"),
        "https://vergabemarktplatz.brandenburg.de/VMPCenter/company/announcements/categoryOverview.do?method=show": ("scrape_brandenburg", "https://vergabemarktplatz.brandenburg.de"),
        "https://saarvpsl.vmstart.de/NetServer/PublicationSearchControllerServlet?function=SearchPublications&Gesetzesgrundlage=All&Category=InvitationToTender&thContext=publications": ("scrape_saarvpsl", "https://saarvpsl.vmstart.de"),
        "https://www.e-vergabe-sh.de/vergabeplattform/vergabeinformationen": ("scrape_e_vergabe_sh", "https://www.e-vergabe-sh.de")
    }

    location_defaults = {
        "https://vergabe.muenchen.de": "Bavaria",
        "https://vergabe.rlp.de": "Rhineland-Palatinate",
        "https://www.evergabe.nrw.de": "North Rhine-Westphalia",
        "https://www.myorder.rib.de": "Bavaria",
        "https://saarvpsl.vmstart.de": "Saarland",
        "https://vergabemarktplatz.brandenburg.de": "Brandenburg",
        "https://vergabe.vmstart.de" : "Rhineland-Palatinate"
    }

    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_url = {executor.submit(scrape_site, (url, scrape_func, source_url)): url for url, (scrape_func, source_url) in websites.items()}
        for future in concurrent.futures.as_completed(future_to_url):
            try:
                tenders = future.result()
                results.extend(tenders)
            except Exception as exc:
                print(f"An error occurred: {exc}")

    # Consolidate keywords for the same tender
    unique_tenders = {}
    for tender in results:
        tender_name = tender['tender_name']
        if tender_name in unique_tenders:
            existing_keywords = unique_tenders[tender_name]['found_keywords'].split(', ')
            new_keywords = tender['found_keywords'].split(', ')
            all_keywords = list(set(existing_keywords + new_keywords))
            unique_tenders[tender_name]['found_keywords'] = ', '.join(all_keywords)
        else:
            # Assign default state if not identified
            if 'state' not in tender or tender['state'] == "not specified":
                tender['state'] = location_defaults.get(tender['source_url'], "not specified")

            # Assign date_published to application_start_date if not defined
            if 'application_start_date' not in tender or tender['application_start_date'] == "not specified":
                tender['application_start_date'] = tender.get('date_published', "not specified")

            unique_tenders[tender_name] = tender

    print(f"Total unique tenders: {len(unique_tenders)}")

    if not unique_tenders:
        return pd.DataFrame()  # Return an empty DataFrame if no results

    # Write the results to a CSV file
    fieldnames = [
        'tender_name',
        'tender_authority',  # Authority part of description
        'application_start_date',  # Start date of application
        'tender_deadline',  # Harmonized key for deadlines
        'period',  # Unique to scrape_bayern
        'tender_location',  # Harmonized key for location/execution place
        'date_published',  # From scrape_bayern and scrape_muenchen
        'tender_type',  # Unique to scrape_muenchen
        'source_url',  # Common across all functions
        'found_keywords',
        'state'  # Add the state column
    ]
    df = pd.DataFrame(unique_tenders.values(), columns=fieldnames)

    # Get the current date and time
    now = datetime.datetime.now()
    formatted_date = now.strftime("%Y-%m-%d_%H-%M-%S")

    # Ensure the directory exists
    output_dir = "master/2_streamlit/Capstone_ATIS_Streamlit"
    os.makedirs(output_dir, exist_ok=True)

    # Save the DataFrame to a CSV file with the date and time in the filename
    filename = os.path.join(output_dir, f'capstone_results_{formatted_date}.csv')
    df.to_csv(filename, index=False)
    print(f"Scraping completed. Data saved to {filename}")

    return df

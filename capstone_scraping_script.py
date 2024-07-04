import os
import pandas as pd
from bs4 import BeautifulSoup
import asyncio
from pyppeteer import launch
from datetime import datetime
from urllib.parse import urljoin

# Define the state coordinates
state_coordinates = {
    'Baden-Württemberg': {'latitude': 48.6616037, 'longitude': 9.3501336},
    'Bavaria': {'latitude': 48.7904472, 'longitude': 11.4978898},
    'Berlin': {'latitude': 52.5200066, 'longitude': 13.404954},
    'Brandenburg': {'latitude': 52.4084186, 'longitude': 12.5316444},
    'Bremen': {'latitude': 53.0792962, 'longitude': 8.8016937},
    'Hamburg': {'latitude': 53.551086, 'longitude': 9.993682},
    'Hesse': {'latitude': 50.6520516, 'longitude': 9.1624376},
    'Mecklenburg-Vorpommern': {'latitude': 53.6126503, 'longitude': 12.4295953},
    'Lower Saxony': {'latitude': 52.6367036, 'longitude': 9.8450824},
    'North Rhine-Westphalia': {'latitude': 51.4332367, 'longitude': 7.6615938},
    'Rhineland-Palatinate': {'latitude': 50.118182, 'longitude': 7.308953},
    'Saarland': {'latitude': 49.3964237, 'longitude': 7.0229607},
    'Saxony': {'latitude': 51.1045407, 'longitude': 13.2017384},
    'Saxony-Anhalt': {'latitude': 51.9507459, 'longitude': 11.6922777},
    'Schleswig-Holstein': {'latitude': 54.219367, 'longitude': 9.696957},
    'Thuringia': {'latitude': 50.9013853, 'longitude': 11.0772807},
}

async def initialize_browser():
    browser = await launch(headless=True, args=['--no-sandbox', '--disable-setuid-sandbox'])
    return browser

def format_date(date_string):
    try:
        date_obj = datetime.strptime(date_string, "%d.%m.%y")
        return date_obj.strftime("%d.%m.%y")
    except ValueError:
        return "not specified"

async def scrape_bayern_pyppeteer(url, keywords, source_url):
    browser = await initialize_browser()
    page = await browser.newPage()
    await page.goto(url)
    content = await page.content()
    soup = BeautifulSoup(content, 'html.parser')
    tenders = []

    tender_blocks = soup.find_all('div', class_='item')
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
            tender_code = title.split()[0]

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
                        start_date, end_date = value.split(' until ')
                        tender_details['application_start_date'] = format_date(start_date)
                        tender_details['application_deadline'] = format_date(end_date)
                    else:
                        tender_details[info_key] = value

            date_div = block.find('div', class_='item-right meta')
            if date_div:
                day = date_div.find('div', class_='date').text.strip()
                month_year = date_div.find('div', class_='month').text.strip()
                publication_date_str = f"{day} {month_year}"
                tender_details['date_published'] = format_date(publication_date_str)
            else:
                tender_details['date_published'] = "not specified"

            tender_details['state'] = "Bavaria"

            tenders.append(tender_details)

    await browser.close()
    return tenders

async def scrape_muenchen_pyppeteer(url, keywords, source_url):
    browser = await initialize_browser()
    page = await browser.newPage()
    await page.goto(url)
    content = await page.content()
    soup = BeautifulSoup(content, 'html.parser')
    tenders = []

    tender_rows = soup.find_all('tr', class_='tableRow clickable-row publicationDetail')
    for row in tender_rows:
        date_published = row.find('td').text.strip()
        tender_name = row.find('td', class_='tender').text.strip()
        tender_authority = row.find('td', class_='tenderAuthority').text.strip()
        tender_type = row.find('td', class_='tenderType').text.strip()
        tender_deadline = row.find('td', class_='tenderDeadline').text.strip()

        tender_deadline_date = tender_deadline.split(' ')[0]  # This assumes the format is "date time"
        formatted_deadline = format_date(tender_deadline_date)

        try:
            date_obj = datetime.strptime(date_published, '%d.%m.%y')
            formatted_date_published = date_obj.strftime("%d.%m.%y")
        except ValueError:
            formatted_date_published = "not specified"

        found_keywords = [keyword for keyword in keywords if keyword.lower() in tender_name.lower()]
        if found_keywords:
            tenders.append({
                'date_published': formatted_date_published,
                'tender_name': tender_name,
                'tender_authority': tender_authority,
                'tender_type': tender_type,
                'tender_deadline': formatted_deadline,
                'source_url': source_url,
                'found_keywords': ', '.join(found_keywords)
            })

    await browser.close()
    return tenders

async def scrape_vmstart_pyppeteer(url, keywords, source_url):
    browser = await initialize_browser()
    page = await browser.newPage()
    await page.goto(url)

    results = []
    for keyword in keywords:
        await page.type('#searchVisible', keyword)
        await page.click('button.btn-mainSearch')
        await page.waitForSelector('div.border.col-lg-12')
        await asyncio.sleep(5)  # Adjust the sleep duration as needed

        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')

        no_results_text = soup.find('h3', class_='color-main')
        if no_results_text and "0 gefundene Ausschreibung" in no_results_text.text:
            print(f"No tenders found for keyword: {keyword}")
            continue

        results_text = soup.find('h3', class_='color-main')
        if results_text and "gefundene Ausschreibung" in results_text.text:
            num_results = int(results_text.text.split()[0])
            print(f"{num_results} tenders found for keyword: {keyword}")

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

                results.append(tender_details)

    await browser.close()
    return results

async def scrape_rheinland_pyppeteer(url, keywords, source_url):
    browser = await initialize_browser()
    page = await browser.newPage()
    await page.goto(url)

    results = []
    for keyword in keywords:
        await page.type('#searchText', keyword)
        await page.click('#searchStart')
        await page.waitForSelector('#listTemplate')
        await asyncio.sleep(5)

        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')

        tender_rows = soup.select("div#listTemplate tbody tr")
        if len(tender_rows) == 0:
            print(f"No tenders found for keyword: {keyword}")
            continue

        for row in tender_rows:
            try:
                cells = row.find_all('td')
                if len(cells) < 5:
                    continue

                date_published = cells[0].find('abbr').text.strip() if cells[0].find('abbr') else "not specified"
                tender_deadline = cells[1].find('abbr').text.strip() if cells[1].find('abbr') else "not specified"
                tender_name = cells[2].text.strip() if cells[2] else "not specified"
                tender_type = cells[3].text.strip() if cells[3] else "not specified"
                tender_authority = cells[4].text.strip() if cells[4] else "not specified"

                formatted_deadline = format_date(tender_deadline) if tender_deadline != "nv" else "not specified"
                formatted_date_published = format_date(date_published)

                results.append({
                    'date_published': formatted_date_published,
                    'tender_name': tender_name,
                    'tender_authority': tender_authority,
                    'tender_type': tender_type,
                    'tender_deadline': formatted_deadline,
                    'source_url': source_url,
                    'found_keywords': keyword
                })

            except Exception as e:
                print(f"An error occurred while parsing row: {e}")

    await browser.close()
    return results

async def scrape_nrw_pyppeteer(url, keywords, source_url):
    browser = await initialize_browser()
    page = await browser.newPage()
    await page.goto(url)

    results = []
    for keyword in keywords:
        await page.type('#searchText', keyword)
        await page.click('#searchStart')
        await page.waitForSelector('#listTemplate')
        await asyncio.sleep(5)

        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')

        tender_rows = soup.select("div#listTemplate tbody tr")
        for row in tender_rows:
            try:
                cells = row.find_all('td')
                if len(cells) < 5 or "Es wurden keine passenden Bekanntmachungen gefunden." in cells[0].text:
                    continue

                date_published = cells[0].find('abbr').text.strip() if cells[0].find('abbr') else "not specified"
                tender_deadline = cells[1].find('abbr').text.strip() if cells[1].find('abbr') else "not specified"
                tender_name = cells[2].text.strip() if cells[2] else "not specified"
                tender_type = cells[3].text.strip() if cells[3] else "not specified"
                tender_authority = cells[4].text.strip() if cells[4] else "not specified"

                formatted_deadline = format_date(tender_deadline) if tender_deadline != "nv" else "not specified"
                formatted_date_published = format_date(date_published)

                results.append({
                    'date_published': formatted_date_published,
                    'tender_name': tender_name,
                    'tender_authority': tender_authority,
                    'tender_type': tender_type,
                    'tender_deadline': formatted_deadline,
                    'source_url': source_url,
                    'found_keywords': keyword
                })

            except Exception as e:
                print(f"An error occurred while parsing row: {e}")

    await browser.close()
    return results

async def scrape_metropoleruhr_pyppeteer(url, keywords, source_url):
    browser = await initialize_browser()
    page = await browser.newPage()
    await page.goto(url)

    results = []
    for keyword in keywords:
        await page.type('#searchText', keyword)
        await page.click('#searchStart')
        await page.waitForSelector('#listTemplate')
        await asyncio.sleep(5)

        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')

        tender_rows = soup.select("div#listTemplate tbody tr")
        for row in tender_rows:
            try:
                cells = row.find_all('td')
                if len(cells) < 5 or "Es wurden keine passenden Bekanntmachungen gefunden." in cells[0].text:
                    continue

                date_published = cells[0].find('abbr').text.strip() if cells[0].find('abbr') else "not specified"
                tender_deadline = cells[1].find('abbr').text.strip() if cells[1].find('abbr') else "not specified"
                tender_name = cells[2].text.strip() if cells[2] else "not specified"
                tender_type = cells[3].text.strip() if cells[3] else "not specified"
                tender_authority = cells[4].text.strip() if cells[4] else "not specified"

                formatted_deadline = format_date(tender_deadline) if tender_deadline != "nv" else "not specified"
                formatted_date_published = format_date(date_published)

                results.append({
                    'date_published': formatted_date_published,
                    'tender_name': tender_name,
                    'tender_authority': tender_authority,
                    'tender_type': tender_type,
                    'tender_deadline': formatted_deadline,
                    'source_url': source_url,
                    'found_keywords': keyword
                })

            except Exception as e:
                print(f"An error occurred while parsing row: {e}")

    await browser.close()
    return results

async def scrape_niedersachsen_pyppeteer(url, keywords, source_url):
    browser = await initialize_browser()
    page = await browser.newPage()
    await page.goto(url)

    results = []
    for keyword in keywords:
        await page.type('#searchText', keyword)
        await page.click('#searchStart')
        await page.waitForSelector('#listTemplate')
        await asyncio.sleep(5)

        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')

        tender_rows = soup.select("div#listTemplate tbody tr")
        for row in tender_rows:
            try:
                cells = row.find_all('td')
                if len(cells) < 5 or "Es wurden keine passenden Bekanntmachungen gefunden." in cells[0].text:
                    continue

                date_published = cells[0].find('abbr').text.strip() if cells[0].find('abbr') else "not specified"
                tender_deadline = cells[1].find('abbr').text.strip() if cells[1].find('abbr') else "not specified"
                tender_name = cells[2].text.strip() if cells[2] else "not specified"
                tender_type = cells[3].text.strip() if cells[3] else "not specified"
                tender_authority = cells[4].text.strip() if cells[4] else "not specified"

                formatted_deadline = format_date(tender_deadline) if tender_deadline != "nv" else "not specified"
                formatted_date_published = format_date(date_published)

                results.append({
                    'date_published': formatted_date_published,
                    'tender_name': tender_name,
                    'tender_authority': tender_authority,
                    'tender_type': tender_type,
                    'tender_deadline': formatted_deadline,
                    'source_url': source_url,
                    'found_keywords': keyword
                })

            except Exception as e:
                print(f"An error occurred while parsing row: {e}")

    await browser.close()
    return results

async def scrape_brandenburg_pyppeteer(url, keywords, source_url):
    browser = await initialize_browser()
    page = await browser.newPage()
    await page.goto(url)

    results = []
    for keyword in keywords:
        await page.type('#searchText', keyword)
        await page.click('#searchStart')
        await page.waitForSelector('#listTemplate')
        await asyncio.sleep(5)

        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')

        tender_rows = soup.select("div#listTemplate tbody tr")
        for row in tender_rows:
            try:
                cells = row.find_all('td')
                if len(cells) < 5 or "Es wurden keine passenden Bekanntmachungen gefunden." in cells[0].text:
                    continue

                date_published = cells[0].find('abbr').text.strip() if cells[0].find('abbr') else "not specified"
                tender_deadline = cells[1].find('abbr').text.strip() if cells[1].find('abbr') else "not specified"
                tender_name = cells[2].text.strip() if cells[2] else "not specified"
                tender_type = cells[3].text.strip() if cells[3] else "not specified"
                tender_authority = cells[4].text.strip() if cells[4] else "not specified"

                formatted_deadline = format_date(tender_deadline) if tender_deadline != "nv" else "not specified"
                formatted_date_published = format_date(date_published)

                results.append({
                    'date_published': formatted_date_published,
                    'tender_name': tender_name,
                    'tender_authority': tender_authority,
                    'tender_type': tender_type,
                    'tender_deadline': formatted_deadline,
                    'source_url': source_url,
                    'found_keywords': keyword
                })

            except Exception as e:
                print(f"An error occurred while parsing row: {e}")

    await browser.close()
    return results

async def scrape_saarvpsl_pyppeteer(url, keywords, source_url):
    browser = await initialize_browser()
    page = await browser.newPage()
    await page.goto(url)

    results = []
    for keyword in keywords:
        await page.type('#searchVisible', keyword)
        await page.click('button.btn-mainSearch')
        await page.waitForSelector('div.border.col-lg-12')
        await asyncio.sleep(5)  # Adjust the sleep duration as needed

        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')

        no_results_text = soup.find('h3', class_='color-main')
        if no_results_text and "0 gefundene Ausschreibung" in no_results_text.text:
            print(f"No tenders found for keyword: {keyword}")
            continue

        results_text = soup.find('h3', class_='color-main')
        if results_text and "gefundene Ausschreibung" in results_text.text:
            num_results = int(results_text.text.split()[0])
            print(f"{num_results} tenders found for keyword: {keyword}")

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

                results.append(tender_details)

    await browser.close()
    return results

async def scrape_e_vergabe_sh_pyppeteer(url, keywords, source_url):
    browser = await initialize_browser()
    page = await browser.newPage()
    await page.goto(url)

    results = []
    for keyword in keywords:
        await page.type('#searchText', keyword)
        await page.click('#searchStart')
        await page.waitForSelector('#listTemplate')
        await asyncio.sleep(5)

        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')

        tender_rows = soup.select("div#listTemplate tbody tr")
        for row in tender_rows:
            try:
                cells = row.find_all('td')
                if len(cells) < 5 or "Es wurden keine passenden Bekanntmachungen gefunden." in cells[0].text:
                    continue

                date_published = cells[0].find('abbr').text.strip() if cells[0].find('abbr') else "not specified"
                tender_deadline = cells[1].find('abbr').text.strip() if cells[1].find('abbr') else "not specified"
                tender_name = cells[2].text.strip() if cells[2] else "not specified"
                tender_type = cells[3].text.strip() if cells[3] else "not specified"
                tender_authority = cells[4].text.strip() if cells[4] else "not specified"

                formatted_deadline = format_date(tender_deadline) if tender_deadline != "nv" else "not specified"
                formatted_date_published = format_date(date_published)

                results.append({
                    'date_published': formatted_date_published,
                    'tender_name': tender_name,
                    'tender_authority': tender_authority,
                    'tender_type': tender_type,
                    'tender_deadline': formatted_deadline,
                    'source_url': source_url,
                    'found_keywords': keyword
                })

            except Exception as e:
                print(f"An error occurred while parsing row: {e}")

    await browser.close()
    return results

async def scrape_all():
    keywords = ["tourism", "consulting", "analysis", "feasibility", "strategy"]
    source_urls = [
        "https://vergabe.muenchen.de/NetServer/PublicationSearchControllerServlet?function=SearchPublications&Gesetzesgrundlage=All&Category=InvitationToTender&thContext=publications",
        "https://vergabe.vmstart.de/NetServer/PublicationSearchControllerServlet?function=SearchPublications&Gesetzesgrundlage=All&Category=InvitationToTender&thContext=publications",
        "https://www.myorder.rib.de/public/publications",
        "https://vergabe.rlp.de/VMPCenter/company/announcements/categoryOverview.do?method=show",
        "https://www.evergabe.nrw.de/VMPCenter/company/announcements/categoryOverview.do?method=show",
        "https://www.vergabe.metropoleruhr.de/VMPSatellite/company/announcements/categoryOverview.do?method=show",
        "https://vergabe.niedersachsen.de/Satellite/company/announcements/categoryOverview.do?method=show",
        "https://vergabemarktplatz.brandenburg.de/VMPCenter/company/announcements/categoryOverview.do?method=show",
        "https://saarvpsl.vmstart.de/NetServer/PublicationSearchControllerServlet?function=SearchPublications&Gesetzesgrundlage=All&Category=InvitationToTender&thContext=publications",
        "https://www.e-vergabe-sh.de/vergabeplattform/vergabeinformationen"
    ]

    tasks = [
        scrape_muenchen_pyppeteer(source_urls[0], keywords, source_urls[0]),
        scrape_vmstart_pyppeteer(source_urls[1], keywords, source_urls[1]),
        scrape_bayern_pyppeteer(source_urls[2], keywords, source_urls[2]),
        scrape_rheinland_pyppeteer(source_urls[3], keywords, source_urls[3]),
        scrape_nrw_pyppeteer(source_urls[4], keywords, source_urls[4]),
        scrape_metropoleruhr_pyppeteer(source_urls[5], keywords, source_urls[5]),
        scrape_niedersachsen_pyppeteer(source_urls[6], keywords, source_urls[6]),
        scrape_brandenburg_pyppeteer(source_urls[7], keywords, source_urls[7]),
        scrape_saarvpsl_pyppeteer(source_urls[8], keywords, source_urls[8]),
        scrape_e_vergabe_sh_pyppeteer(source_urls[9], keywords, source_urls[9]),
    ]

    results = await asyncio.gather(*tasks)
    return [item for sublist in results for item in sublist]

if __name__ == "__main__":
    results = asyncio.run(scrape_all())
    df = pd.DataFrame(results)
    df.to_csv('tenders.csv', index=False)

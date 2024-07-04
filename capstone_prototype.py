import os
import subprocess
import pandas as pd
from bs4 import BeautifulSoup
import asyncio
from pyppeteer import launch

# Install Chromium, ChromeDriver, and necessary libraries
def install_chromium():
    if not os.path.isfile('/usr/bin/chromium-browser'):
        subprocess.run(['apt-get', 'update'])
        subprocess.run(['apt-get', 'install', '-y', 'chromium-browser'])
        subprocess.run(['apt-get', 'install', '-y', 'chromium-chromedriver'])
        subprocess.run(['apt-get', 'install', '-y', 'libgbm-dev'])

install_chromium()

# Setup Pyppeteer to use the installed Chromium
async def initialize_browser():
    browser = await launch(headless=True, executablePath='/usr/bin/chromium-browser', args=['--no-sandbox'])
    return browser

async def scrape_muenchen_pyppeteer(url, keywords, source_url):
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

# Define other scraping functions similarly for each website
# ...

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

def format_date(date_str):
    try:
        return pd.to_datetime(date_str, format="%d.%m.%Y").strftime("%Y-%m-%d")
    except ValueError:
        return "not specified"

if __name__ == "__main__":
    results = asyncio.run(scrape_all())
    df = pd.DataFrame(results)
    df.to_csv('tenders.csv', index=False)

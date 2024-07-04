# Install Chromium, ChromeDriver, and necessary libraries
def install_chromium():
    if not os.path.isfile('/usr/bin/chromium-browser'):
        subprocess.run(['apt-get', 'update'])
        subprocess.run(['apt-get', 'install', '-y', 'chromium-browser'])
        subprocess.run(['apt-get', 'install', '-y', 'chromium-chromedriver'])
        subprocess.run(['apt-get', 'install', '-y', 'libgbm-dev'])

install_chromium()

# Setup Selenium to use the installed Chromium
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

def initialize_browser():
    print("Initializing browser...")
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.binary_location = '/usr/bin/chromium-browser'
    browser = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)
    return browser

# Cache the scraping function to avoid redundant calls
@st.cache_data
def fetch_latest_data():
    return scrape_all()

# Fetch the latest data
df = fetch_latest_data()

# Replace "not specified" with NaN in the tender_deadline column
df.replace({'tender_deadline': {"not specified": pd.NA}}, inplace=True)

# Convert date columns to datetime format
date_columns = ['application_start_date', 'tender_deadline', 'date_published']
for col in date_columns:
    df[col] = pd.to_datetime(df[col], format="%d.%m.%y", errors='coerce')

# Calculate application_period and published_period
df['application_period'] = (df['tender_deadline'] - df['application_start_date']).dt.days
df['published_period'] = (pd.Timestamp.now() - df['date_published']).dt.days

# Add coordinates to the dataframe
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

# Add coordinates to the dataframe based on state
def add_coordinates(df):
    df['latitude'] = df['state'].apply(lambda x: state_coordinates[x]['latitude'] if x in state_coordinates else None)
    df['longitude'] = df['state'].apply(lambda x: state_coordinates[x]['longitude'] if x in state_coordinates else None)
    return df

df = add_coordinates(df)

# Add 'ALL' option to unique states
unique_states = ['ALL'] + list(df['state'].dropna().unique())

# Get unique keywords
df['found_keywords'] = df['found_keywords'].str.split(', ')
all_keywords = df['found_keywords'].explode().unique()

def display_overview(df, location_column, date_columns):
    st.header("Company Overview")
    
    st.markdown("""
    **Erlebniskontor GmbH** is a consultancy with over 25 years of expertise in creating and managing innovative visitor centers, brand worlds, and exhibitions. Specializing in the seamless integration of location, concept, and operation, the company ensures the success of tourism and cultural projects. With a focus on collaboration, they tailor experiences to meet the needs of clients and audiences alike. Services include comprehensive economic analyses, feasibility studies, and strategic planning to establish a solid foundation for each project.  
    **Current Challenge**  
    In Germany's federal system, each state issues its tenders separately, requiring the exhaustive effort of searching through 16 different portals, in addition to federal tenders.  
    **Operational Inefficiency**  
    The need to manually sift through numerous tender platforms results in a significant drain on time and resources.  
    **GOALS**  
    * Develop a fully functional Automated Tender Identification System (ATIS).  
    * Generate daily reports of new, relevant tender opportunities.  
    * Save time and increase efficiency in operational processes, allowing more focus on creative and experiential aspects.
    """)

    st.subheader("Filtered Tender Data")
    
    selected_states = st.sidebar.multiselect("Filter by State", unique_states, default=['ALL'])
    selected_keywords = st.sidebar.multiselect("Filter by Keyword", options=['ALL'] + list(all_keywords), default=['ALL'])
    
    for col in date_columns:
        date_range = st.sidebar.date_input(f"Filter by {col.replace('_', ' ').title()} Range", [])
        if len(date_range) == 2:
            start_date, end_date = date_range
            df = df[(df[col] >= pd.to_datetime(start_date)) & (df[col] <= pd.to_datetime(end_date))]

    if 'ALL' not in selected_states:
        df = df[df[location_column].isin(selected_states)]
    if 'ALL' not in selected_keywords:
        df = df[df['found_keywords'].apply(lambda x: any(kw in x for kw in selected_keywords))]

    st.dataframe(df)
    st.write(f"Number of rows: {df.shape[0]}")

    # Create a function to download filtered data as Excel
    def to_excel(df):
        output = BytesIO()
        writer = pd.ExcelWriter(output, engine='xlsxwriter')
        df.to_excel(writer, index=False, sheet_name='Sheet1')
        writer.close()
        processed_data = output.getvalue()
        return processed_data

    st.download_button(
        label="Download filtered data as Excel",
        data=to_excel(df),
        file_name='filtered_tenders.xlsx',
        mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )


def display_statistics(df, location_column):
    st.header("Statistics Summary")

    df['application_period'] = (pd.to_datetime(df['tender_deadline'], format="%d.%m.%y") - pd.to_datetime(df['application_start_date'], format="%d.%m.%y")).dt.days
    df['published_period'] = (datetime.now() - pd.to_datetime(df['date_published'], format="%d.%m.%y")).dt.days
    
    stat_df = df.groupby(location_column).agg({
        'application_period': 'mean',
        'published_period': 'mean'
    }).reset_index()

    stat_df['application_period'] = stat_df['application_period'].round(2)
    stat_df['published_period'] = stat_df['published_period'].round(2)
    stat_df.columns = ['State', 'Average Application Period (days)', 'Average Published Period (days)']

    st.write(stat_df)

    st.header("Bar Charts")
    criteria = st.radio("Select criteria for bar chart", ['State', 'Keywords by State'])

    if criteria == 'State':
        bar_data = df.groupby(location_column).size().reset_index(name='count')
        st.bar_chart(bar_data.set_index(location_column)['count'])
    else:
        keyword_col = 'found_keywords'
        bar_data = df[keyword_col].explode().groupby(df[location_column]).value_counts().reset_index(name='count')
        bar_data.columns = [location_column, 'Keyword', 'count']
        bar_data_pivot = bar_data.pivot(index='Keyword', columns=location_column, values='count').fillna(0)
        st.bar_chart(bar_data_pivot)

    st.header("Publication Dates")
    pub_dates = df['date_published'].value_counts().reset_index(name='count')
    pub_dates.columns = ['Date', 'Count']
    pub_dates['Date'] = pd.to_datetime(pub_dates['Date'], format="%d.%m.%y")
    pub_dates = pub_dates.sort_values(by='Date')
    min_date = pub_dates['Date'].min().to_pydatetime()
    max_date = pub_dates['Date'].max().to_pydatetime()
    selected_date_range = st.slider("Select Date Range", min_value=min_date, max_value=max_date, value=(min_date, max_date), format="YYYY-MM-DD")
    pub_dates_filtered = pub_dates[(pub_dates['Date'] >= selected_date_range[0]) & (pub_dates['Date'] <= selected_date_range[1])]
    st.line_chart(pub_dates_filtered.set_index('Date')['Count'])



def display_map(df):
    st.header("Tender Locations on Map")
    if 'latitude' in df.columns and 'longitude' in df.columns:
        df_map = df.dropna(subset=['latitude', 'longitude']).copy()
        df_map['count'] = df_map.groupby(['latitude', 'longitude'])['tender_name'].transform('count')
        st.pydeck_chart(
            pdk.Deck(
                map_style="mapbox://styles/mapbox/light-v9",
                initial_view_state=pdk.ViewState(
                    latitude=df_map["latitude"].mean(),
                    longitude=df_map["longitude"].mean(),
                    zoom=6,
                    pitch=50,
                ),
                layers=[
                    pdk.Layer(
                        "ScatterplotLayer",
                        data=df_map,
                        get_position=["longitude", "latitude"],
                        get_color=[200, 30, 0, 160],
                        get_radius=10000,
                        pickable=True,
                        auto_highlight=True,
                    ),
                    pdk.Layer(
                        "TextLayer",
                        data=df_map,
                        get_position=["longitude", "latitude"],
                        get_text="count",
                        get_size=16,
                        get_color=[0, 0, 0],
                        get_alignment_baseline="'bottom'",
                    ),
                ],
                tooltip={"text": "State: {state}\nNumber of Tenders: {count}"}
            )
        )

def main():
    st.title("Automated Tender Identification System (ATIS)")

    # Define the columns
    location_column = 'state'
    date_columns = ['application_start_date', 'tender_deadline', 'date_published']

    tab_overview, tab_stats, tab_map = st.tabs(["Overview", "Statistics", "Map"])

    with tab_overview:
        display_overview(df, location_column, date_columns)
    with tab_stats:
        display_statistics(df, location_column)
    with tab_map:
        display_map(df)

if __name__ == "__main__":
    main()

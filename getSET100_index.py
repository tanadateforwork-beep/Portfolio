import requests
from datetime import datetime
import ctds
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

server_ip = ''
server_port = 
conn = ctds.connect(server=, port=, user='', password='', database='', timeout=60000)

c = conn.cursor()
curr_date = datetime.now().strftime('%Y-%m-%d')
curr_year = datetime.now().strftime('%Y')

def get_cookies(url):
    """Fetch cookies from the specified URL using Selenium."""
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--headless")  # Run in headless mode
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--remote-debugging-port=9222")
    
    service = Service(executable_path='/usr/bin/chromedriver')
    for _ in range(15):
        try:
            with webdriver.Chrome(service=service, options=chrome_options) as driver:
                driver.get(url)
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
                cookies = driver.get_cookies()
            
            return "; ".join([f"{cookie['name']}={cookie['value']}" for cookie in cookies])
        except requests.exceptions.RequestException as e:
            print(f'Error making request: {e}')
    return None

def fetch_data(url, headers):
    """Make a GET request to the specified URL and return the JSON data."""
    for _ in range(15):
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f'Error making request: {e}')
    return None

# Fetch holiday data
holiday_url = f'https://www.set.or.th/en/about/event-calendar/holiday?year={curr_year}'
cookies = get_cookies(holiday_url)
holiday_api_url = f'https://www.set.or.th/api/set/holiday/year/{curr_year}?lang=en'
holiday_list = []

headers_holidays = {
    "Accept": "application/json, text/plain, */*",
    "Cookie": cookies,
    "Referer": holiday_url,
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
}

holiday_data = fetch_data(holiday_api_url, headers_holidays)
if holiday_data:
    for holiday in holiday_data:
        date_str = holiday['date']
        date_obj = datetime.fromisoformat(date_str[:-6])
        formatted_date = date_obj.strftime('%Y-%m-%d')
        holiday_list.append(formatted_date)

# Fetch SET100 index data
index_url = f'https://www.set.or.th/en/market/index/set100/overview'
cookies = get_cookies(index_url)
# Search compostion?lang=en in network tab to find the API endpoint
index_api_url = f'https://www.set.or.th/api/set/index/set100/composition?lang=en'

headers_index = {
    "Accept": "application/json, text/plain, */*",
    "Cookie": cookies,
    "Referer": index_url,
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
}
index_data = fetch_data(index_api_url, headers_index)
is_holiday = curr_date.strip() in [holiday.strip() for holiday in holiday_list]

if not is_holiday:
    if index_data:
        for stock in index_data.get("composition", {}).get("stockInfos", []):
            try:
                symbol = stock['symbol']
                open_value = stock['open']
                high_value = stock['high']
                low_value = stock['low']
                close_value = stock['last']
                c.execute('''INSERT INTO SET100_OHLC (Date, Symbol, Open_, High, Low, Close_) 
                            VALUES (:0, :1, :2, :3, :4, :5)''',(curr_date, symbol, open_value, high_value, low_value, close_value))
            except Exception as e:
                print(f"Insert failed for {stock['symbol']}: {e}")
    conn.commit()
    c.close()
    conn.close()
    print("Data Inserted")
    
else:
    print(f"Data already exists for {curr_date}")
    c.close()
    conn.close()
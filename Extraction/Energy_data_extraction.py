import os
import time
import logging
import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


# ================================
# CONFIGURATION
# ================================
load_dotenv(dotenv_path="energy.env")

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")

if not all([MONGO_URI, DB_NAME, COLLECTION_NAME]):
    raise ValueError("❌ Missing one or more environment variables. Check your energy.env file!")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

BASE_URL = "https://africa-energy-portal.org/country/"


# ================================
# MONGODB CONNECTION
# ================================
def connect_mongo():
    """Connect to MongoDB Atlas."""
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        logging.info(f"✅ Connected to MongoDB: {DB_NAME}.{COLLECTION_NAME}")
        return collection
    except Exception as e:
        logging.error(f"❌ MongoDB connection failed: {e}")
        raise


# ================================
# SELENIUM SCRAPER
# ================================
class AfricaEnergyScraper:
    def __init__(self):
        self.driver = self._init_driver()
        self.collection = connect_mongo()

    def _init_driver(self):
        """Initialize Chrome WebDriver."""
        logging.info("🚀 Launching Chrome WebDriver...")
        chrome_options = Options()
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        logging.info("✅ Chrome WebDriver launched successfully.")
        return driver

    def get_country_list(self):
        """List of all 54 African countries as per Africa Energy Portal."""
        return [
            "algeria", "angola", "benin", "botswana", "burkina-faso", "burundi",
            "cameroon", "cape-verde", "central-african-republic", "chad", "comoros",
            "congo", "djibouti", "egypt", "equatorial-guinea", "eritrea", "eswatini",
            "ethiopia", "gabon", "gambia", "ghana", "guinea", "guinea-bissau",
            "ivory-coast", "kenya", "lesotho", "liberia", "libya", "madagascar",
            "malawi", "mali", "mauritania", "mauritius", "morocco", "mozambique",
            "namibia", "niger", "nigeria", "rwanda", "sao-tome-and-principe", "senegal",
            "seychelles", "sierra-leone", "somalia", "south-africa", "south-sudan",
            "sudan", "tanzania", "togo", "tunisia", "uganda", "zambia", "zimbabwe"
        ]

    def scrape_country(self, country):
        """Scrape tables for a given country."""
        url = f"{BASE_URL}{country}"
        logging.info(f"🌍 Scraping {country.title()} - {url}")

        try:
            self.driver.get(url)
            time.sleep(6)

            # Skip if 404 page
            if "Page not found" in self.driver.page_source or "404" in self.driver.title:
                logging.warning(f"⚠️ {country.title()} page not found.")
                return pd.DataFrame()

            tables = self.driver.find_elements(By.TAG_NAME, "table")
            all_data = []

            for table in tables:
                try:
                    html = table.get_attribute("outerHTML")
                    df = pd.read_html(html)[0]
                    df["country"] = country.title()
                    all_data.append(df)
                except Exception as parse_err:
                    logging.warning(f"⚠️ Could not parse a table on {country.title()}: {parse_err}")

            if all_data:
                combined = pd.concat(all_data, ignore_index=True)
                logging.info(f"✅ {country.title()}: Extracted {len(combined)} rows.")
                return combined
            else:
                logging.warning(f"⚠️ {country.title()}: No tables found.")
                return pd.DataFrame()

        except Exception as e:
            logging.error(f"❌ Error scraping {country.title()}: {e}")
            return pd.DataFrame()

    def store_to_mongo(self, df):
        """Store scraped data into MongoDB."""
        if df.empty:
            logging.warning("⚠️ No data to store.")
            return

        try:
            records = df.to_dict("records")
            self.collection.insert_many(records)
            logging.info(f"💾 Stored {len(records)} records in MongoDB collection.")
        except Exception as e:
            logging.error(f"❌ MongoDB insert error: {e}")

    def run(self):
        """Run full scrape for all countries."""
        all_results = pd.DataFrame()

        for country in self.get_country_list():
            df = self.scrape_country(country)
            if not df.empty:
                all_results = pd.concat([all_results, df], ignore_index=True)

        if not all_results.empty:
            self.store_to_mongo(all_results)
            all_results.to_csv("energy_data_backup.csv", index=False)
            logging.info("📂 Data saved locally as energy_data_backup.csv")
        else:
            logging.warning("⚠️ No data scraped from any country.")

    def close(self):
        """Close Chrome WebDriver."""
        logging.info("🧹 Closing Chrome WebDriver...")
        self.driver.quit()


# ================================
# MAIN EXECUTION
# ================================
def main():
    logging.info("🚀 Starting Africa Energy Scraper (All 54 Countries)...")
    scraper = AfricaEnergyScraper()

    try:
        scraper.run()
    finally:
        input("🧭 Press Enter to close Chrome after inspection...")
        scraper.close()


if __name__ == "__main__":
    main()

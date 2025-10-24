Here’s a clean, ready-to-use **`README.md`** file for your *African Energy Data Extraction* project:

---

````markdown
# African Energy Data Extraction Project

## Overview
This project automates the extraction of energy-related data for all 54 African countries from the AfricaT Energy Portal, covering the years 2000–2024. The extracted data includes key indicators such as electricity generation, access, consumption, and renewable energy. The data is formatted and stored in a MongoDB collection for easy analysis and visualization.

## Features
- Extracts energy datasets for all African countries.
- Covers multiple metrics across sectors and sub-sectors.
- Includes data for each year from 2000 to 2024.
- Stores structured data in MongoDB.
- Ensures consistent formatting and validation of metrics.

## Requirements
- Python 3.8 or higher  
- Google Chrome and ChromeDriver  
- MongoDB Atlas account  
- `.env` file containing MongoDB credentials  
- Required Python libraries:
  - pandas  
  - pymongo  
  - selenium  
  - python-dotenv  
  - tqdm

## Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/african-energy-data.git
   cd african-energy-data
````

2. Install dependencies:

   ```bash
   pip install pandas pymongo selenium python-dotenv tqdm
   ```

## Environment Setup

Create a file named `.env` in the project directory with the following structure:

```bash
MONGO_URI="your_mongodb_connection_string"
DB_NAME="africa_energy"
COLLECTION_NAME="energy_indicators"
```

## Usage

Run the extraction script:

```bash
python Energy_data_extraction.py
```

## Data Format

Each record in the MongoDB collection follows this structure:

```json
{
  "country": "Kenya",
  "country_serial": 32,
  "metric": "Electricity generation",
  "unit": "GWh",
  "sector": "Power",
  "sub_sector": "Generation",
  "sub_sub_sector": "Renewables",
  "source_link": "https://africatenergyportal.org/...",
  "source": "AfricaT Energy Portal",
  "2000": 4500,
  "2001": 4700,
  "...": "...",
  "2024": 9200
}
```

## Validation

* Confirms presence of data for all years (2000–2024).
* Checks consistency in metric names and units.
* Logs missing or incomplete data for review.

## Author

Developed by **Shadrack Muchemi Karimi**
LuxDev Internship Project – African Energy Data Initiative

```

---

Would you like me to include a short **“License”** or **“Acknowledgments”** section at the end for professional completeness?
```

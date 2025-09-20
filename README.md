# Vahan Dashboard Scraper

This project automates data extraction from the official **Vahan Dashboard** (https://vahan.parivahan.gov.in/vahan4dashboard/).  
It collects **Vehicle Class** and **Vehicle Category** data for all Indian states/UTs across years and months, and saves the output into Excel and CSV files.

---

## ✨ Features
- Scrapes data for **all states and UTs** listed in the Vahan dashboard.
- Captures **Vehicle Class** and **Vehicle Category** tables.
- Extracts **all rows as-is** (no filtering/standardization).
- Includes metadata columns: `State`, `Year`, `Month`, `Table`.
- Parallel scraping with **multithreading (5 workers)** for faster execution.
- Saves results into:
  - `vahan_data_raw.xlsx`
  - `vahan_data_raw.csv`

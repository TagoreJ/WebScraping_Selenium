# final_scraper.py
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import time, os, re
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm   # pip install tqdm

OUTPUT_FILE = "v11.xlsx"
MAX_WORKERS = 5
BASE_URL = "https://vahan.parivahan.gov.in/vahan4dashboard/"

# ---------- helpers ----------
def to_int(val):
    if val is None:
        return None
    s = str(val).strip().replace(",", "")
    s = re.sub(r"[^\d\-]", "", s)   # remove non-digit (keep minus)
    try:
        return int(s) if s != "" else None
    except:
        return None

MONTH_MAP = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12
}

def month_to_date(year:int, month_str:str):
    m = MONTH_MAP.get(month_str.upper()[:3])
    if m:
        return f"{year:04d}-{m:02d}-01"
    return None

# ---------- robust find with retries ----------
def safe_find_elements(driver, by, selector, timeout=8):
    wait = WebDriverWait(driver, timeout)
    try:
        wait.until(EC.presence_of_all_elements_located((by, selector)))
    except:
        pass
    # return whatever is present now (maybe empty)
    return driver.find_elements(by, selector)

def safe_click(driver, element):
    """Click via JS to avoid some overlay issues"""
    driver.execute_script("arguments[0].click();", element)

# ---------- scrape single state ----------
def scrape_state(state_name):
    driver = webdriver.Chrome()
    driver.maximize_window()
    driver.get(BASE_URL)
    wait = WebDriverWait(driver, 20)

    class_data = []
    category_data = []

    try:
        # open Vehicle Registration tab
        vehicle_reg_tab = wait.until(EC.element_to_be_clickable((By.XPATH, "//span[contains(text(),'Vehicle Registration')]")))
        vehicle_reg_tab.click()
        time.sleep(1.0)

        # open state dropdown and pick state
        wait.until(EC.element_to_be_clickable((By.ID, "j_idt44_label"))).click()
        time.sleep(0.8)
        state_items = safe_find_elements(driver, By.XPATH, "//ul[@id='j_idt44_items']/li", timeout=6)
        picked = False
        for item in state_items:
            if item.text.strip().startswith(state_name.split("(")[0].strip()):
                safe_click(driver, item)
                picked = True
                break
        if not picked:
            print(f"⚠️ State not found in dropdown: {state_name}")
            driver.quit()
            return class_data, category_data

        # click refresh
        refresh_btn = wait.until(EC.element_to_be_clickable((By.ID, "j_idt49")))
        safe_click(driver, refresh_btn)
        time.sleep(2.5)

        # collect year links
        year_links = safe_find_elements(driver, By.XPATH, "//a[contains(@id,'j_idt') and contains(text(),':')]", timeout=8)
        years = list({link.text.replace(":", "").strip() for link in year_links})
        years = [y for y in years if y.isdigit()]
        years.sort()

        for year in years:
            # click year link fresh
            try:
                year_link = wait.until(EC.element_to_be_clickable((By.XPATH, f"//a[contains(text(),'{year}:')]")))
                safe_click(driver, year_link)
                # allow UI to update
                time.sleep(1.6)
            except Exception as e:
                print(f"   ❌ Year click failed for {state_name} {year}: {e}")
                continue

            # Month loop: find blocks then index iterate (re-find each iteration to avoid stale refs)
            month_blocks = safe_find_elements(driver, By.XPATH, "//div[contains(@class,'link_month')]", timeout=6)
            for i in range(len(month_blocks)):
                # re-find to avoid stale elements (the page refreshes on each month click)
                try:
                    month_blocks = safe_find_elements(driver, By.XPATH, "//div[contains(@class,'link_month')]", timeout=6)
                    if i >= len(month_blocks):
                        break
                    block = month_blocks[i]

                    # extract month name and total (label might be present or empty)
                    try:
                        a_el = block.find_element(By.TAG_NAME, "a")
                        month_name = a_el.text.strip()
                    except:
                        month_name = ""
                    try:
                        label_el = block.find_element(By.TAG_NAME, "label")
                        month_total_raw = label_el.text.strip()
                    except:
                        month_total_raw = None

                    # click the month link (fresh element)
                    try:
                        # re-find the <a> inside this block and click via JS
                        a_el = block.find_element(By.TAG_NAME, "a")
                        safe_click(driver, a_el)
                    except Exception as e:
                        # try clicking via global xpath using month_name if present
                        try:
                            fallback = driver.find_element(By.XPATH, f"//div[contains(@class,'link_month')]//a[normalize-space(text())='{month_name}']")
                            safe_click(driver, fallback)
                        except:
                            print(f"         ❌ Couldn't click month {month_name} for {state_name}-{year}: {e}")
                            continue

                    # wait for panels to update: wait for panel_vhClass table presence (or small sleep)
                    try:
                        wait.until(EC.presence_of_element_located((By.XPATH, "//div[@id='panel_vhClass']//table")))
                    except:
                        # some months have no rows, give a small pause and continue
                        time.sleep(1.0)

                    # small post-load settle
                    time.sleep(0.8)

                    month_total = to_int(month_total_raw)

                    # --- parse Vehicle Class table robustly preserving group headers ---
                    class_rows = safe_find_elements(driver, By.XPATH, "//div[@id='panel_vhClass']//table//tr", timeout=4)
                    current_group = None
                    for row in class_rows:
                        cols = row.find_elements(By.TAG_NAME, "td")
                        # treat a single-td row (or rows where 2nd cell empty) as a group header
                        if len(cols) == 1:
                            txt = cols[0].text.strip()
                            if txt:
                                current_group = txt
                            continue
                        if len(cols) >= 2:
                            cat = cols[0].text.strip()
                            tot = cols[1].text.strip()
                            class_data.append({
                                "State": state_name,
                                "Year": int(year),
                                "Month": month_name,
                                "Month_Num": MONTH_MAP.get(month_name.upper()[:3]),
                                "Date": month_to_date(int(year), month_name),
                                "Month_Total": month_total,
                                "Group": current_group,
                                "Category": cat,
                                "Total": to_int(tot),
                                "Table": "Vehicle Class"
                            })

                    # --- parse Vehicle Category table robustly preserving group headers ---
                    cat_rows = safe_find_elements(driver, By.XPATH, "//div[@id='panel_vhCatg']//table//tr", timeout=4)
                    current_group = None
                    for row in cat_rows:
                        cols = row.find_elements(By.TAG_NAME, "td")
                        if len(cols) == 1:
                            txt = cols[0].text.strip()
                            if txt:
                                current_group = txt
                            continue
                        if len(cols) >= 2:
                            cat = cols[0].text.strip()
                            tot = cols[1].text.strip()
                            category_data.append({
                                "State": state_name,
                                "Year": int(year),
                                "Month": month_name,
                                "Month_Num": MONTH_MAP.get(month_name.upper()[:3]),
                                "Date": month_to_date(int(year), month_name),
                                "Month_Total": month_total,
                                "Group": current_group,
                                "Category": cat,
                                "Total": to_int(tot),
                                "Table": "Vehicle Category"
                            })

                except Exception as e:
                    print(f"         ❌ Month error in {state_name}({i})-{year}: {e}")
                    # continue with next month

    except Exception as e:
        print(f"❌ State {state_name} failed overall: {e}")

    driver.quit()
    return class_data, category_data

# ---------- gather list of states ----------
def get_state_list():
    driver = webdriver.Chrome()
    driver.get(BASE_URL)
    wait = WebDriverWait(driver, 20)

    vehicle_reg_tab = wait.until(EC.element_to_be_clickable((By.XPATH, "//span[contains(text(),'Vehicle Registration')]")))
    vehicle_reg_tab.click()
    time.sleep(1.0)

    driver.find_element(By.ID, "j_idt44_label").click()
    time.sleep(0.8)

    items = safe_find_elements(driver, By.XPATH, "//ul[@id='j_idt44_items']/li", timeout=6)
    state_names = [it.text.strip() for it in items if "All Vahan4" not in it.text]
    driver.quit()
    return state_names

# ---------- main ----------
def main():
    states = get_state_list()
    print(f"📌 States found: {len(states)}")
    for s in states:
        print("  -", s)

    all_class = []
    all_category = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(scrape_state, st): st for st in states}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Scraping Progress", unit="state"):
            st = futures[future]
            try:
                cls, cat = future.result()
                all_class.extend(cls)
                all_category.extend(cat)
            except Exception as e:
                print(f"⚠️ Error in {st}: {e}")

    # ---------- create per-state sheets and combined ----------
    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        # per-state sheets (2 sheets per state)
        for st in states:
            df_cls = pd.DataFrame([r for r in all_class if r["State"] == st])
            if not df_cls.empty:
                # reorder columns nicely
                df_cls = df_cls[["State","Year","Month","Month_Num","Date","Month_Total","Group","Category","Total"]]
                df_cls.to_excel(writer, sheet_name=f"{st[:24]}_Class", index=False)

            df_cat = pd.DataFrame([r for r in all_category if r["State"] == st])
            if not df_cat.empty:
                df_cat = df_cat[["State","Year","Month","Month_Num","Date","Month_Total","Group","Category","Total"]]
                df_cat.to_excel(writer, sheet_name=f"{st[:24]}_Category", index=False)

        # combined sheet for Power BI
        combined = pd.DataFrame(all_class + all_category)
        if not combined.empty:
            combined = combined[["State","Year","Month","Month_Num","Date","Month_Total","Table","Group","Category","Total"]]
            combined.to_excel(writer, sheet_name="All_Combined", index=False)

    print("✅ Done. Output:", OUTPUT_FILE)

if __name__ == "__main__":
    main()

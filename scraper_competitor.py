# my_scraper_web/scraper_competitor.py
# ============================================================
# Amazon Competitor Scraper (Helium 10 Cerebro) - UPDATED
# - Fixes multi-ASIN issue by hard/soft reset between ASINs
# - Opens the marketplace dropdown automatically
# - Email/Password are optional; read from env/.env automatically
# - Optional persistent Chrome profile to reduce re-logins
# ============================================================

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    ElementClickInterceptedException,
)
from selenium.webdriver.common.action_chains import ActionChains

import time
import pandas as pd
import os
import re
from typing import Tuple, Optional

# Try to load .env if present (non-fatal if package not installed)
def _maybe_load_dotenv():
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except Exception:
        pass

_maybe_load_dotenv()

# ============================================================
# Marketplace Mapping
# ============================================================
marketplace_map = {
    "india": "amazon.in", "in": "amazon.in",
    "us": "amazon.com", "usa": "amazon.com", "america": "amazon.com",
    "uk": "amazon.co.uk", "united kingdom": "amazon.co.uk",
    "de": "amazon.de", "germany": "amazon.de",
    "fr": "amazon.fr", "france": "amazon.fr",
    "it": "amazon.it", "italy": "amazon.it",
    "es": "amazon.es", "spain": "amazon.es",
    "ca": "amazon.ca", "canada": "amazon.ca",
    "mx": "amazon.com.mx", "mexico": "amazon.com.mx",
    "nl": "amazon.nl", "netherlands": "amazon.nl",
    "jp": "amazon.co.jp", "japan": "amazon.co.jp",
    "au": "amazon.com.au", "australia": "amazon.com.au",
    "ae": "amazon.ae", "uae": "amazon.ae",
    "br": "amazon.com.br", "brazil": "amazon.com.br",
    "sa": "amazon.sa", "saudi": "amazon.sa",
}

# ============================================================
# Credentials Loader
# ============================================================
def _load_h10_creds(email: Optional[str], password: Optional[str]) -> Tuple[str, str]:
    """
    Returns Helium10 (email, password). If not supplied, reads from env.
    Supports .env if python-dotenv is installed.
    Required env var names:
      - H10_EMAIL
      - H10_PASSWORD
    """
    e = email or os.getenv("H10_EMAIL")
    p = password or os.getenv("H10_PASSWORD")
    if not e or not p:
        raise RuntimeError(
            "Helium 10 credentials not found. "
            "Set environment variables H10_EMAIL and H10_PASSWORD (or supply email/password)."
        )
    return e, p

# ============================================================
# Helper Functions
# ============================================================
def find_first_non_asin_line(text):
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    for l in lines:
        if re.search(r"\bB0[A-Z0-9]{8}\b", l):
            continue
        if len(l) < 5:
            continue
        if l.lower() in ("copy", "amazon"):
            continue
        return l
    return ""

# ---- NEW: robust wait for competitor results to fully load ----
def _wait_for_competitor_results(driver, asin: str, card_xpath: str, max_wait: int = 120):
    """
    Waits (robustly) until the competitor page for THIS ASIN is fully loaded.
    - Waits for loaders/spinners to disappear (best-effort, generic selectors).
    - Ensures at least one competitor card is present.
    - Ensures DOM is 'stable' for a few consecutive checks (cards count stops changing).
    - Verifies the page context contains the current ASIN (best-effort guard).
    """  
    t0 = time.time()
    last_count = -1
    stable_checks = 0

    js_loading = """
        const sel = [
          '[data-testid="loading"]','[data-testid="spinner"]','[aria-busy="true"]',
          '.loading','.is-loading','.spinner','.progress',
          'div[class*="Loader"]','div[class*="loader"]','div[role="progressbar"]'
        ];
        for (const s of sel) {
            const el = document.querySelector(s);
            if (el && el.offsetParent !== null) return true;
        }
        return false;
    """

    while time.time() - t0 < max_wait:
        try:
            loading = driver.execute_script(js_loading)
            if loading:
                time.sleep(0.6)
                continue
        except Exception:
            pass

        cards = driver.find_elements(By.XPATH, card_xpath)
        count_now = len(cards)

        if count_now > 0:
            if count_now == last_count:
                stable_checks += 1
            else:
                stable_checks = 0
                last_count = count_now

            try:
                has_asin = driver.execute_script(
                    "return (document.body && document.body.innerText || '').indexOf(arguments[0]) !== -1;",
                    asin
                )
            except Exception:
                has_asin = True

            if stable_checks >= 3 and has_asin:
                return

        time.sleep(0.7)

    raise TimeoutException(f"Competitor results did not fully load within {max_wait}s for ASIN {asin}")

# ---- NEW: robust dropdown opener ----
def open_marketplace_dropdown(driver, wait):
    """
    Robustly opens the small marketplace dropdown to the left of the ASIN input.
    Works even when class names are obfuscated and only 'data-open' exists.
    """
    asin_input = wait.until(
        EC.visibility_of_element_located(
            (By.XPATH,
             "//input[contains(@placeholder,'Enter up to 10 product identifiers')]"
             " | //input[contains(@placeholder,'Enter up to 10 product identifiers for keyword comparison')]"
             " | //input[contains(@placeholder,'Enter') and @type='text']"
             )
        )
    )
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", asin_input)

    try:
        if driver.find_elements(By.XPATH, "//div[@role='option' and contains(., 'amazon.')]"):
            return
    except Exception:
        pass

    candidates = []

    for xp in [
        "ancestor::form[1]//span[@data-open]",
        "ancestor::form[1]//div[@data-open]",
        "preceding::span[@data-open][1]",
        "preceding::div[@data-open][1]",
    ]:
        try:
            el = asin_input.find_element(By.XPATH, xp)
            candidates.append(el)
        except Exception:
            pass

    for xp in [
        "ancestor::div[1]//*[(@role='button' or @tabindex='0') and (./*[name()='svg'] or .//img)]",
        "ancestor::form[1]//*[(@role='button' or @tabindex='0') and (./*[name()='svg'] or .//img)]",
    ]:
        try:
            el = asin_input.find_element(By.XPATH, xp)
            candidates.append(el)
        except Exception:
            pass

    if not candidates:
        candidates = driver.find_elements(By.CSS_SELECTOR, "span[data-open], div[data-open]")

    last_error = None
    for cand in candidates:
        try:
            if not cand.is_displayed():
                continue
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", cand)
            ActionChains(driver).move_to_element(cand).pause(0.1).perform()
            try:
                driver.execute_script("arguments[0].click();", cand)
            except ElementClickInterceptedException:
                cand.click()

            try:
                wait.until(lambda d: (cand.get_attribute("data-open") or "").lower() == "true")
            except TimeoutException:
                wait.until(EC.visibility_of_element_located(
                    (By.XPATH, "//div[@role='option' and contains(., 'amazon.')]")
                ))
            return
        except Exception as e:
            last_error = e
            continue

    raise Exception(f"Could not open marketplace dropdown automatically. Last error: {last_error}")

def select_marketplace_by_value(driver, wait, domain_value: str):
    try:
        option = wait.until(EC.presence_of_element_located((
            By.XPATH, f"//div[@role='option' and contains(., 'www.{domain_value}')]"
        )))
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", option)
        time.sleep(0.2)
        option.click()
        print(f"✅ Marketplace selected: www.{domain_value}")
    except TimeoutException:
        raise Exception(f"❌ Marketplace option for 'www.{domain_value}' not found")

# ---- NEW: SOFT RESET between ASINs ----
def _soft_reset_cerebro(driver, wait, verbose=True):
    """
    Tries to return Cerebro to a 'fresh' state without reloading the page.
    - Clears input via Ctrl+A Delete, removes any chips/pills.
    - Clicks Clear/Reset/New Search if present.
    - Scrolls to top and waits for cards to vanish (best effort).
    """
    try:
        # Scroll to top
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(0.2)

        # Locate the ASIN input
        asin_input_box = wait.until(
            EC.presence_of_element_located((By.XPATH,
                "//input[contains(@placeholder,'Enter up to 10 product identifiers')]"
                " | //input[contains(@placeholder,'Enter up to 10 product identifiers for keyword comparison')]"
                " | //input[contains(@placeholder,'Enter') and @type='text']"
            ))
        )
        driver.execute_script("arguments[0].focus();", asin_input_box)
        time.sleep(0.1)

        # Clear using keyboard (more reliable on React inputs)
        asin_input_box.send_keys(Keys.CONTROL + "a")
        asin_input_box.send_keys(Keys.DELETE)
        time.sleep(0.1)

        # Remove chips/pills (if any)
        # Try multiple patterns (close icons, remove buttons, x icons)
        for xp in [
            "//button[contains(@aria-label, 'Remove') or contains(@aria-label, 'remove')]",
            "//div[contains(@class,'chip') or contains(@class,'tag')]//button",
            "//span[contains(@class,'close') or contains(@class,'Clear') or contains(@class,'remove')]/ancestor::button[1]",
            "//button[.//*[name()='svg' and (contains(@class,'close') or contains(@class,'x'))]]",
        ]:
            btns = driver.find_elements(By.XPATH, xp)
            for b in btns:
                try:
                    if b.is_displayed():
                        driver.execute_script("arguments[0].click();", b)
                        time.sleep(0.05)
                except Exception:
                    pass

        # Click Clear/Reset/New Search if visible
        for xp in [
            "//button[normalize-space()='Clear']",
            "//button[normalize-space()='Reset']",
            "//button[contains(., 'New Search')]",
            "//button[contains(@data-testid,'clear')]",
        ]:
            btns = driver.find_elements(By.XPATH, xp)
            for b in btns:
                try:
                    if b.is_displayed():
                        driver.execute_script("arguments[0].click();", b)
                        time.sleep(0.1)
                except Exception:
                    pass

        # Best-effort wait for cards to disappear
        try:
            WebDriverWait(driver, 5).until(
                EC.invisibility_of_element_located((By.XPATH, "//div[contains(@class,'sc-hlqirL') and contains(@class,'sc-bZTyFN')]"))
            )
        except Exception:
            pass

        if verbose:
            print("🧽 Soft reset completed.")
        return True
    except Exception as e:
        if verbose:
            print(f"🧽 Soft reset failed (non-fatal): {e}")
        return False

# ---- NEW: HARD RELOAD (with marketplace re-select) ----
def _hard_reload_cerebro(driver, wait, marketplace_value: str, url: str):
    """
    Fully reloads Cerebro and re-selects marketplace to guarantee a clean state.
    """
    print("🔄 Performing hard reload for a clean state...")
    try:
        nonce = int(time.time() * 1000)
        driver.get(f"{url}&r={nonce}")
    except TimeoutException:
        driver.refresh()

    # Re-open dropdown & re-select marketplace (kept small + robust)
    open_marketplace_dropdown(driver, wait)
    select_marketplace_by_value(driver, wait, marketplace_value)

# ============================================================
# Core Runner
# ============================================================
def run_competitor_scraper(
    email: Optional[str] = None,
    password: Optional[str] = None,
    asin_input: str = "",
    marketplace_input_raw: str = "",
    save_path: str = ".",
    file_name: str = "competitors",
    headless: bool = True,
    persistent_profile: bool = True,
    user_data_dir: Optional[str] = None,
) -> str:
    """
    Runs the Helium10 Cerebro competitor scrape.
    - email/password are OPTIONAL now. If omitted, values are pulled from env (.env supported).
    - optional persistent Chrome profile to reduce repeated logins / 2FA.
    """

    # --- creds (new behavior) ---
    email, password = _load_h10_creds(email, password)

    asin_list = [a.strip() for a in asin_input.replace(",", " ").split() if a.strip()]
    marketplace_input = (marketplace_input_raw or "").strip().lower()
    marketplace_value = marketplace_map.get(marketplace_input, marketplace_input_raw.strip())
    if not marketplace_value:
        raise ValueError("Marketplace is required (e.g., 'us', 'india', 'uk').")

    # --- Chrome Setup ---
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")

    # Persist browser session to avoid frequent logins (optional)
    if persistent_profile:
        profile_dir = user_data_dir or os.path.join(os.path.expanduser("~"), ".h10_chrome_profile")
        os.makedirs(profile_dir, exist_ok=True)
        chrome_options.add_argument(f"--user-data-dir={profile_dir}")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )
    wait = WebDriverWait(driver, 30)

    # ---------------- Login ----------------
    driver.get("https://members.helium10.com/user/signin")
    try:
        try:
            if not headless:
                driver.maximize_window()
        except Exception:
            pass

        if "signin" not in driver.current_url.lower():
            print("🔐 Session appears logged in (via persistent profile).")
        else:
            print("🔑 Logging in with saved credentials...")
            email_input_box = wait.until(EC.visibility_of_element_located((By.ID, "loginform-email")))
            email_input_box.clear()
            email_input_box.send_keys(email)
            time.sleep(0.5)

            password_input_box = wait.until(EC.visibility_of_element_located((By.ID, "loginform-password")))
            password_input_box.clear()
            password_input_box.send_keys(password)
            time.sleep(0.5)

            login_button = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//button[normalize-space(text())='Log In']"))
            )
            driver.execute_script("arguments[0].click();", login_button)

            time.sleep(2)

        if "captcha" in driver.page_source.lower():
            print("⚠️ Captcha detected! Please solve it manually (waiting up to 2 minutes)...")
            WebDriverWait(driver, 120).until(
                EC.presence_of_element_located((By.XPATH, "//div[contains(text(), 'Tools') or contains(text(), 'Dashboard')]"))
            )

        elif "verify" in driver.current_url.lower():
            print("⚠️ Verification (OTP/2FA) detected! Complete it manually (waiting up to 3 minutes)...")
            WebDriverWait(driver, 180).until(
                EC.presence_of_element_located((By.XPATH, "//div[contains(text(), 'Tools') or contains(text(), 'Dashboard')]"))
            )

        wait.until(EC.presence_of_element_located((By.XPATH, "//div[contains(text(), 'Tools') or contains(text(), 'Dashboard')]")))
        safe_email = email[:2] + "***" + email[email.find("@")-1:] if "@" in email and len(email) > 3 else "***"
        print(f"✅ Logged in successfully as {safe_email}")

    except TimeoutException:
        print("❌ Login failed: Page elements not found (check selectors or internet speed)")
        driver.quit()
        raise
    except Exception as e:
        print("❌ Login failed:", e)
        driver.quit()
        raise

    # ---------------- Open Cerebro ----------------
    print("🌐 Opening ...")
    driver.set_page_load_timeout(300)  # Increased timeout to handle slow loads
    cerebro_url = "https://members.helium10.com/cerebro?accountId=1546562595"
    try:
        driver.get(cerebro_url)
    except TimeoutException:
        print("⚠️ Page load timed out, refreshing...")
        driver.refresh()

    competitor_results = []

    # ---------------- Open dropdown & Select Marketplace ----------------
    try:
        print(f"🛒 Opening marketplace dropdown ...")
        open_marketplace_dropdown(driver, wait)
        print(f"🛒 Selecting marketplace: {marketplace_value} ...")
        select_marketplace_by_value(driver, wait, marketplace_value)
    except Exception as e:
        print("❌ Marketplace selection failed:", e)
        driver.quit()
        raise

    # ---------------- Scrape Competitors ----------------
    CARD_XPATH = "//div[contains(@class,'sc-hlqirL') and contains(@class,'sc-bZTyFN')]"

    for idx, asin in enumerate(asin_list, start=1):
        print(f"\n==============================")
        print(f"🔎 [{idx}/{len(asin_list)}] Searching for {asin}...")

        # Always start each iteration from a clean state
        soft_ok = _soft_reset_cerebro(driver, wait, verbose=(idx == 1 or True))

        if not soft_ok:
            _hard_reload_cerebro(driver, wait, marketplace_value, cerebro_url)

        # Re-locate input fresh every loop
        asin_input_box = wait.until(EC.presence_of_element_located((By.XPATH,
            "//input[contains(@placeholder,'Enter up to 10 product identifiers')]"
            " | //input[contains(@placeholder,'Enter up to 10 product identifiers for keyword comparison')]"
            " | //input[contains(@placeholder,'Enter') and @type='text']"
        )))
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", asin_input_box)
        driver.execute_script("arguments[0].focus();", asin_input_box)
        time.sleep(0.1)

        # Enter ASIN (use keyboard so React sees it) + press Enter
        asin_input_box.send_keys(asin)
        time.sleep(0.15)
        asin_input_box.send_keys(Keys.RETURN)
        time.sleep(0.6)

        # Click "Get Competitors" (re-find each time to avoid stale handles)
        try:
            get_competitors_button = wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button[data-testid='getcompetitors']"))
            )
            driver.execute_script("arguments[0].scrollIntoView(true);", get_competitors_button)
            driver.execute_script("arguments[0].click();", get_competitors_button)
            print("✅ Clicked Get Competitors")
        except TimeoutException:
            print(f"⚠️ Get Competitors button not found for {asin}, attempting HARD reload and retry once...")
            _hard_reload_cerebro(driver, wait, marketplace_value, cerebro_url)
            try:
                # Re-type and retry after reload
                asin_input_box = wait.until(EC.presence_of_element_located((By.XPATH,
                    "//input[contains(@placeholder,'Enter up to 10 product identifiers')]"
                    " | //input[contains(@placeholder,'Enter up to 10 product identifiers for keyword comparison')]"
                    " | //input[contains(@placeholder,'Enter') and @type='text']"
                )))
                asin_input_box.send_keys(asin)
                asin_input_box.send_keys(Keys.RETURN)
                get_competitors_button = wait.until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "button[data-testid='getcompetitors']"))
                )
                driver.execute_script("arguments[0].click();", get_competitors_button)
                print("✅ Clicked Get Competitors (after reload)")
            except Exception:
                print(f"❌ Could not click Get Competitors for {asin}, skipping...")
                continue

        # ---- Wait until competitor page fully loads for THIS ASIN ----
        try:
            _wait_for_competitor_results(driver, asin, CARD_XPATH, max_wait=120)
            print("✅ Competitor page fully loaded (stable) and context matches ASIN.")
        except TimeoutException as _e:
            # If we timed out, check if page still shows previous ASIN -> hard reload + retry once
            body_has_current = False
            try:
                body_has_current = driver.execute_script(
                    "return (document.body && document.body.innerText || '').indexOf(arguments[0]) !== -1;", asin
                )
            except Exception:
                pass

            if not body_has_current:
                print("⚠️ Page context didn't switch to current ASIN. Hard reloading and retrying once...")
                _hard_reload_cerebro(driver, wait, marketplace_value, cerebro_url)
                # retry enter + click + wait once
                try:
                    asin_input_box = wait.until(EC.presence_of_element_located((By.XPATH,
                        "//input[contains(@placeholder,'Enter up to 10 product identifiers')]"
                        " | //input[contains(@placeholder,'Enter up to 10 product identifiers for keyword comparison')]"
                        " | //input[contains(@placeholder,'Enter') and @type='text']"
                    )))
                    asin_input_box.send_keys(asin)
                    asin_input_box.send_keys(Keys.RETURN)
                    get_competitors_button = wait.until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, "button[data-testid='getcompetitors']"))
                    )
                    driver.execute_script("arguments[0].click();", get_competitors_button)
                    _wait_for_competitor_results(driver, asin, CARD_XPATH, max_wait=120)
                    print("✅ Loaded after one hard-reload retry.")
                except Exception as e:
                    print(f"❌ Second attempt failed for {asin}: {e}. Skipping...")
                    continue
            else:
                print(f"⚠️ Timed out but page contains ASIN {asin}. Proceeding to scrape best-effort...")

        # Ensure at least one card is present (best effort)
        try:
            wait.until(EC.presence_of_element_located((By.XPATH, CARD_XPATH)))
        except TimeoutException:
            print("⚠️ No competitor cards loaded yet.")
        time.sleep(0.8)

        # -------- Reveal ALL cards (lazy loader) --------
        max_stall_loops = 10
        prev_count = -1
        stall_loops = 0
        while True:
            current_cards = driver.find_elements(By.XPATH, CARD_XPATH)
            count_now = len(current_cards)

            if count_now == 0:
                time.sleep(0.6)
                continue

            if count_now == prev_count:
                stall_loops += 1
            else:
                stall_loops = 0
                prev_count = count_now

            try:
                driver.execute_script("arguments[0].scrollIntoView({block:'end'});", current_cards[-1])
                driver.execute_script("window.scrollBy(0, 250);")
            except Exception:
                pass

            time.sleep(1.0)
            if stall_loops >= max_stall_loops:
                break

        # Fetch cards
        try:
            competitor_cards = wait.until(EC.presence_of_all_elements_located((By.XPATH, CARD_XPATH)))
        except TimeoutException:
            competitor_cards = []

        print(f"ℹ️ Found {len(competitor_cards)} card(s) for {asin})")

        # Scrape each card
        for card in competitor_cards:
            try:
                ActionChains(driver).move_to_element(card).perform()
                time.sleep(0.15)
            except Exception:
                pass

            title, competitor_asin, product_link, marketplace_flag = "", "", "", ""

            # Title
            try:
                title_elem = card.find_element(By.XPATH, ".//div[contains(@class,'sc-eAuMPQ')]")
                title = title_elem.text.strip()
            except Exception:
                title = find_first_non_asin_line(card.text)

            # Competitor ASIN
            try:
                asin_elem = card.find_element(By.XPATH, ".//span[contains(@class,'sc-MHjuz')]")
                competitor_asin = asin_elem.text.strip()
            except Exception:
                m = re.search(r"\b(B0[A-Z0-9]{8})\b", card.text)
                if m:
                    competitor_asin = m.group(1)

            # Product Link
            try:
                link_elem = card.find_element(By.XPATH, ".//a[contains(@class,'sc-hbaYEB')]")
                product_link = link_elem.get_attribute("href").strip()
            except Exception:
                if competitor_asin and marketplace_value:
                    product_link = f"https://{marketplace_value}/dp/{competitor_asin}"

            # Marketplace Flag
            try:
                marketplace_elem = card.find_element(By.XPATH, ".//span[contains(@class,'sc-hiTDLB')]")
                marketplace_flag = marketplace_elem.text.strip()
            except Exception:
                marketplace_flag = ""

            competitor_results.append({
                "Search_ASIN": asin,
                "Competitor_ASIN": competitor_asin,
                "Title": title,
                "Marketplace": marketplace_flag,
                "Product_Link": product_link
            })

        print(f"✅ Scraped {len(competitor_cards)} competitor(s) for {asin}")

        # Post-scrape: proactively clear to reduce bleed-over to next loop
        _soft_reset_cerebro(driver, wait, verbose=False)

    # ---------------- Save Results ----------------
    os.makedirs(save_path or ".", exist_ok=True)
    out_file = os.path.join(save_path, f"{file_name}.xlsx")

    if competitor_results:
        pd.DataFrame(competitor_results).to_excel(out_file, sheet_name="Competitors", index=False)
        print(f"\n✅ Competitors sheet saved ({len(competitor_results)} rows) -> {out_file}")
    else:
        print("\nℹ️ No competitor rows; wrote header-only 'Competitors' sheet")
        pd.DataFrame(
            columns=["Search_ASIN", "Competitor_ASIN", "Title", "Marketplace", "Product_Link"]
        ).to_excel(out_file, sheet_name="Competitors", index=False)

    driver.quit()
    print("\n🏁 Scraping completed")
    return out_file

# my_scraper_web/scraper_products.py
import requests
from bs4 import BeautifulSoup, Tag
import pandas as pd
import os
import re
import time
import random
from typing import Optional, List, Dict, Tuple, Any

# --- Marketplaces ---
MARKETPLACES = {
    "Amazon India": "https://www.amazon.in/dp/",
    "Amazon USA": "https://www.amazon.com/dp/",
    "Amazon SG": "https://www.amazon.sg/dp/",
    "Amazon AE": "https://www.amazon.ae/dp/",
    "Amazon UK": "https://www.amazon.co.uk/dp/"
}

# --- Marketplace aliases (case-insensitive) ---
_MARKETPLACE_ALIASES = {
    # India
    "india": "Amazon India", "in": "Amazon India",
    # United States
    "us": "Amazon USA", "usa": "Amazon USA", "united states": "Amazon USA", "america": "Amazon USA",
    # Singapore
    "sg": "Amazon SG", "singapore": "Amazon SG",
    # United Arab Emirates
    "ae": "Amazon AE", "uae": "Amazon AE", "united arab emirates": "Amazon AE",
    # United Kingdom
    "uk": "Amazon UK", "gb": "Amazon UK", "great britain": "Amazon UK", "united kingdom": "Amazon UK",
}

# --- Allowed product info fields ---
ALLOWED_ITEM_DETAILS = [
    "Brand", "Model Number", "Country of Origin",
    "Customer Reviews", "Best Sellers Rank", "Manufacturer", "Packer"
]

# --- Request headers (updated to match real browser traffic) ---
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
]

def get_random_headers(referer: str = "https://www.amazon.in/") -> Dict[str, str]:
    """Generate random headers to avoid detection"""
    ua = random.choice(USER_AGENTS)
    return {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
        "sec-ch-ua": "\"Google Chrome\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
    }

HEADERS = get_random_headers()

# Create a session for persistent cookies
session = requests.Session()
session.headers.update(HEADERS)

def _clean_text(s: str) -> str:
    if not s:
        return "N/A"
    s = " ".join(s.split())
    return s.strip()

def _clean_seller_name(raw: str) -> str:
    if not raw:
        return "N/A"
    s = raw.strip()
    s = re.sub(r"\d+(\.\d+)?\s*out of\s*5", "", s, flags=re.I)
    s = re.sub(r"\(\s*[\d,]+\s*(ratings|rating)?\s*\)", "", s, flags=re.I)
    s = re.sub(r"\d+%.*", "", s)
    s = re.sub(r"Sold by\s*[:\-–]?", "", s, flags=re.I)
    s = re.sub(r"Ships from\s*[:\-–]?", "", s, flags=re.I)
    s = " ".join(s.split())
    return s.strip() or "N/A"

def _extract_label_from_text(block_text, label):
    if not block_text:
        return None
    lt = block_text
    idx = lt.lower().find(label.lower())
    if idx == -1:
        return None
    remainder = lt[idx + len(label):].strip()
    stoppers = ["Ships from", "Sold by", "Delivery", "Condition", "Add to Cart", "Add to Basket", "Buy Now", "Details"]
    stop_positions = []
    for s in stoppers:
        pos = remainder.lower().find(s.lower())
        if pos != -1:
            stop_positions.append(pos)
    if stop_positions:
        end = min(stop_positions)
        candidate = remainder[:end].strip()
    else:
        candidate = remainder[:200].strip()
    candidate = re.sub(r"\d+(\.\d+)?\s*out of\s*5", "", candidate, flags=re.I)
    candidate = re.sub(r"\(\s*[\d,]+\s*(ratings|rating)?\s*\)", "", candidate, flags=re.I)
    candidate = re.sub(r"\d+%.*", "", candidate, flags=re.I)
    candidate = re.sub(r"Details$", "", candidate, flags=re.I)
    candidate = " ".join(candidate.split())
    return candidate.strip() or None

def _find_offer_price(block, price_block=None):
    candidates = []
    if price_block is not None:
        candidates.append(price_block)
    candidates.append(block)
    for pb in candidates:
        el = pb.select_one("span.a-price > span.a-offscreen")
        if el and el.get_text(strip=True):
            return _clean_text(el.get_text())
        el = pb.select_one("span.a-offscreen")
        if el and el.get_text(strip=True):
            return _clean_text(el.get_text())
        whole = pb.select_one("span.a-price-whole")
        if whole and whole.get_text(strip=True):
            fraction = pb.select_one("span.a-price-fraction")
            symbol = pb.select_one("span.a-price-symbol")
            whole_txt = whole.get_text(strip=True)
            frac_txt = fraction.get_text(strip=True) if fraction else ""
            sym_txt = symbol.get_text(strip=True) if symbol else ""
            return _clean_text(f"{sym_txt}{whole_txt}{frac_txt}")
        el = pb.select_one(".offer-price, .price, .a-color-price")
        if el and el.get_text(strip=True):
            m = re.search(r"(₹\s?[\d,]+(?:\.\d+)?|\$\s?[\d,]+(?:\.\d+)?|£\s?[\d,]+(?:\.\d+)?)", el.get_text(" ", strip=True))
            if m:
                return _clean_text(m.group(0))
        fulltext = (price_block.get_text(" ", strip=True) if price_block is not None else block.get_text(" ", strip=True))
        m = re.search(r"(₹\s?[\d,]+(?:\.\d+)?|\$\s?[\d,]+(?:\.\d+)?|£\s?[\d,]+(?:\.\d+)?)", fulltext)
        if m:
            return _clean_text(m.group(0))
    return "N/A"

# ------------------ Availability (NEW, focused change) ------------------
def _extract_availability_exact(soup: BeautifulSoup) -> Optional[str]:
    """
    Extract the exact availability phrase from reliable Amazon nodes.
    Returns one of:
      - 'In stock'
      - 'Only X left in stock'
      - 'Out of stock'
      - 'Currently unavailable'
    or None if not found.
    """
    selectors = [
        "#availabilityInsideBuyBox_feature_div span",  # buybox variant
        "#availability span",                          # classic availability node
        "#availabilityInsideBuyBox_feature_div",       # container fallback
        "#availability",
    ]
    patterns = [
        r"currently unavailable",
        r"out of stock",
        r"only\s+\d+\s+left(?:\s+in\s+stock)?",
        r"in stock",
    ]

    def _match_phrase(src_text: str) -> Optional[str]:
        if not src_text:
            return None
        text = " ".join(src_text.split())
        low = text.lower()
        for pat in patterns:
            m = re.search(pat, low, flags=re.I)
            if m:
                s, e = m.span()
                phrase = text[s:e].strip()
                phrase = re.sub(r"[.\s]+$", "", phrase)  # drop trailing "." etc
                if re.search(r"only\s+\d+\s+left", phrase, re.I):
                    num = re.search(r"\d+", phrase).group(0)
                    return f"Only {num} left in stock"
                if re.search(r"currently unavailable", phrase, re.I):
                    return "Currently unavailable"
                if re.search(r"out of stock", phrase, re.I):
                    return "Out of stock"
                if re.search(r"in stock", phrase, re.I):
                    return "In stock"
                return phrase
        return None

    # Structured nodes first
    for sel in selectors:
        el = soup.select_one(sel)
        if not el:
            continue
        phrase = _match_phrase(el.get_text(" ", strip=True))
        if phrase:
            return phrase

    # Last resort: search whole page text
    page_text = soup.get_text(" ", strip=True)
    return _match_phrase(page_text)
# -----------------------------------------------------------------------

# ------------------ Offer Price (NEW, focused change) ------------------
def _extract_offer_price_exact(soup: BeautifulSoup) -> Optional[str]:
    """
    Extract the exact 'price to pay' shown on the product page, handling cases
    where <span class="a-offscreen"> is empty and the visible price is inside
    a span with aria-hidden="true"> containing a-price-symbol/whole/fraction.
    """
    containers = [
        "#corePriceDisplay_desktop_feature_div",
        "#corePrice_feature_div",
        "#apex_desktop",
        ".reinventPricePriceToPayMargin",
        ".a-section.a-spacing-none.aok-align-center.aok-relative",
    ]

    # 1) Prefer the explicit 'a-offscreen' node if it contains a value
    for sel in containers:
        c = soup.select_one(sel)
        if not c:
            continue
        off = c.select_one("span.a-price > span.a-offscreen")
        if off and off.get_text(strip=True):
            return _clean_text(off.get_text())

    off_global = soup.select_one("span.a-price > span.a-offscreen")
    if off_global and off_global.get_text(strip=True):
        return _clean_text(off_global.get_text())

    # 2) Compose from the aria-hidden block (the pattern in your screenshot)
    def compose_from(root):
        block = root.select_one("span[aria-hidden='true']") or root
        whole = block.select_one("span.a-price-whole")
        symbol = block.select_one("span.a-price-symbol")
        fraction = block.select_one("span.a-price-fraction")
        if whole and whole.get_text(strip=True):
            sym = (symbol.get_text(strip=True) if symbol else "")
            wh = whole.get_text(strip=True)
            fr = (fraction.get_text(strip=True) if fraction else "")
            # Amazon omits decimals for INR/JPY often; if fraction exists, join with '.'
            return _clean_text(f"{sym}{wh}{('.' + fr) if fr else ''}")
        return None

    for sel in containers:
        c = soup.select_one(sel)
        if not c:
            continue
        val = compose_from(c)
        if val:
            return val

    # 3) Global fallback: any visible price structure on the page
    val = compose_from(soup)
    if val:
        return val

    # 4) Text regex fallback
    text = soup.get_text(" ", strip=True)
    m = re.search(r"(₹\s?[\d,]+(?:\.\d+)?|\$\s?[\d,]+(?:\.\d+)?|£\s?[\d,]+(?:\.\d+)?)", text)
    if m:
        return _clean_text(m.group(0))

    return None
# ----------------------------------------------------------------------

def scrape_buybox_offers(asin, base_url, marketplace_name):
    offers = []
    domain = base_url.split("/dp/")[0].rstrip("/")
    ajax_url = f"{domain}/gp/aod/ajax/ref=dp_aod_NEW_mbc?asin={asin}"
    ajax_headers = get_random_headers(referer=f"{base_url}{asin}")
    ajax_headers["X-Requested-With"] = "XMLHttpRequest"
    time.sleep(random.uniform(1, 3))
    try:
        resp = session.get(ajax_url, headers=ajax_headers, timeout=15)
    except requests.RequestException:
        return offers
    if resp.status_code != 200 or not resp.text:
        return offers
    soup = BeautifulSoup(resp.text, "html.parser")
    offer_blocks = soup.select("div.a-section.a-spacing-none.a-padding-base.aod-information-block.aod-clear-float")
    for block in offer_blocks:
        bt = _clean_text(block.get_text(" ", strip=True))
        seller_price = _find_offer_price(block)
        discount_val = "NO %"
        disc_block = block.select_one("span.a-size-medium.a-color-price.aok-align-center.centralizedApexPriceSavingsPercentageMargin.centralizedApexPriceSavingsOverrides")
        if disc_block:
            txt = disc_block.get_text(strip=True)
            m = re.search(r"-?\d{1,3}\s*%", txt)
            if m:
                discount_val = m.group(0)
        el_mrp = block.select_one("span.a-text-price, .aod-mrp")
        mrp_val = _clean_text(el_mrp.get_text()) if el_mrp else "N/A"
        sold_elem = block.select_one("[id^='aod-offer-soldBy']")
        sold_by = _clean_text(sold_elem.get_text()) if sold_elem else _extract_label_from_text(bt, "Sold by")
        sold_by = _clean_seller_name(sold_by) if sold_by else "N/A"
        ships_elem = block.select_one("[id^='aod-offer-shipsFrom']")
        ships_from = _clean_text(ships_elem.get_text()) if ships_elem else _extract_label_from_text(bt, "Ships from")
        ships_from = _clean_seller_name(ships_from) if ships_from else "N/A"
        delivery_info = _extract_label_from_text(bt, "Delivery") or "N/A"
        condition = "N/A"
        for cand in ["New", "Used", "Refurbished", "Renewed", "Collectible"]:
            if re.search(rf"\b{cand}\b", bt, re.I):
                condition = cand
                break
        rating_val = "N/A"
        rating_block = block.select_one("div#aod-offer-seller-rating")
        if rating_block:
            stars_el = rating_block.select_one("i.a-icon-star-mini")
            if stars_el:
                stars_txt = stars_el.get("aria-label") or stars_el.get_text(strip=True)
            else:
                stars_txt = ""
            count_txt = rating_block.get_text(" ", strip=True)
            m = re.search(r"(\d(\.\d)? out of 5 stars \([\d,]+ ratings\))", count_txt)
            if m:
                rating_val = m.group(1)
            elif stars_txt:
                rating_val = stars_txt
        perf_elem = block.find(string=re.compile(r"\d+% positive", re.I))
        seller_perf = _clean_text(perf_elem) if perf_elem else "N/A"
        offers.append({
            "ASIN": asin,
            "Marketplace": marketplace_name,
            "Offer Price": seller_price or "N/A",
            "Discount %": discount_val,
            "MRP": mrp_val or "N/A",
            "Ships From": ships_from or "N/A",
            "Sold By": sold_by or "N/A",
            "Delivery Info": delivery_info or "N/A",
            "Condition": condition,
            "Rating": rating_val,
            "Seller Performance": seller_perf
        })
    return offers

def scrape_asin_from_url(asin, base_url, marketplace_name):
    url = f"{base_url}{asin}"
    try:
        time.sleep(10.0)
        # Rotate headers to avoid detection
        headers = get_random_headers(referer=base_url.rstrip("/"))
        response = requests.get(url, headers=headers, timeout=15)
    except requests.RequestException:
        return None, []
    if response.status_code != 200:
        return None, []
    soup = BeautifulSoup(response.text, 'html.parser')
    title_el = soup.find("span", {"id": "productTitle"})
    if not title_el:
        return None, []
    title = _clean_text(title_el.get_text())
    rating_value = None
    total_reviews = None
    avg_block = soup.find(id="averageCustomerReviews_feature_div")
    if avg_block:
        stars_el = avg_block.select_one("span.a-icon-alt")
        rating_value = _clean_text(stars_el.get_text()) if stars_el else None
        total_el = avg_block.select_one("#acrCustomerReviewText")
        if total_el and total_el.get_text(strip=True):
            total_reviews = _clean_text(total_el.get_text())
        else:
            txt = avg_block.get_text(" ", strip=True)
            m = re.search(r"([\d,]+)\s*(ratings|rating)\b", txt, flags=re.I)
            if m:
                total_reviews = m.group(0)
    else:
        stars_el = soup.select_one("span.a-icon-alt")
        rating_value = _clean_text(stars_el.get_text()) if stars_el else None
        total_el = soup.find("span", {"id": "acrCustomerReviewText"})
        total_reviews = _clean_text(total_el.get_text()) if total_el else None

    # --- Social Bought ---
    social_bought = "N/A"
    el_social = soup.find(id="social-proofing-faceout-title-tk_bought")
    if not el_social:
        el_social = soup.find(id=re.compile(r"social-proofing-faceout-title.*bought", re.I))
    if el_social and el_social.get_text(strip=True):
        social_bought = _clean_text(el_social.get_text(" ", strip=True))

    # --- Price per Unit ---
    price_per_unit = "N/A"
    ppu_el = soup.select_one(".pricePerUnit")
    if not ppu_el:
        ppu_el = soup.find("span", class_=re.compile(r"pricePerUnit", re.I))
    if ppu_el and ppu_el.get_text(strip=True):
        price_per_unit = _clean_text(ppu_el.get_text(" ", strip=True))

    # --- Discount % (robust across variants) ---
    product_discount = "NO %"

    disc_candidates = soup.select(
        "span.savingsPercentage, "
        "span.reinventPriceSavingsPercentageMargin.savingsPercentage, "
        "span.centralizedApexPriceSavingsPercentageMargin.centralizedApexPriceSavingsOverrides"
    )

    if not disc_candidates:
        disc_container = soup.select_one("div.a-section.a-spacing-none.aok-align-center.aok-relative")
        if disc_container:
            disc_candidates = disc_container.select(
                "span.savingsPercentage, "
                "span.reinventPriceSavingsPercentageMargin.savingsPercentage"
            )

    for el in disc_candidates or []:
        txt = el.get_text(" ", strip=True)
        m = re.search(r"(-?\d{1,3})\s*%", txt)
        if m:
            num = m.group(1)
            product_discount = f"-{num}%" if not num.startswith("-") else f"{num}%"
            break

    if product_discount == "NO %":
        off = soup.select_one("span.aok-offscreen")
        if off:
            m = re.search(r"(-?\d{1,3})\s*%(\s*savings)?", off.get_text(" ", strip=True), flags=re.I)
            if m:
                num = m.group(1)
                product_discount = f"-{num}%" if not num.startswith("-") else f"{num}%"

    # --- NEW: Badge texts ---

    # Amazon's Choice
    amazons_choice_text = "unavailable"
    ac_container = soup.select_one("span.aok-float-left.mvt-ac-badge-rectangle")
    if ac_container:
        inner_text = ac_container.get_text(" ", strip=True)
        if re.search(r"Amazon[’']s\s+Choice", inner_text, re.I):
            amazons_choice_text = "Amazon's Choice"

    # Deal badge text
    deal_badge_text = "unavailable"
    db_el = soup.find(id="dealBadgeSupportingText")
    if db_el:
        txt = (db_el.get("aria-label") or db_el.get_text(" ", strip=True) or "").strip()
        if txt:
            deal_badge_text = txt

    # #1 Best Seller
    best_seller_text = "unavailable"
    bs_container = soup.select_one("span.a-size-small.a-color-inverse[aria-label*='Best Seller']")
    if bs_container:
        inner_text = bs_container.get_text(" ", strip=True)
        if re.search(r"#\s*1\s+Best\s+Seller", inner_text, re.I):
            best_seller_text = "#1 Best Seller"

    mrp = None
    mrp_el = soup.find("span", {"class": "a-text-price"})
    if mrp_el:
        mrp = _clean_text(mrp_el.get_text().replace("M.R.P.:", "").strip())
    else:
        mrp_el2 = soup.select_one(".priceBlockStrikePriceString, .a-size-base.a-color-secondary.a-text-strike")
        if mrp_el2:
            mrp = _clean_text(mrp_el2.get_text())

    # --- Item details ---
    item_details = {}
    prod_info = soup.find("div", {"id": "prodDetails"})
    if not prod_info:
        prod_info = soup.find("table", {"id": "productDetails_techSpec_section_1"})
    if prod_info:
        rows = prod_info.find_all("tr")
        for row in rows:
            cols = row.find_all(["th", "td"])
            if len(cols) == 2:
                key = cols[0].get_text(strip=True)
                value = cols[1].get_text(strip=True)
                if key in ALLOWED_ITEM_DETAILS:
                    item_details[key] = value

    brand = item_details.get("Brand", "N/A")
    if brand == "N/A":
        overview = soup.find("div", {"id": "productOverview_feature_div"})
        if overview:
            rows = overview.find_all("tr")
            for row in rows:
                cols = row.find_all("td")
                if len(cols) == 2 and "Brand" in cols[0].get_text(strip=True):
                    brand = cols[1].get_text(strip=True)
                    break
    if brand == "N/A":
        bullets = soup.find("div", {"id": "detailBullets_feature_div"})
        if bullets:
            for li in bullets.find_all("span", {"class": "a-text-bold"}):
                if "Brand" in li.get_text():
                    nxt = li.find_next("span")
                    if nxt:
                        brand = nxt.get_text(strip=True)
                        break
    if brand == "N/A":
        byline = soup.find("a", {"id": "bylineInfo"})
        if byline:
            brand = byline.get_text(strip=True)
    item_details["Brand"] = brand

    # ---------- BOX DATA ----------
    box_data = {"Availability": "N/A", "Delivery info": "N/A", "Ships from": "N/A", "Sold by": "N/A", "Gift options": "Not available"}

    # Precise availability first
    precise_availability = _extract_availability_exact(soup)
    if precise_availability:
        box_data["Availability"] = precise_availability

    # Existing buybox parsing (kept as-is, only fills if still N/A)
    buybox = soup.find("div", {"id": "desktop_qualifiedBuyBox"}) or soup.find("div", {"id": "buybox"})
    if buybox:
        text = _clean_text(buybox.get_text(" ", strip=True))
        if box_data["Availability"] in ("N/A", "", None):
            if re.search(r"\bIn Stock\b", text, re.IGNORECASE):
                box_data["Availability"] = "In stock"
            elif re.search(r"\bOut of Stock\b", text, re.IGNORECASE):
                box_data["Availability"] = "Out of stock"
        a_box = buybox.select_one("div.a-box-inner")
        if a_box:
            try:
                ships_label = a_box.find(string=re.compile(r"\bShips from\b", re.I))
                if ships_label:
                    val = ships_label.parent.find_next_sibling()
                    if val and val.get_text(strip=True):
                        box_data["Ships from"] = _clean_seller_name(val.get_text())
                    else:
                        found = _extract_label_from_text(a_box.get_text(" ", strip=True), "Ships from")
                        if found:
                            box_data["Ships from"] = _clean_seller_name(found)
            except Exception:
                pass
            try:
                sold_label = a_box.find(string=re.compile(r"\bSold by\b", re.I))
                if sold_label:
                    val = sold_label.parent.find_next_sibling()
                    if val and val.get_text(strip=True):
                        box_data["Sold by"] = _clean_seller_name(val.get_text())
                    else:
                        found = _extract_label_from_text(a_box.get_text(" ", strip=True), "Sold by")
                        if found:
                            box_data["Sold by"] = _clean_seller_name(found)
            except Exception:
                pass
        if box_data["Ships from"] in ("N/A", ""):
            found = _extract_label_from_text(text, "Ships from")
            if found:
                box_data["Ships from"] = _clean_seller_name(found)
        if box_data["Sold by"] in ("N/A", "", None):
            found = _extract_label_from_text(text, "Sold by")
            if found:
                box_data["Sold by"] = _clean_seller_name(found)
        delivery_box = soup.find("div", {"id": "mir-layout-DELIVERY_BLOCK-slot-PRIMARY_DELIVERY_MESSAGE_LARGE"})
        if delivery_box:
            delivery_text = _clean_text(delivery_box.get_text(" ", strip=True))
            delivery_text = delivery_text.replace("Details", "").replace("Update location", "").strip()
            box_data["Delivery info"] = delivery_text
        gift_label = buybox.find(string=re.compile(r"Gift options", re.I))
        if gift_label:
            box_data["Gift options"] = "Available"

    # -------------------- OFFER PRICE (replaced with new extractor) --------------------
    offer_price = _extract_offer_price_exact(soup)
    # ----------------------------------------------------------------------------------

    mrp_val = mrp if mrp else "N/A"

    product_data = {
        "ASIN": asin,
        "Marketplace": marketplace_name,
        "Product Title": title,
        "Rating": rating_value or "N/A",
        "Total Reviews": total_reviews or "N/A",
        "Social Bought": social_bought,
        # --- NEW badge fields (always present) ---
        "Amazons_Choice_Text": amazons_choice_text,
        "Deal_Badge_Text": deal_badge_text,
        "Best_Seller_Text": best_seller_text,
        # -----------------------------------------
        "Discount %": product_discount,
        "Offer Price": offer_price or "N/A",
        "Price per Unit": price_per_unit,
        "MRP": mrp_val
    }

    for col in ALLOWED_ITEM_DETAILS:
        product_data[col] = item_details.get(col, "N/A")
    product_data.update(box_data)

    buybox_sellers = scrape_buybox_offers(asin, base_url, marketplace_name)
    return product_data, buybox_sellers

def _resolve_marketplace(user_input: Optional[str]) -> Optional[tuple]:
    """
    Map the user-provided marketplace string to (marketplace_name, base_url).
    Returns None if input is None or unrecognized.
    """
    if not user_input:
        return None
    key = user_input.strip().lower()
    mp_name = _MARKETPLACE_ALIASES.get(key)
    if not mp_name:
        return None
    base_url = MARKETPLACES.get(mp_name)
    if not base_url:
        return None
    return mp_name, base_url

def run_products_scraper(asin_input: str, save_path: str, file_name: str, marketplace: Optional[str] = None) -> str:
    """
    If 'marketplace' is provided (e.g., 'india', 'us', 'sg', 'ae', 'uk'), search only there;
    otherwise iterate through all MARKETPLACES in order.
    """
    asin_list = [a.strip() for a in asin_input.replace(",", " ").split() if a.strip()]
    all_main = []
    all_buybox = []
    MAX_RETRIES = 5

    resolved = _resolve_marketplace(marketplace)
    if resolved:
        specific_marketplaces = [resolved]
        print(f"🌐 Using selected marketplace: {resolved[0]} ({resolved[1]})")
    else:
        specific_marketplaces = list(MARKETPLACES.items())
        if marketplace:
            print(f"⚠️ Unrecognized marketplace '{marketplace}'. Proceeding with all marketplaces in default order.")

    for asin in asin_list:
        print(f"\n🔎 Searching for {asin} ...")
        asin_found = False

        for marketplace_name, base_url in specific_marketplaces:
            data = None
            sellers = []
            retries = 0
        while retries < MAX_RETRIES and data is None:
            try:
                data, sellers = scrape_asin_from_url(asin, base_url, marketplace_name)
            except Exception as e:
                print(f"⚠️ Error scraping {asin} in {marketplace_name}: {str(e)}")
                data = None
                sellers = []
            if data:
                print(f"✅ Found {asin} in {marketplace_name}")
                all_main.append(data)
                if sellers:
                    all_buybox.extend(sellers)
                    print(f"📌 Found {len(sellers)} sellers in {marketplace_name}")
                asin_found = True
                break
            else:
                retries += 1
                if retries < MAX_RETRIES:
                    print(f"❌ Not found {asin} in {marketplace_name}, retry {retries}/{MAX_RETRIES} ...")
                    time.sleep(3)
            if asin_found:
                break

        if not asin_found:
            print(f"⚠️ Skipped {asin}, not found in any marketplace after {MAX_RETRIES} retries.")
            all_main.append({
                "ASIN": asin, "Marketplace": "N/A", "Product Title": "Not Found",
                "Rating": "N/A", "Total Reviews": "N/A", "Social Bought": "N/A",
                "Amazons_Choice_Text": "unavailable",
                "Deal_Badge_Text": "unavailable",
                "Best_Seller_Text": "unavailable",
                "Discount %": "N/A",
                "Offer Price": "N/A", "Price per Unit": "N/A", "MRP": "N/A", "Brand": "N/A", "Model Number": "N/A",
                "Country of Origin": "N/A", "Customer Reviews": "N/A", "Best Sellers Rank": "N/A",
                "Manufacturer": "N/A", "Packer": "N/A", "Availability": "N/A",
                "Delivery info": "N/A", "Ships from": "N/A", "Sold by": "N/A", "Gift options": "N/A"
            })

    full_path = os.path.join(save_path, f"{file_name}.xlsx")
    with pd.ExcelWriter(full_path, engine="openpyxl") as writer:
        df_main = pd.DataFrame(all_main)
        final_columns = [
            "ASIN", "Marketplace", "Product Title", "Rating", "Total Reviews",
            "Social Bought",
            # --- ensure these always appear in output ---
            "Amazons_Choice_Text", "Deal_Badge_Text", "Best_Seller_Text",
            # -------------------------------------------
            "Discount %", "Offer Price", "Price per Unit",
            "MRP", "Brand", "Model Number",
            "Country of Origin", "Customer Reviews", "Best Sellers Rank",
            "Manufacturer", "Packer", "Availability", "Delivery info",
            "Ships from", "Sold by", "Gift options"
        ]
        df_main = df_main.reindex(columns=final_columns, fill_value="N/A")
        df_main.to_excel(writer, sheet_name="Products", index=False)

        # ===== Only change below: reorder OtherSellers columns so "Sold By" is before "Marketplace" =====
        if all_buybox:
            df_sellers = pd.DataFrame(all_buybox)
            final_cols = [
                "ASIN",
                "Sold By",        # moved before Marketplace (requested)
                "Marketplace",
                "Offer Price", "Discount %", "MRP",
                "Ships From", "Delivery Info", "Condition",
                "Rating", "Seller Performance"
            ]
            df_sellers = df_sellers.reindex(columns=final_cols, fill_value="N/A")
        else:
            df_sellers = pd.DataFrame(columns=[
                "ASIN",
                "Sold By",        # moved before Marketplace (requested)
                "Marketplace",
                "Offer Price", "Discount %", "MRP",
                "Ships From", "Delivery Info", "Condition",
                "Rating", "Seller Performance"
            ])
        # ================================================================================================
        df_sellers.to_excel(writer, sheet_name="OtherSellers", index=False)

    print(f"\n✅ Data saved successfully at {full_path}")
    return full_path

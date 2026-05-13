"""Product discovery module for IRIP.

Automatically finds newly launched smartphones in India (₹10,000–₹35,000) and
normalises them to the product catalog format for import via ProductCatalogService.

Sources:
  - 91mobiles.com  — India-specific price list with local pricing
  - gsmarena.com   — Global spec database; used for full tech spec extraction

Runs in GitHub Actions (NOT on Render). Design principles:
  - Never raises — all errors return empty results or None
  - Resilient CSS selectors with multiple fallbacks per field
  - Deduplicates by slugified brand + model_name before enrichment
  - Calls get_full_specs lazily (only for products not already in catalog)

Typical usage:
    discovery = ProductDiscovery()
    result = discovery.run_discovery(months_back=6)
    # result = {"discovered": N, "new_products": [...], "existing_skipped": M}
"""
from __future__ import annotations

import hashlib
import re
import time
from datetime import date, timedelta
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Tag

# ============================================================
# Module-level helpers
# ============================================================

_MONTH_MAP: dict[str, int] = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    "january": 1, "february": 2, "march": 3, "april": 4,
    "june": 6, "july": 7, "august": 8, "september": 9,
    "october": 10, "november": 11, "december": 12,
}


def _clean_price(price_text: str) -> int | None:
    """Parse Indian price strings to int.

    Handles: "₹12,999" / "Rs. 12,999" / "12999" / "₹12,999 - ₹14,999" (lower bound).
    Returns None on any parse failure.
    """
    if not price_text:
        return None
    text = price_text.strip()
    # Take the first number group (handles ranges — lower bound is what we care about)
    m = re.search(r"[\d,]+", text.replace("₹", "").replace("Rs", "").replace(".", ""))
    if not m:
        return None
    try:
        return int(m.group(0).replace(",", ""))
    except ValueError:
        return None


def _price_band(price: int | None) -> str:
    """Map an integer price to the catalog price band string."""
    if price is None:
        return "10000-35000"
    if price <= 15000:
        return "10000-15000"
    if price <= 20000:
        return "15000-20000"
    if price <= 25000:
        return "20000-25000"
    return "25000-35000"


def _slugify(text: str) -> str:
    """Convert 'Infinix Hot 50 Pro' → 'infinix_hot_50_pro'."""
    slug = text.lower().strip()
    slug = re.sub(r"[^a-z0-9\s]", "", slug)
    slug = re.sub(r"\s+", "_", slug)
    return slug.strip("_")


def _parse_discovery_date(date_text: str) -> str:
    """Parse a launch date string to YYYY-MM-DD.

    Handles:
      "2025, March 7"  → "2025-03-07"
      "March 2025"     → "2025-03-01"
      "Mar 2025"       → "2025-03-01"
      "2025-03"        → "2025-03-01"
    Returns "" on failure.
    """
    text = date_text.strip()

    # "2025, March 7" or "2025, March"
    m = re.search(r"(\d{4}),?\s+([A-Za-z]+)(?:\s+(\d{1,2}))?", text)
    if m:
        year_str, month_str, day_str = m.groups()
        month_num = _MONTH_MAP.get(month_str.lower()[:3]) or _MONTH_MAP.get(month_str.lower())
        if month_num:
            try:
                day = int(day_str) if day_str else 1
                return date(int(year_str), month_num, day).isoformat()
            except ValueError:
                pass

    # "March 2025" or "Mar 2025"
    m = re.search(r"([A-Za-z]+)\s+(\d{4})", text)
    if m:
        month_str, year_str = m.groups()
        month_num = _MONTH_MAP.get(month_str.lower()[:3]) or _MONTH_MAP.get(month_str.lower())
        if month_num:
            try:
                return date(int(year_str), month_num, 1).isoformat()
            except ValueError:
                pass

    # ISO fragment "2025-03" or "2025-03-07"
    m = re.match(r"(\d{4})-(\d{2})(?:-(\d{2}))?", text)
    if m:
        year_str, month_str, day_str = m.groups()
        try:
            day = int(day_str) if day_str else 1
            return date(int(year_str), int(month_str), day).isoformat()
        except ValueError:
            pass

    return ""


def _extract_brand(product_name: str, known_brands: list[str] | None = None) -> str:
    """Extract brand from a product name by checking known prefixes."""
    name = product_name.strip()
    candidates = known_brands or [
        "Samsung", "Xiaomi", "Redmi", "POCO", "Realme", "Vivo", "OPPO",
        "Motorola", "OnePlus", "iQOO", "Nothing", "Lava", "Tecno", "Infinix",
        "itel", "Nokia", "Google", "Apple", "ASUS", "Sony", "Honor",
    ]
    for brand in candidates:
        if name.lower().startswith(brand.lower()):
            return brand
    # Fall back to first word
    return name.split()[0] if name else ""


def _text(element: Tag | None, sep: str = " ") -> str:
    if element is None:
        return ""
    return element.get_text(separator=sep, strip=True)


def _find_first(container: Tag, selectors: list[tuple[str, dict[str, Any]]]) -> Tag | None:
    for tag, attrs in selectors:
        el = container.find(tag, attrs)  # type: ignore[arg-type]
        if el is not None:
            return el  # type: ignore[return-value]
    return None


# ============================================================
# Spec field extractors for GSMArena
# ============================================================

def _gsmarena_spec_table(soup: BeautifulSoup) -> dict[str, str]:
    """Parse the #specs-list table into a flat {spec_label: value} dict."""
    specs: dict[str, str] = {}
    spec_div = soup.find("div", {"id": "specs-list"})
    if spec_div is None:
        return specs
    for row in spec_div.find_all("tr"):
        ttl_el = row.find("td", {"class": "ttl"})
        nfo_el = row.find("td", {"class": "nfo"})
        if ttl_el and nfo_el:
            key = _text(ttl_el).lower()
            value = _text(nfo_el)
            if key and value:
                specs[key] = value
    return specs


def _91mobiles_spec_table(soup: BeautifulSoup) -> dict[str, str]:
    """Parse 91mobiles spec tables into a flat {spec_label: value} dict."""
    specs: dict[str, str] = {}
    # 91mobiles uses <table class="spec-table"> or <div class="spec-sheet">
    tables = soup.find_all("table", class_=re.compile(r"spec", re.I))
    if not tables:
        tables = soup.find_all("table")
    for table in tables:
        for row in table.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) >= 2:
                key = _text(cells[0]).lower()
                value = _text(cells[1])
                if key and value:
                    specs[key] = value
    return specs


def _extract_gsmarena_specs(soup: BeautifulSoup, specs_url: str) -> dict[str, Any]:
    """Map GSMArena spec table to IRIP catalog fields."""
    raw = _gsmarena_spec_table(soup)

    def _get(*labels: str) -> str:
        for label in labels:
            for key, val in raw.items():
                if label in key:
                    return val
        return ""

    # Display
    display_raw = _get("size", "display size")
    display_size_m = re.search(r"(\d+\.?\d*)\s*inches?", display_raw, re.I)
    display_size = f"{display_size_m.group(1)} inch" if display_size_m else None

    display_type_raw = _get("type", "display type")
    display_type: str | None = None
    for dtype in ("AMOLED", "OLED", "LCD", "IPS", "TFT", "LTPO"):
        if dtype.lower() in display_type_raw.lower():
            display_type = dtype
            break

    refresh_raw = _get("refresh")
    refresh_m = re.search(r"(\d+)\s*hz", refresh_raw, re.I)
    refresh_rate = f"{refresh_m.group(1)}Hz" if refresh_m else None

    # Platform
    chipset_raw = _get("chipset", "cpu")
    chipset = chipset_raw.split("(")[0].strip() if chipset_raw else None

    os_raw = _get("os")
    android_m = re.search(r"android\s*([\d.]+)", os_raw, re.I)
    android_version = f"Android {android_m.group(1)}" if android_m else (os_raw[:30] or None)

    # Memory
    ram_raw = _get("ram")
    ram = ram_raw[:20] if ram_raw else None

    storage_raw = _get("internal", "storage")
    storage = storage_raw[:30] if storage_raw else None

    # Battery
    battery_raw = _get("capacity", "battery")
    battery_m = re.search(r"(\d[\d,]+)\s*mah", battery_raw, re.I)
    battery_capacity = f"{battery_m.group(1)} mAh" if battery_m else None

    charging_raw = _get("charging")
    charging_m = re.search(r"(\d+)\s*w", charging_raw, re.I)
    charging_wattage = f"{charging_m.group(1)}W" if charging_m else None

    # Camera
    rear_raw = _get("main camera", "rear camera", "triple", "dual", "single")
    rear_m = re.search(r"(\d+)\s*mp", rear_raw, re.I)
    rear_camera = f"{rear_m.group(1)}MP" if rear_m else None

    front_raw = _get("selfie", "front camera")
    front_m = re.search(r"(\d+)\s*mp", front_raw, re.I)
    front_camera = f"{front_m.group(1)}MP" if front_m else None

    # Body
    dimensions_raw = _get("dimensions")
    weight_m = re.search(r"(\d+(?:\.\d+)?)\s*g\b", dimensions_raw, re.I)
    weight = f"{weight_m.group(1)}g" if weight_m else None

    thickness_m = re.search(r"(\d+\.?\d*)\s*mm\s*thick", dimensions_raw, re.I)
    if not thickness_m:
        # Fallback: last float in dimensions string is usually thickness
        floats = re.findall(r"\d+\.\d+", dimensions_raw)
        thickness = f"{floats[-1]}mm" if floats else None
    else:
        thickness = f"{thickness_m.group(1)}mm"

    # 5G
    network_raw = _get("technology", "network")
    network_5g: str = "Yes" if "5g" in network_raw.lower() else "No"

    # Launch date
    launch_raw = _get("announced", "status")
    launch_date = _parse_discovery_date(launch_raw)

    return {
        "display_size": display_size,
        "display_type": display_type,
        "refresh_rate": refresh_rate,
        "chipset": chipset,
        "android_version": android_version,
        "ram": ram,
        "storage": storage,
        "battery_capacity": battery_capacity,
        "charging_wattage": charging_wattage,
        "rear_camera": rear_camera,
        "front_camera": front_camera,
        "weight": weight,
        "thickness": thickness,
        "network_5g": network_5g,
        "launch_date": launch_date or None,
        "official_url": specs_url,
    }


def _extract_91mobiles_specs(soup: BeautifulSoup, specs_url: str) -> dict[str, Any]:
    """Map 91mobiles spec table to IRIP catalog fields."""
    raw = _91mobiles_spec_table(soup)

    def _get(*labels: str) -> str:
        for label in labels:
            for key, val in raw.items():
                if label in key:
                    return val
        return ""

    # Display
    display_size_raw = _get("display size", "screen size")
    display_size_m = re.search(r"(\d+\.?\d*)\s*inch", display_size_raw, re.I)
    display_size = f"{display_size_m.group(1)} inch" if display_size_m else None

    display_type_raw = _get("display type", "screen type")
    display_type: str | None = None
    for dtype in ("AMOLED", "OLED", "LCD", "IPS", "TFT", "LTPO"):
        if dtype.lower() in display_type_raw.lower():
            display_type = dtype
            break

    refresh_raw = _get("refresh rate")
    refresh_m = re.search(r"(\d+)\s*hz", refresh_raw, re.I)
    refresh_rate = f"{refresh_m.group(1)}Hz" if refresh_m else None

    chipset_raw = _get("processor", "chipset", "cpu")
    chipset = chipset_raw[:50] if chipset_raw else None

    os_raw = _get("operating system", "os")
    android_version = os_raw[:40] if os_raw else None

    ram_raw = _get("ram")
    ram = ram_raw[:20] if ram_raw else None

    storage_raw = _get("internal storage", "storage")
    storage = storage_raw[:30] if storage_raw else None

    battery_raw = _get("battery capacity", "battery")
    battery_m = re.search(r"(\d[\d,]+)\s*mah", battery_raw, re.I)
    battery_capacity = f"{battery_m.group(1)} mAh" if battery_m else None

    charging_raw = _get("fast charging", "charging")
    charging_m = re.search(r"(\d+)\s*w", charging_raw, re.I)
    charging_wattage = f"{charging_m.group(1)}W" if charging_m else None

    rear_raw = _get("rear camera", "main camera", "primary camera")
    rear_m = re.search(r"(\d+)\s*mp", rear_raw, re.I)
    rear_camera = f"{rear_m.group(1)}MP" if rear_m else None

    front_raw = _get("front camera", "selfie")
    front_m = re.search(r"(\d+)\s*mp", front_raw, re.I)
    front_camera = f"{front_m.group(1)}MP" if front_m else None

    weight_raw = _get("weight")
    weight_m = re.search(r"(\d+(?:\.\d+)?)\s*g", weight_raw, re.I)
    weight = f"{weight_m.group(1)}g" if weight_m else None

    thickness_raw = _get("thickness", "depth")
    thickness_m = re.search(r"(\d+\.?\d*)\s*mm", thickness_raw, re.I)
    thickness = f"{thickness_m.group(1)}mm" if thickness_m else None

    network_raw = _get("network type", "connectivity")
    network_5g = "Yes" if "5g" in network_raw.lower() else "No"

    launch_raw = _get("launch date", "announced")
    launch_date = _parse_discovery_date(launch_raw)

    return {
        "display_size": display_size,
        "display_type": display_type,
        "refresh_rate": refresh_rate,
        "chipset": chipset,
        "android_version": android_version,
        "ram": ram,
        "storage": storage,
        "battery_capacity": battery_capacity,
        "charging_wattage": charging_wattage,
        "rear_camera": rear_camera,
        "front_camera": front_camera,
        "weight": weight,
        "thickness": thickness,
        "network_5g": network_5g,
        "launch_date": launch_date or None,
        "official_url": specs_url,
    }


# ============================================================
# Scraper class
# ============================================================


class ProductDiscovery:
    """Discover newly launched India-market smartphones (₹10k–₹35k).

    Sources 91mobiles (India-specific pricing) and GSMArena (global spec DB).
    Resilient to UI changes via fallback selectors; never raises on parse errors.
    """

    OWN_BRANDS: set[str] = {"tecno", "infinix", "itel", "transsion"}

    _PRICE_MIN = 10_000
    _PRICE_MAX = 35_000

    _91MOBILES_URL = (
        "https://www.91mobiles.com/mobile-phones-between-10000-35000-price-list-in-india"
    )
    _91MOBILES_FALLBACK_URL = "https://www.91mobiles.com/india-mobile-price-list/"

    _GSMARENA_SEARCH_URL = (
        "https://www.gsmarena.com/search.php3"
        "?chM=1&sOSes=2&sortBy=date&sAvailabilities=1&chPriceMin=9500&chPriceMax=36000"
    )

    _HEADERS: dict[str, str] = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-IN,en;q=0.9,hi;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;"
            "q=0.9,image/avif,image/webp,*/*;q=0.8"
        ),
        "Connection": "keep-alive",
    }

    def __init__(self) -> None:
        self._session = requests.Session()
        self._session.headers.update(self._HEADERS)

    # ----------------------------------------------------------
    # Internal fetch helper
    # ----------------------------------------------------------

    def _get(self, url: str, retries: int = 2, delay: float = 2.0) -> requests.Response | None:
        """GET with simple retry on 429/503. Returns None on all-attempts failure."""
        wait = delay
        for attempt in range(retries + 1):
            try:
                response = self._session.get(url, timeout=20)
                if response.status_code in (429, 503):
                    if attempt < retries:
                        time.sleep(wait)
                        wait *= 2
                    continue
                if response.status_code == 200:
                    return response
            except Exception:
                if attempt < retries:
                    time.sleep(wait)
                    wait *= 2
        return None

    def _soup(self, url: str) -> BeautifulSoup | None:
        """Fetch and parse URL. Returns None on any error."""
        response = self._get(url)
        if response is None:
            return None
        try:
            return BeautifulSoup(response.content, "lxml")
        except Exception:
            return None

    # ----------------------------------------------------------
    # 91mobiles discovery
    # ----------------------------------------------------------

    def discover_from_91mobiles(self, months_back: int = 6) -> list[dict[str, Any]]:
        """Scrape 91mobiles price list for phones between ₹10k–₹35k in India.

        Tries the price-band URL first; falls back to the general list with
        price filtering. Returns raw product dicts (not yet normalized to catalog).
        """
        cutoff = (date.today() - timedelta(days=30 * months_back)).isoformat()
        products: list[dict[str, Any]] = []

        # Try primary price-range URL, then general listing
        for url in (self._91MOBILES_URL, self._91MOBILES_FALLBACK_URL):
            soup = self._soup(url)
            if soup is None:
                continue

            items = self._parse_91mobiles_listings(soup, base_url=url)
            if items:
                products.extend(items)
                break

        # Filter: price in range, launch date within months_back
        filtered: list[dict[str, Any]] = []
        for p in products:
            price = p.get("current_price_int")
            if price is not None and not (self._PRICE_MIN <= price <= self._PRICE_MAX):
                continue
            launch = p.get("launch_date", "")
            if launch and cutoff and launch < cutoff:
                continue
            filtered.append(p)

        return filtered

    def _parse_91mobiles_listings(
        self, soup: BeautifulSoup, base_url: str
    ) -> list[dict[str, Any]]:
        """Extract phone listings from a 91mobiles listing page.

        Tries multiple CSS selector strategies as the site updates frequently.
        """
        products: list[dict[str, Any]] = []

        # Strategy 1: standard product list items
        items = soup.find_all("li", class_=re.compile(r"productListItem|product-item|phone-item", re.I))

        # Strategy 2: article/div cards
        if not items:
            items = soup.find_all(
                ["div", "article"],
                class_=re.compile(r"product.?card|phone.?card|listing.?item", re.I),
            )

        # Strategy 3: any element with a child heading + price
        if not items:
            items = [
                el for el in soup.find_all(["li", "div"], recursive=True)
                if el.find(["h2", "h3", "h4"]) and el.find(class_=re.compile(r"price", re.I))
            ][:60]

        base = base_url.rstrip("/")
        parsed_base = urlparse(base)
        site_root = f"{parsed_base.scheme}://{parsed_base.netloc}"

        for item in items[:80]:
            try:
                # Name
                name_el = _find_first(item, [
                    ("h3", {}),
                    ("h2", {}),
                    ("h4", {}),
                    ("a", {"class": re.compile(r"product.?title|name|title", re.I)}),
                    ("span", {"class": re.compile(r"product.?name|name", re.I)}),
                ])
                model_name = _text(name_el)
                if not model_name or len(model_name) < 4:
                    continue

                # Price
                price_el = _find_first(item, [
                    ("span", {"class": re.compile(r"price", re.I)}),
                    ("div", {"class": re.compile(r"price", re.I)}),
                    ("p", {"class": re.compile(r"price", re.I)}),
                ])
                price_text = _text(price_el)
                price_int = _clean_price(price_text)

                # Specs URL
                link_el = item.find("a", href=re.compile(r"/spec|/price|/review", re.I))
                if link_el is None:
                    link_el = item.find("a", href=True)
                href = link_el.get("href", "") if link_el else ""
                if href and not href.startswith("http"):
                    href = urljoin(site_root, href)

                brand = _extract_brand(model_name)

                products.append({
                    "model_name": model_name,
                    "brand": brand,
                    "launch_date": "",
                    "current_price": price_text,
                    "current_price_int": price_int,
                    "specs_url": href,
                    "source_name": "91mobiles",
                })
            except Exception:
                continue

        return products

    # ----------------------------------------------------------
    # GSMArena discovery
    # ----------------------------------------------------------

    def discover_from_gsmarena(self, months_back: int = 6) -> list[dict[str, Any]]:
        """Scrape GSMArena recently-launched Android phones (sorted by date).

        Fetches multiple pages of search results. Price filtering happens at the
        normalize step — GSMArena lists USD prices, not INR, so we include all
        returned phones and rely on spec enrichment + 91mobiles cross-reference.
        """
        cutoff = (date.today() - timedelta(days=30 * months_back)).isoformat()
        products: list[dict[str, Any]] = []

        for page in range(1, 4):  # pages 1–3 cover ~75 results
            url = f"{self._GSMARENA_SEARCH_URL}&fDisplayInchesMin=5&page={page}"
            soup = self._soup(url)
            if soup is None:
                break

            items = self._parse_gsmarena_results(soup)
            if not items:
                break

            for item in items:
                launch = item.get("launch_date", "")
                if launch and cutoff and launch < cutoff:
                    continue  # older than months_back — stop paging early for sorted results
                products.append(item)

            if page < 3:
                time.sleep(1.5)

        return products

    def _parse_gsmarena_results(self, soup: BeautifulSoup) -> list[dict[str, Any]]:
        """Extract phone listings from a GSMArena search results page."""
        products: list[dict[str, Any]] = []

        # GSMArena search results use div.makers > ul > li
        makers_div = soup.find("div", {"class": "makers"})
        if makers_div is None:
            makers_div = soup.find("div", id="review-body")

        container = makers_div or soup

        items = container.find_all("li", recursive=True)
        if not items:
            items = container.find_all("div", class_=re.compile(r"search.?result|phone.?card", re.I))

        for item in items[:40]:
            try:
                link_el = item.find("a", href=re.compile(r"\.php3?$|\.php3?#", re.I))
                if link_el is None:
                    link_el = item.find("a", href=True)
                if link_el is None:
                    continue

                href = link_el.get("href", "")
                if href and not href.startswith("http"):
                    href = f"https://www.gsmarena.com/{href.lstrip('/')}"

                name_el = item.find("strong") or item.find(["h3", "h4", "span"])
                model_name = _text(name_el)
                if not model_name or len(model_name) < 4:
                    continue

                # GSMArena spans: first = launch date, second = brief specs
                spans = item.find_all("span")
                launch_date = ""
                for span in spans:
                    span_text = _text(span)
                    parsed = _parse_discovery_date(span_text)
                    if parsed:
                        launch_date = parsed
                        break

                brand = _extract_brand(model_name)

                products.append({
                    "model_name": model_name,
                    "brand": brand,
                    "launch_date": launch_date,
                    "current_price": None,
                    "current_price_int": None,
                    "specs_url": href,
                    "source_name": "gsmarena",
                })
            except Exception:
                continue

        return products

    # ----------------------------------------------------------
    # Spec enrichment
    # ----------------------------------------------------------

    def get_full_specs(self, specs_url: str) -> dict[str, Any]:
        """Fetch a product spec page and extract structured spec fields.

        Routes to a GSMArena-specific or 91mobiles-specific parser based on URL.
        Returns a dict with all spec keys; any missing spec is None.
        All errors return an empty dict — never raises.
        """
        if not specs_url or not specs_url.startswith("http"):
            return {}

        soup = self._soup(specs_url)
        if soup is None:
            return {}

        try:
            if "gsmarena.com" in specs_url:
                return _extract_gsmarena_specs(soup, specs_url)
            else:
                return _extract_91mobiles_specs(soup, specs_url)
        except Exception:
            return {}

    # ----------------------------------------------------------
    # Normalization
    # ----------------------------------------------------------

    def normalize_to_catalog_format(self, raw_product: dict[str, Any]) -> dict[str, Any]:
        """Convert raw scraped product dict to IRIP product catalog schema.

        Produces a dict whose keys match ProductCatalogService.CATALOG_FIELDS so
        the caller can write it directly to a CSV and pass to import_csv_text().
        """
        model_name: str = raw_product.get("model_name", "").strip()
        brand: str = raw_product.get("brand", "").strip()
        product_name: str = raw_product.get("product_name", model_name).strip()

        if not product_name and model_name:
            product_name = model_name

        brand_lower = brand.lower()
        is_own = brand_lower in self.OWN_BRANDS

        price_int: int | None = raw_product.get("current_price_int")
        current_price_raw: str = str(raw_product.get("current_price") or "")
        if price_int is None and current_price_raw:
            price_int = _clean_price(current_price_raw)

        price_band = _price_band(price_int)

        # Build comparison_group based on ownership + price_band
        if is_own:
            comparison_group = "own_product"
        else:
            comparison_group = "competitor"

        # Spec fields forwarded from get_full_specs (all optional)
        launch_date: str = raw_product.get("launch_date") or ""
        display_size: Any = raw_product.get("display_size")
        display_type: Any = raw_product.get("display_type")
        refresh_rate: Any = raw_product.get("refresh_rate")
        ram: Any = raw_product.get("ram")
        storage: Any = raw_product.get("storage")
        battery_capacity: Any = raw_product.get("battery_capacity")
        charging_wattage: Any = raw_product.get("charging_wattage")
        chipset: Any = raw_product.get("chipset")
        rear_camera: Any = raw_product.get("rear_camera")
        front_camera: Any = raw_product.get("front_camera")
        android_version: Any = raw_product.get("android_version")
        weight: Any = raw_product.get("weight")
        thickness: Any = raw_product.get("thickness")
        network_5g: str = raw_product.get("network_5g") or "No"
        official_url: Any = raw_product.get("official_url") or raw_product.get("specs_url")
        source_name: str = raw_product.get("source_name", "product_discovery")

        # Slug for product_id: brand_modelname
        slug_base = f"{_slugify(brand)} {_slugify(model_name)}" if brand else _slugify(model_name)
        product_id = _slugify(slug_base)

        return {
            "product_id": product_id,
            "company_name": "Transsion Holdings" if is_own else brand,
            "brand": brand,
            "product_name": product_name,
            "model_name": model_name,
            "series_name": "",
            "is_own_product": "true" if is_own else "false",
            "launch_date": launch_date,
            "launch_market": "India",
            "price_band": price_band,
            "current_price": str(price_int) if price_int else (current_price_raw or ""),
            "ram": ram or "",
            "storage": storage or "",
            "battery_capacity": battery_capacity or "",
            "charging_wattage": charging_wattage or "",
            "chipset": chipset or "",
            "display_size": display_size or "",
            "display_type": display_type or "",
            "refresh_rate": refresh_rate or "",
            "screen_to_body_ratio": "",
            "rear_camera": rear_camera or "",
            "front_camera": front_camera or "",
            "android_version": android_version or "",
            "custom_ui": "",
            "network_5g": network_5g,
            "weight": weight or "",
            "thickness": thickness or "",
            "official_url": official_url or "",
            "marketplace_url": "",
            "comparison_group": comparison_group,
            "source_name": source_name,
            "source_confidence": "scraped_web",
        }

    # ----------------------------------------------------------
    # Main orchestration
    # ----------------------------------------------------------

    def run_discovery(self, months_back: int = 6) -> dict[str, Any]:
        """Discover new phones, enrich with full specs, normalize to catalog format.

        Deduplicates across both sources by slugified brand+model.
        Skips products already in the IRIP product catalog.
        Fetches full specs for each new product (polite delay between requests).

        Returns:
            {
                "discovered": total unique raw products found,
                "new_products": list of normalized catalog dicts,
                "existing_skipped": count of already-known products skipped,
            }
        """
        # Load existing catalog to skip known products
        existing_slugs: set[str] = set()
        try:
            from app.services.product_catalog_service import ProductCatalogService
            for item in ProductCatalogService().list_catalog():
                pid = str(item.get("product_id") or "")
                if pid:
                    existing_slugs.add(pid)
        except Exception:
            pass

        # Collect from both sources
        raw_91: list[dict[str, Any]] = []
        raw_gsm: list[dict[str, Any]] = []
        try:
            raw_91 = self.discover_from_91mobiles(months_back=months_back)
        except Exception:
            pass
        try:
            time.sleep(1.5)
            raw_gsm = self.discover_from_gsmarena(months_back=months_back)
        except Exception:
            pass

        # Deduplicate by slug (brand + model)
        seen_slugs: set[str] = set()
        unique_raw: list[dict[str, Any]] = []

        for product in raw_91 + raw_gsm:
            model = product.get("model_name", "")
            brand = product.get("brand", "")
            slug = _slugify(f"{brand} {model}")
            if slug in seen_slugs:
                continue
            seen_slugs.add(slug)
            unique_raw.append(product)

        discovered_count = len(unique_raw)

        # Filter out already-known products
        existing_skipped = 0
        new_raw: list[dict[str, Any]] = []
        for product in unique_raw:
            model = product.get("model_name", "")
            brand = product.get("brand", "")
            slug = _slugify(f"{brand} {model}")
            if slug in existing_slugs:
                existing_skipped += 1
            else:
                new_raw.append(product)

        # Enrich each new product with full specs
        new_products: list[dict[str, Any]] = []
        for i, product in enumerate(new_raw):
            specs_url = product.get("specs_url", "")
            if specs_url:
                try:
                    specs = self.get_full_specs(specs_url)
                    product.update({k: v for k, v in specs.items() if v is not None})
                except Exception:
                    pass
                if i < len(new_raw) - 1:
                    time.sleep(1.5)

            normalized = self.normalize_to_catalog_format(product)
            new_products.append(normalized)

        return {
            "discovered": discovered_count,
            "new_products": new_products,
            "existing_skipped": existing_skipped,
        }

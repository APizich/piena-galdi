import json
import os
import re
import time
import urllib.parse
from typing import Dict, List, Optional, Tuple
import concurrent.futures

import requests

# --- Constants ---------------------------------------------------------------
COMMONS_API_URL = "https://commons.wikimedia.org/w/api.php"
USER_AGENT = "LatviaMilkStandsApp/6.0 (GitHub Actions Builder; Wikimedia-only)"
OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://lz4.overpass-api.de/api/interpreter",
    "https://overpass.openstreetmap.fr/api/interpreter",
]

# Ensure we are looking for data.json in the correct directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(SCRIPT_DIR, "data.json")

# Set up a requests Session for connection pooling
session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})

# --- Helper functions --------------------------------------------------------
def load_existing_data() -> Dict[str, Dict]:
    """Loads the existing data.json so we can skip unchanged images."""
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            # Create a dictionary keyed by OSM ID for instant lookups
            return {f"{item['osm_type']}_{item['osm_id']}": item for item in data}
    except (FileNotFoundError, json.JSONDecodeError):
        print("No existing data.json found. Building from scratch...")
        return {}

def clean_html(raw_html: str) -> str:
    if not raw_html:
        return ""
    cleanr = re.compile(r"<.*?>")
    return re.sub(cleanr, "", raw_html).strip()

def first_non_empty(*values: Optional[str]) -> str:
    for value in values:
        if value and str(value).strip():
            return str(value).strip()
    return ""

def normalize_commons_title(raw_value: str) -> str:
    if not raw_value:
        return ""
    value = raw_value.split(";", 1)[0].strip()
    if not value:
        return ""

    lower_value = value.lower()
    
    # FILTER JUNK TAGS
    if lower_value in ["yes", "no"] or lower_value.startswith(("mapillary", "flickr", "google", "http://mapillary", "https://mapillary")):
        return ""

    if lower_value.startswith("http://") or lower_value.startswith("https://"):
        if "commons.wikimedia.org/wiki/" in value:
            title = value.split("/wiki/", 1)[1]
            title = urllib.parse.unquote(title.split("?", 1)[0].split("#", 1)[0])
            if title.startswith(("File:", "Category:")):
                return title
            return f"File:{title}"
        if "upload.wikimedia.org/" in value:
            filename = urllib.parse.unquote(value.rsplit("/", 1)[-1])
            if filename:
                return filename if filename.startswith("File:") else f"File:{filename}"
        return ""

    value = urllib.parse.unquote(value)
    if value.startswith(("File:", "Category:")):
        return value
    return f"File:{value}"

def get_commons_file_data(title: str, lang: str) -> Dict[str, str]:
    if not title:
        return {"image_url": "", "commons_page": "", "commons_title": "", "wiki_description": "", "image_date": ""}

    # RATE LIMIT PROTECTOR: Give Wikimedia breathing room between requests
    time.sleep(0.5)

    params = {
        "action": "query", "titles": title, "prop": "imageinfo",
        "iiprop": "url|extmetadata", "iiextmetadatalanguage": lang,
        "iiextmetadatafilter": "ImageDescription|DateTimeOriginal", "format": "json",
    }
    try:
        response = session.get(COMMONS_API_URL, params=params, timeout=20)
        response.raise_for_status()
        pages = response.json().get("query", {}).get("pages", {})
        for page_info in pages.values():
            info = page_info.get("imageinfo", [{}])[0]
            if info:
                desc = clean_html(info.get("extmetadata", {}).get("ImageDescription", {}).get("value", ""))
                date = clean_html(info.get("extmetadata", {}).get("DateTimeOriginal", {}).get("value", ""))
                return {
                    "image_url": info.get("url", ""),
                    "commons_page": info.get("descriptionurl", ""),
                    "commons_title": page_info.get("title", title),
                    "wiki_description": desc,
                    "image_date": date,
                }
    except requests.RequestException:
        pass
    return {"image_url": "", "commons_page": "", "commons_title": title, "wiki_description": "", "image_date": ""}

def get_first_file_from_commons_category(category_title: str, lang: str) -> Dict[str, str]:
    if not category_title.startswith("Category:"):
        return get_commons_file_data(category_title, lang)

    time.sleep(0.5) # Rate limit protector

    params = {
        "action": "query", "generator": "categorymembers", "gcmtitle": category_title,
        "gcmtype": "file", "gcmlimit": 5, "prop": "imageinfo",
        "iiprop": "url|extmetadata", "iiextmetadatalanguage": lang,
        "iiextmetadatafilter": "ImageDescription|DateTimeOriginal", "format": "json",
    }
    try:
        response = session.get(COMMONS_API_URL, params=params, timeout=20)
        response.raise_for_status()
        pages = response.json().get("query", {}).get("pages", {})
        sorted_pages = sorted(pages.values(), key=lambda p: p.get("title", ""))
        for page_info in sorted_pages:
            info = page_info.get("imageinfo", [{}])[0]
            if info:
                desc = clean_html(info.get("extmetadata", {}).get("ImageDescription", {}).get("value", ""))
                date = clean_html(info.get("extmetadata", {}).get("DateTimeOriginal", {}).get("value", ""))
                return {
                    "image_url": info.get("url", ""),
                    "commons_page": info.get("descriptionurl", ""),
                    "commons_title": page_info.get("title", ""),
                    "wiki_description": desc,
                    "image_date": date,
                }
    except requests.RequestException:
        pass
    return {"image_url": "", "commons_page": "", "commons_title": category_title, "wiki_description": "", "image_date": ""}

def resolve_wikimedia_image(tags: Dict[str, str]) -> Dict[str, str]:
    candidates = [("wikimedia_commons", tags.get("wikimedia_commons", "")), ("image", tags.get("image", ""))]

    for source_tag, raw_value in candidates:
        title = normalize_commons_title(raw_value)
        if not title: continue

        if title.startswith("Category:"):
            res_en = get_first_file_from_commons_category(title, "en")
            res_lv = get_first_file_from_commons_category(title, "lv")
        else:
            res_en = get_commons_file_data(title, "en")
            res_lv = get_commons_file_data(title, "lv")

        if res_en.get("image_url"):
            res_en["source_tag"] = source_tag
            res_en["wiki_description_en"] = res_en.pop("wiki_description", "")
            res_en["wiki_description_lv"] = res_lv.get("wiki_description", "")
            return res_en

    return {"image_url": "", "commons_page": "", "commons_title": "", "wiki_description_en": "", "wiki_description_lv": "", "source_tag": "", "image_date": ""}

def fetch_osm_elements() -> Tuple[List[Dict], Dict[str, str]]:
    query = """
    [out:json][timeout:90];
    area["ISO3166-1"="LV"]["admin_level"="2"]->.latvia;
    (
      node["man_made"="milk_churn_stand"](area.latvia);
      way["man_made"="milk_churn_stand"](area.latvia);
    );
    out center tags;
    """
    
    for endpoint in OVERPASS_ENDPOINTS:
        print(f"Trying Overpass server: {endpoint}...")
        try:
            response = requests.post(endpoint, data={"data": query}, headers={"User-Agent": USER_AGENT}, timeout=120)
            response.raise_for_status()
            data = response.json()
            
            if "remark" in data:
                print(f"  [!] Server is busy/error: {data['remark']}")
                continue 
                
            elements = data.get("elements", [])
            
            if len(elements) > 0:
                return elements, {"ok": "true", "message": f"Fetched from {endpoint}"}
            else:
                print("  [!] Server returned 0 elements. Trying next...")
                
        except Exception as e:
            print(f"  [!] Connection failed: {e}")
            time.sleep(2)
            
    return [], {"ok": "false", "message": "Failed to fetch from all Overpass endpoints."}

def process_single_element(element: Dict, old_data: Dict[str, Dict]) -> Optional[Dict]:
    tags = element.get("tags", {})
    lat = element.get("lat", element.get("center", {}).get("lat"))
    lon = element.get("lon", element.get("center", {}).get("lon"))

    if lat is None or lon is None: 
        return None

    osm_type = element.get("type", "")
    osm_id = element.get("id", "")
    unique_id = f"{osm_type}_{osm_id}"
    
    # Check what the raw image tag is currently on OpenStreetMap
    raw_image_tag_value = tags.get("wikimedia_commons") or tags.get("image") or ""

    # SAFEGUARD LOGIC: Did the image tag change?
    old_record = old_data.get(unique_id)
    
    if old_record and old_record.get("raw_image_tag") == raw_image_tag_value:
        # The tag hasn't changed! Instantly reuse the old Wikimedia data.
        commons = {
            "wiki_description_en": old_record.get("wiki_desc_en", ""),
            "wiki_description_lv": old_record.get("wiki_desc_lv", ""),
            "image_url": old_record.get("image", ""),
            "image_date": old_record.get("image_date", ""),
            "commons_page": old_record.get("commons_page", ""),
            "commons_title": old_record.get("commons_title", ""),
            "source_tag": old_record.get("image_source_tag", "")
        }
    else:
        # It's a new location, OR the image tag changed. Query Wikimedia.
        commons = resolve_wikimedia_image(tags)

    return {
        "osm_type": osm_type,
        "osm_id": osm_id,
        "lat": lat,
        "lon": lon,
        "name_default": first_non_empty(tags.get("name")),
        "name_lv": first_non_empty(tags.get("name:lv")),
        "name_en": first_non_empty(tags.get("name:en")),
        "osm_desc_default": first_non_empty(tags.get("description"), tags.get("note"), tags.get("fixme")),
        "osm_desc_lv": first_non_empty(tags.get("description:lv"), tags.get("note:lv")),
        "osm_desc_en": first_non_empty(tags.get("description:en"), tags.get("note:en")),
        "wiki_desc_en": commons.get("wiki_description_en", ""),
        "wiki_desc_lv": commons.get("wiki_description_lv", ""),
        "image": commons.get("image_url", ""),
        "image_date": commons.get("image_date", ""),
        "commons_page": commons.get("commons_page", ""),
        "commons_title": commons.get("commons_title", ""),
        "image_source_tag": commons.get("source_tag", ""),
        "raw_image_tag": raw_image_tag_value, # Save this so we can check it next time!
    }

def build_places_list(elements: List[Dict], old_data: Dict[str, Dict]) -> List[Dict]:
    places = []
    total = len(elements)
    
    # 2 WORKERS LIMIT: Prevents Wikimedia from banning us
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        # Submit tasks, passing the old data dict to each
        futures = [executor.submit(process_single_element, el, old_data) for el in elements]
        
        for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
            result = future.result()
            if result:
                places.append(result)
            
            print(f"\rProcessing data: {i}/{total} ({(i/total)*100:.1f}%) complete...", end="", flush=True)
            
    print() 
    return places

if __name__ == "__main__":
    print("Loading previous state...")
    old_data = load_existing_data()

    print("Fetching OSM elements...")
    elements, status = fetch_osm_elements()
    
    if status["ok"] == "true":
        print(f"Found {len(elements)} map objects. Comparing against previous data...")
        start_time = time.time()
        
        places = build_places_list(elements, old_data)
        
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(places, f, ensure_ascii=False, indent=2)
            
        elapsed = time.time() - start_time
        print(f"Success! Saved {len(places)} locations to data.json in {elapsed:.1f} seconds.")
    else:
        print("Error fetching data:", status["message"])

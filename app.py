"""
Property Owner Finder - Backend API (v2)
Helps Appear Here's landlord team identify commercial property owners in London.

Data sources:
1. Companies House API (free) - companies registered at address, officers & PSCs
2. Land Registry CCOD/OCOD datasets (free) - all UK/overseas company-owned property
3. Land Registry Business Gateway API (paid, £3/search) - definitive title register
4. LinkedIn - Google search links to find individuals
"""

import os
import shutil
import re
import csv
import json
import time
import urllib.parse
from pathlib import Path
from werkzeug.utils import secure_filename
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import requests

app = Flask(__name__, static_folder="static")
CORS(app)
app.config['MAX_CONTENT_LENGTH'] = 2 * 1024 * 1024 * 1024  # 2GB max upload

# ── Configuration ──────────────────────────────────────────────────────────────

COMPANIES_HOUSE_API_KEY = os.environ.get("COMPANIES_HOUSE_API_KEY", "")
COMPANIES_HOUSE_BASE = "https://api.company-information.service.gov.uk"

# Land Registry Business Gateway (optional – for automated title searches)
LR_BUSINESS_GATEWAY_USER = os.environ.get("LR_BUSINESS_GATEWAY_USER", "")
LR_BUSINESS_GATEWAY_PASS = os.environ.get("LR_BUSINESS_GATEWAY_PASS", "")

# Path to CCOD/OCOD CSV files (downloaded from Land Registry – free)
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(os.path.dirname(__file__), "data"))

# Optional: Google Drive file IDs for auto-loading on startup after a redeploy.
# Set these as Railway env vars so data survives container restarts.
# e.g. CCOD_GDRIVE_ID=14gBap0Xaozz-p-1NsoFJ6zM6C_rAhcL7
CCOD_GDRIVE_ID = os.environ.get("CCOD_GDRIVE_ID", "")
OCOD_GDRIVE_ID = os.environ.get("OCOD_GDRIVE_ID", "")

# Rate limiting for Companies House API (600 requests per 5 minutes)
_last_ch_request_time = 0
CH_MIN_INTERVAL = 0.5

# ── File-path cache (NO data loaded into RAM) ──────────────────────────────────
# We store only the file path, not the rows. Searches stream through the file.
_ccod_path = None
_ocod_path = None
_ccod_row_count = None
_ocod_row_count = None
# Postcode → list of byte offsets index (built once after load, O(1) lookups)
_ccod_index = {}
_ocod_index = {}


def _rate_limit_ch():
    global _last_ch_request_time
    elapsed = time.time() - _last_ch_request_time
    if elapsed < CH_MIN_INTERVAL:
        time.sleep(CH_MIN_INTERVAL - elapsed)
    _last_ch_request_time = time.time()


def ch_get(endpoint, params=None):
    """Make an authenticated GET to Companies House API."""
    if not COMPANIES_HOUSE_API_KEY:
        return None, "Companies House API key not configured."
    _rate_limit_ch()
    url = f"{COMPANIES_HOUSE_BASE}{endpoint}"
    try:
        resp = requests.get(url, params=params, auth=(COMPANIES_HOUSE_API_KEY, ""), timeout=(2, 3))
        if resp.status_code == 200:
            return resp.json(), None
        elif resp.status_code == 404:
            return None, None
        elif resp.status_code == 429:
            return None, "Rate limit exceeded. Wait a moment."
        else:
            return None, f"API error: {resp.status_code}"
    except requests.RequestException as e:
        return None, f"Connection error: {str(e)}"


# ── Address Helpers ────────────────────────────────────────────────────────────

def extract_postcode(address):
    match = re.search(r"[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}", address.upper())
    return match.group(0) if match else None


def extract_street_components(address):
    parts = [p.strip() for p in address.split(",")]
    return parts[0] if parts else address


def normalise_for_matching(text):
    """Lowercase, strip punctuation, collapse whitespace – for fuzzy address matching."""
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def address_match_score(query_address, candidate_address):
    """Score how well two addresses match (0-10 scale)."""
    q = normalise_for_matching(query_address)
    c = normalise_for_matching(candidate_address)

    if q == c:
        return 10

    score = 0
    q_postcode = extract_postcode(query_address)
    c_postcode = extract_postcode(candidate_address)

    # Postcode match is strong signal
    if q_postcode and c_postcode:
        if q_postcode.replace(" ", "") == c_postcode.replace(" ", ""):
            score += 4
        elif q_postcode.split()[0] == c_postcode.split()[0]:
            score += 2

    # House/building number check.
    # Strip the postcode portion so we don't accidentally match on postcode digits.
    pc_pattern = r'[A-Za-z]{1,2}\d[A-Za-z\d]?\s*\d[A-Za-z]{2}'
    q_no_pc = re.sub(pc_pattern, '', q, flags=re.IGNORECASE).strip()
    c_no_pc = re.sub(pc_pattern, '', c, flags=re.IGNORECASE).strip()
    # The first numeric token in the address is the house/building number.
    q_num_m = re.search(r'\b(\d+[a-z]?)\b', q_no_pc)
    c_num_m = re.search(r'\b(\d+[a-z]?)\b', c_no_pc)
    if q_num_m and c_num_m:
        if q_num_m.group(1) == c_num_m.group(1):
            score += 2   # bonus for correct number
        else:
            score -= 4   # strong penalty for wrong number

    # Street-level word overlap
    q_words = set(w for w in q.split() if len(w) > 2)
    c_words = set(w for w in c.split() if len(w) > 2)
    # Remove very common words
    common = {"london", "street", "road", "lane", "avenue", "place", "court",
              "house", "floor", "unit", "ground", "first", "second", "third",
              "england", "united", "kingdom", "greater"}
    q_words -= common
    c_words -= common

    if q_words and c_words:
        overlap = len(q_words & c_words)
        score += min(overlap * 1.5, 6)

    return score


# ── CCOD / OCOD Dataset Search (STREAMING – zero RAM overhead) ────────────────

def _find_ccod_path():
    """Locate the CCOD CSV file. Returns Path or None. Does NOT load any data."""
    global _ccod_path
    if _ccod_path is not None and _ccod_path.exists():
        return _ccod_path
    data_dir = Path(DATA_DIR)
    if data_dir.exists():
        for f in data_dir.glob("CCOD_*"):
            _ccod_path = f
            return _ccod_path
        for f in data_dir.glob("ccod*"):
            _ccod_path = f
            return _ccod_path
    return None


def _find_ocod_path():
    """Locate the OCOD CSV file. Returns Path or None. Does NOT load any data."""
    global _ocod_path
    if _ocod_path is not None and _ocod_path.exists():
        return _ocod_path
    data_dir = Path(DATA_DIR)
    if data_dir.exists():
        for f in data_dir.glob("OCOD_*"):
            _ocod_path = f
            return _ocod_path
        for f in data_dir.glob("ocod*"):
            _ocod_path = f
            return _ocod_path
    return None


def _count_csv_rows(path):
    """
    Count data rows in a CSV. Uses `wc -l` (fast C subprocess, no GIL) and
    falls back to Python streaming if wc is unavailable.
    """
    if not path or not path.exists():
        return 0
    # Fast path: wc -l gives line count in ~1 s even for multi-GB files
    try:
        import subprocess
        res = subprocess.run(
            ["wc", "-l", str(path)],
            capture_output=True, text=True, timeout=60
        )
        if res.returncode == 0:
            lines = int(res.stdout.strip().split()[0])
            return max(0, lines - 1)  # subtract the header row
    except Exception as e:
        print(f"[wc] fast count failed ({e}), falling back to Python scan")
    # Slow fallback: pure-Python streaming scan
    count = 0
    try:
        with open(str(path), "r", encoding="utf-8", errors="replace") as f:
            reader = csv.reader(f)
            next(reader, None)  # skip header
            for _ in reader:
                count += 1
    except Exception as e:
        print(f"Count error: {e}")
    return count


def _build_postcode_index(path):
    """
    Stream through the CSV once and build a postcode→[byte_offset] index.
    Returns (index_dict, row_count).
    Subsequent lookups use this index to seek directly to matching rows,
    making searches O(matches) instead of O(total_rows).
    """
    index = {}
    count = 0
    if not path or not path.exists():
        return index, count
    try:
        with open(str(path), "rb") as f:
            header_bytes = f.readline()
            header = header_bytes.decode("utf-8", errors="replace").rstrip("\r\n")
            # Find the Postcode column index
            cols = [c.strip().strip('"') for c in next(csv.reader([header]))]
            try:
                pc_idx = cols.index("Postcode")
            except ValueError:
                print(f"[index] 'Postcode' column not found in {path}. Found: {cols[:10]}")
                return index, count
            while True:
                offset = f.tell()
                line_bytes = f.readline()
                if not line_bytes:
                    break
                count += 1
                try:
                    line = line_bytes.decode("utf-8", errors="replace").rstrip("\r\n")
                    fields = next(csv.reader([line]))
                    if len(fields) > pc_idx:
                        pc = fields[pc_idx].replace(" ", "").upper()
                        if pc:
                            if pc not in index:
                                index[pc] = []
                            index[pc].append(offset)
                except Exception:
                    pass
    except Exception as e:
        print(f"[index] Error building index for {path}: {e}")
    return index, count


def _trigger_index_build(path, ftype):
    """Kick off a background thread to build the postcode index for ftype='ccod'|'ocod'."""
    import threading
    def _build():
        global _ccod_index, _ocod_index
        print(f"[index] Building postcode index for {ftype.upper()}…")
        idx, count = _build_postcode_index(path)
        if ftype == "ccod":
            _ccod_index = idx
        else:
            _ocod_index = idx
        unique = len(idx)
        print(f"[index] {ftype.upper()} index ready — {unique:,} unique postcodes covering {count:,} rows")
    threading.Thread(target=_build, daemon=True).start()


def _search_csv_indexed(path, index, address, source_name):
    """
    Fast lookup using the pre-built postcode index.
    Opens the file only for the rows matching the queried postcode.
    Falls back to streaming scan if no postcode in address.
    """
    if not path or not path.exists():
        return []
    if not index:
        # Index not built yet — file is still downloading or being indexed.
        # Do NOT fall back to _search_csv: scanning a partially-written
        # multi-GB file is CPU-intensive and blocks all gunicorn threads.
        return []

    postcode = extract_postcode(address)
    if not postcode:
        return _search_csv(path, address, source_name)

    postcode_norm = postcode.replace(" ", "").upper()
    postcode_district = postcode.split()[0].upper() if " " in postcode else postcode_norm

    # Gather offsets: exact postcode match.
    # District-level fallback only runs when the caller supplied just a district
    # (e.g. "W9") rather than a full postcode (e.g. "W9 2DU").  A full postcode
    # contains a space, so if we have one and nothing matched exactly, the
    # property simply isn't in this dataset — don't scan the whole district.
    has_full_postcode = " " in postcode  # "W9 2DU" → True; "W9" → False
    offsets = list(index.get(postcode_norm, []))
    if not offsets and not has_full_postcode and postcode_district != postcode_norm:
        for pc, pc_offsets in index.items():
            if pc.startswith(postcode_district):
                offsets.extend(pc_offsets[:500])  # cap per-postcode to stay sane

    if not offsets:
        return []

    results = []
    try:
        with open(str(path), "r", encoding="utf-8", errors="replace") as fh:
            reader = csv.DictReader(fh)
            fieldnames = reader.fieldnames or []

        with open(str(path), "rb") as fh:
            for offset in offsets:
                try:
                    fh.seek(offset)
                    line = fh.readline().decode("utf-8", errors="replace").rstrip("\r\n")
                    values = next(csv.reader([line]))
                    row = dict(zip(fieldnames, values))

                    prop_addr = row.get("Property Address", "")
                    row_postcode_display = row.get("Postcode", "").strip()
                    candidate = prop_addr.strip()
                    if row_postcode_display and row_postcode_display not in candidate:
                        candidate = candidate + " " + row_postcode_display

                    score = address_match_score(address, candidate)
                    if score >= 4:
                        proprietors = []
                        for i in range(1, 5):
                            name = row.get(f"Proprietor Name ({i})", "").strip()
                            if not name:
                                continue
                            proprietors.append({
                                "name": name,
                                "company_reg_no": row.get(f"Company Registration No. ({i})", "").strip(),
                                "category": row.get(f"Proprietorship Category ({i})", "").strip(),
                                "address": row.get(f"Proprietor ({i}) Address (1)", "").strip(),
                                "country_incorporated": row.get(f"Country Incorporated ({i})", "").strip(),
                            })
                        results.append({
                            "source": source_name,
                            "title_number": row.get("Title Number", "").strip(),
                            "tenure": row.get("Tenure", "").strip(),
                            "property_address": candidate,
                            "district": row.get("District", "").strip(),
                            "proprietors": proprietors,
                            "match_score": score,
                        })
                except Exception:
                    pass
    except Exception as e:
        print(f"[indexed_search] Error in {path}: {e}")

    return results


def _save_row_count(path, count):
    """Persist the row count to a tiny sidecar file so it survives restarts."""
    try:
        Path(str(path) + ".count").write_text(str(count))
    except Exception:
        pass


def _read_row_count(path):
    """Read the cached row count from the sidecar file (instant)."""
    if not path:
        return None
    try:
        p = Path(str(path) + ".count")
        if p.exists():
            return int(p.read_text().strip())
    except Exception:
        pass
    return None


def _search_csv(path, address, source_name):
    """
    Stream through a CSV file row-by-row and return rows matching address.
    Memory usage is O(matches), not O(total rows) – safe for 5 GB files.

    NOTE: CCOD/OCOD CSVs have a separate "Postcode" column – the "Property Address"
    field contains only the street address without postcode. We must check both.
    """
    if not path or not path.exists():
        return []

    postcode = extract_postcode(address)
    # Normalised postcode for fast pre-filter (no spaces, uppercase)
    postcode_norm = postcode.replace(" ", "").upper() if postcode else None
    # District (outward code) for looser fallback, e.g. "W1F" from "W1F 8SJ"
    postcode_district = postcode.split()[0].upper() if postcode and " " in postcode else postcode_norm
    results = []

    try:
        with open(str(path), "r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            for row in reader:
                prop_addr = row.get("Property Address", "")
                # CCOD/OCOD store the postcode in its own column
                row_postcode = row.get("Postcode", "").replace(" ", "").upper()

                # Fast pre-filter: postcode must match (or street if no postcode given).
                # If the caller supplied a full postcode (contains a space, e.g. "W9 2DU"),
                # only accept exact matches — do not expand to the whole district.
                has_full_postcode = postcode and " " in postcode
                if postcode_norm:
                    if row_postcode != postcode_norm:
                        if has_full_postcode:
                            continue  # full postcode given → exact match only
                        # district-only fallback (e.g. query had just "W9")
                        if not (postcode_district and row_postcode.startswith(postcode_district)):
                            continue
                else:
                    street = extract_street_components(address)
                    if normalise_for_matching(street) not in normalise_for_matching(prop_addr):
                        continue

                # Build a combined candidate address for scoring (street + postcode)
                row_postcode_display = row.get("Postcode", "").strip()
                candidate = prop_addr.strip()
                if row_postcode_display and row_postcode_display not in candidate:
                    candidate = candidate + " " + row_postcode_display

                score = address_match_score(address, candidate)
                if score >= 4:
                    proprietors = []
                    for i in range(1, 5):
                        name = row.get(f"Proprietor Name ({i})", "").strip()
                        if not name:
                            continue
                        proprietors.append({
                            "name": name,
                            "company_reg_no": row.get(f"Company Registration No. ({i})", "").strip(),
                            "category": row.get(f"Proprietorship Category ({i})", "").strip(),
                            "address": row.get(f"Proprietor ({i}) Address (1)", "").strip(),
                            "country_incorporated": row.get(f"Country Incorporated ({i})", "").strip(),
                        })
                    results.append({
                        "source": source_name,
                        "title_number": row.get("Title Number", "").strip(),
                        "tenure": row.get("Tenure", "").strip(),
                        "property_address": candidate,
                        "district": row.get("District", "").strip(),
                        "proprietors": proprietors,
                        "match_score": score,
                    })
    except Exception as e:
        print(f"CSV search error in {path}: {e}")

    return results


def search_ccod_ocod(address):
    """
    Search CCOD and OCOD datasets for properties matching the given address.
    Uses the postcode index (O(matches)) when built; falls back to streaming scan.
    """
    ccod_path = _find_ccod_path()
    ocod_path = _find_ocod_path()

    results = []
    for path, source_name, index in [
        (ccod_path, "CCOD", _ccod_index),
        (ocod_path, "OCOD", _ocod_index),
    ]:
        if path:
            if index:
                results.extend(_search_csv_indexed(path, index, address, source_name))
            # If index not built yet (file downloading), skip — never fall back to
            # full scan of a partially-written file as that blocks all threads.

    results.sort(key=lambda x: x["match_score"], reverse=True)
    return results[:10]


# ── Land Registry Business Gateway API ────────────────────────────────────────

def lr_business_gateway_search(address):
    """
    Search Land Registry Business Gateway for title information.
    Requires approved Business Gateway account.
    Uses the Property Description enquiry (£3 per search).
    """
    if not LR_BUSINESS_GATEWAY_USER or not LR_BUSINESS_GATEWAY_PASS:
        return None, "not_configured"

    postcode = extract_postcode(address)
    street = extract_street_components(address)

    soap_body = f"""<?xml version="1.0" encoding="UTF-8"?>
    <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
        xmlns:ns="http://www.oscre.org/ns/eReg-Final/2011/RequestSearchByPropertyDescriptionV2_0">
      <soap:Header>
        <wsse:Security xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
          <wsse:UsernameToken>
            <wsse:Username>{LR_BUSINESS_GATEWAY_USER}</wsse:Username>
            <wsse:Password>{LR_BUSINESS_GATEWAY_PASS}</wsse:Password>
          </wsse:UsernameToken>
        </wsse:Security>
      </soap:Header>
      <soap:Body>
        <ns:RequestSearchByPropertyDescriptionV2_0Service>
          <ns:MessageId>POF-{int(time.time())}</ns:MessageId>
          <ns:Product>
            <ns:ExternalReference>POF-{int(time.time())}</ns:ExternalReference>
            <ns:CustomerReference>AppearHere</ns:CustomerReference>
            <ns:SubjectProperty>
              <ns:Address>
                <ns:BuildingName/>
                <ns:BuildingNumber/>
                <ns:StreetName>{street}</ns:StreetName>
                <ns:CityName>London</ns:CityName>
                <ns:PostcodeZone>{postcode or ''}</ns:PostcodeZone>
              </ns:Address>
            </ns:SubjectProperty>
          </ns:Product>
        </ns:RequestSearchByPropertyDescriptionV2_0Service>
      </soap:Body>
    </soap:Envelope>"""

    try:
        resp = requests.post(
            "https://bgtest.landregistry.gov.uk/b2b/BGWS/SearchByPropertyDescriptionV2_0WebService",
            data=soap_body,
            headers={"Content-Type": "text/xml; charset=utf-8"},
            auth=(LR_BUSINESS_GATEWAY_USER, LR_BUSINESS_GATEWAY_PASS),
            timeout=30,
        )

        if resp.status_code == 200:
            import xml.etree.ElementTree as ET
            root = ET.fromstring(resp.text)
            titles = []
            for elem in root.iter():
                if "TitleNumber" in elem.tag:
                    titles.append({"title_number": elem.text})
                elif "ProprietorName" in elem.tag or "Proprietor" in elem.tag:
                    if titles:
                        titles[-1]["proprietor"] = elem.text
            return titles, None
        else:
            return None, f"Business Gateway error: {resp.status_code}"
    except Exception as e:
        return None, f"Business Gateway connection error: {str(e)}"


# ── Companies House Lookups ────────────────────────────────────────────────────

def search_companies_by_address(address):
    results = []
    postcode = extract_postcode(address)
    street = extract_street_components(address)

    if postcode:
        data, err = ch_get("/advanced-search/companies", params={
            "location": postcode, "size": 20, "status": "active",
        })
        if data and "items" in data:
            for company in data["items"]:
                reg_addr = company.get("registered_office_address", {})
                reg_str = " ".join(filter(None, [
                    reg_addr.get("address_line_1", ""),
                    reg_addr.get("address_line_2", ""),
                    reg_addr.get("postal_code", ""),
                ])).upper()

                score = 0
                if postcode and postcode.replace(" ", "") in reg_str.replace(" ", ""):
                    score += 2
                street_words = [w for w in street.upper().split() if len(w) > 2]
                for word in street_words:
                    if word in reg_str:
                        score += 1

                if score >= 2:
                    results.append({
                        "company_number": company.get("company_number"),
                        "company_name": company.get("company_name"),
                        "company_status": company.get("company_status"),
                        "registered_address": reg_addr,
                        "date_of_creation": company.get("date_of_creation"),
                        "company_type": company.get("company_type"),
                        "relevance_score": score,
                        "source": "companies_house_registered",
                    })

    if not results:
        data, err = ch_get("/search/companies", params={"q": street, "items_per_page": 20})
        if data and "items" in data:
            for company in data["items"]:
                reg_addr = company.get("registered_office_address", {})
                reg_str = " ".join(filter(None, [
                    reg_addr.get("address_line_1", ""),
                    reg_addr.get("address_line_2", ""),
                    reg_addr.get("postal_code", ""),
                ])).upper()

                score = 0
                if postcode and postcode.replace(" ", "") in reg_str.replace(" ", ""):
                    score += 3
                street_words = [w for w in street.upper().split() if len(w) > 2]
                for word in street_words:
                    if word in reg_str:
                        score += 1

                if score >= 2:
                    results.append({
                        "company_number": company.get("company_number"),
                        "company_name": company.get("company_name"),
                        "company_status": company.get("company_status", ""),
                        "registered_address": reg_addr,
                        "date_of_creation": company.get("date_of_creation"),
                        "company_type": company.get("company_type"),
                        "relevance_score": score,
                        "source": "companies_house_search",
                    })

    seen = set()
    unique = []
    for r in sorted(results, key=lambda x: x["relevance_score"], reverse=True):
        cn = r["company_number"]
        if cn not in seen:
            seen.add(cn)
            unique.append(r)

    return unique[:10]


def get_company_officers(company_number):
    data, err = ch_get(f"/company/{company_number}/officers")
    if err:
        return [], err
    officers = []
    if data and "items" in data:
        for officer in data["items"]:
            if officer.get("resigned_on"):
                continue
            officers.append({
                "name": officer.get("name", ""),
                "role": officer.get("officer_role", ""),
                "appointed_on": officer.get("appointed_on", ""),
                "nationality": officer.get("nationality", ""),
                "occupation": officer.get("occupation", ""),
                "address": officer.get("address", {}),
            })
    return officers, None


def get_company_pscs(company_number):
    data, err = ch_get(f"/company/{company_number}/persons-with-significant-control")
    if err:
        return [], err
    pscs = []
    if data and "items" in data:
        for psc in data["items"]:
            if psc.get("ceased_on"):
                continue
            pscs.append({
                "name": psc.get("name", ""),
                "kind": psc.get("kind", ""),
                "natures_of_control": psc.get("natures_of_control", []),
                "nationality": psc.get("nationality", ""),
                "country_of_residence": psc.get("country_of_residence", ""),
                "notified_on": psc.get("notified_on", ""),
            })
    return pscs, None


def get_company_details(company_number):
    data, err = ch_get(f"/company/{company_number}")
    if err:
        return None, err
    return data, None


# ── Cross-referencing logic ────────────────────────────────────────────────────

def cross_reference_results(ch_companies, lr_results):
    """
    Cross-reference Companies House companies with Land Registry data.
    Flag when the actual property owner (from LR) differs from companies
    registered at the address (from CH).
    """
    insights = []

    lr_owner_names = set()
    lr_company_numbers = set()
    for lr in lr_results:
        for prop in lr.get("proprietors", []):
            if prop["name"]:
                lr_owner_names.add(normalise_for_matching(prop["name"]))
            if prop.get("company_reg_no"):
                lr_company_numbers.add(prop["company_reg_no"].strip())

    ch_company_names = set()
    ch_company_numbers = set()
    for ch in ch_companies:
        ch_company_names.add(normalise_for_matching(ch["company_name"]))
        ch_company_numbers.add(ch["company_number"])

    if lr_owner_names and ch_company_names:
        overlap_names = lr_owner_names & ch_company_names
        overlap_numbers = lr_company_numbers & ch_company_numbers

        if overlap_names or overlap_numbers:
            insights.append({
                "type": "confirmed",
                "message": "Land Registry owner matches a company registered at this address. High confidence match.",
            })
        elif lr_owner_names and ch_company_names:
            lr_list = ", ".join(lr_owner_names)
            ch_list = ", ".join(ch_company_names)
            insights.append({
                "type": "mismatch",
                "message": (
                    f"The Land Registry owner ({lr_list}) does NOT match the companies "
                    f"registered at this address ({ch_list}). The registered company may "
                    f"be a tenant, not the property owner."
                ),
            })

    if lr_results and not ch_companies:
        insights.append({
            "type": "info",
            "message": "Owner found via Land Registry data but no company registered at this address on Companies House.",
        })

    if ch_companies and not lr_results:
        insights.append({
            "type": "info",
            "message": (
                "Companies found registered at this address, but no Land Registry match found "
                "in the CCOD/OCOD data. The property may be individually owned, or the CCOD/OCOD "
                "data may not cover this title. Use the Land Registry title search link for definitive info."
            ),
        })

    return insights


# ── Link Generators ────────────────────────────────────────────────────────────

def generate_land_registry_links(address):
    encoded = urllib.parse.quote(address)
    postcode = extract_postcode(address)
    links = {
        "title_search": f"https://search-property-information.service.gov.uk/search/search-by-address?address={encoded}",
        "price_paid": "https://landregistry.data.gov.uk/app/ppd",
    }
    if postcode:
        links["price_paid_search"] = f"https://landregistry.data.gov.uk/app/ppd/search?postcode={urllib.parse.quote(postcode)}"
    return links


def generate_linkedin_search(person_name, company_name=None, location="London"):
    query_parts = ['site:linkedin.com/in/', f'"{person_name}"']
    if company_name:
        query_parts.append(f'"{company_name}"')
    if location:
        query_parts.append(f'"{location}"')
    query = " ".join(query_parts)
    return f"https://www.google.com/search?q={urllib.parse.quote(query)}"


# ── API Routes ─────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory("static", "index.html")


@app.route("/api/status")
def status():
    ccod_path = _find_ccod_path()
    ocod_path = _find_ocod_path()
    # Use cached sidecar counts (instant); fall back to 1 if file exists but no count yet
    ccod_count = _read_row_count(ccod_path) or (1 if ccod_path else 0)
    ocod_count = _read_row_count(ocod_path) or (1 if ocod_path else 0)
    return jsonify({
        "status": "ok",
        "companies_house_configured": bool(COMPANIES_HOUSE_API_KEY),
        "land_registry_gateway_configured": bool(LR_BUSINESS_GATEWAY_USER),
        "ccod_loaded": ccod_count,
        "ocod_loaded": ocod_count,
        "ccod_index_size": len(_ccod_index),
        "ocod_index_size": len(_ocod_index),
    })


@app.route("/api/ping")
def ping():
    """Instant health-check — returns immediately with no logic."""
    return jsonify({"pong": True, "ts": time.time()})


@app.route("/api/lookup", methods=["POST"])
def lookup_property():
    data = request.json
    address = data.get("address", "").strip()
    if not address:
        return jsonify({"error": "Address is required"}), 400
    skip_ch = data.get("skip_ch", False) or request.args.get("skip_ch") == "1"

    result = {
        "address": address,
        "postcode": extract_postcode(address),
        "companies": [],
        "land_registry_data": [],
        "land_registry_links": generate_land_registry_links(address),
        "land_registry_gateway": None,
        "people": [],
        "insights": [],
        "warnings": [],
    }

    # ── Source 1: CCOD/OCOD free datasets ──
    lr_results = search_ccod_ocod(address)
    result["land_registry_data"] = lr_results

    # If we found LR data, also look up those companies on Companies House
    lr_company_numbers = set()
    for lr in lr_results:
        for prop in lr.get("proprietors", []):
            reg_no = prop.get("company_reg_no", "").strip()
            if reg_no:
                lr_company_numbers.add(reg_no)

    # ── Source 2: Land Registry Business Gateway (if configured) ──
    if LR_BUSINESS_GATEWAY_USER:
        gateway_results, gateway_err = lr_business_gateway_search(address)
        if gateway_err and gateway_err != "not_configured":
            result["warnings"].append(f"Land Registry Gateway: {gateway_err}")
        elif gateway_results:
            result["land_registry_gateway"] = gateway_results

    # ── Source 3: Companies House ──
    # Run the entire CH block in a thread with a hard 20-second wall-clock cap.
    # If it doesn't finish in time we still return the LR data immediately.
    import concurrent.futures as _cf

    ch_companies = []
    _ch_warnings = []

    def _run_ch():
        _companies = []
        _warnings = []
        ch_budget = time.time() + 20  # hard 20-second budget

        _companies = search_companies_by_address(address)

        # Fetch details for LR-confirmed company numbers not already in list.
        lr_detail_done = 0
        for reg_no in lr_company_numbers:
            if lr_detail_done >= 3 or time.time() > ch_budget:
                break
            if any(c["company_number"] == reg_no for c in _companies):
                continue
            details, err = get_company_details(reg_no)
            lr_detail_done += 1
            if details:
                _companies.append({
                    "company_number": reg_no,
                    "company_name": details.get("company_name", ""),
                    "company_status": details.get("company_status", ""),
                    "registered_address": details.get("registered_office_address", {}),
                    "date_of_creation": details.get("date_of_creation"),
                    "company_type": details.get("type", ""),
                    "relevance_score": 10,
                    "source": "land_registry_owner",
                })

        _companies.sort(key=lambda c: 0 if c.get("source") == "land_registry_owner" else 1)

        _all_people = {}
        lr_enriched = 0
        ch_addr_enriched = 0
        for company in _companies:
            if time.time() > ch_budget:
                company["officers"] = []
                company["pscs"] = []
                continue
            is_lr_owner = company.get("source") == "land_registry_owner"
            if is_lr_owner and lr_enriched >= 3:
                company["officers"] = []
                company["pscs"] = []
                continue
            if not is_lr_owner and ch_addr_enriched >= 2:
                company["officers"] = []
                company["pscs"] = []
                continue

            cn = company["company_number"]
            officers, err = get_company_officers(cn)
            if err:
                _warnings.append(f"Officers for {cn}: {err}")
            company["officers"] = officers

            pscs, err = get_company_pscs(cn)
            if err:
                _warnings.append(f"PSCs for {cn}: {err}")
            company["pscs"] = pscs

            if is_lr_owner:
                lr_enriched += 1
            else:
                ch_addr_enriched += 1

            for officer in officers:
                name = officer["name"]
                if name not in _all_people:
                    _all_people[name] = {"name": name, "roles": [], "companies": []}
                _all_people[name]["roles"].append(officer["role"])
                _all_people[name]["companies"].append(company["company_name"])

            for psc in pscs:
                name = psc["name"]
                if name not in _all_people:
                    _all_people[name] = {"name": name, "roles": [], "companies": []}
                _all_people[name]["roles"].append("Person with Significant Control")
                if company["company_name"] not in _all_people[name]["companies"]:
                    _all_people[name]["companies"].append(company["company_name"])

        return _companies, _all_people, _warnings

    if COMPANIES_HOUSE_API_KEY and not skip_ch:
        # IMPORTANT: do NOT use the context-manager form of ThreadPoolExecutor —
        # its __exit__ calls shutdown(wait=True) which blocks until the thread
        # finishes regardless of the timeout on result().  Instead, call
        # shutdown(wait=False) ourselves so we truly abandon the thread if it
        # runs over time.
        _executor = _cf.ThreadPoolExecutor(max_workers=1)
        _future = _executor.submit(_run_ch)
        try:
            ch_companies, _ch_people_map, _ch_warnings = _future.result(timeout=22)
        except _cf.TimeoutError:
            ch_companies = []
            _ch_people_map = {}
            _ch_warnings = ["Companies House lookup timed out — try again for enrichment."]
            result["warnings"].extend(_ch_warnings)
        except Exception as _e:
            ch_companies = []
            _ch_people_map = {}
            _ch_warnings = []
        finally:
            _executor.shutdown(wait=False)  # abandon thread, don't block

        all_people = _ch_people_map

        for name, person in all_people.items():
            person["linkedin_search"] = generate_linkedin_search(
                name,
                company_name=person["companies"][0] if person["companies"] else None,
            )

        result["companies"] = ch_companies
        result["people"] = list(all_people.values())
    else:
        result["warnings"].append("Companies House API key not set. Set COMPANIES_HOUSE_API_KEY for company lookups.")

    # ── Cross-reference ──
    result["insights"] = cross_reference_results(ch_companies, lr_results)

    if not ch_companies and not lr_results:
        result["warnings"].append(
            "No ownership data found from any source. Use the Land Registry title search link "
            "(£3) for definitive ownership – it covers all properties including individually owned ones."
        )

    return jsonify(result)


@app.route("/api/batch", methods=["POST"])
def batch_lookup():
    data = request.json
    addresses = data.get("addresses", [])
    if not addresses:
        return jsonify({"error": "At least one address is required"}), 400
    if len(addresses) > 50:
        return jsonify({"error": "Maximum 50 addresses per batch"}), 400

    results = []
    for addr in addresses:
        addr = addr.strip()
        if not addr:
            continue
        with app.test_request_context("/api/lookup", method="POST",
                                       json={"address": addr},
                                       content_type="application/json"):
            resp = lookup_property()
            if hasattr(resp, "get_json"):
                results.append(resp.get_json())
            else:
                results.append(resp[0].get_json())

    return jsonify({"results": results, "total": len(results)})


@app.route("/api/upload-data", methods=["POST"])
def upload_data():
    """Upload CCOD or OCOD CSV file via the browser."""
    global _ccod_path, _ocod_path, _ccod_row_count, _ocod_row_count

    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    file = request.files["file"]
    filename = secure_filename(file.filename)

    if not filename.lower().endswith(".csv"):
        return jsonify({"error": "File must be a CSV"}), 400

    data_dir = Path(DATA_DIR)
    data_dir.mkdir(parents=True, exist_ok=True)

    dest = data_dir / filename
    file.save(str(dest))

    fn_upper = filename.upper()
    if "CCOD" in fn_upper:
        _ccod_path = dest
        _ccod_row_count = _count_csv_rows(dest)
        _save_row_count(dest, _ccod_row_count)
        _trigger_index_build(dest, "ccod")
        return jsonify({"status": "ok", "type": "CCOD", "records": _ccod_row_count, "filename": filename})
    elif "OCOD" in fn_upper:
        _ocod_path = dest
        _ocod_row_count = _count_csv_rows(dest)
        _save_row_count(dest, _ocod_row_count)
        _trigger_index_build(dest, "ocod")
        return jsonify({"status": "ok", "type": "OCOD", "records": _ocod_row_count, "filename": filename})
    else:
        return jsonify({
            "status": "ok",
            "type": "unknown",
            "message": "File saved but could not determine if CCOD or OCOD. Rename to include CCOD or OCOD in the filename.",
            "filename": filename,
        })


@app.route("/api/upload-chunk", methods=["POST"])
def upload_chunk():
    """Accept one chunk of a chunked CSV upload; assemble when all chunks received."""
    global _ccod_path, _ocod_path, _ccod_row_count, _ocod_row_count
    chunk_file = request.files.get("chunk")
    upload_id = request.form.get("uploadId", "")
    chunk_idx = int(request.form.get("chunkIndex", 0))
    total = int(request.form.get("totalChunks", 1))
    filename = secure_filename(request.form.get("filename", "upload.csv"))
    if not chunk_file or not upload_id:
        return jsonify({"error": "Missing chunk or uploadId"}), 400
    tmp_dir = Path("/tmp") / upload_id
    tmp_dir.mkdir(parents=True, exist_ok=True)
    chunk_file.save(str(tmp_dir / "chunk_{:06d}".format(chunk_idx)))
    received = len(list(tmp_dir.glob("chunk_*")))
    if received < total:
        return jsonify({"status": "chunk_ok", "received": received, "total": total})
    data_dir = Path(DATA_DIR)
    data_dir.mkdir(parents=True, exist_ok=True)
    dest = data_dir / filename
    with open(str(dest), "wb") as out:
        for i in range(total):
            with open(str(tmp_dir / "chunk_{:06d}".format(i)), "rb") as cf:
                out.write(cf.read())
    shutil.rmtree(str(tmp_dir), ignore_errors=True)
    fn_upper = filename.upper()
    if "CCOD" in fn_upper:
        _ccod_path = dest
        _ccod_row_count = _count_csv_rows(dest)
        _save_row_count(dest, _ccod_row_count)
        _trigger_index_build(dest, "ccod")
        return jsonify({"status": "ok", "type": "CCOD", "records": _ccod_row_count, "filename": filename})
    elif "OCOD" in fn_upper:
        _ocod_path = dest
        _ocod_row_count = _count_csv_rows(dest)
        _save_row_count(dest, _ocod_row_count)
        _trigger_index_build(dest, "ocod")
        return jsonify({"status": "ok", "type": "OCOD", "records": _ocod_row_count, "filename": filename})
    return jsonify({"status": "ok", "type": "unknown", "filename": filename})


@app.route("/api/load-from-url", methods=["POST"])
def load_from_url():
    """
    Kick off a background download of a CCOD or OCOD CSV from Google Drive.
    Returns immediately — the download + index build happen in a daemon thread
    so no gunicorn thread is ever tied up for the full 9-minute download.
    """
    body = request.get_json(force=True, silent=True) or {}
    url = body.get("url", "").strip()
    ftype = body.get("type", "").lower()
    if not url:
        return jsonify({"error": "No URL provided"}), 400
    import re as _re
    gd = _re.search(r"/(?:file/d/|open[?]id=)([a-zA-Z0-9_-]{20,})", url)
    if not gd:
        return jsonify({"error": "Could not find a Google Drive file ID in that URL"}), 400
    file_id = gd.group(1)

    def _do_download():
        global _ccod_path, _ocod_path, _ccod_row_count, _ocod_row_count
        try:
            session = requests.Session()
            session.headers["User-Agent"] = "Mozilla/5.0"
            base = "https://drive.google.com/uc"
            params = {"export": "download", "id": file_id}
            r1 = session.get(base, params=params, timeout=60)
            r1.raise_for_status()
            confirm = None
            for k, v in session.cookies.items():
                if "download_warning" in k:
                    confirm = v
                    break
            if confirm:
                params["confirm"] = confirm
            elif "text/html" in r1.headers.get("Content-Type", ""):
                base = "https://drive.usercontent.google.com/download"
                params["confirm"] = "t"
                params["authuser"] = "0"
            resp = session.get(base, params=params, stream=True, timeout=600)
            resp.raise_for_status()
            filename = "CCOD_data.csv" if ftype == "ccod" else "OCOD_data.csv"
            data_dir = Path(DATA_DIR)
            data_dir.mkdir(parents=True, exist_ok=True)
            dest = data_dir / filename
            written = 0
            with open(str(dest), "wb") as f:
                for chunk in resp.iter_content(chunk_size=8 * 1024 * 1024):
                    if chunk:
                        f.write(chunk)
                        written += len(chunk)
            if written < 1000:
                print(f"[load] {ftype.upper()} download too small ({written} bytes)")
                return
            if ftype == "ccod":
                _ccod_path = dest
                _ccod_row_count = _count_csv_rows(dest)
                _save_row_count(dest, _ccod_row_count)
                _trigger_index_build(dest, "ccod")
            else:
                _ocod_path = dest
                _ocod_row_count = _count_csv_rows(dest)
                _save_row_count(dest, _ocod_row_count)
                _trigger_index_build(dest, "ocod")
            print(f"[load] {ftype.upper()} ready — {written:,} bytes")
        except Exception as exc:
            print(f"[load] {ftype.upper()} download failed: {exc}")

    import threading as _threading
    _threading.Thread(target=_do_download, daemon=True).start()
    return jsonify({"status": "started", "type": ftype.upper(),
                    "message": "Download started in background. Poll /api/status to track progress."})


@app.route("/api/debug-csv-headers")
def debug_csv_headers():
    """Temporary: return first row + headers of CCOD/OCOD so we can verify column names."""
    result = {}
    for name, path_fn in [("ccod", _find_ccod_path), ("ocod", _find_ocod_path)]:
        path = path_fn()
        if not path or not path.exists():
            result[name] = "file not found"
            continue
        try:
            with open(str(path), "r", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                headers = reader.fieldnames
                first_row = next(reader, None)
            result[name] = {"headers": headers, "sample_row": dict(first_row) if first_row else None}
        except Exception as e:
            result[name] = f"error: {e}"
    return jsonify(result)


@app.route("/settings")
def settings_page():
    """Settings page for uploading data files and configuring API keys."""
    return send_from_directory("static", "settings.html")


@app.route("/api/company/<company_number>")
def company_detail(company_number):
    details, err = get_company_details(company_number)
    if err:
        return jsonify({"error": err}), 500
    if not details:
        return jsonify({"error": "Company not found"}), 404
    officers, _ = get_company_officers(company_number)
    pscs, _ = get_company_pscs(company_number)
    return jsonify({"details": details, "officers": officers, "pscs": pscs})


def _gdrive_download(file_id, dest_path):
    """Download a file from Google Drive by file ID. Returns (bytes_written, error)."""
    session = requests.Session()
    session.headers["User-Agent"] = "Mozilla/5.0"
    base = "https://drive.google.com/uc"
    params = {"export": "download", "id": file_id}
    try:
        r1 = session.get(base, params=params, timeout=60)
        r1.raise_for_status()
    except Exception as exc:
        return 0, str(exc)
    confirm = None
    for k, v in session.cookies.items():
        if "download_warning" in k:
            confirm = v
            break
    if confirm:
        params["confirm"] = confirm
    elif "text/html" in r1.headers.get("Content-Type", ""):
        base = "https://drive.usercontent.google.com/download"
        params["confirm"] = "t"
        params["authuser"] = "0"
    try:
        resp = session.get(base, params=params, stream=True, timeout=600)
        resp.raise_for_status()
    except Exception as exc:
        return 0, str(exc)
    written = 0
    Path(dest_path).parent.mkdir(parents=True, exist_ok=True)
    with open(str(dest_path), "wb") as f:
        for chunk in resp.iter_content(chunk_size=8 * 1024 * 1024):
            if chunk:
                f.write(chunk)
                written += len(chunk)
    return written, None


def _auto_load_from_env():
    """
    If CCOD_GDRIVE_ID / OCOD_GDRIVE_ID env vars are set and the data files
    are missing, download them automatically. Runs once at startup in a
    background thread so it doesn't block the web server from starting.
    """
    global _ccod_path, _ocod_path, _ccod_row_count, _ocod_row_count
    import threading

    def _load():
        global _ccod_path, _ocod_path, _ccod_row_count, _ocod_row_count
        data_dir = Path(DATA_DIR)
        data_dir.mkdir(parents=True, exist_ok=True)

        for ftype, gdrive_id, filename, path_var, count_var in [
            ("CCOD", CCOD_GDRIVE_ID, "CCOD_data.csv", "_ccod_path", "_ccod_row_count"),
            ("OCOD", OCOD_GDRIVE_ID, "OCOD_data.csv", "_ocod_path", "_ocod_row_count"),
        ]:
            if not gdrive_id:
                continue
            dest = data_dir / filename
            if dest.exists() and dest.stat().st_size > 10000:
                print(f"[startup] {ftype} already present at {dest}")
                if ftype == "CCOD":
                    _ccod_path = dest
                    _ccod_row_count = _read_row_count(dest)
                else:
                    _ocod_path = dest
                    _ocod_row_count = _read_row_count(dest)
                _trigger_index_build(dest, ftype.lower())
                continue
            print(f"[startup] Downloading {ftype} from Google Drive (id={gdrive_id})…")
            written, err = _gdrive_download(gdrive_id, dest)
            if err or written < 1000:
                print(f"[startup] {ftype} download failed: {err or 'too small'}")
                continue
            print(f"[startup] {ftype} downloaded ({written:,} bytes). Counting rows…")
            count = _count_csv_rows(dest)
            _save_row_count(dest, count)
            if ftype == "CCOD":
                _ccod_path = dest
                _ccod_row_count = count
            else:
                _ocod_path = dest
                _ocod_row_count = count
            print(f"[startup] {ftype} ready — {count:,} records")
            _trigger_index_build(dest, ftype.lower())

    t = threading.Thread(target=_load, daemon=True)
    t.start()


# ── Main ───────────────────────────────────────────────────────────────────────

# Trigger auto-load when the module is imported (i.e. on every container start)
_auto_load_from_env()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))

    print(f"\n{'='*60}")
    print(f" Property Owner Finder v2")
    print(f" http://localhost:{port}")
    print(f" Companies House API: {'OK' if COMPANIES_HOUSE_API_KEY else 'NOT SET'}")
    print(f" LR Business Gateway: {'OK' if LR_BUSINESS_GATEWAY_USER else 'NOT SET (optional)'}")
    print(f" CCOD auto-load: {'enabled (id=' + CCOD_GDRIVE_ID + ')' if CCOD_GDRIVE_ID else 'disabled'}")
    print(f" OCOD auto-load: {'enabled (id=' + OCOD_GDRIVE_ID + ')' if OCOD_GDRIVE_ID else 'disabled'}")
    print(f"{'='*60}\n")
    app.run(host="0.0.0.0", port=port, debug=True)

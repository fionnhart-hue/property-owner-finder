"""
Property Owner Finder - Backend API (v2)
Helps Appear Here's landlord team identify commercial property owners in London.

Data sources:
1. Companies House API (free) — companies registered at address, officers & PSCs
2. Land Registry CCOD/OCOD datasets (free) — all UK/overseas company-owned property
3. Land Registry Business Gateway API (paid, £3/search) — definitive title register
4. LinkedIn — Google search links to find individuals
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

# ─── Configuration ───────────────────────────────────────────────────────────

COMPANIES_HOUSE_API_KEY = os.environ.get("COMPANIES_HOUSE_API_KEY", "")
COMPANIES_HOUSE_BASE = "https://api.company-information.service.gov.uk"

# Land Registry Business Gateway (optional — for automated title searches)
LR_BUSINESS_GATEWAY_USER = os.environ.get("LR_BUSINESS_GATEWAY_USER", "")
LR_BUSINESS_GATEWAY_PASS = os.environ.get("LR_BUSINESS_GATEWAY_PASS", "")

# Path to CCOD/OCOD CSV files (downloaded from Land Registry — free)
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(os.path.dirname(__file__), "data"))

# Rate limiting for Companies House API (600 requests per 5 minutes)
_last_ch_request_time = 0
CH_MIN_INTERVAL = 0.5


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
        resp = requests.get(url, params=params, auth=(COMPANIES_HOUSE_API_KEY, ""), timeout=15)
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


# ─── Address Helpers ─────────────────────────────────────────────────────────

def extract_postcode(address):
    match = re.search(r"[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}", address.upper())
    return match.group(0) if match else None


def extract_street_components(address):
    parts = [p.strip() for p in address.split(",")]
    return parts[0] if parts else address


def normalise_for_matching(text):
    """Lowercase, strip punctuation, collapse whitespace — for fuzzy address matching."""
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


# ─── CCOD / OCOD Dataset Search (FREE Land Registry data) ───────────────────

_ccod_data = None
_ocod_data = None


def _load_ccod():
    """Load Commercial and Corporate Ownership Data CSV into memory."""
    global _ccod_data
    if _ccod_data is not None:
        return _ccod_data

    ccod_path = None
    data_dir = Path(DATA_DIR)
    if data_dir.exists():
        # Find the CCOD CSV (filename varies by month)
        for f in data_dir.glob("CCOD_*"):
            ccod_path = f
            break
        if not ccod_path:
            for f in data_dir.glob("ccod*"):
                ccod_path = f
                break

    if not ccod_path or not ccod_path.exists():
        _ccod_data = []
        return _ccod_data

    print(f"  Loading CCOD data from {ccod_path}...")
    records = []
    try:
        with open(ccod_path, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            for row in reader:
                records.append(row)
        print(f"  Loaded {len(records):,} CCOD records")
    except Exception as e:
        print(f"  Error loading CCOD: {e}")

    _ccod_data = records
    return _ccod_data


def _load_ocod():
    """Load Overseas Companies Ownership Data CSV into memory."""
    global _ocod_data
    if _ocod_data is not None:
        return _ocod_data

    ocod_path = None
    data_dir = Path(DATA_DIR)
    if data_dir.exists():
        for f in data_dir.glob("OCOD_*"):
            ocod_path = f
            break
        if not ocod_path:
            for f in data_dir.glob("ocod*"):
                ocod_path = f
                break

    if not ocod_path or not ocod_path.exists():
        _ocod_data = []
        return _ocod_data

    print(f"  Loading OCOD data from {ocod_path}...")
    records = []
    try:
        with open(ocod_path, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            for row in reader:
                records.append(row)
        print(f"  Loaded {len(records):,} OCOD records")
    except Exception as e:
        print(f"  Error loading OCOD: {e}")

    _ocod_data = records
    return _ocod_data


def search_ccod_ocod(address):
    """
    Search CCOD and OCOD datasets for properties matching the given address.
    Returns list of matches with owner company info.
    """
    ccod = _load_ccod()
    ocod = _load_ocod()

    postcode = extract_postcode(address)
    results = []

    # CCOD columns: Title Number, Tenure, Property Address, District, Region,
    #               Proprietor Name (1), Company Registration No. (1),
    #               Proprietorship Category (1), Proprietor (1) Address (1), ...
    for dataset, source_name in [(ccod, "CCOD"), (ocod, "OCOD")]:
        for row in dataset:
            prop_addr = row.get("Property Address", "")

            # Quick pre-filter by postcode for performance
            if postcode:
                if postcode.replace(" ", "").upper() not in prop_addr.replace(" ", "").upper():
                    continue
            else:
                # Without postcode, require at least street match
                street = extract_street_components(address)
                if normalise_for_matching(street) not in normalise_for_matching(prop_addr):
                    continue

            score = address_match_score(address, prop_addr)
            if score >= 4:
                # Extract up to 4 proprietors from CCOD/OCOD columns
                proprietors = []
                for i in range(1, 5):
                    name_key = f"Proprietor Name ({i})"
                    reg_key = f"Company Registration No. ({i})"
                    cat_key = f"Proprietorship Category ({i})"
                    addr_key = f"Proprietor ({i}) Address (1)"
                    country_key = f"Country Incorporated ({i})"

                    name = row.get(name_key, "").strip()
                    if not name:
                        continue
                    proprietors.append({
                        "name": name,
                        "company_reg_no": row.get(reg_key, "").strip(),
                        "category": row.get(cat_key, "").strip(),
                        "address": row.get(addr_key, "").strip(),
                        "country_incorporated": row.get(country_key, "").strip(),
                    })

                results.append({
                    "source": source_name,
                    "title_number": row.get("Title Number", "").strip(),
                    "tenure": row.get("Tenure", "").strip(),
                    "property_address": prop_addr.strip(),
                    "district": row.get("District", "").strip(),
                    "proprietors": proprietors,
                    "match_score": score,
                })

    results.sort(key=lambda x: x["match_score"], reverse=True)
    return results[:10]


# ─── Land Registry Business Gateway API ─────────────────────────────────────

def lr_business_gateway_search(address):
    """
    Search Land Registry Business Gateway for title information.
    Requires approved Business Gateway account.
    Uses the Property Description enquiry (£3 per search).
    """
    if not LR_BUSINESS_GATEWAY_USER or not LR_BUSINESS_GATEWAY_PASS:
        return None, "not_configured"

    # The Business Gateway uses a SOAP/XML API
    # Documentation: https://www.gov.uk/guidance/hm-land-registry-business-gateway
    postcode = extract_postcode(address)
    street = extract_street_components(address)

    # Build the SOAP request for RequestSearchByPropertyDescriptionV2_0
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
            # Parse XML response to extract title numbers and proprietor names
            # (Simplified — full XML parsing would use lxml in production)
            import xml.etree.ElementTree as ET
            root = ET.fromstring(resp.text)

            titles = []
            # Extract title data from SOAP response
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


# ─── Companies House Lookups ─────────────────────────────────────────────────

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


# ─── Cross-referencing logic ─────────────────────────────────────────────────

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
        # Check if LR owner matches any CH company
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


# ─── Link Generators ─────────────────────────────────────────────────────────

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


# ─── API Routes ──────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory("static", "index.html")


@app.route("/api/status")
def status():
    ccod = _load_ccod()
    ocod = _load_ocod()
    return jsonify({
        "status": "ok",
        "companies_house_configured": bool(COMPANIES_HOUSE_API_KEY),
        "land_registry_gateway_configured": bool(LR_BUSINESS_GATEWAY_USER),
        "ccod_loaded": len(ccod),
        "ocod_loaded": len(ocod),
    })


@app.route("/api/lookup", methods=["POST"])
def lookup_property():
    data = request.json
    address = data.get("address", "").strip()
    if not address:
        return jsonify({"error": "Address is required"}), 400

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

    # If we found LR data, also look up those companies on Companies House for officers/PSCs
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
    ch_companies = []
    if COMPANIES_HOUSE_API_KEY:
        # Search for companies registered at the address
        ch_companies = search_companies_by_address(address)

        # Also look up companies found via Land Registry
        for reg_no in lr_company_numbers:
            # Check if already found
            if any(c["company_number"] == reg_no for c in ch_companies):
                continue
            details, err = get_company_details(reg_no)
            if details:
                ch_companies.append({
                    "company_number": reg_no,
                    "company_name": details.get("company_name", ""),
                    "company_status": details.get("company_status", ""),
                    "registered_address": details.get("registered_office_address", {}),
                    "date_of_creation": details.get("date_of_creation"),
                    "company_type": details.get("type", ""),
                    "relevance_score": 10,
                    "source": "land_registry_owner",
                })

        # Get officers and PSCs for each company
        all_people = {}
        for company in ch_companies:
            cn = company["company_number"]
            officers, err = get_company_officers(cn)
            if err:
                result["warnings"].append(f"Officers for {cn}: {err}")
            company["officers"] = officers

            pscs, err = get_company_pscs(cn)
            if err:
                result["warnings"].append(f"PSCs for {cn}: {err}")
            company["pscs"] = pscs

            for officer in officers:
                name = officer["name"]
                if name not in all_people:
                    all_people[name] = {"name": name, "roles": [], "companies": []}
                all_people[name]["roles"].append(officer["role"])
                all_people[name]["companies"].append(company["company_name"])

            for psc in pscs:
                name = psc["name"]
                if name not in all_people:
                    all_people[name] = {"name": name, "roles": [], "companies": []}
                all_people[name]["roles"].append("Person with Significant Control")
                if company["company_name"] not in all_people[name]["companies"]:
                    all_people[name]["companies"].append(company["company_name"])

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
            "(£3) for definitive ownership — it covers all properties including individually owned ones."
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
    global _ccod_data, _ocod_data

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

    # Reset cached data so it reloads on next query
    fn_upper = filename.upper()
    if "CCOD" in fn_upper:
        _ccod_data = None
        records = _load_ccod()
        return jsonify({"status": "ok", "type": "CCOD", "records": len(records), "filename": filename})
    elif "OCOD" in fn_upper:
        _ocod_data = None
        records = _load_ocod()
        return jsonify({"status": "ok", "type": "OCOD", "records": len(records), "filename": filename})
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
    global _ccod_data, _ocod_data
    chunk_file = request.files.get("chunk")
    upload_id  = request.form.get("uploadId", "")
    chunk_idx  = int(request.form.get("chunkIndex", 0))
    total      = int(request.form.get("totalChunks", 1))
    filename   = secure_filename(request.form.get("filename", "upload.csv"))
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
        _ccod_data = None
        records = _load_ccod()
        return jsonify({"status": "ok", "type": "CCOD", "records": len(records), "filename": filename})
    elif "OCOD" in fn_upper:
        _ocod_data = None
        records = _load_ocod()
        return jsonify({"status": "ok", "type": "OCOD", "records": len(records), "filename": filename})
    return jsonify({"status": "ok", "type": "unknown", "filename": filename})


@app.route("/api/load-from-url", methods=["POST"])
def load_from_url():
    """Download a CSV from a URL (e.g. Google Drive) and load it as CCOD or OCOD."""
    global _ccod_data, _ocod_data
    body = request.get_json(force=True, silent=True) or {}
    url  = body.get("url", "").strip()
    ftype = body.get("type", "").lower()  # "ccod" or "ocod"

    if not url:
        return jsonify({"error": "No URL provided"}), 400

    # Convert Google Drive share link to direct download
    import re as _re
    gd = _re.search(r'/file/d/([^/]+)', url)
    if gd:
        file_id = gd.group(1)
        url = f"https://drive.google.com/uc?export=download&id={file_id}&confirm=t"

    try:
        session = requests.Session()
        session.headers.update({"User-Agent": "Mozilla/5.0"})
        # First request — may return HTML warning page for large GDrive files
        r1 = session.get(url, stream=False, timeout=60)
        r1.raise_for_status()
        ct = r1.headers.get("Content-Type", "")
        if "text/html" in ct:
            # Extract confirmation token from Google Drive warning page
            tok = None
            m = _re.search(r'confirm=([0-9A-Za-z_]+)', r1.text)
            if m:
                tok = m.group(1)
            # Try usercontent.google.com direct download
            if gd:
                dl_url = f"https://drive.usercontent.google.com/download?id={gd.group(1)}&export=download&confirm=t"
                if tok:
                    dl_url += f"&uuid={tok}"
            else:
                dl_url = url + ("&confirm=t" if "?" in url else "?confirm=t")
            resp = session.get(dl_url, stream=True, timeout=600)
            resp.raise_for_status()
        else:
            resp = r1
            resp.raw.decode_content = True
    except Exception as e:
        return jsonify({"error": f"Download failed: {e}"}), 502

    # Determine filename
    cd = resp.headers.get("Content-Disposition", "")
    fn_match = _re.search(r'filename="?([^";]+)"?', cd)
    filename = fn_match.group(1) if fn_match else (ftype.upper() + "_download.csv")
    filename = secure_filename(filename)
    if not filename.lower().endswith(".csv"):
        filename += ".csv"
    if ftype == "ccod" and "CCOD" not in filename.upper():
        filename = "CCOD_" + filename
    if ftype == "ocod" and "OCOD" not in filename.upper():
        filename = "OCOD_" + filename

    data_dir = Path(DATA_DIR)
    data_dir.mkdir(parents=True, exist_ok=True)
    dest = data_dir / filename

    with open(str(dest), "wb") as f:
        for chunk in resp.iter_content(chunk_size=8 * 1024 * 1024):
            if chunk:
                f.write(chunk)

    fn_upper = filename.upper()
    if "CCOD" in fn_upper:
        _ccod_data = None
        records = _load_ccod()
        return jsonify({"status": "ok", "type": "CCOD", "records": len(records), "filename": filename})
    elif "OCOD" in fn_upper:
        _ocod_data = None
        records = _load_ocod()
        return jsonify({"status": "ok", "type": "OCOD", "records": len(records), "filename": filename})
    return jsonify({"status": "ok", "type": "unknown", "filename": filename})


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


# ─── Main ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))

    # Pre-load datasets on startup
    ccod = _load_ccod()
    ocod = _load_ocod()

    print(f"\n{'='*60}")
    print(f"  Property Owner Finder v2")
    print(f"  http://localhost:{port}")
    print(f"  Companies House API:     {'OK' if COMPANIES_HOUSE_API_KEY else 'NOT SET'}")
    print(f"  LR Business Gateway:     {'OK' if LR_BUSINESS_GATEWAY_USER else 'NOT SET (optional)'}")
    print(f"  CCOD records loaded:     {len(ccod):,}")
    print(f"  OCOD records loaded:     {len(ocod):,}")
    print(f"{'='*60}\n")
    app.run(host="0.0.0.0", port=port, debug=True)

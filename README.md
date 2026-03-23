# Property Owner Finder — Appear Here

A web tool that helps the landlord team research commercial property ownership in London. Enter an address (or batch of addresses) and the tool will:

1. **Companies House** — Search for companies registered at that address, retrieve their directors and persons with significant control (PSCs)
2. **Land Registry** — Generate direct links to HM Land Registry's title search (£3 per title) and price paid data
3. **LinkedIn** — Generate Google search links to find identified individuals on LinkedIn

## Quick Start

### 1. Get a Companies House API Key (free)

1. Go to [https://developer.company-information.service.gov.uk/](https://developer.company-information.service.gov.uk/)
2. Register for a free account
3. Create a new application → select **Live** environment
4. Copy your **API key**

### 2. Install & Run

```bash
# Install dependencies
pip install -r requirements.txt

# Set your API key
export COMPANIES_HOUSE_API_KEY="your-api-key-here"

# Run the app
python app.py
```

Then open [http://localhost:5000](http://localhost:5000) in your browser.

### 3. Using the Tool

**Single Lookup:** Enter a full property address (including postcode) and click "Search Owner".

**Batch Lookup:** Switch to the Batch tab, paste up to 50 addresses (one per line), and click "Search All". Results appear progressively.

**Export:** Click "Export CSV" to download all results as a spreadsheet-ready CSV file.

## How It Works

### Data Flow

```
Address Input
    │
    ├─→ Companies House API (free)
    │     ├─→ Search companies by postcode & street
    │     ├─→ Get officers (directors, secretaries)
    │     └─→ Get PSCs (persons with significant control)
    │
    ├─→ Land Registry (links generated)
    │     ├─→ Title search link (£3 per title, most definitive)
    │     └─→ Price paid data link (free)
    │
    └─→ LinkedIn (Google search links generated)
          └─→ Pre-built search: site:linkedin.com/in/ "Person Name" "Company"
```

### Important Notes

- **Companies House** only finds companies *registered* at that address. Many property-owning companies are registered at their accountant's or solicitor's address instead. If no company is found, the Land Registry title search is your best bet.
- **Land Registry** is the most reliable source for property ownership. The title register (£3) will name the legal owner — whether individual, UK company, or overseas entity.
- **LinkedIn links** open a Google search targeting LinkedIn profiles. They won't always find the right person, but they're a quick starting point.

## API Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/api/status` | GET | Check if API is configured |
| `/api/lookup` | POST | Look up a single address |
| `/api/batch` | POST | Look up multiple addresses |
| `/api/company/<number>` | GET | Get details for a specific company |

### Example: Single Lookup

```bash
curl -X POST http://localhost:5000/api/lookup \
  -H "Content-Type: application/json" \
  -d '{"address": "47 Berwick Street, London, W1F 8SJ"}'
```

### Example: Batch Lookup

```bash
curl -X POST http://localhost:5000/api/batch \
  -H "Content-Type: application/json" \
  -d '{"addresses": ["47 Berwick Street, London, W1F 8SJ", "12 Carnaby Street, London, W1F 9PS"]}'
```

## Optional: Land Registry Business Gateway

For high-volume automated title searches, you can apply for Land Registry Business Gateway access:

1. Visit [https://www.gov.uk/guidance/hm-land-registry-business-gateway](https://www.gov.uk/guidance/hm-land-registry-business-gateway)
2. This provides API access to title searches (still £3 per search)
3. Useful if you're doing 100+ lookups per week and want to automate the Land Registry step too

## Extending the Tool

**Add Google Maps / Places API** to validate addresses and get precise location data. Set `GOOGLE_MAPS_API_KEY` and uncomment the relevant section in `app.py`.

**Add Land Registry API** if you get Business Gateway access. This would let the tool automatically pull the registered owner without manual clicks.

**Add Overseas Entities Register** — since Aug 2022, overseas entities owning UK property must register with Companies House. Search the register at [https://find-and-update.company-information.service.gov.uk/register-an-overseas-entity](https://find-and-update.company-information.service.gov.uk/register-an-overseas-entity).

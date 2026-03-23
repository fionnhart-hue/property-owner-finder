# How to get Property Owner Finder running for the team

No coding required. You need to do 4 things, and anyone on the team can handle steps 1 and 2.

---

## What you need to do

### 1. Get a Companies House API key (free — 5 mins)

This lets the tool look up company directors and owners automatically.

- Go to **https://developer.company-information.service.gov.uk/**
- Click **Register** and sign up with your Appear Here email
- Once logged in, click **"Create an application"**
- Name: `Property Owner Finder` → Environment: **Live** → Create
- You'll see an **API key** — copy it and save it somewhere (you'll paste it into Railway in step 3)

---

### 2. Download the Land Registry ownership data (free — 10 mins)

This is a file that lists every property in England & Wales owned by a company. It's the most powerful part of the tool.

- Go to **https://use-land-property-data.service.gov.uk/**
- Sign in (create a free account with your Appear Here email)
- Search for and download these two files:
  - **"UK companies that own property in England and Wales"** (called CCOD — it's a big file, ~1GB)
  - **"Overseas companies that own property in England and Wales"** (called OCOD — much smaller)
- Save them somewhere on your computer — you'll upload them through the tool's settings page later

**Set a monthly reminder** to re-download these files (Land Registry updates them monthly).

---

### 3. Deploy the app to Railway (free tier or ~$5/month — 15 mins)

Railway is a hosting service that gives the tool a URL anyone on the team can access. No coding involved.

1. **Create a GitHub account** if you don't have one: https://github.com/signup
2. **Create a new repository**
3. **Upload the project files**
4. **Deploy to Railway**
5. **Add your API key**
6. **Get your URL**

---

### 4. Upload the Land Registry data files

- Open your Railway URL in a browser
- Click **"Settings & Data Upload"** at the top
- Upload the CCOD file
- Upload the OCOD file
- Go back to the main page — the status bar should show green dots

---

## Quick reference

| What | Where | Cost |
|---|---|---|
| Companies House API key | developer.company-information.service.gov.uk | Free |
| CCOD/OCOD data files | use-land-property-data.service.gov.uk | Free |
| Railway hosting | railway.app | Free tier or ~$5/month |
| LinkedIn | Automatic Google search links | Free |

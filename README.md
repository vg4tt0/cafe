# CAFE — Calendly → Freshworks Sync

**Performance:** Processes 8,000+ appointments in ~8 minutes (4.5 min Calendly fetch + ~2 min Freshsales upsert).

---

## What it does

1. Fetches all active Freshsales contacts matching the configured status IDs
2. For each contact, fetches their upcoming Calendly events (next 4 months, sorted soonest first)
3. Finds the first active event per contact and builds a Freshsales bulk upsert payload — next appointment type, date, and host
4. If a contact has no upcoming active appointments, their appointment fields are cleared
5. Posts the payload to Freshsales in batches of 100, up to 8 concurrent jobs, polling each until `SUCCESS`. Retries up to 10 times on 405 if the concurrent job limit is hit.

---

## Schedule

Runs automatically at **8am, 4pm, and midnight** (Sydney time) via Google Cloud Scheduler.

---

## Infrastructure

Runs on **Google Cloud Run Jobs** inside a private project in the `australia-southeast1` region. API tokens are stored in **Google Secret Manager**.

No Dockerfile is required. Google Cloud handles containerisation automatically via buildpacks. A `Procfile` is included to instruct the buildpack to run `python main.py` directly rather than a web server. To deploy:

```
gcloud run jobs deploy calendly-events --source . --region australia-southeast1 --project <YOUR_PROJECT_ID>
```

---

## Environment variables

| Variable | Description |
|---|---|
| `FRESHSALES_DOMAIN` | Freshsales subdomain (e.g. `mycompany`) |
| `FRESHSALES_API_KEY` | Freshsales API token |
| `CALENDLY_TOKEN` | Calendly personal access token |
| `CALENDLY_ORG` | Calendly organization URI (e.g. `https://api.calendly.com/organizations/YOUR_ORG_ID`) |

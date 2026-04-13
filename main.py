"""Fetch Freshsales contacts by status and their upcoming Calendly events."""
from __future__ import annotations

import asyncio
import json
import time
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from utils import RateLimiter

_cal_limiter = RateLimiter(max_calls=490, period=60.0)
_cal_sem: asyncio.Semaphore | None = None

# ── Configuration ─────────────────────────────────────────────────────────────
FS_DOMAIN  = os.environ["FRESHSALES_DOMAIN"]
FS_API_KEY = os.environ["FRESHSALES_API_KEY"]
CAL_TOKEN  = os.environ["CALENDLY_TOKEN"]
CAL_ORG    = os.environ["CALENDLY_ORG"]

FS_SEARCH_URL = f"https://{FS_DOMAIN}.myfreshworks.com/crm/sales/api/filtered_search/contact"
FS_UPSERT_URL = f"https://{FS_DOMAIN}.myfreshworks.com/crm/sales/api/contacts/bulk_upsert"
CAL_URL = "https://api.calendly.com/scheduled_events"

STATUS_IDS: list[str] = [
    "50000022594", "50000027492", "50000503116", "50000648486",
    "50000648488", "50000027496", "50000545835", "50000648489",
    "50000155699", "50000648491", "50000155700", "50000155701",
    "50000648492", "50000648610",
]

_EMPTY_CF = {"cf_next_appointment": None, "cf_next_appointment_date": None, "cf_next_appointment_with": None}

# ── Freshsales ────────────────────────────────────────────────────────────────
def fs_headers() -> dict[str, str]:
    return {"Authorization": f"Token token={FS_API_KEY}", "Content-Type": "application/json"}

async def fetch_contacts(client: httpx.AsyncClient) -> list[dict[str, Any]]:
    contacts, page = [], 1
    while True:
        body = {"filter_rule": [{"attribute": "contact_status_id", "operator": "is_in", "value": STATUS_IDS}],
                "sort": "updated_at", "sort_type": "desc", "page": page, "per_page": 100}
        resp = await client.post(FS_SEARCH_URL, headers=fs_headers(), json=body)
        resp.raise_for_status()
        batch = resp.json().get("contacts", [])
        if not batch: break
        contacts.extend(batch)
        page += 1
    return contacts

def contact_emails(c: dict[str, Any]) -> list[str]:
    return [e["value"] for e in c.get("emails", []) if e.get("value")]

def _active_event_fields(ev: dict[str, Any]) -> dict[str, Any]:
    host = (ev.get("event_memberships") or [{}])[0].get("user_name")
    return {"cf_next_appointment": ev.get("name"), "cf_next_appointment_date": ev.get("start_time"), "cf_next_appointment_with": host}

def appointment_fields(events: list[dict[str, Any]]) -> dict[str, Any]:
    ev = next((e for e in events if e.get("status") == "active"), None)
    cf = _active_event_fields(ev) if ev else _EMPTY_CF
    return {"custom_field": cf}

async def _post_batch(batch: list[dict[str, Any]], client: httpx.AsyncClient) -> str:
    for attempt in range(10):
        resp = await client.post(FS_UPSERT_URL, headers=fs_headers(), json={"contacts": batch})
        if resp.status_code != 405:
            break
        await asyncio.sleep(5)
    resp.raise_for_status()
    url = resp.json()["job_status_url"]
    print(f"Batch job: {url}", flush=True)
    return url

async def poll_job(url: str, client: httpx.AsyncClient) -> dict[str, Any]:
    while True:
        resp = await client.get(url, headers=fs_headers())
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") in ("SUCCESS",):
            return data
        await asyncio.sleep(2)

async def _run_batch(b: list[dict[str, Any]], client: httpx.AsyncClient, sem: asyncio.Semaphore) -> dict[str, Any]:
    async with sem:
        url = await _post_batch(b, client)
        return await poll_job(url, client)

async def upsert_contacts(contacts, evts, client) -> list[dict[str, Any]]:
    payload = [{"id": str(c["id"]), "data": appointment_fields(evs)} for c, evs in zip(contacts, evts)]
    batches = [payload[i:i + 100] for i in range(0, len(payload), 100)]
    sem = asyncio.Semaphore(8)
    return list(await asyncio.gather(*[_run_batch(b, client, sem) for b in batches]))

# ── Calendly ──────────────────────────────────────────────────────────────────
def cal_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {CAL_TOKEN}", "Content-Type": "application/json"}

async def _cal_request(email: str, client: httpx.AsyncClient) -> httpx.Response:
    now, fmt = datetime.now(timezone.utc), "%Y-%m-%dT%H:%M:%SZ"
    return await client.get(CAL_URL, headers=cal_headers(), params={
        "organization": CAL_ORG, "invitee_email": email, "sort": "start_time:asc",
        "count": "100", "min_start_time": now.strftime(fmt), "max_start_time": (now + timedelta(days=122)).strftime(fmt),
    })

async def _throttled_request(email: str, client: httpx.AsyncClient) -> httpx.Response:
    global _cal_sem
    if _cal_sem is None:
        _cal_sem = asyncio.Semaphore(8)
    async with _cal_sem:
        await _cal_limiter.wait()
        return await _cal_request(email, client)

async def fetch_events(email: str, client: httpx.AsyncClient) -> list[dict[str, Any]]:
    resp = await _throttled_request(email, client)
    if resp.status_code == 429:
        await asyncio.sleep(60)
        return await fetch_events(email, client)
    resp.raise_for_status()
    return resp.json().get("collection", [])

# ── Aggregation ───────────────────────────────────────────────────────────────
async def events_for_contact(c: dict[str, Any], client: httpx.AsyncClient) -> list[dict[str, Any]]:
    results = await asyncio.gather(*[fetch_events(email, client) for email in contact_emails(c)])
    return sorted([ev for batch in results for ev in batch], key=lambda e: e["start_time"])

# ── Entry point ───────────────────────────────────────────────────────────────
async def _fetch_all_events(contacts: list[dict[str, Any]], client: httpx.AsyncClient) -> list:
    t0 = time.time()
    evts = await asyncio.gather(*[events_for_contact(c, client) for c in contacts])
    print(f"Calendly: {time.time() - t0:.1f}s", flush=True)
    return list(evts)

async def _upsert_all(contacts, evts, client) -> list[dict[str, Any]]:
    t1 = time.time()
    results = await upsert_contacts(contacts, evts, client)
    print(f"Upsert: {time.time() - t1:.1f}s", flush=True)
    return results

def _report_failures(results: list[dict[str, Any]]) -> None:
    total_failed = 0
    for r in results:
        data = json.loads(r.get("data") or "{}")
        failed = data.get("record_status", {}).get("failed", 0)
        total_failed += failed
        for record in data.get("detailed_failure_report", []):
            print(f"FAILED: {record}", flush=True)
    print(f"Done. {sum(json.loads(r.get('data') or '{}').get('record_status', {}).get('succeeded', 0) for r in results)} succeeded, {total_failed} failed.", flush=True)

async def _main() -> None:
    async with httpx.AsyncClient(timeout=30) as client:
        contacts = await fetch_contacts(client)
        evts = await _fetch_all_events(contacts, client)
        results = await _upsert_all(contacts, evts, client)
    _report_failures(results)

if __name__ == "__main__":
    asyncio.run(_main())

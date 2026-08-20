"""Fetch Freshsales contacts by status and their upcoming Calendly events."""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import time
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from utils import RateLimiter

_cal_limiter = RateLimiter(max_calls=490, period=60.0)
_cal_sem: asyncio.Semaphore | None = None
_cal_requests_started = 0
_cal_requests_finished = 0
_cal_requests_in_flight = 0
_cal_timeouts = 0

# ── Configuration ─────────────────────────────────────────────────────────────
FS_DOMAIN  = os.environ["FRESHSALES_DOMAIN"]
FS_API_KEY = os.environ["FRESHSALES_API_KEY"]
CAL_TOKEN  = os.environ["CALENDLY_TOKEN"]
CAL_ORG    = os.environ["CALENDLY_ORG"]

FS_SEARCH_URL = f"https://{FS_DOMAIN}.myfreshworks.com/crm/sales/api/filtered_search/contact"
FS_UPSERT_URL = f"https://{FS_DOMAIN}.myfreshworks.com/crm/sales/api/contacts/bulk_upsert"
FS_CONTACT_URL = f"https://{FS_DOMAIN}.myfreshworks.com/crm/sales/api/contacts"
CAL_URL = "https://api.calendly.com/scheduled_events"

STATUS_IDS: list[str] = [
    "50000022594", "50000027492", "50000503116", "50000648486",
    "50000648488", "50000027496", "50000545835", "50000648489",
    "50000155699", "50000648491", "50000155700", "50000155701",
    "50000648492", "50000648610",
]

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

def _next_appt_is_blank(custom_field: dict[str, Any]) -> bool:
    """True only if the contact has no next appointment recorded.

    Cafe must never overwrite a populated value: clinical appointments are set
    by Sunny / Medirecords, which Cafe (Calendly-only) cannot see. Treating a
    populated value as 'not blank' protects it from being clobbered or cleared.
    """
    cf = custom_field or {}
    return cf.get("cf_next_appointment") in (None, "") and cf.get("cf_next_appointment_date") in (None, "")

async def fetch_contact_custom_field(contact_id: Any, client: httpx.AsyncClient) -> dict[str, Any]:
    """Read a single contact's current custom_field (the bulk filtered_search
    response does not include custom fields, so we must GET per candidate)."""
    resp = await client.get(f"{FS_CONTACT_URL}/{contact_id}", headers=fs_headers())
    resp.raise_for_status()
    return (resp.json().get("contact") or {}).get("custom_field") or {}

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
    # Fill-only-if-blank. Cafe NEVER clears and NEVER overwrites: a contact's
    # next-appointment fields are written only when BOTH (a) Calendly has an
    # upcoming active event for them AND (b) the contact currently has no next
    # appointment recorded. Contacts with no active Calendly event are skipped
    # entirely (so a clinical appointment set by Sunny is left untouched).
    candidates = []
    for c, evs in zip(contacts, evts):
        active = next((e for e in evs if e.get("status") == "active"), None)
        if active:
            candidates.append((c, active))

    read_sem = asyncio.Semaphore(8)
    async def _consider(c, active):
        async with read_sem:
            current = await fetch_contact_custom_field(c["id"], client)
        if _next_appt_is_blank(current):
            return {"id": str(c["id"]), "data": {"custom_field": _active_event_fields(active)}}
        return None

    considered = await asyncio.gather(*[_consider(c, a) for c, a in candidates])
    payload = [p for p in considered if p]
    print(f"Fill-if-blank: {len(payload)} of {len(candidates)} Calendly-bookable contacts were blank and will be filled.", flush=True)
    if not payload:
        return []
    batches = [payload[i:i + 100] for i in range(0, len(payload), 100)]
    sem = asyncio.Semaphore(8)
    return list(await asyncio.gather(*[_run_batch(b, client, sem) for b in batches]))

# ── Calendly ──────────────────────────────────────────────────────────────────
def cal_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {CAL_TOKEN}", "Content-Type": "application/json"}

def _cal_email_ref(email: str) -> str:
    """Return a stable, non-reversible identifier for an email within this token."""
    return hmac.new(
        CAL_TOKEN.encode(), email.strip().lower().encode(), hashlib.sha256
    ).hexdigest()[:12]

async def _cal_request(email: str, client: httpx.AsyncClient) -> httpx.Response:
    global _cal_requests_started, _cal_requests_finished, _cal_requests_in_flight, _cal_timeouts

    _cal_requests_started += 1
    request_number = _cal_requests_started
    _cal_requests_in_flight += 1
    email_ref = _cal_email_ref(email)
    started = time.monotonic()

    if request_number == 1 or request_number % 100 == 0:
        print(
            "Calendly progress: "
            f"request={request_number} finished={_cal_requests_finished} "
            f"in_flight={_cal_requests_in_flight} timeouts={_cal_timeouts} "
            f"email_ref={email_ref}",
            flush=True,
        )

    now, fmt = datetime.now(timezone.utc), "%Y-%m-%dT%H:%M:%SZ"
    try:
        response = await client.get(CAL_URL, headers=cal_headers(), params={
            "organization": CAL_ORG, "invitee_email": email, "sort": "start_time:asc",
            "count": "50", "min_start_time": now.strftime(fmt), "max_start_time": (now + timedelta(days=122)).strftime(fmt),
        })
        elapsed = time.monotonic() - started
        if elapsed >= 10 or response.status_code >= 400:
            print(
                "Calendly response: "
                f"request={request_number} status={response.status_code} "
                f"elapsed={elapsed:.1f}s finished={_cal_requests_finished + 1} "
                f"in_flight={_cal_requests_in_flight} email_ref={email_ref}",
                flush=True,
            )
        return response
    except httpx.TimeoutException as exc:
        _cal_timeouts += 1
        elapsed = time.monotonic() - started
        print(
            "Calendly timeout: "
            f"request={request_number} type={type(exc).__name__} "
            f"elapsed={elapsed:.1f}s started={_cal_requests_started} "
            f"finished={_cal_requests_finished + 1} "
            f"in_flight={_cal_requests_in_flight} timeouts={_cal_timeouts} "
            f"email_ref={email_ref}",
            flush=True,
        )
        raise
    except httpx.TransportError as exc:
        elapsed = time.monotonic() - started
        print(
            "Calendly transport error: "
            f"request={request_number} type={type(exc).__name__} "
            f"elapsed={elapsed:.1f}s finished={_cal_requests_finished + 1} "
            f"in_flight={_cal_requests_in_flight} email_ref={email_ref}",
            flush=True,
        )
        raise
    finally:
        _cal_requests_in_flight -= 1
        _cal_requests_finished += 1

async def _throttled_request(email: str, client: httpx.AsyncClient) -> httpx.Response:
    global _cal_sem
    if _cal_sem is None:
        _cal_sem = asyncio.Semaphore(8)
    async with _cal_sem:
        await _cal_limiter.wait()
        return await _cal_request(email, client)

async def fetch_events(email: str, client: httpx.AsyncClient) -> list[dict[str, Any]]:
    max_attempts = 5
    retryable_statuses = {429, 500, 502, 503, 504}

    for attempt in range(1, max_attempts + 1):
        try:
            resp = await _throttled_request(email, client)
            resp.raise_for_status()
            return resp.json().get("collection", [])
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            if status not in retryable_statuses or attempt == max_attempts:
                raise
            delay = 60 if status == 429 else min(2 ** (attempt - 1), 30)
            reason = f"HTTP {status}"
        except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError) as exc:
            if attempt == max_attempts:
                raise
            delay = min(2 ** (attempt - 1), 30)
            reason = type(exc).__name__

        print(
            f"Calendly request failed ({reason}); retrying in {delay}s "
            f"(attempt {attempt + 1}/{max_attempts})",
            flush=True,
        )
        await asyncio.sleep(delay)

    raise RuntimeError("Calendly retry loop ended unexpectedly")

# ── Aggregation ───────────────────────────────────────────────────────────────
async def events_for_contact(c: dict[str, Any], client: httpx.AsyncClient) -> list[dict[str, Any]]:
    emails = contact_emails(c)
    results = await asyncio.gather(*[fetch_events(email, client) for email in emails], return_exceptions=True)
    batches = []
    for email, result in zip(emails, results):
        if isinstance(result, BaseException):
            print(f"Calendly lookup failed for contact {c.get('id')} ({_cal_email_ref(email)}): {result!r}", flush=True)
            continue
        batches.append(result)
    return sorted([ev for batch in batches for ev in batch], key=lambda e: e["start_time"])

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
    print("Sync started.")
    timeout_config = httpx.Timeout(40.0)
    async with httpx.AsyncClient(timeout=timeout_config) as client:
        contacts = await fetch_contacts(client)
        email_lookups = sum(len(contact_emails(contact)) for contact in contacts)
        print(
            f"Freshsales loaded: contacts={len(contacts)} "
            f"calendly_lookups={email_lookups}",
            flush=True,
        )
        evts = await _fetch_all_events(contacts, client)
        results = await _upsert_all(contacts, evts, client)
    _report_failures(results)
    print("Sync finished.")

if __name__ == "__main__":
    asyncio.run(_main())

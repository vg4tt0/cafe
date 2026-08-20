"""Tests for main.py and utils.py."""
import asyncio
import json
import os
import pytest
import httpx
from unittest.mock import patch, AsyncMock

os.environ.setdefault("FRESHSALES_DOMAIN", "test")
os.environ.setdefault("FRESHSALES_API_KEY", "test")
os.environ.setdefault("CALENDLY_TOKEN", "test")
os.environ.setdefault("CALENDLY_ORG", "test")

from main import (
    contact_emails,
    appointment_fields,
    _active_event_fields,
    fetch_events,
    events_for_contact,
    fetch_contacts,
    poll_job,
    upsert_contacts,
    _post_batch,
    _run_batch,
    _report_failures,
    _fetch_all_events,
    _upsert_all,
    fs_headers,
    cal_headers,
    _cal_limiter,
)
from utils import RateLimiter


# ── contact_emails ─────────────────────────────────────────────────────────────

def test_contact_emails_returns_values():
    c = {"emails": [{"value": "a@b.com"}, {"value": "c@d.com"}]}
    assert contact_emails(c) == ["a@b.com", "c@d.com"]

def test_contact_emails_skips_empty():
    c = {"emails": [{"value": "a@b.com"}, {"value": ""}]}
    assert contact_emails(c) == ["a@b.com"]

def test_contact_emails_no_emails_key():
    assert contact_emails({}) == []


# ── _active_event_fields ───────────────────────────────────────────────────────

def test_active_event_fields():
    ev = {"name": "Month 6", "start_time": "2026-05-01T10:00:00Z", "event_memberships": [{"user_name": "Staff Member"}]}
    result = _active_event_fields(ev)
    assert result["cf_next_appointment"] == "Month 6"
    assert result["cf_next_appointment_date"] == "2026-05-01T10:00:00Z"
    assert result["cf_next_appointment_with"] == "Staff Member"

def test_active_event_fields_no_memberships():
    ev = {"name": "Check-in", "start_time": "2026-05-01T10:00:00Z", "event_memberships": []}
    assert _active_event_fields(ev)["cf_next_appointment_with"] is None


# ── appointment_fields ─────────────────────────────────────────────────────────

def test_appointment_fields_active_event():
    events = [{"status": "active", "name": "Month 6", "start_time": "2026-05-01T10:00:00Z",
                "event_memberships": [{"user_name": "Dr Smith"}]}]
    result = appointment_fields(events)["custom_field"]
    assert result["cf_next_appointment"] == "Month 6"
    assert result["cf_next_appointment_date"] == "2026-05-01T10:00:00Z"
    assert result["cf_next_appointment_with"] == "Staff Member"

def test_appointment_fields_skips_cancelled():
    events = [{"status": "cancelled", "name": "Old", "start_time": "2026-04-01T10:00:00Z",
                "event_memberships": [{"user_name": "Dr Smith"}]}]
    result = appointment_fields(events)["custom_field"]
    assert result["cf_next_appointment"] is None

def test_appointment_fields_empty():
    result = appointment_fields([])
    assert result == {"custom_field": {"cf_next_appointment": None, "cf_next_appointment_date": None, "cf_next_appointment_with": None}}


# ── RateLimiter ────────────────────────────────────────────────────────────────

def test_rate_limiter_allows_under_limit():
    async def run():
        limiter = RateLimiter(max_calls=5, period=60.0)
        for _ in range(5):
            await limiter.wait()
        assert len(limiter._calls) == 5
    asyncio.run(run())

def test_rate_limiter_lock_created_lazily():
    limiter = RateLimiter(max_calls=5, period=60.0)
    assert limiter._lock is None
    async def run():
        await limiter.wait()
        assert limiter._lock is not None
    asyncio.run(run())


# ── fetch_events ───────────────────────────────────────────────────────────────

def test_fetch_events_retries_on_429():
    call_count = {"n": 0}
    def handler(req):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return httpx.Response(429)
        return httpx.Response(200, json={"collection": []})
    async def run():
        with patch("main.asyncio.sleep", new_callable=AsyncMock):
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                result = await fetch_events("test@example.com", client)
        assert call_count["n"] == 2
        assert result == []
    asyncio.run(run())

def test_fetch_events_retries_on_read_timeout(capsys):
    call_count = {"n": 0}
    def handler(req):
        call_count["n"] += 1
        if call_count["n"] < 3:
            raise httpx.ReadTimeout("timed out", request=req)
        return httpx.Response(200, json={"collection": []})
    async def run():
        with patch("main.asyncio.sleep", new_callable=AsyncMock) as sleep:
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                result = await fetch_events("test@example.com", client)
        assert call_count["n"] == 3
        assert sleep.await_count == 2
        assert result == []
    asyncio.run(run())
    output = capsys.readouterr().out
    assert "Calendly timeout:" in output
    assert "request=" in output
    assert "email_ref=" in output

def test_fetch_events_raises_on_error():
    call_count = {"n": 0}
    def handler(req):
        call_count["n"] += 1
        return httpx.Response(500)
    async def run():
        transport = httpx.MockTransport(handler)
        with patch("main.asyncio.sleep", new_callable=AsyncMock):
            async with httpx.AsyncClient(transport=transport) as client:
                with pytest.raises(httpx.HTTPStatusError):
                    await fetch_events("test@example.com", client)
        assert call_count["n"] == 5
    asyncio.run(run())


# ── poll_job ───────────────────────────────────────────────────────────────────

def test_poll_job_returns_when_complete():
    responses = iter([
        httpx.Response(200, json={"status": "IN_PROGRESS"}),
        httpx.Response(200, json={"status": "SUCCESS"}),
    ])
    async def run():
        transport = httpx.MockTransport(lambda req: next(responses))
        async with httpx.AsyncClient(transport=transport) as client:
            with patch("main.asyncio.sleep", new_callable=AsyncMock):
                result = await poll_job("https://example.com/job/123", client)
            assert result["status"] == "SUCCESS"
    asyncio.run(run())


# ── events_for_contact ─────────────────────────────────────────────────────────

def test_events_for_contact_sorted():
    async def run():
        events = [
            {"start_time": "2026-06-01T10:00:00Z", "status": "active"},
            {"start_time": "2026-05-01T10:00:00Z", "status": "active"},
        ]
        transport = httpx.MockTransport(
            lambda req: httpx.Response(200, json={"collection": events})
        )
        async with httpx.AsyncClient(transport=transport) as client:
            c = {"emails": [{"value": "a@b.com"}]}
            result = await events_for_contact(c, client)
            assert result[0]["start_time"] < result[1]["start_time"]
    asyncio.run(run())

def test_events_for_contact_no_emails():
    async def run():
        transport = httpx.MockTransport(
            lambda req: httpx.Response(200, json={"collection": []})
        )
        async with httpx.AsyncClient(transport=transport) as client:
            result = await events_for_contact({}, client)
            assert result == []
    asyncio.run(run())


# ── fs_headers / cal_headers ───────────────────────────────────────────────────

def test_fs_headers():
    h = fs_headers()
    assert "Authorization" in h
    assert h["Content-Type"] == "application/json"

def test_cal_headers():
    h = cal_headers()
    assert h["Authorization"].startswith("Bearer ")
    assert h["Content-Type"] == "application/json"


# ── fetch_contacts ─────────────────────────────────────────────────────────────

def test_fetch_contacts_paginates():
    page = {"page": 0}
    def handler(req):
        page["page"] += 1
        if page["page"] == 1:
            return httpx.Response(200, json={"contacts": [{"id": 1}, {"id": 2}]})
        return httpx.Response(200, json={"contacts": []})
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            contacts = await fetch_contacts(client)
            assert len(contacts) == 2
    asyncio.run(run())


# ── _post_batch ────────────────────────────────────────────────────────────────

def test_post_batch_returns_job_url():
    async def run():
        transport = httpx.MockTransport(
            lambda req: httpx.Response(200, json={"job_status_url": "https://example.com/job/1"})
        )
        async with httpx.AsyncClient(transport=transport) as client:
            url = await _post_batch([{"id": "1", "data": {}}], client)
            assert url == "https://example.com/job/1"
    asyncio.run(run())

def test_post_batch_raises_on_error():
    async def run():
        transport = httpx.MockTransport(
            lambda req: httpx.Response(400, json={"error": "bad request"})
        )
        async with httpx.AsyncClient(transport=transport) as client:
            with pytest.raises(httpx.HTTPStatusError):
                await _post_batch([{"id": "1", "data": {}}], client)
    asyncio.run(run())

def test_post_batch_retries_on_405():
    call_count = {"n": 0}
    def handler(req):
        call_count["n"] += 1
        if call_count["n"] < 3:
            return httpx.Response(405)
        return httpx.Response(200, json={"job_status_url": "https://example.com/job/1"})
    async def run():
        with patch("main.asyncio.sleep", new_callable=AsyncMock):
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                url = await _post_batch([{"id": "1", "data": {}}], client)
        assert url == "https://example.com/job/1"
        assert call_count["n"] == 3
    asyncio.run(run())

def test_post_batch_raises_after_max_retries():
    async def run():
        with patch("main.asyncio.sleep", new_callable=AsyncMock):
            async with httpx.AsyncClient(transport=httpx.MockTransport(lambda req: httpx.Response(405))) as client:
                with pytest.raises(httpx.HTTPStatusError):
                    await _post_batch([{"id": "1", "data": {}}], client)
    asyncio.run(run())


# ── _run_batch ─────────────────────────────────────────────────────────────────

def test_run_batch():
    async def run():
        def handler(req):
            if "bulk_upsert" in str(req.url):
                return httpx.Response(200, json={"job_status_url": "https://example.com/job/1"})
            return httpx.Response(200, json={"status": "SUCCESS"})
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            sem = asyncio.Semaphore(1)
            with patch("main.asyncio.sleep", new_callable=AsyncMock):
                result = await _run_batch([{"id": "1", "data": {}}], client, sem)
            assert result["status"] == "SUCCESS"
    asyncio.run(run())


# ── upsert_contacts ────────────────────────────────────────────────────────────

def test_upsert_contacts_batches_and_polls():
    async def run():
        def handler(req):
            if "bulk_upsert" in str(req.url):
                return httpx.Response(200, json={"job_status_url": "https://example.com/job/1"})
            return httpx.Response(200, json={"status": "SUCCESS"})
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            contacts = [{"id": i} for i in range(3)]
            evts = [[] for _ in range(3)]
            with patch("main.asyncio.sleep", new_callable=AsyncMock):
                results = await upsert_contacts(contacts, evts, client)
            assert len(results) == 1
            assert results[0]["status"] == "SUCCESS"
    asyncio.run(run())


# ── _report_failures ───────────────────────────────────────────────────────────

def test_report_failures_no_failures(capsys):
    results = [{"data": json.dumps({"record_status": {"succeeded": 5, "failed": 0}, "detailed_failure_report": []})}]
    _report_failures(results)
    out = capsys.readouterr().out
    assert "5 succeeded, 0 failed" in out

def test_report_failures_with_failures(capsys):
    results = [{"data": json.dumps({"record_status": {"succeeded": 2, "failed": 1}, "detailed_failure_report": [{"id": "99", "errors": "bad"}]})}]
    _report_failures(results)
    out = capsys.readouterr().out
    assert "FAILED:" in out
    assert "2 succeeded, 1 failed" in out

def test_report_failures_empty_data(capsys):
    _report_failures([{"data": None}])
    out = capsys.readouterr().out
    assert "0 succeeded, 0 failed" in out


# ── _fetch_all_events / _upsert_all ───────────────────────────────────────────

def test_fetch_all_events():
    async def run():
        transport = httpx.MockTransport(lambda req: httpx.Response(200, json={"collection": []}))
        async with httpx.AsyncClient(transport=transport) as client:
            contacts = [{"emails": [{"value": "a@b.com"}]}]
            result = await _fetch_all_events(contacts, client)
            assert result == [[]]
    asyncio.run(run())

def test_upsert_all():
    async def run():
        def handler(req):
            if "bulk_upsert" in str(req.url):
                return httpx.Response(200, json={"job_status_url": "https://example.com/job/1"})
            return httpx.Response(200, json={"status": "SUCCESS"})
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with patch("main.asyncio.sleep", new_callable=AsyncMock):
                results = await _upsert_all([{"id": 1}], [[]], client)
            assert results[0]["status"] == "SUCCESS"
    asyncio.run(run())


# ── RateLimiter sleep path ─────────────────────────────────────────────────────

def test_rate_limiter_sleeps_when_limit_reached():
    import time
    async def run():
        limiter = RateLimiter(max_calls=2, period=0.2)
        await limiter.wait()
        await limiter.wait()
        start = time.time()
        await limiter.wait()
        assert time.time() - start >= 0.1
    asyncio.run(run())

import json
import statistics
import time
from concurrent.futures import ThreadPoolExecutor

from demo_client import Demo


def main():
    demo = Demo()
    try:
        with ThreadPoolExecutor(max_workers=20) as pool:
            events = list(pool.map(lambda _: demo.event("same-measurement"), range(30)))
        assert len({event["id"] for event in events}) == 1
        start = time.monotonic()
        first = demo.wait(demo.enqueue())
        issue_ms = (time.monotonic() - start) * 1000
        assert first["calculation"]["cumulative_cents"] == 1
        demo.event("late", "100")
        second = demo.wait(demo.enqueue())
        assert second["revision"] == 2 and second["calculation"]["delta_cents"] == 0
        demo.request(
            "POST",
            "/corrections",
            json={
                "event_id": "credit",
                "original_id": events[0]["id"],
                "quantity": "500",
                "reason": "Source counted the same usage twice",
            },
        )
        third = demo.wait(demo.enqueue())
        assert third["calculation"]["delta_cents"] == -1
        assert third["calculation"]["cumulative_cents"] == 0
        assert demo.request("GET", "/invoices/" + first["id"]) == first
        for invoice in [first, second, third]:
            demo.verify(invoice)
        unchanged = demo.wait(demo.enqueue())
        assert unchanged["id"] == third["id"]
        times = []
        for _ in range(50):
            start = time.monotonic()
            demo.request("GET", "/invoices/" + first["id"])
            times.append((time.monotonic() - start) * 1000)
        print(
            json.dumps(
                {
                    "ok": True,
                    "concurrent_duplicate_requests": 30,
                    "distinct_events": 1,
                    "original_invoice_unchanged": True,
                    "late_event_zero_cent_adjustment": True,
                    "credit_cents": -1,
                    "replayed_invoices": 3,
                    "unchanged_period_reused": True,
                    "invoice_completion_ms": round(issue_ms, 2),
                    "read_requests": len(times),
                    "read_p50_ms": round(statistics.median(times), 2),
                    "read_p95_ms": round(sorted(times)[47], 2),
                },
                indent=2,
            )
        )
    finally:
        demo.close()


if __name__ == "__main__":
    main()

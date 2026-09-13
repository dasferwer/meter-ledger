import os
import time
from uuid import uuid4

import httpx

ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "local-demo-meterledger-admin-change-before-deployment")
PERIOD = "2024-01"


class Demo:
    def __init__(self, base_url=None):
        self.client = httpx.Client(
            base_url=base_url or os.getenv("BASE_URL", "http://localhost:8000"), timeout=45
        )
        response = self.client.post(
            "/admin/tenants",
            headers={"X-Admin-Token": ADMIN_TOKEN},
            json={"name": "Verification " + str(uuid4())[:8]},
        )
        response.raise_for_status()
        tenant = response.json()
        self.tenant_id = tenant["id"]
        self.client.headers["X-API-Key"] = tenant["api_key"]

    def request(self, method, path, **kwargs):
        response = self.client.request(method, path, **kwargs)
        response.raise_for_status()
        return response.json()

    def event(self, key=None, quantity="500"):
        return self.request(
            "POST",
            "/events",
            json={
                "event_id": key or str(uuid4()),
                "metric": "api_calls",
                "quantity": quantity,
                "occurred_at": "2024-01-15T00:00:00Z",
            },
        )

    def enqueue(self):
        return self.request(
            "POST",
            "/billing/issue",
            headers={"Idempotency-Key": str(uuid4())},
            json={"period": PERIOD},
        )

    def wait(self, job, timeout=75):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            state = self.request("GET", "/jobs/" + job["id"])
            if state["status"] == "done":
                return self.request("GET", "/invoices/" + state["invoice_id"])
            if state["status"] == "failed":
                raise RuntimeError("Invoice job failed")
            time.sleep(0.1)
        raise TimeoutError("Invoice job did not finish")

    def verify(self, invoice):
        result = self.request("GET", f"/invoices/{invoice['id']}/verify")
        assert result["matches"], result
        return result

    def close(self):
        self.client.close()

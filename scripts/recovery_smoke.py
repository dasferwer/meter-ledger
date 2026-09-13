import json
import os
import subprocess
import time
from pathlib import Path

from demo_client import Demo

ROOT = Path(__file__).resolve().parents[1]
BASE_URL = os.getenv("BASE_URL", "http://localhost:8210")


def compose(*args, extra_env=None):
    return subprocess.run(
        ["docker", "compose", *args],
        cwd=ROOT,
        env={**os.environ, **(extra_env or {})},
        capture_output=True,
        text=True,
        check=True,
        timeout=90,
    ).stdout.strip()


def sql(query):
    return compose(
        "exec", "-T", "database", "psql", "-U", "postgres", "-d", "billing", "-At", "-c", query
    )


def wait_for(predicate, timeout=45):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.3)
    raise TimeoutError("Recovery precondition was not reached")


def restart(role, variable, delay):
    compose("up", "-d", "--no-deps", "--force-recreate", role, extra_env={variable: str(delay)})


def queue_messages():
    output = compose(
        "exec",
        "-T",
        "rabbitmq",
        "rabbitmqctl",
        "list_queues",
        "name",
        "messages",
        "--no-table-headers",
        "--quiet",
    )
    for line in output.splitlines():
        columns = line.split()
        if len(columns) == 2 and columns[0] == "meterledger.invoice":
            return int(columns[1])
    raise RuntimeError("Invoice queue is missing")


def main():
    results = {}
    demos = []
    try:
        demo = Demo(BASE_URL)
        demos.append(demo)
        compose("stop", "--timeout", "10", "rabbitmq")
        demo.event()
        job = demo.enqueue()
        assert demo.request("GET", "/jobs/" + job["id"])["status"] == "pending"
        compose("start", "rabbitmq")
        invoice = demo.wait(job)
        demo.verify(invoice)
        results["broker_outage_preserves_usage_and_invoice_request"] = True

        demo = Demo(BASE_URL)
        demos.append(demo)
        restart("worker", "WORKER_BEFORE_COMMIT_DELAY", 30)
        demo.event()
        job = demo.enqueue()
        wait_for(
            lambda: (
                int(
                    sql(
                        "SELECT count(*) FROM pg_stat_activity WHERE datname='billing' AND usename='billing' AND state='idle in transaction' AND query LIKE '%UPDATE jobs SET status=%'"
                    )
                )
                > 0
            )
        )
        compose("kill", "-s", "SIGKILL", "worker")
        assert sql(f"SELECT count(*) FROM invoices WHERE tenant_id='{demo.tenant_id}'") == "0"
        restart("worker", "WORKER_BEFORE_COMMIT_DELAY", 0)
        invoice = demo.wait(job)
        demo.verify(invoice)
        assert sql(f"SELECT count(*) FROM invoices WHERE tenant_id='{demo.tenant_id}'") == "1"
        results["worker_killed_before_commit_rolls_back_and_recovers"] = True

        demo = Demo(BASE_URL)
        demos.append(demo)
        wait_for(lambda: queue_messages() == 0)
        compose("stop", "--timeout", "10", "worker")
        restart("dispatcher", "DISPATCHER_AFTER_PUBLISH_DELAY", 30)
        demo.event()
        job = demo.enqueue()

        wait_for(lambda: queue_messages() >= 1)
        compose("kill", "-s", "SIGKILL", "dispatcher")
        assert sql(f"SELECT published_at IS NULL FROM outbox WHERE job_id='{job['id']}'") == "t"
        restart("dispatcher", "DISPATCHER_AFTER_PUBLISH_DELAY", 0)
        wait_for(
            lambda: int(sql(f"SELECT publications FROM outbox WHERE job_id='{job['id']}'")) >= 1
        )
        wait_for(lambda: queue_messages() >= 2)
        compose("start", "worker")
        invoice = demo.wait(job)
        demo.verify(invoice)
        assert sql(f"SELECT count(*) FROM invoices WHERE tenant_id='{demo.tenant_id}'") == "1"
        results["publisher_confirm_crash_gap_does_not_duplicate_invoice"] = True
        print(json.dumps({"ok": True, "scenarios": results}, indent=2))
    finally:
        compose("start", "rabbitmq")
        restart("worker", "WORKER_BEFORE_COMMIT_DELAY", 0)
        restart("dispatcher", "DISPATCHER_AFTER_PUBLISH_DELAY", 0)
        for demo in demos:
            demo.close()


if __name__ == "__main__":
    main()

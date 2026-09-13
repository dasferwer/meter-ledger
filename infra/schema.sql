CREATE TABLE tenants (
    id uuid PRIMARY KEY,
    name text NOT NULL,
    key_hash text NOT NULL UNIQUE,
    sequence bigint NOT NULL DEFAULT 0 CHECK (sequence >= 0),
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE rates (
    id uuid PRIMARY KEY,
    metric text NOT NULL CHECK (metric IN ('api_calls','storage_gb_hours','compute_seconds')),
    effective_at timestamptz NOT NULL,
    unit_price numeric(15,8) NOT NULL CHECK (unit_price > 0 AND unit_price != 'NaN'),
    currency text NOT NULL DEFAULT 'USD' CHECK (currency = 'USD'),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE(metric, effective_at)
);
CREATE TABLE events (
    id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL REFERENCES tenants(id),
    sequence bigint NOT NULL,
    external_id text NOT NULL,
    body_hash text NOT NULL,
    metric text NOT NULL,
    quantity numeric(16,6) NOT NULL CHECK (quantity != 0 AND quantity != 'NaN'),
    occurred_at timestamptz NOT NULL,
    received_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    rate_id uuid NOT NULL REFERENCES rates(id),
    unit_price numeric(15,8) NOT NULL,
    original_id uuid,
    reason text,
    UNIQUE(tenant_id, sequence),
    UNIQUE(tenant_id, external_id),
    UNIQUE(tenant_id, id),
    FOREIGN KEY(tenant_id, original_id) REFERENCES events(tenant_id, id),
    CHECK ((quantity > 0 AND original_id IS NULL AND reason IS NULL) OR
           (quantity < 0 AND original_id IS NOT NULL AND reason IS NOT NULL))
);
CREATE INDEX events_period ON events(tenant_id, occurred_at, sequence);
CREATE INDEX events_correction ON events(tenant_id, original_id) WHERE original_id IS NOT NULL;
CREATE TABLE invoices (
    id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL REFERENCES tenants(id),
    period text NOT NULL,
    revision integer NOT NULL CHECK (revision > 0),
    cutoff bigint NOT NULL,
    previous_id uuid REFERENCES invoices(id),
    calculation jsonb NOT NULL,
    document_hash text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE(tenant_id, period, revision)
);
CREATE TABLE jobs (
    id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL REFERENCES tenants(id),
    request_key text NOT NULL,
    period text NOT NULL,
    status text NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','done','failed')),
    attempts integer NOT NULL DEFAULT 0,
    invoice_id uuid REFERENCES invoices(id),
    error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    UNIQUE(tenant_id, request_key)
);
CREATE TABLE outbox (
    id uuid PRIMARY KEY,
    job_id uuid NOT NULL UNIQUE REFERENCES jobs(id),
    published_at timestamptz,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    publications integer NOT NULL DEFAULT 0
);
CREATE TABLE heartbeats (name text PRIMARY KEY, seen_at timestamptz NOT NULL DEFAULT now());

CREATE FUNCTION immutable_record() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'Billing history is append-only: %', TG_TABLE_NAME;
END;
$$;
CREATE TRIGGER rates_immutable BEFORE UPDATE OR DELETE ON rates FOR EACH ROW EXECUTE FUNCTION immutable_record();
CREATE TRIGGER events_immutable BEFORE UPDATE OR DELETE ON events FOR EACH ROW EXECUTE FUNCTION immutable_record();
CREATE TRIGGER invoices_immutable BEFORE UPDATE OR DELETE ON invoices FOR EACH ROW EXECUTE FUNCTION immutable_record();
GRANT USAGE ON SCHEMA public TO billing;
GRANT SELECT, INSERT ON rates, events, invoices TO billing;
GRANT SELECT, INSERT, UPDATE ON tenants, jobs, outbox, heartbeats TO billing;

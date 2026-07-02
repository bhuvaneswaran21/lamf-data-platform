CREATE SCHEMA IF NOT EXISTS lamf_src;
SET search_path TO lamf_src;

CREATE TABLE IF NOT EXISTS customer (
    customer_id     UUID PRIMARY KEY,
    full_name       TEXT,
    pan             TEXT,
    email           TEXT,
    phone           TEXT,
    dob             DATE,
    kyc_status      TEXT,
    kyc_type        TEXT,
    risk_category   TEXT,
    city            TEXT,
    state           TEXT,
    pincode         TEXT,
    created_at      TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS scheme (
    scheme_code      TEXT PRIMARY KEY,
    isin             TEXT,
    scheme_name      TEXT,
    amc              TEXT,
    category         TEXT,
    eligible_ltv_pct NUMERIC(5,2)
);

CREATE TABLE IF NOT EXISTS mf_holding (
    holding_id    UUID PRIMARY KEY,
    customer_id   UUID REFERENCES customer(customer_id),
    folio_number  TEXT,
    scheme_code   TEXT REFERENCES scheme(scheme_code),
    isin          TEXT,
    units_pledged NUMERIC(18,4),
    units_total   NUMERIC(18,4),
    rta           TEXT,
    lien_status   TEXT,
    lien_ref      TEXT,
    as_of_date    DATE
);

CREATE TABLE IF NOT EXISTS loan (
    loan_id          UUID PRIMARY KEY,
    customer_id      UUID REFERENCES customer(customer_id),
    product_type     TEXT,
    sanctioned_limit NUMERIC(15,2),
    outstanding      NUMERIC(15,2),
    applied_ltv_pct  NUMERIC(5,2),
    status           TEXT,
    annual_rate      NUMERIC(5,2),
    disbursed_at     TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS disbursement (
    disbursement_id UUID PRIMARY KEY,
    loan_id         UUID REFERENCES loan(loan_id),
    customer_id     UUID,
    amount          NUMERIC(15,2),
    utr             TEXT,
    disbursed_at    TIMESTAMPTZ,
    status          TEXT
);

CREATE TABLE IF NOT EXISTS repayment (
    repayment_id   UUID PRIMARY KEY,
    loan_id        UUID REFERENCES loan(loan_id),
    customer_id    UUID,
    amount         NUMERIC(15,2),
    principal_comp NUMERIC(15,2),
    interest_comp  NUMERIC(15,2),
    mode           TEXT,
    paid_at        TIMESTAMPTZ,
    status         TEXT
);

ALTER TABLE customer     REPLICA IDENTITY FULL;
ALTER TABLE loan         REPLICA IDENTITY FULL;
ALTER TABLE mf_holding   REPLICA IDENTITY FULL;
ALTER TABLE disbursement REPLICA IDENTITY FULL;
ALTER TABLE repayment    REPLICA IDENTITY FULL;

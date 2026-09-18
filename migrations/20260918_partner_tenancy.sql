-- Partner tenancy (Postgres). Applied in production via init_db + create_all;
-- this file is the reviewable schema. Empty tables are a no-op for staff auth.

CREATE TABLE IF NOT EXISTS partners (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    slug VARCHAR(128) NOT NULL UNIQUE,
    drive_folder_id VARCHAR(128),
    enabled_tools JSONB NOT NULL DEFAULT '[]'::jsonb,
    config JSONB,
    active INTEGER NOT NULL DEFAULT 1,
    deactivated_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS partner_users (
    id SERIAL PRIMARY KEY,
    partner_id INTEGER NOT NULL REFERENCES partners(id),
    email VARCHAR(255) NOT NULL UNIQUE,
    active INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_partner_users_partner_id ON partner_users (partner_id);

ALTER TABLE clients ADD COLUMN IF NOT EXISTS partner_id INTEGER REFERENCES partners(id);
CREATE INDEX IF NOT EXISTS ix_clients_partner_id ON clients (partner_id);

CREATE TABLE IF NOT EXISTS partner_lead_collisions (
    id SERIAL PRIMARY KEY,
    partner_id INTEGER NOT NULL REFERENCES partners(id),
    submitted_by_email VARCHAR(255) NOT NULL,
    submitted_business_name VARCHAR(255) NOT NULL,
    existing_client_id INTEGER NOT NULL REFERENCES clients(id),
    payload_json TEXT,
    files_json TEXT,
    status VARCHAR(32) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_partner_lead_collisions_partner_id
    ON partner_lead_collisions (partner_id);

CREATE TABLE IF NOT EXISTS partner_audit_events (
    id SERIAL PRIMARY KEY,
    partner_id INTEGER NOT NULL REFERENCES partners(id),
    email VARCHAR(255) NOT NULL,
    action VARCHAR(64) NOT NULL,
    target_type VARCHAR(64) NOT NULL,
    target_id VARCHAR(64),
    path VARCHAR(255),
    detail_json TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_partner_audit_events_partner_id
    ON partner_audit_events (partner_id);

-- Tables for the mappings in config.yaml. Column names must match the topic
-- captures and payload fields exactly; the bridge does not create tables.

CREATE TABLE IF NOT EXISTS energy_readings (
    id            bigserial PRIMARY KEY,
    site          text        NOT NULL,
    machine       text        NOT NULL,
    kwh           double precision,
    voltage_v     double precision,
    current_a     double precision,
    power_factor  double precision,
    received_at   timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS energy_readings_machine_time
    ON energy_readings (site, machine, received_at DESC);

CREATE TABLE IF NOT EXISTS machine_status (
    id            bigserial PRIMARY KEY,
    site          text        NOT NULL,
    machine       text        NOT NULL,
    state         text,
    rpm           integer,
    alarms        jsonb,
    received_at   timestamptz NOT NULL
);

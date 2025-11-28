BEGIN;

-- Ticket status enum: pending -> done/failed/cancelled
CREATE TYPE ticket_status AS ENUM (
    'pending',
    'done',
    'failed',
    'cancelled'
);

CREATE TABLE tickets (
    id           UUID PRIMARY KEY,
    status       ticket_status NOT NULL DEFAULT 'pending',
    runat        TIMESTAMPTZ   NOT NULL DEFAULT NOW(),  -- run at time
    -- lower nice ⇒ higher priority
    nice         SMALLINT      NOT NULL DEFAULT 52,
    type         TEXT          NOT NULL,
    ctime        TIMESTAMPTZ   NOT NULL DEFAULT NOW(),  -- creation time
    mtime        TIMESTAMPTZ   NULL,                    -- last modification time
    attempts     INTEGER       NOT NULL DEFAULT 0,
    payload      JSONB         NULL,
    error_reason JSONB         NULL
);

CREATE INDEX idx_tickets_pending_runat_nice
    ON tickets (runat, nice)
    WHERE status = 'pending';

-- mtime should change only when status or runat change
CREATE OR REPLACE FUNCTION set_mtime_on_meaningful_change()
RETURNS trigger AS $$
BEGIN
    IF ROW(NEW.status, NEW.runat)
       IS DISTINCT FROM
       ROW(OLD.status, OLD.runat)
    THEN
        NEW.mtime := NOW();
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_tickets_set_mtime
BEFORE UPDATE ON tickets
FOR EACH ROW
EXECUTE FUNCTION set_mtime_on_meaningful_change();

COMMIT;

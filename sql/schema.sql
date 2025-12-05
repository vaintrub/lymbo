-- Ticket status enum
CREATE TYPE ticket_status AS ENUM ('pending', 'done', 'failed', 'cancelled');

-- Tickets table
CREATE TABLE tickets (
	id           UUID PRIMARY KEY,
	status       ticket_status NOT NULL DEFAULT 'pending',
	runat        TIMESTAMPTZ   NOT NULL DEFAULT NOW(),  -- run at time
	nice         SMALLINT      NOT NULL DEFAULT 512,    -- lower nice ⇒ higher priority
	type         TEXT          NOT NULL,
	ctime        TIMESTAMPTZ   NOT NULL DEFAULT NOW(),  -- creation time
	mtime        TIMESTAMPTZ   NULL,                    -- last modification time
	attempts     INTEGER       NOT NULL DEFAULT 0,
	payload      JSONB         NULL,
	error_reason JSONB         NULL
);

-- Indexes
CREATE INDEX idx_tickets_pending_runat_nice ON tickets (runat, nice)
WHERE status = 'pending';

-- Trigger for tickets.mtime
CREATE OR REPLACE FUNCTION tickets_update_mtime()
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

CREATE TRIGGER tickets_update_mtime_trg
	BEFORE UPDATE ON tickets
	FOR EACH ROW
	EXECUTE FUNCTION tickets_update_mtime();

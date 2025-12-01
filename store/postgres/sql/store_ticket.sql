INSERT INTO tickets (id, status, runat, nice, type, ctime, mtime, attempts, payload, error_reason)
VALUES (@id, @status, @runat, @nice, @type, @ctime, @mtime, @attempts, @payload, @error_reason)
ON CONFLICT (id) DO UPDATE SET
status = EXCLUDED.status,
runat = EXCLUDED.runat,
nice = EXCLUDED.nice,
type = EXCLUDED.type,
ctime = EXCLUDED.ctime,
mtime = EXCLUDED.mtime,
attempts = EXCLUDED.attempts,
payload = EXCLUDED.payload,
error_reason = EXCLUDED.error_reason
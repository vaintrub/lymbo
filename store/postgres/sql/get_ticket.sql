SELECT
	'ticket' as ticket,
	id,
	status,
	runat,
	nice,
	type,
	ctime,
	mtime,
	attempts,
	payload,
	error_reason
FROM tickets
WHERE id = @id;
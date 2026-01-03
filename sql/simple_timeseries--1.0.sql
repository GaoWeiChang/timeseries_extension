-- ==========================================
-- utils.c
-- ==========================================

CREATE FUNCTION get_current_timestamp_seconds()
RETURNS bigint
AS 'MODULE_PATHNAME', 'get_current_timestamp_seconds'
LANGUAGE C STRICT;

COMMENT ON FUNCTION get_current_timestamp_seconds() IS 'Get current timestamp in seconds since epoch';


/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2014-2016, BigSQL
 *
 *-------------------------------------------------------------------------
 */

CREATE FUNCTION cstar_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION cstar_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER cstar_fdw
  HANDLER cstar_fdw_handler
  VALIDATOR cstar_fdw_validator;

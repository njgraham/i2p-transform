-- PCORNetLoader_death_cause
-- Taken from PCORNetLoader_ora
-- https://github.com/SCILHS/i2p-transform
-- Define death cause routines/views, clean up last build

BEGIN
PMN_DROPSQL('DROP TABLE death_cause');
END;
/
CREATE TABLE death_cause(
	PATID varchar(50) NOT NULL,
	DEATH_CAUSE varchar(8) NOT NULL,
	DEATH_CAUSE_CODE varchar(2) NOT NULL,
	DEATH_CAUSE_TYPE varchar(2) NOT NULL,
	DEATH_CAUSE_SOURCE varchar(2) NOT NULL,
	DEATH_CAUSE_CONFIDENCE varchar(2) NULL
)
/

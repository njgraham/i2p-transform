-- PCORNetLoader_death
-- Taken from PCORNetLoader_ora
-- https://github.com/SCILHS/i2p-transform
-- Define death routines/views, clean up last build

BEGIN
PMN_DROPSQL('DROP TABLE death');
END;
/
CREATE TABLE death(
	PATID varchar(50) NOT NULL,
	DEATH_DATE date NOT NULL,
	DEATH_DATE_IMPUTE varchar(2) NULL,
	DEATH_SOURCE varchar(2) NOT NULL,
	DEATH_MATCH_CONFIDENCE varchar(2) NULL
)
/

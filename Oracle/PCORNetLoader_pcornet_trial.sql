-- PCORNetLoader_trial
-- Taken from PCORNetLoader_ora
-- https://github.com/SCILHS/i2p-transform
-- Define trial routines/views, clean up last build

BEGIN
PMN_DROPSQL('DROP TABLE pcornet_trial');
END;
/
CREATE TABLE pcornet_trial(
	PATID varchar(50) NOT NULL,
	TRIALID varchar(20) NOT NULL,
	PARTICIPANTID varchar(50) NOT NULL,
	TRIAL_SITEID varchar(50) NULL,
	TRIAL_ENROLL_DATE date NULL,
	TRIAL_END_DATE date NULL,
	TRIAL_WITHDRAW_DATE date NULL,
	TRIAL_INVITE_CODE varchar(20) NULL
)
/

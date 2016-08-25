-------------------------------------------------------------------------------------------
-- PCORNetLoader Script
-- Orignal MSSQL Verion Contributors: Jeff Klann, PhD; Aaron Abend; Arturo Torres
-- Translate to Oracle version: by Kun Wei(Wake Forest)
-- Version 0.6.2, bugfix release, 1/6/16 (create table and pcornetreport bugs)
-- Version 6.01, release to SCILHS, 10/15/15
-- Prescribing/dispensing bugfixes (untested) inserted by Jeff Klann 12/10/15
--
--
-- This is Orace Verion ELT v6 script to build PopMedNet database
-- Instructions:
--     (please see the original MSSQL version script.)
-------------------------------------------------------------------------------------------

--For undefining data/meta schema variables (SQLDeveloper at least)
--undef i2b2_data_schema;
--undef i2b2_meta_schema;
--undef datamart_id;
--undef datamart_name;
--undef network_id;
--undef network_name;

create or replace procedure pcornetloader as
begin

end pcornetloader;
/


BEGIN
pcornetloader; --- you may want to run sql statements one by one in the pcornetloader procedure :)
END;
/

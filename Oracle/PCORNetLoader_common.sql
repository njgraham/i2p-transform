-- PCORNetLoader_common
-- Taken from PCORNetLoader_ora
-- https://github.com/SCILHS/i2p-transform
-- Define common routines
create or replace PROCEDURE GATHER_TABLE_STATS(table_name VARCHAR2) AS 
  BEGIN
  DBMS_STATS.GATHER_TABLE_STATS (
          ownname => 'PCORNET_CDM', -- This doesn't work as a parameter for some reason.
          tabname => table_name,
          estimate_percent => 50, -- Percentage picked somewhat arbitrarily
          cascade => TRUE,
          degree => 16 
          );
END GATHER_TABLE_STATS;
/

create or replace PROCEDURE PMN_DROPSQL(sqlstring VARCHAR2) AS 
  BEGIN
      EXECUTE IMMEDIATE sqlstring;
  EXCEPTION
      WHEN OTHERS THEN NULL;
END PMN_DROPSQL;
/

create or replace FUNCTION PMN_IFEXISTS(objnamestr VARCHAR2, objtypestr VARCHAR2) RETURN BOOLEAN AS 
cnt NUMBER;
BEGIN
  SELECT COUNT(*)
   INTO cnt
    FROM USER_OBJECTS
  WHERE  upper(OBJECT_NAME) = upper(objnamestr)
         and upper(object_type) = upper(objtypestr);
  
  IF( cnt = 0 )
  THEN
    --dbms_output.put_line('NO!');
    return FALSE;  
  ELSE
   --dbms_output.put_line('YES!'); 
   return TRUE;
  END IF;

END PMN_IFEXISTS;
/


create or replace PROCEDURE PMN_Execuatesql(sqlstring VARCHAR2) AS 
BEGIN
  EXECUTE IMMEDIATE sqlstring;
  dbms_output.put_line(sqlstring);
END PMN_ExecuateSQL;
/


CREATE OR REPLACE SYNONYM I2B2FACT FOR "&&i2b2_data_schema".OBSERVATION_FACT
/

CREATE OR REPLACE SYNONYM I2B2MEDFACT FOR OBSERVATION_FACT_MEDS
/

create or replace view i2b2visit as select * from "&&i2b2_data_schema".VISIT_DIMENSION where START_DATE >= to_date('&&min_visit_date_dd_mon_rrrr','dd-mon-rrrr') and (END_DATE is NULL or END_DATE < CURRENT_DATE) and (START_DATE <CURRENT_DATE)
/


CREATE OR REPLACE SYNONYM pcornet_med FOR  "&&i2b2_meta_schema".pcornet_med
/

CREATE OR REPLACE SYNONYM pcornet_lab FOR  "&&i2b2_meta_schema".pcornet_lab
/

CREATE OR REPLACE SYNONYM pcornet_diag FOR  "&&i2b2_meta_schema".pcornet_diag
/

CREATE OR REPLACE SYNONYM pcornet_demo FOR  "&&i2b2_meta_schema".pcornet_demo
/

CREATE OR REPLACE SYNONYM pcornet_proc FOR  "&&i2b2_meta_schema".pcornet_proc
/

CREATE OR REPLACE SYNONYM pcornet_vital FOR  "&&i2b2_meta_schema".pcornet_vital
/

CREATE OR REPLACE SYNONYM pcornet_enc FOR  "&&i2b2_meta_schema".pcornet_enc
/

create or replace FUNCTION GETDATAMARTID RETURN VARCHAR2 IS 
BEGIN 
    RETURN '&&datamart_id';
END;
/

CREATE OR REPLACE FUNCTION GETDATAMARTNAME RETURN VARCHAR2 AS 
BEGIN 
    RETURN '&&datamart_name';
END;
/

CREATE OR REPLACE FUNCTION GETDATAMARTPLATFORM RETURN VARCHAR2 AS 
BEGIN 
    RETURN '02'; -- 01 is MSSQL, 02 is Oracle
END;
/

/* TODO: Consider building the loyalty cohort as designed: 
https://github.com/njgraham/SCILHS-utils/blob/master/LoyaltyCohort/LoyaltyCohort-ora.sql

For now, let's count all patients for testing with the KUMC test patients.
*/

--create or replace view i2b2loyalty_patients as (select patient_num,to_date('01-Jul-2010','dd-mon-rrrr') period_start,to_date('01-Jul-2014','dd-mon-rrrr') period_end from "&&i2b2_data_schema".loyalty_cohort_patient_summary where BITAND(filter_set, 61511) = 61511 and patient_num in (select patient_num from i2b2patient))
--/



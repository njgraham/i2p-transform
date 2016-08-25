-- PCORNetLoader_dispensing
-- Taken from PCORNetLoader_ora
-- https://github.com/SCILHS/i2p-transform
-- Define dispensing routines/views, clean up last build


/* ORA-04068: existing state of packages has been discarded
ORA-04065: not executed, altered or dropped stored procedure "PCORNETDISPENSING"
ORA-06508: PL/SQL: could not find program unit being called: "PCORNETDISPENSING"
ORA-06512: at "PCORNETLOADER", line 14
ORA-06512: at line 2
04068. 00000 -  "existing state of packages%s%s%s has been discarded"
*Cause:    One of errors 4060 - 4067 when attempt to execute a stored
           procedure.
*Action:   Try again after proper re-initialization of any application's
           state.

The above error only happens when we call PCORNetDispensing _and_ PCORNetPrescribing
from within pcornetloader.  When running either individually, the error does not
happen.

Skipping dispensing as per gpc-dev notes:
http://listserv.kumc.edu/pipermail/gpc-dev/attachments/20160223/8d79fa70/attachment-0001.pdf
> LV: the dispensing side [?] is not mandatory? we just did Rx, since that
> what we have in our i2b2
*/
BEGIN
PMN_DROPSQL('DROP TABLE dispensing');
END;
/
CREATE TABLE dispensing(
	DISPENSINGID varchar(19)  primary key,
	PATID varchar(50) NOT NULL,
	PRESCRIBINGID varchar(19)  NULL,
	DISPENSE_DATE date NOT NULL,
	NDC varchar (11) NOT NULL,
	DISPENSE_SUP int, 
	DISPENSE_AMT int, 
	RAW_NDC varchar (50)
)
/


BEGIN
PMN_DROPSQL('DROP sequence  dispensing_seq');
END;
/
create sequence  dispensing_seq
/

create or replace trigger dispensing_trg
before insert on dispensing
for each row
begin
  select dispensing_seq.nextval into :new.DISPENSINGID from dual;
end;
/


/* TODO: When compiling PCORNetDispensing:

Error(53,16): PL/SQL: ORA-00942: table or view does not exist (amount)
 - supply, also used, is created above in the prescribing function

Also, Error(57,57): PL/SQL: ORA-00904: "MO"."PCORI_NDC": invalid identifier
*/
whenever sqlerror continue;
drop table amount;

create table amount(
  nval_num number(18,5), 
	encounter_num number(38,0), 
	concept_cd varchar2(50 byte)
  ); 

alter table "&&i2b2_meta_schema".pcornet_med add (
  pcori_ndc varchar2(1000) -- arbitrary
  );
whenever sqlerror exit;

create or replace procedure PCORNetDispensing as
sqltext varchar2(4000);
begin

PMN_DROPSQL('drop index dispensing_patid');

PMN_DROPSQL('DROP TABLE supply');
sqltext := 'create table supply as '||
'(select nval_num,encounter_num,concept_cd from i2b2fact supply '||
'        inner join encounter enc on enc.patid = supply.patient_num and enc.encounterid = supply.encounter_Num '||
'      join pcornet_med supplycode  '||
'        on supply.modifier_cd = supplycode.c_basecode '||
'        and supplycode.c_fullname like ''\PCORI_MOD\RX_DAYS_SUPPLY\'' ) ';
PMN_EXECUATESQL(sqltext);


PMN_DROPSQL('DROP TABLE amount');
sqltext := 'create table amount as '||
'(select nval_num,encounter_num,concept_cd from i2b2fact amount '||
'     join pcornet_med amountcode '||
'        on amount.modifier_cd = amountcode.c_basecode '||
'        and amountcode.c_fullname like ''\PCORI_MOD\RX_QUANTITY\'') ';
PMN_EXECUATESQL(sqltext);
        
-- insert data with outer joins to ensure all records are included even if some data elements are missing

insert into dispensing (
	PATID
    ,PRESCRIBINGID
	,DISPENSE_DATE -- using start_date from i2b2
    ,NDC --using pcornet_med pcori_ndc - new column!
    ,DISPENSE_SUP ---- modifier nval_num
    ,DISPENSE_AMT  -- modifier nval_num
--    ,RAW_NDC
)
select  m.patient_num, null,m.start_date, NVL(mo.pcori_ndc,'NA')
    ,max(supply.nval_num) sup, max(amount.nval_num) amt 
from i2b2fact m inner join pcornet_med mo
on m.concept_cd = mo.c_basecode
inner join encounter enc on enc.encounterid = m.encounter_Num

    -- jgk bugfix 11/2 - we weren't filtering dispensing events
    inner join (select pcori_basecode,c_fullname,encounter_num,concept_cd from i2b2fact basis
        inner join encounter enc on enc.patid = basis.patient_num and enc.encounterid = basis.encounter_Num
     join pcornet_med basiscode 
        on basis.modifier_cd = basiscode.c_basecode
        and basiscode.c_fullname='\PCORI_MOD\RX_BASIS\DI\') basis
    on m.encounter_num = basis.encounter_num
    and m.concept_cd = basis.concept_Cd 

    left join  supply
    on m.encounter_num = supply.encounter_num
    and m.concept_cd = supply.concept_Cd


    left join  amount
    on m.encounter_num = amount.encounter_num
    and m.concept_cd = amount.concept_Cd

group by m.encounter_num ,m.patient_num, m.start_date,  mo.pcori_ndc;

execute immediate 'create index dispensing_patid on dispensing (PATID)';

end PCORNetDispensing;
/


BEGIN
--PCORNetDispensing;
END;
/

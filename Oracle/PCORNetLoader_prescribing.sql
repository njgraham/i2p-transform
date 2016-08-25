-- PCORNetLoader_prescribing
-- Taken from PCORNetLoader_ora
-- https://github.com/SCILHS/i2p-transform
-- Define prescribing routines/views, clean up last build

BEGIN
PMN_DROPSQL('DROP TABLE prescribing');
END;
/
CREATE TABLE prescribing(
	PRESCRIBINGID varchar(19)  primary key,
	PATID varchar(50) NOT NULL,
	ENCOUNTERID  varchar(50) NULL,
	RX_PROVIDERID varchar(50) NULL, -- NOTE: The spec has a _ before the ID, but this is inconsistent.
	RX_ORDER_DATE date NULL,
	RX_ORDER_TIME varchar (5) NULL,
	RX_START_DATE date NULL,
	RX_END_DATE date NULL,
	RX_QUANTITY int NULL,
	RX_REFILLS int NULL,
	RX_DAYS_SUPPLY int NULL,
	RX_FREQUENCY varchar(2) NULL,
	RX_BASIS varchar (2) NULL,
	RXNORM_CUI int NULL,
	RAW_RX_MED_NAME varchar (50) NULL,
	RAW_RX_FREQUENCY varchar (50) NULL,
	RAW_RXNORM_CUI varchar (50) NULL
)
/


BEGIN
PMN_DROPSQL('DROP sequence  prescribing_seq');
END;
/
create sequence  prescribing_seq
/

create or replace trigger prescribing_trg
before insert on prescribing
for each row
begin
  select prescribing_seq.nextval into :new.PRESCRIBINGID from dual;
end;
/

/* TODO: When compiling PCORNetPrescribing, I got Error(93,15): 
  PL/SQL: ORA-00942: table or view does not exist
At compile time, it's complaining about the fact tables don't exist that are 
created in the function itself.  I created them ahead of time - SQL taken from
the procedure.
*/
whenever sqlerror continue;
drop table basis;
drop table freq;
drop table quantity;
drop table refills;
drop table supply;

create table basis (
  pcori_basecode varchar2(50 byte), 
  c_fullname varchar2(700 byte), 
  encounter_num number(38,0), 
  concept_cd varchar2(50 byte),
  instance_num number(18,0),
  start_date date,
  provider_id varchar2(50 byte),
  modifier_cd varchar2(100 byte)
  ) ;
  
create table freq (
  pcori_basecode varchar2(50 byte), 
  encounter_num number(38,0), 
  concept_cd varchar2(50 byte),
  instance_num number(18,0),
  start_date date,
  provider_id varchar2(50 byte),
  modifier_cd varchar2(100 byte)
  );

create table quantity(
  nval_num number(18,5), 
  encounter_num number(38,0), 
  concept_cd varchar2(50 byte),
  instance_num number(18,0),
  start_date date,
  provider_id varchar2(50 byte),
  modifier_cd varchar2(100 byte)
  );
  
create table refills(
  nval_num number(18,5), 
  encounter_num number(38,0), 
  concept_cd varchar2(50 byte),
  instance_num number(18,0),
  start_date date,
  provider_id varchar2(50 byte),
  modifier_cd varchar2(100 byte)
  );

create table supply(
  nval_num number(18,5), 
  encounter_num number(38,0), 
  concept_cd varchar2(50 byte),
  instance_num number(18,0),
  start_date date,
  provider_id varchar2(50 byte),
  modifier_cd varchar2(100 byte)
  );

whenever sqlerror exit;

create or replace procedure PCORNetPrescribing as
sqltext varchar2(4000);
begin

PMN_DROPSQL('drop index prescribing_patid');
PMN_DROPSQL('drop index prescribing_encounterid');


PMN_DROPSQL('DROP TABLE basis');
sqltext := 'create table basis as '||
'(select pcori_basecode,c_fullname,instance_num,start_date,provider_id,concept_cd,encounter_num,modifier_cd from i2b2medfact basis '||
'        inner join encounter enc on enc.patid = basis.patient_num and enc.encounterid = basis.encounter_Num '||
'     join pcornet_med basiscode  '||
'        on basis.modifier_cd = basiscode.c_basecode '||
'        and basiscode.c_fullname like ''\PCORI_MOD\RX_BASIS\%'') ';
PMN_EXECUATESQL(sqltext);

execute immediate 'create unique index basis_idx on basis (instance_num, start_date, provider_id, concept_cd, encounter_num, modifier_cd)';
GATHER_TABLE_STATS('BASIS');

PMN_DROPSQL('DROP TABLE freq');
sqltext := 'create table freq as '||
'(select pcori_basecode,instance_num,start_date,provider_id,concept_cd,encounter_num,modifier_cd from i2b2medfact freq '||
'        inner join encounter enc on enc.patid = freq.patient_num and enc.encounterid = freq.encounter_Num '||
'     join pcornet_med freqcode  '||
'        on freq.modifier_cd = freqcode.c_basecode '||
'        and freqcode.c_fullname like ''\PCORI_MOD\RX_FREQUENCY\%'') ';
PMN_EXECUATESQL(sqltext);

execute immediate 'create unique index freq_idx on freq (instance_num, start_date, provider_id, concept_cd, encounter_num, modifier_cd)';
GATHER_TABLE_STATS('FREQ');

PMN_DROPSQL('DROP TABLE quantity');
sqltext := 'create table quantity as '||
'(select nval_num,instance_num,start_date,provider_id,concept_cd,encounter_num,modifier_cd from i2b2medfact quantity '||
'        inner join encounter enc on enc.patid = quantity.patient_num and enc.encounterid = quantity.encounter_Num '||
'     join pcornet_med quantitycode  '||
'        on quantity.modifier_cd = quantitycode.c_basecode '||
'        and quantitycode.c_fullname like ''\PCORI_MOD\RX_QUANTITY\'') ';

PMN_EXECUATESQL(sqltext);

execute immediate 'create unique index quantity_idx on quantity (instance_num, start_date, provider_id, concept_cd, encounter_num, modifier_cd)';
GATHER_TABLE_STATS('QUANTITY');
        
PMN_DROPSQL('DROP TABLE refills');
sqltext := 'create table refills as   '||
'(select nval_num,instance_num,start_date,provider_id,concept_cd,encounter_num,modifier_cd from i2b2medfact refills '||
'        inner join encounter enc on enc.patid = refills.patient_num and enc.encounterid = refills.encounter_Num '||
'     join pcornet_med refillscode  '||
'        on refills.modifier_cd = refillscode.c_basecode '||
'        and refillscode.c_fullname like ''\PCORI_MOD\RX_REFILLS\'') ';
PMN_EXECUATESQL(sqltext);

execute immediate 'create unique index refills_idx on refills (instance_num, start_date, provider_id, concept_cd, encounter_num, modifier_cd)';
GATHER_TABLE_STATS('REFILLS');
        
PMN_DROPSQL('DROP TABLE supply');  
sqltext := 'create table supply as  '||
'(select nval_num,instance_num,start_date,provider_id,concept_cd,encounter_num,modifier_cd from i2b2medfact supply '||
'        inner join encounter enc on enc.patid = supply.patient_num and enc.encounterid = supply.encounter_Num '||
'     join pcornet_med supplycode  '||
'        on supply.modifier_cd = supplycode.c_basecode '||
'        and supplycode.c_fullname like ''\PCORI_MOD\RX_DAYS_SUPPLY\'')  ';
PMN_EXECUATESQL(sqltext);

execute immediate 'create unique index supply_idx on supply (instance_num, start_date, provider_id, concept_cd, encounter_num, modifier_cd)';
GATHER_TABLE_STATS('SUPPLY');

-- insert data with outer joins to ensure all records are included even if some data elements are missing
insert into prescribing (
	PATID
    ,encounterid
    ,RX_PROVIDERID
	,RX_ORDER_DATE -- using start_date from i2b2
	,RX_ORDER_TIME  -- using time start_date from i2b2
	,RX_START_DATE
	,RX_END_DATE 
    ,RXNORM_CUI --using pcornet_med pcori_cui - new column!
    ,RX_QUANTITY ---- modifier nval_num
    ,RX_REFILLS  -- modifier nval_num
    ,RX_DAYS_SUPPLY -- modifier nval_num
    ,RX_FREQUENCY --modifier with basecode lookup
    ,RX_BASIS --modifier with basecode lookup
--    ,RAW_RX_MED_NAME, --not filling these right now
--    ,RAW_RX_FREQUENCY,
--    ,RAW_RXNORM_CUI
)
select distinct  m.patient_num, m.Encounter_Num,m.provider_id,  m.start_date order_date,  to_char(m.start_date,'HH:MI'), m.start_date start_date, m.end_date, mo.pcori_cui
    ,quantity.nval_num quantity, refills.nval_num refills, supply.nval_num supply, substr(freq.pcori_basecode, instr(freq.pcori_basecode, ':') + 1, 2) frequency, 
    substr(basis.pcori_basecode, instr(basis.pcori_basecode, ':') + 1, 2) basis
 from i2b2medfact m inner join pcornet_med mo on m.concept_cd = mo.c_basecode 
inner join encounter enc on enc.encounterid = m.encounter_Num
-- TODO: This join adds several minutes to the load - must be debugged

    left join basis
    on m.encounter_num = basis.encounter_num
    and m.concept_cd = basis.concept_Cd
    and m.start_date = basis.start_date
    and m.provider_id = basis.provider_id
    and m.modifier_cd = basis.modifier_cd

    left join  freq
    on m.encounter_num = freq.encounter_num
    and m.concept_cd = freq.concept_Cd
    and m.start_date = freq.start_date
    and m.provider_id = freq.provider_id
    and m.modifier_cd = freq.modifier_cd

    left join quantity 
    on m.encounter_num = quantity.encounter_num
    and m.concept_cd = quantity.concept_Cd
    and m.start_date = quantity.start_date
    and m.provider_id = quantity.provider_id
    and m.modifier_cd = quantity.modifier_cd

    left join refills
    on m.encounter_num = refills.encounter_num
    and m.concept_cd = refills.concept_Cd
    and m.start_date = refills.start_date
    and m.provider_id = refills.provider_id
    and m.modifier_cd = refills.modifier_cd

    left join supply
    on m.encounter_num = supply.encounter_num
    and m.concept_cd = supply.concept_Cd
    and m.start_date = supply.start_date
    and m.provider_id = supply.provider_id
    and m.modifier_cd = supply.modifier_cd

where (basis.c_fullname is null or basis.c_fullname like '\PCORI_MOD\RX_BASIS\PR\%');

execute immediate 'create index prescribing_patid on prescribing (PATID)';
execute immediate 'create index prescribing_encounterid on prescribing (ENCOUNTERID)';
GATHER_TABLE_STATS('PRESCRIBING');

end PCORNetPrescribing;
/

BEGIN
PCORNetPrescribing;
END;
/

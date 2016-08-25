-- PCORNetLoader_condition
-- Taken from PCORNetLoader_ora
-- https://github.com/SCILHS/i2p-transform
-- Define condition routines/views, clean up last build

BEGIN
PMN_DROPSQL('DROP TABLE condition');
END;
/
CREATE TABLE condition(
	CONDITIONID varchar(19)  primary key,
	PATID varchar(50) NOT NULL,
	ENCOUNTERID  varchar(50) NULL,
	REPORT_DATE  date NULL,
	RESOLVE_DATE  date NULL,
	ONSET_DATE  date NULL,
	CONDITION_STATUS varchar(2) NULL,
	CONDITION varchar(18) NOT NULL,
	CONDITION_TYPE varchar(2) NOT NULL,
	CONDITION_SOURCE varchar(2) NOT NULL,
	RAW_CONDITION_STATUS varchar(2) NULL,
	RAW_CONDITION varchar(18) NULL,
	RAW_CONDITION_TYPE varchar(2) NULL,
	RAW_CONDITION_SOURCE varchar(2) NULL
)
/

BEGIN
PMN_DROPSQL('DROP sequence  condition_seq');
END;
/
create sequence  condition_seq
/

create or replace trigger condition_trg
before insert on condition
for each row
begin
  select condition_seq.nextval into :new.CONDITIONID from dual;
end;
/

create or replace procedure PCORNetCondition as
sqltext varchar2(4000);
begin

PMN_DROPSQL('drop index condition_patid');
PMN_DROPSQL('drop index condition_encounterid');

PMN_DROPSQL('DROP TABLE sourcefact2');

sqltext := 'create table sourcefact2 as '||
	'select distinct patient_num, encounter_num, provider_id, concept_cd, start_date, dxsource.pcori_basecode dxsource, dxsource.c_fullname '||
	'from i2b2fact factline '||
    'inner join encounter enc on enc.patid = factline.patient_num and enc.encounterid = factline.encounter_Num '||
    'inner join pcornet_diag dxsource on factline.modifier_cd =dxsource.c_basecode '||
	'where dxsource.c_fullname like ''\PCORI_MOD\CONDITION_OR_DX\%''';
PMN_EXECUATESQL(sqltext);

sqltext := 'insert into condition (patid, encounterid, report_date, resolve_date, condition, condition_type, condition_status, condition_source) '||
'select distinct factline.patient_num, min(factline.encounter_num) encounterid, min(factline.start_date) report_date, NVL(max(factline.end_date),null) resolve_date, diag.pcori_basecode,  '||
'SUBSTR(diag.c_fullname,18,2) condition_type,   '||
'	NVL2(max(factline.end_date) , ''RS'', ''NI'') condition_status,  '|| -- Imputed so might not be entirely accurate
'	NVL(SUBSTR(max(dxsource),INSTR(max(dxsource), '':'')+1,2),''NI'') condition_source '||
'from i2b2fact factline '||
'inner join encounter enc on enc.patid = factline.patient_num and enc.encounterid = factline.encounter_Num '||
'inner join pcornet_diag diag on diag.c_basecode  = factline.concept_cd    '||
' left outer join sourcefact2 sf '||
'on	factline.patient_num=sf.patient_num '||
'and factline.encounter_num=sf.encounter_num '||
'and factline.provider_id=sf.provider_id '||
'and factline.concept_cd=sf.concept_Cd '||
'and factline.start_date=sf.start_Date   '||
'where diag.c_fullname like ''\PCORI\DIAGNOSIS\%'' '||
'and sf.c_fullname like ''\PCORI_MOD\CONDITION_OR_DX\CONDITION_SOURCE\%'' '||
'group by factline.patient_num, diag.pcori_basecode, diag.c_fullname ';

PMN_EXECUATESQL(sqltext);

execute immediate 'create index condition_patid on condition (PATID)';
execute immediate 'create index condition_encounterid on condition (ENCOUNTERID)';

end PCORNetCondition;
/

BEGIN
PCORNetCondition;
END;
/

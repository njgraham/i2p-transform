-- PCORNetLoader_diagnosis
-- Taken from PCORNetLoader_ora
-- https://github.com/SCILHS/i2p-transform
-- Define diagnosis routines/views, clean up last build

BEGIN
PMN_DROPSQL('DROP TABLE diagnosis');
END;
/

CREATE TABLE diagnosis(
	DIAGNOSISID varchar(19)  primary key,
	PATID varchar(50) NOT NULL,
	ENCOUNTERID varchar(50) NOT NULL,
	ENC_TYPE varchar(2) NULL,
	ADMIT_DATE date NULL,
	PROVIDERID varchar(50) NULL,
	DX varchar(18) NOT NULL,
	DX_TYPE varchar(2) NOT NULL,
	DX_SOURCE varchar(2) NOT NULL,
	PDX varchar(2) NULL,
	RAW_DX varchar(50) NULL,
	RAW_DX_TYPE varchar(50) NULL,
	RAW_DX_SOURCE varchar(50) NULL,
	RAW_ORIGDX varchar(50) NULL,
	RAW_PDX varchar(50) NULL
)
/

BEGIN
PMN_DROPSQL('DROP sequence  diagnosis_seq');
END;
/
create sequence  diagnosis_seq
/

create or replace trigger diagnosis_trg
before insert on diagnosis
for each row
begin
  select diagnosis_seq.nextval into :new.DIAGNOSISID from dual;
end;
/

create or replace procedure PCORNetDiagnosis as
sqltext varchar2(4000);
begin

PMN_DROPSQL('drop index diagnosis_patid');
PMN_DROPSQL('drop index diagnosis_encounterid');

PMN_DROPSQL('DROP TABLE sourcefact'); -- associated indexes will be dropped as well

sqltext := 'create table sourcefact as '||
	'select distinct patient_num, encounter_num, provider_id, concept_cd, start_date, dxsource.pcori_basecode dxsource, dxsource.c_fullname '||
	'from i2b2fact factline '||
    'inner join encounter enc on enc.patid = factline.patient_num and enc.encounterid = factline.encounter_Num '||
    'inner join pcornet_diag dxsource on factline.modifier_cd =dxsource.c_basecode '||
	'where dxsource.c_fullname like ''\PCORI_MOD\CONDITION_OR_DX\%''';
PMN_EXECUATESQL(sqltext);

execute immediate 'create index sourcefact_idx on sourcefact (patient_num, encounter_num, provider_id, concept_cd, start_date)';
GATHER_TABLE_STATS('SOURCEFACT');

PMN_DROPSQL('DROP TABLE pdxfact');

sqltext := 'create table pdxfact as '||
	'select distinct patient_num, encounter_num, provider_id, concept_cd, start_date, dxsource.pcori_basecode pdxsource,dxsource.c_fullname  '||
	'from i2b2fact factline '||
    'inner join encounter enc on enc.patid = factline.patient_num and enc.encounterid = factline.encounter_Num '||
    'inner join pcornet_diag dxsource on factline.modifier_cd =dxsource.c_basecode '||
	'and dxsource.c_fullname like ''\PCORI_MOD\PDX\%''';
PMN_EXECUATESQL(sqltext);

execute immediate 'create index pdxfact_idx on pdxfact (patient_num, encounter_num, provider_id, concept_cd, start_date)';
GATHER_TABLE_STATS('PDXFACT');

sqltext := 'insert into diagnosis (patid,			encounterid,	enc_type, admit_date, providerid, dx, dx_type, dx_source, pdx) '||
'select distinct factline.patient_num, factline.encounter_num encounterid,	enc_type, factline.start_date, factline.provider_id, diag.pcori_basecode,  '||
'SUBSTR(diag.c_fullname,18,2) dxtype,   '||
'	CASE WHEN enc_type=''AV'' THEN ''FI'' ELSE nvl(SUBSTR(dxsource,INSTR(dxsource,'':'')+1,2) ,''NI'')END, '||
'	nvl(SUBSTR(pdxsource,INSTR(pdxsource, '':'')+1,2),''NI'') '|| -- jgk bugfix 9/28/15 
'from i2b2fact factline '||
'inner join encounter enc on enc.patid = factline.patient_num and enc.encounterid = factline.encounter_Num '||
' left outer join sourcefact '||
'on	factline.patient_num=sourcefact.patient_num '||
'and factline.encounter_num=sourcefact.encounter_num '||
'and factline.provider_id=sourcefact.provider_id '||
'and factline.concept_cd=sourcefact.concept_Cd '||
'and factline.start_date=sourcefact.start_Date '||
'left outer join pdxfact '||
'on	factline.patient_num=pdxfact.patient_num '||
'and factline.encounter_num=pdxfact.encounter_num '||
'and factline.provider_id=pdxfact.provider_id '||
'and factline.concept_cd=pdxfact.concept_cd '||
'and factline.start_date=pdxfact.start_Date '||
'inner join pcornet_diag diag on diag.c_basecode  = factline.concept_cd '||
'where diag.c_fullname like ''\PCORI\DIAGNOSIS\%''  '||
'and (sourcefact.c_fullname like ''\PCORI_MOD\CONDITION_OR_DX\DX_SOURCE\%'' or sourcefact.c_fullname is null) ';

PMN_EXECUATESQL(sqltext);

execute immediate 'create index diagnosis_patid on diagnosis (PATID)';
execute immediate 'create index diagnosis_encounterid on diagnosis (ENCOUNTERID)';
GATHER_TABLE_STATS('DIAGNOSIS');

end PCORNetDiagnosis;
/


BEGIN
PCORNetDiagnosis;
END;
/

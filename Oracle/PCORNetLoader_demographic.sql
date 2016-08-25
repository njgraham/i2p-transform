-- PCORNetLoader_demographic
-- Taken from PCORNetLoader_ora
-- https://github.com/SCILHS/i2p-transform
-- Define demographic routines/views, clean up last build

BEGIN
PMN_DROPSQL('DROP TABLE i2b2patient_list');
END;
/

CREATE table i2b2patient_list as 
select * from
(
select DISTINCT PATIENT_NUM from I2B2FACT where START_DATE > to_date('&&min_pat_list_date_dd_mon_rrrr','dd-mon-rrrr')
) where ROWNUM<100000000
/

create or replace VIEW i2b2patient as select * from "&&i2b2_data_schema".PATIENT_DIMENSION where PATIENT_NUM in (select PATIENT_NUM from i2b2patient_list)
/


BEGIN
PMN_DROPSQL('DROP TABLE pcornet_codelist');
END;
/

create table pcornet_codelist(codetype varchar2(20), code varchar2(50))
/
create or replace procedure pcornet_parsecode (codetype in varchar, codestring in varchar) as

tex varchar(2000);
pos number(9);
readstate char(1) ;
nextchar char(1) ;
val varchar(50);

begin

val:='';
readstate:='F';
pos:=0;
tex := codestring;
FOR pos IN 1..length(tex)
LOOP
--	dbms_output.put_line(val);
    	nextchar:=substr(tex,pos,1);
	if nextchar!=',' then
		if nextchar='''' then
			if readstate='F' then
				val:='';
				readstate:='T';
			else
				insert into pcornet_codelist values (codetype,val);
				val:='';
				readstate:='F'  ;
			end if;
		else
			if readstate='T' then
				val:= val || nextchar;
			end if;
		end if;
	end if;
END LOOP;

end pcornet_parsecode;
/



create or replace procedure pcornet_popcodelist as

codedata varchar(2000);
onecode varchar(20);
codetype varchar(20);

cursor getcodesql is
select 'RACE',c_dimcode from pcornet_demo where c_fullname like '\PCORI\DEMOGRAPHIC\RACE%'
union
select 'SEX',c_dimcode from pcornet_demo where c_fullname like '\PCORI\DEMOGRAPHIC\SEX%'
union
select 'HISPANIC',c_dimcode from pcornet_demo where c_fullname like '\PCORI\DEMOGRAPHIC\HISPANIC%';


begin
open getcodesql;
LOOP 
	fetch getcodesql into codetype,codedata;
	EXIT WHEN getcodesql%NOTFOUND ;
 	pcornet_parsecode (codetype,codedata );
end loop;

close getcodesql ;
end pcornet_popcodelist;
/

BEGIN
PMN_DROPSQL('DROP TABLE demographic');
END;
/
CREATE TABLE demographic(
	PATID varchar(50) NOT NULL,
	BIRTH_DATE date NULL,
	BIRTH_TIME varchar(5) NULL,
	SEX varchar(2) NULL,
	HISPANIC varchar(2) NULL,
	BIOBANK_FLAG varchar(1) DEFAULT 'N',
	RACE varchar(2) NULL,
	RAW_SEX varchar(50) NULL,
	RAW_HISPANIC varchar(50) NULL,
	RAW_RACE varchar(50) NULL
)
/

create or replace procedure PCORNetDemographic as 

sqltext varchar2(4000); 
cursor getsql is 
--1 --  S,R,NH
	select 'insert into demographic(raw_sex,PATID, BIRTH_DATE, BIRTH_TIME,SEX, HISPANIC, RACE) '||
	'	select ''1'',patient_num, '||
	'	birth_date, '||
	'	to_char(birth_date,''HH:MI''), '||
	''''||sex.pcori_basecode||''','||
	'''NI'','||
	''''||race.pcori_basecode||''''||
	' from i2b2patient p '||
	'	where lower(p.sex_cd) in ('||lower(sex.c_dimcode)||') '||
	'    and    lower(p.race_cd) in ('||lower(race.c_dimcode)||') '||
	'   and lower(nvl(p.ethnicity_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''HISPANIC'') '
	from pcornet_demo race, pcornet_demo sex
	where race.c_fullname like '\PCORI\DEMOGRAPHIC\RACE%'
	and race.c_visualattributes like 'L%'
	and sex.c_fullname like '\PCORI\DEMOGRAPHIC\SEX%'
	and sex.c_visualattributes like 'L%'
union -- A - S,R,H
select 'insert into demographic(raw_sex,PATID, BIRTH_DATE, BIRTH_TIME,SEX, HISPANIC, RACE) '||
	'	select ''A'',patient_num, '||
	'	birth_date, '||
	'	to_char(birth_date,''HH:MI''), '||
	''''||sex.pcori_basecode||''','||
	''''||hisp.pcori_basecode||''','||
	''''||race.pcori_basecode||''''||
	' from i2b2patient p '||
	'	where lower(p.sex_cd) in ('||lower(sex.c_dimcode)||') '||
	'	and	lower(p.race_cd) in ('||lower(race.c_dimcode)||') '||
	'	and	lower(p.ethnicity_cd) in ('||lower(hisp.c_dimcode)||') '
	from pcornet_demo race, pcornet_demo hisp, pcornet_demo sex
	where race.c_fullname like '\PCORI\DEMOGRAPHIC\RACE%'
	and race.c_visualattributes like 'L%'
	and hisp.c_fullname like '\PCORI\DEMOGRAPHIC\HISPANIC%'
	and hisp.c_visualattributes like 'L%'
	and sex.c_fullname like '\PCORI\DEMOGRAPHIC\SEX%'
	and sex.c_visualattributes like 'L%'
union --2 S, nR, nH
	select 'insert into demographic(raw_sex,PATID, BIRTH_DATE, BIRTH_TIME,SEX, HISPANIC, RACE) '||
	'	select ''2'',patient_num, '||
	'	birth_date, '||
	'	to_char(birth_date,''HH:MI''), '||
	''''||sex.pcori_basecode||''','||
	'''NI'','||
	'''NI'''||
	' from i2b2patient p '||
	'	where lower(nvl(p.sex_cd,''xx'')) in ('||lower(sex.c_dimcode)||') '||
	'	and	lower(nvl(p.race_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''RACE'') '||
	'   and lower(nvl(p.ethnicity_cd,''ni'')) not in (select lower(code) from pcornet_codelist where codetype=''HISPANIC'') '
	from pcornet_demo sex
	where sex.c_fullname like '\PCORI\DEMOGRAPHIC\SEX%'
	and sex.c_visualattributes like 'L%'
union --3 -- nS,R, NH
	select 'insert into demographic(raw_sex,PATID, BIRTH_DATE, BIRTH_TIME,SEX, HISPANIC, RACE) '||
	'	select ''3'',patient_num, '||
	'	birth_date, '||
	'	to_char(birth_date,''HH:MI''), '||
	'''NI'','||
	'''NI'','||
	''''||race.pcori_basecode||''''||
	' from i2b2patient p '||
	'	where lower(nvl(p.sex_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''SEX'') '||
	'	and	lower(p.race_cd) in ('||lower(race.c_dimcode)||') '||
	'   and lower(nvl(p.ethnicity_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''HISPANIC'')'
	from pcornet_demo race
	where race.c_fullname like '\PCORI\DEMOGRAPHIC\RACE%'
	and race.c_visualattributes like 'L%'
union --B -- nS,R, H
	select 'insert into demographic(raw_sex,PATID, BIRTH_DATE, BIRTH_TIME,SEX, HISPANIC, RACE) '||
	'	select ''B'',patient_num, '||
	'	birth_date, '||
	'	to_char(birth_date,''HH:MI''), '||
	'''NI'','||
	''''||hisp.pcori_basecode||''','||
	''''||race.pcori_basecode||''''||
	' from i2b2patient p '||
	'	where lower(nvl(p.sex_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''SEX'') '||
	'	and	lower(p.race_cd) in ('||lower(race.c_dimcode)||') '||
	'	and	lower(p.ethnicity_cd) in ('||lower(hisp.c_dimcode)||') '
	from pcornet_demo race,pcornet_demo hisp
	where race.c_fullname like '\PCORI\DEMOGRAPHIC\RACE%'
	and race.c_visualattributes like 'L%'
	and hisp.c_fullname like '\PCORI\DEMOGRAPHIC\HISPANIC%'
	and hisp.c_visualattributes like 'L%'
union --4 -- S, NR, H
	select 'insert into demographic(raw_sex,PATID, BIRTH_DATE, BIRTH_TIME,SEX, HISPANIC, RACE) '||
	'	select ''4'',patient_num, '||
	'	birth_date, '||
	'	to_char(birth_date,''HH:MI''), '||
	''''||sex.pcori_basecode||''','||
	''''||hisp.pcori_basecode||''','||
	'''NI'''||
	' from i2b2patient p '||
	'	where lower(nvl(p.sex_cd,''NI'')) in ('||lower(sex.c_dimcode)||') '||
	'	and lower(nvl(p.ethnicity_cd,''NI'')) in ('||lower(hisp.c_dimcode)||') '||
	'	and lower(nvl(p.race_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''RACE'') '
	from pcornet_demo sex, pcornet_demo hisp
	where sex.c_fullname like '\PCORI\DEMOGRAPHIC\SEX%'
	and sex.c_visualattributes like 'L%'
	and hisp.c_fullname like '\PCORI\DEMOGRAPHIC\HISPANIC%'
	and hisp.c_visualattributes like 'L%'
union --5 -- NS, NR, H
	select 'insert into demographic(raw_sex,PATID, BIRTH_DATE, BIRTH_TIME,SEX, HISPANIC, RACE) '||
	'	select ''5'',patient_num, '||
	'	birth_date, '||
	'	to_char(birth_date,''HH:MI''), '||
	'''NI'','||
	''''||hisp.pcori_basecode||''','||
	'''NI'''||
	' from i2b2patient p '||
	'	where lower(nvl(p.sex_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''SEX'') '||
	'	and lower(nvl(p.race_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''RACE'') '||
	'	and lower(nvl(p.ethnicity_cd,''NI'')) in ('||lower(hisp.c_dimcode)||') '
  from pcornet_demo hisp
	where hisp.c_fullname like '\PCORI\DEMOGRAPHIC\HISPANIC%'
	and hisp.c_visualattributes like 'L%'
union --6 -- NS, NR, nH
	select 'insert into demographic(raw_sex,PATID, BIRTH_DATE, BIRTH_TIME,SEX, HISPANIC, RACE) '||
	'	select ''6'',patient_num, '||
	'	birth_date, '||
	'	to_char(birth_date,''HH:MI''), '||
	'''NI'','||
	'''NI'','||
	'''NI'''||
	' from i2b2patient p '||
	'	where lower(nvl(p.sex_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''SEX'') '||
	'	and lower(nvl(p.ethnicity_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''HISPANIC'') '||
	'   and lower(nvl(p.race_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''RACE'') ' 
	from dual;

begin    
pcornet_popcodelist;

PMN_DROPSQL('drop index demographic_patid');

OPEN getsql;
LOOP
FETCH getsql INTO sqltext;
	EXIT WHEN getsql%NOTFOUND;  
--	insert into st values (sqltext); 
	execute immediate sqltext; 
	COMMIT;
END LOOP;
CLOSE getsql;

execute immediate 'create index demographic_patid on demographic (PATID)';
GATHER_TABLE_STATS('DEMOGRAPHIC');

end PCORNetDemographic; 
/

BEGIN
PCORNetDemographic;
END;
/

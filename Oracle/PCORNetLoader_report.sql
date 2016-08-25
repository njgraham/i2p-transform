-- PCORNetLoader_report
-- Taken from PCORNetLoader_ora
-- https://github.com/SCILHS/i2p-transform
-- Define report routines/views, clean up last build

BEGIN
PMN_DROPSQL('DROP TABLE i2pReport');
END;
/

create table i2pReport (runid number, rundate date, concept varchar(20), sourceval number, destval number, diff number)
/

BEGIN
insert into i2preport (runid) values (0);
pcornet_popcodelist;
END;
/

create or replace PROCEDURE pcornetReport
as
i2b2pats  number;
i2b2Encounters  number;
i2b2facts number;
i2b2dxs number;
i2b2procs number;
i2b2lcs number;

pmnpats  number;
encounters number;
pmndx number;
pmnprocs number;
pmnfacts number;
pmnenroll number;
vital number;



pmnlabs number;
prescribings number;
dispensings number;
pmncond number;


v_runid number;
begin
select count(*) into i2b2Pats   from i2b2patient;
select count(*) into i2b2Encounters   from i2b2visit i inner join demographic d on i.patient_num=d.patid;


select count(*) into pmnPats   from demographic;
select count(*) into encounters   from encounter e ;
select count(*) into pmndx   from diagnosis;
select count(*) into pmnprocs  from procedures;

select count(*) into pmncond from condition;
select count(*) into pmnenroll  from enrollment;
select count(*) into vital  from vital;
select count(*) into pmnlabs from lab_result_cm;
select count(*) into prescribings from prescribing;
select count(*) into dispensings from dispensing;

select max(runid) into v_runid from i2pReport;
v_runid := v_runid + 1;
insert into i2pReport values( v_runid, SYSDATE, 'Pats', i2b2pats, pmnpats, i2b2pats-pmnpats);
insert into i2pReport values( v_runid, SYSDATE, 'Enrollment', i2b2pats, pmnenroll, i2b2pats-pmnpats);

insert into i2pReport values(v_runid, SYSDATE, 'Encounters', i2b2Encounters, encounters, i2b2encounters-encounters);
insert into i2pReport values(v_runid, SYSDATE, 'DX',		null,		pmndx,		null);
insert into i2pReport values(v_runid, SYSDATE, 'PX',		null,		pmnprocs,	null);
insert into i2pReport values(v_runid, SYSDATE, 'Condition',	null,		pmncond,	null);
insert into i2pReport values(v_runid, SYSDATE, 'Vital',		null,		vital,	null);
insert into i2pReport values(v_runid, SYSDATE, 'Labs',		null,		pmnlabs,	null);
insert into i2pReport values(v_runid, SYSDATE, 'Prescribing',	null,		prescribings,null);
insert into i2pReport values(v_runid, SYSDATE, 'Dispensing',	null,		dispensings,	null);

end pcornetReport;
/

select concept "Data Type",sourceval "From i2b2",destval "In PopMedNet", diff "Difference" from i2preport where RUNID = (select max(RUNID) from I2PREPORT);

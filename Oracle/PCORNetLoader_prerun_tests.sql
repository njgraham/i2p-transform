-- Make sure the ethnicity code has been added to the patient dimension
-- See update_ethnicity_pdim.sql
select ethnicity_cd from "&&i2b2_data_schema".patient_dimension where 1=0;

-- Make sure that the providerid column has been added to the visit dimension
select providerid from "&&i2b2_data_schema".visit_dimension where 1=0;

-- Make sure the inout_cd has been populated
-- See heron_encounter_style.sql
select case when qty = 0 then 1/0 else 1 end inout_cd_populated from (
  select count(*) qty from "&&i2b2_data_schema".visit_dimension where inout_cd is not null
  );

-- Make sure the RXNorm mapping table exists
select rxcui from "&&i2b2_etl_schema".clarity_med_id_to_rxcui@id where 1=0;

-- Make sure the observation fact medication table is populated
select case when qty > 0 then 1 else 1/0 end obs_fact_meds_populated from (
  select count(*) qty from observation_fact_meds
  );

-- this query extracts the cohort and every possible hour they were in the ICU
-- this table is joined to other tables on ICUSTAY_ID and CHARTTIME
select
  cohort.subject_id, cohort.hadm_id, cohort.icustay_id, hr
from `bd4h-project.test.New_cohort` cohort, UNNEST(GENERATE_ARRAY(-24,datetime_diff(outtime,intime,hour))) as hr
where cohort.excluded = 0
order by cohort.subject_id, cohort.hadm_id, cohort.icustay_id;


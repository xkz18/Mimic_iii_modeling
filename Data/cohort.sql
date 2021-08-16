with 
ce as(select chartevent.icustay_id, 
  datetime_trunc(DATETIME_ADD(min(charttime), interval 59 minute),hour) as intime_hr,
  datetime_trunc(DATETIME_ADD(max(charttime), interval 59 minute),hour) as outtime_hr
  
  from physionet-data.mimiciii_clinical.chartevents chartevent
  inner join physionet-data.mimiciii_clinical.icustays icustay
    on chartevent.icustay_id = icustay.icustay_id
    and chartevent.charttime > DATETIME_SUB(icustay.intime,interval 12 hour)
    and chartevent.charttime < DATETIME_SUB(icustay.outtime,interval 12 hour)
  where itemid in (211,220045)
  group by chartevent.icustay_id),
  
icu as(select icustays.subject_id, ce.icustay_id,
  row_number() over (partition by icustays.subject_id order by ce.intime_hr) as icustay_num
  from physionet-data.mimiciii_clinical.icustays as icustays
  left join ce using (icustay_id))
  
select icustay.subject_id, icustay.hadm_id, icustay.icustay_id, icustay.dbsource, 
  ce.intime_hr as intime, ce.outtime_hr as outtime, 
  round(DATETIME_DIFF(admission.admittime, patient.dob, day)/ 365.242, 4) as age, 
  patient.gender, admission.ethnicity, admission.admission_type, icu.icustay_num,
  admission.HOSPITAL_EXPIRE_FLAG, patient.expire_flag, icustay.los as icu_los,
  case when patient.dod <= datetime_add(admission.admittime,interval 30 day) then 1 else 0 end
  as THIRTYDAY_EXPIRE_FLAG, 
  DATETIME_DIFF(admission.dischtime, admission.admittime,second)/60.0/60.0/24.0 as hosp_los,
  DATETIME_DIFF(admission.deathtime, ce.intime_hr, hour)/60.0/60.0 as hosp_deathtime_hours,
  DATETIME_DIFF(patient.dod, ce.intime_hr, hour)/60.0/60.0 as deathtime_hours,
  admission.deathtime as deadthtime_check,
  case when round(DATETIME_DIFF(admission.admittime,patient.dob,day)/ 365.242, 4) <= 16 then 1 else 0 end 
    as exclusion_adult, 
  case when admission.HAS_CHARTEVENTS_DATA = 0 or icustay.intime is null or icustay.outtime is null 
    or ce.intime_hr is null or ce.outtime_hr is null then 1
    else 0 end as exclusion_valid_data,
  case when datetime_diff(ce.outtime_hr,ce.intime_hr,hour) <= 4 then 1
    else 0 end as exclusion_short_stay,
  case when (lower(diagnosis) like '%organ donor%' and deathtime is not null)
    or (lower(diagnosis) like '%donor account%' and deathtime is not null) then 1 else 0 end 
    as exclusion_organ_donor,
  case when round(DATETIME_DIFF(admission.admittime,patient.dob,day) / 365.242, 4) <= 16 
    or admission.HAS_CHARTEVENTS_DATA = 0 or icustay.intime is null or icustay.outtime is null
    or ce.intime_hr is null or ce.outtime_hr is null 
    or datetime_diff(ce.outtime_hr,ce.intime_hr,hour) <= 4 
    or(lower(diagnosis) like '%organ donor%' and deathtime is not null)
    or (lower(diagnosis) like '%donor account%' and deathtime is not null) then 1
    else 0 end as excluded
    
from physionet-data.mimiciii_clinical.icustays icustay
inner join physionet-data.mimiciii_clinical.admissions admission on icustay.hadm_id = admission.hadm_id
inner join physionet-data.mimiciii_clinical.patients patient on icustay.subject_id = patient.subject_id
inner join icu using (icustay_id)
left join ce on icustay.icustay_id = ce.icustay_id
order by icustay.icustay_id




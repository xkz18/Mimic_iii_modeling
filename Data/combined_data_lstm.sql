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
  left join ce using (icustay_id)),

cohort as(select icustay.subject_id, icustay.hadm_id, icustay.icustay_id, icustay.dbsource, 
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
  order by icustay.icustay_id),
  
cohort_hour as(select cohort.subject_id, cohort.hadm_id, cohort.icustay_id, hr
  from cohort, UNNEST(GENERATE_ARRAY(-24,datetime_diff(outtime,intime,hour))) as hr
  where cohort.excluded = 0
  order by cohort.subject_id, cohort.hadm_id, cohort.icustay_id),

gcs_Phase_1 as
(select basics.ICUSTAY_ID, basics.charttime, 
  max(case when basics.itemid = 454 then basics.valuenum else null end) as GCSMotor,
  max(case when basics.itemid = 723 then basics.valuenum else null end) as GCSVerbal,
  max(case when basics.itemid = 184 then basics.valuenum else null end) as GCSEyes,
  case when max(case when basics.itemid = 723 then basics.valuenum else null end) = 0 then 1 else 0 end as EndoTrachFlag,
  ROW_NUMBER () OVER (PARTITION BY basics.ICUSTAY_ID ORDER BY basics.charttime ASC) as rn
  from (select chartevent.icustay_id, chartevent.charttime, 
    case when chartevent.ITEMID in (723,223900) then 723
    when chartevent.ITEMID in (454,223901) then 454
    when chartevent.ITEMID in (184,220739) then 184
    else chartevent.ITEMID end as ITEMID, 
    case when chartevent.ITEMID = 723 and chartevent.VALUE = '1.0 ET/Trach' then 0
    when chartevent.ITEMID = 223900 and chartevent.VALUE = 'No Response-ETT' then 0
    else VALUENUM end as VALUENUM
    from physionet-data.mimiciii_clinical.chartevents chartevent
    inner join cohort
    on chartevent.icustay_id = cohort.icustay_id and cohort.excluded = 0
    where chartevent.ITEMID in (184, 454, 723, 223900, 223901, 220739) and chartevent.error!= 1) basics
    group by basics.ICUSTAY_ID, basics.charttime), 
  
gcs_Phase_2 as (
  select a.*, b.GCSVerbal as GCSVerbalPrev, b.GCSMotor as GCSMotorPrev, b.GCSEyes as GCSEyesPrev, 
  case when a.GCSVerbal = 0 or (a.GCSVerbal is null and b.GCSVerbal = 0) then 15
    when b.GCSVerbal = 0 then coalesce(a.GCSMotor,6) + coalesce(a.GCSVerbal,5) + coalesce(a.GCSEyes,4)
    else coalesce(a.GCSMotor,coalesce(b.GCSMotor,6)) + coalesce(a.GCSVerbal,coalesce(b.GCSVerbal,5))
      + coalesce(a.GCSEyes,coalesce(b.GCSEyes,4)) end as GCS
  from gcs_Phase_1 a left join gcs_Phase_1 b on a.ICUSTAY_ID = b.ICUSTAY_ID and a.rn = b.rn+1
  and b.charttime > datetime_sub(a.charttime, interval 6 hour)),
    
gcs_Phase_3 as
(select gcs_Phase_2.icustay_id, charttime, GCS, 
  datetime_diff(gcs_Phase_2.charttime,cohort.intime,hour) as hr, 
  coalesce(GCSMotor,GCSMotorPrev) as GCSMotor, 
  coalesce(GCSVerbal,GCSVerbalPrev) as GCSVerbal, 
  coalesce(GCSEyes,GCSEyesPrev) as GCSEyes, 
  case when coalesce(GCSMotor,GCSMotorPrev) is null then 0 else 1 end
  + case when coalesce(GCSVerbal,GCSVerbalPrev) is null then 0 else 1 end
  + case when coalesce(GCSEyes,GCSEyesPrev) is null then 0 else 1 end as components_measured,
  EndoTrachFlag
  from gcs_Phase_2 inner join cohort 
  on gcs_Phase_2.icustay_id = cohort.icustay_id and cohort.excluded = 0), 

gcs_Phase_4 as
(select icustay_id, hr, GCS, GCSMotor, GCSVerbal, GCSEyes, EndoTrachFlag, 
  ROW_NUMBER() over(PARTITION BY icustay_id, hr ORDER BY components_measured DESC, endotrachflag, gcs, charttime desc) as rn
  from gcs_Phase_3),

gcs as (select icustay_id, hr, GCS, GCSMotor, GCSVerbal, GCSEyes, EndoTrachFlag
from gcs_Phase_4 where rn = 1
order by icustay_id, hr),

bg as(select labevent.hadm_id, labevent.charttime,
  max(case when itemid = 50800 then value else null end) as SPECIMEN,
  avg(case when itemid = 50801 and valuenum > 0 then valuenum else null end) as AADO2,
  avg(case when itemid = 50802 and valuenum > 0 then valuenum else null end) as BASEEXCESS,
  avg(case when itemid = 50803 and valuenum > 0 then valuenum else null end) as BICARBONATE,
  avg(case when itemid = 50804 and valuenum > 0 then valuenum else null end) as TOTALCO2,
  avg(case when itemid = 50805 and valuenum > 0 then valuenum else null end) as CARBOXYHEMOGLOBIN,
  avg(case when itemid = 50806 and valuenum > 0 then valuenum else null end) as CHLORIDE,
  avg(case when itemid = 50808 and valuenum > 0 then valuenum else null end) as CALCIUM,
  avg(case when itemid = 50809 and valuenum > 0 then valuenum else null end) as GLUCOSE,
  avg(case when itemid = 50810 and valuenum <= 100 then valuenum else null end) as HEMATOCRIT,
  avg(case when itemid = 50811 and valuenum > 0 then valuenum else null end) as HEMOGLOBIN,
  max(case when itemid = 50812 then value else null end) as INTUBATED,
  avg(case when itemid = 50813 and valuenum > 0 then valuenum else null end) as LACTATE,
  avg(case when itemid = 50814 and valuenum > 0 then valuenum else null end) as METHEMOGLOBIN,
  avg(case when itemid = 50815 and valuenum > 0 and valuenum <=  70 then valuenum else null end) as O2FLOW,
  avg(case when itemid = 50816 and valuenum > 0 and valuenum <= 100 then valuenum else null end) as FIO2,
  avg(case when itemid = 50817 and valuenum > 0 and valuenum <= 100 then valuenum else null end) as SO2,
  avg(case when itemid = 50818 and valuenum > 0 then valuenum else null end) as PCO2,
  avg(case when itemid = 50819 and valuenum > 0 then valuenum else null end) as PEEP,
  avg(case when itemid = 50820 and valuenum > 0 then valuenum else null end) as PH,
  avg(case when itemid = 50821 and valuenum <= 800 then valuenum else null end) as PO2,
  avg(case when itemid = 50822 and valuenum > 0 then valuenum else null end) as POTASSIUM,
  avg(case when itemid = 50823 and valuenum > 0 then valuenum else null end) as REQUIREDO2,
  avg(case when itemid = 50824 and valuenum > 0 then valuenum else null end) as SODIUM,
  avg(case when itemid = 50825 and valuenum > 0 then valuenum else null end) as TEMPERATURE,
  avg(case when itemid = 50826 and valuenum > 0 then valuenum else null end) as TIDALVOLUME,
  avg(case when itemid = 50827 and valuenum > 0 then valuenum else null end) as VENTILATIONRATE,
  avg(case when itemid = 50828 and valuenum > 0 then valuenum else null end) as VENTILATOR,
from physionet-data.mimiciii_clinical.labevents labevent
where labevent.ITEMID in
(50800, 50801, 50802, 50803, 50804, 50805, 50806, 50807, 50808, 50809, 50810, 50811, 
  50812, 50813, 50814, 50815, 50816, 50817, 50818, 50819, 50820, 50821, 50822, 50823, 
  50824, 50825, 50826, 50827, 50828, 51545)
group by labevent.hadm_id, labevent.charttime
having count(case when itemid = 50800 then value else null end) < 2),

ART_Phase1 as (select HADM_ID, CHARTTIME, avg(valuenum) as SpO2
  from physionet-data.mimiciii_clinical.chartevents
  where ITEMID in (646, 220277) and valuenum > 0 and valuenum <= 100
  group by HADM_ID, CHARTTIME),
  
ART_Phase2 as
(select HADM_ID, CHARTTIME, 
  max(case when itemid = 223835 then 
            case when valuenum > 0 and valuenum <= 1 then valuenum * 100
              when valuenum > 1 and valuenum < 21 then null
              when valuenum >= 21 and valuenum <= 100 then valuenum
              else null end
           when itemid in (3420, 3422) then valuenum
           when itemid = 190 and valuenum > 0.20 and valuenum < 1 then valuenum * 100
           else null end) as fio2_chartevents
  from physionet-data.mimiciii_clinical.chartevents
  where ITEMID in (3420, 190, 223835, 3422) and valuenum > 0 and valuenum < 100 and error != 1
  group by HADM_ID, CHARTTIME),

ART_Phase3 as
(select bg.*, datetime_diff(bg.charttime,cohort.intime,hour) as hr,
  ROW_NUMBER() OVER (partition by bg.hadm_id, bg.charttime order by ART_Phase1.charttime DESC) as lastRowSpO2,
  ART_Phase1.spo2
from bg inner join cohort
on bg.hadm_id = cohort.hadm_id and cohort.excluded = 0
left join ART_Phase1 on bg.hadm_id = ART_Phase1.hadm_id
and ART_Phase1.charttime between datetime_sub(bg.charttime,interval 2 hour) and bg.charttime
where bg.po2 is not null),

ART_Phase4 as (select ART_Phase3.*, 
  ROW_NUMBER() OVER (partition by ART_Phase3.hadm_id, ART_Phase3.charttime order by ART_Phase2.charttime DESC) as lastRowFiO2, 
  ROW_NUMBER() over (partition by ART_Phase3.hadm_id, ART_Phase3.hr order by ART_Phase3.charttime DESC) as lastRowInHour, 
  ART_Phase2.fio2_chartevents,  
  1/(1+exp(-(-0.02544 + 0.04598 * po2
  + coalesce(-0.15356 * spo2             , -0.15356 *   97.49420 +    0.13429)
  + coalesce( 0.00621 * fio2_chartevents ,  0.00621 *   51.49550 +   -0.24958)
  + coalesce( 0.10559 * hemoglobin       ,  0.10559 *   10.32307 +    0.05954)
  + coalesce( 0.13251 * so2              ,  0.13251 *   93.66539 +   -0.23172)
  + coalesce(-0.01511 * pco2             , -0.01511 *   42.08866 +   -0.01630)
  + coalesce( 0.01480 * fio2             ,  0.01480 *   63.97836 +   -0.31142)
  + coalesce(-0.00200 * aado2            , -0.00200 *  442.21186 +   -0.01328)
  + coalesce(-0.03220 * bicarbonate      , -0.03220 *   22.96894 +   -0.06535)
  + coalesce( 0.05384 * totalco2         ,  0.05384 *   24.72632 +   -0.01405)
  + coalesce( 0.08202 * lactate          ,  0.08202 *    3.06436 +    0.06038)
  + coalesce( 0.10956 * ph               ,  0.10956 *    7.36233 +   -0.00617)
  + coalesce( 0.00848 * o2flow           ,  0.00848 *    7.59362 +   -0.35803)
  ))) as SPECIMEN_PROB
from ART_Phase3 left join ART_Phase2 
on ART_Phase3.hadm_id = ART_Phase2.hadm_id and ART_Phase2.fio2_chartevents > 0
and ART_Phase2.charttime between datetime_sub(ART_Phase3.charttime,interval 4 hour) and ART_Phase3.charttime
where ART_Phase3.lastRowSpO2 = 1),

art_bg as(select ART_Phase4.hadm_id, ART_Phase4.charttime, ART_Phase4.hr, SPECIMEN, 
  case when SPECIMEN is not null then SPECIMEN
    when SPECIMEN_PROB > 0.75 then 'ART'
    else null end as SPECIMEN_PRED, 
  SPECIMEN_PROB, SO2, spo2, PO2, PCO2, fio2_chartevents, FIO2, AADO2,
  case when  PO2 is not null and pco2 is not null and coalesce(FIO2, fio2_chartevents) is not null
    then (coalesce(FIO2, fio2_chartevents)/100) * (760 - 47) - (pco2/0.8) - po2
    else null end as AADO2_calc, 
  case when PO2 is not null and coalesce(FIO2, fio2_chartevents) is not null
    then 100*PO2/(coalesce(FIO2, fio2_chartevents)) else null end as PaO2FiO2Ratio, 
  PH, BASEEXCESS, BICARBONATE, TOTALCO2, HEMATOCRIT, HEMOGLOBIN, CARBOXYHEMOGLOBIN, METHEMOGLOBIN,
  CHLORIDE, CALCIUM, TEMPERATURE, POTASSIUM, SODIUM, LACTATE, GLUCOSE, INTUBATED, TIDALVOLUME, 
  VENTILATIONRATE, VENTILATOR, PEEP, O2Flow, REQUIREDO2
from ART_Phase4 where lastRowFiO2 = 1 and lastRowInHour = 1 and (SPECIMEN = 'ART' or SPECIMEN_PROB > 0.75)
order by hadm_id, hr),

lab as(select lab_data.hadm_id, lab_data.hr, 
  avg(CASE WHEN label = 'ANION GAP' THEN valuenum ELSE null END) as ANIONGAP,
  avg(CASE WHEN label = 'ALBUMIN' THEN valuenum ELSE null END) as ALBUMIN,
  avg(CASE WHEN label = 'BANDS' THEN valuenum ELSE null END) as BANDS,
  avg(CASE WHEN label = 'BICARBONATE' THEN valuenum ELSE null END) as BICARBONATE,
  avg(CASE WHEN label = 'BILIRUBIN' THEN valuenum ELSE null END) as BILIRUBIN,
  avg(CASE WHEN label = 'CREATININE' THEN valuenum ELSE null END) as CREATININE,
  avg(CASE WHEN label = 'CHLORIDE' THEN valuenum ELSE null END) as CHLORIDE,
  avg(CASE WHEN label = 'GLUCOSE' THEN valuenum ELSE null END) as GLUCOSE,
  avg(CASE WHEN label = 'HEMATOCRIT' THEN valuenum ELSE null END) as HEMATOCRIT,
  avg(CASE WHEN label = 'HEMOGLOBIN' THEN valuenum ELSE null END) as HEMOGLOBIN,
  avg(CASE WHEN label = 'LACTATE' THEN valuenum ELSE null END) as LACTATE,
  avg(CASE WHEN label = 'PLATELET' THEN valuenum ELSE null END) as PLATELET,
  avg(CASE WHEN label = 'POTASSIUM' THEN valuenum ELSE null END) as POTASSIUM,
  avg(CASE WHEN label = 'PTT' THEN valuenum ELSE null END) as PTT,
  avg(CASE WHEN label = 'INR' THEN valuenum ELSE null END) as INR,
  avg(CASE WHEN label = 'PT' THEN valuenum ELSE null END) as PT,
  avg(CASE WHEN label = 'SODIUM' THEN valuenum ELSE null end) as SODIUM,
  avg(CASE WHEN label = 'BUN' THEN valuenum ELSE null end) as BUN,
  avg(CASE WHEN label = 'WBC' THEN valuenum ELSE null end) as WBC
from (select labevent.hadm_id, datetime_diff(labevent.charttime,cohort.intime,hour) as hr, 
  CASE WHEN itemid = 50868 THEN 'ANION GAP'
       WHEN itemid = 50862 THEN 'ALBUMIN'
       WHEN itemid = 51144 THEN 'BANDS'
       WHEN itemid = 50882 THEN 'BICARBONATE'
       WHEN itemid = 50885 THEN 'BILIRUBIN'
       WHEN itemid = 50912 THEN 'CREATININE'
       WHEN itemid = 50902 THEN 'CHLORIDE'
       WHEN itemid = 50931 THEN 'GLUCOSE'
       WHEN itemid = 51221 THEN 'HEMATOCRIT'
       WHEN itemid = 51222 THEN 'HEMOGLOBIN'
       WHEN itemid = 50813 THEN 'LACTATE'
       WHEN itemid = 51265 THEN 'PLATELET'
       WHEN itemid = 50971 THEN 'POTASSIUM'
       WHEN itemid = 51275 THEN 'PTT'
       WHEN itemid = 51237 THEN 'INR'
       WHEN itemid = 51274 THEN 'PT'
       WHEN itemid = 50983 THEN 'SODIUM'
       WHEN itemid = 51006 THEN 'BUN'
       WHEN itemid = 51300 THEN 'WBC'
       WHEN itemid = 51301 THEN 'WBC'
       ELSE null END AS label,
  CASE WHEN itemid = 50862 and valuenum >    10 THEN null -- g/dL 'ALBUMIN'
       WHEN itemid = 50868 and valuenum > 10000 THEN null -- mEq/L 'ANION GAP'
       WHEN itemid = 51144 and valuenum <     0 THEN null -- immature band forms, %
       WHEN itemid = 51144 and valuenum >   100 THEN null -- immature band forms, %
       WHEN itemid = 50882 and valuenum > 10000 THEN null -- mEq/L 'BICARBONATE'
       WHEN itemid = 50885 and valuenum >   150 THEN null -- mg/dL 'BILIRUBIN'
       WHEN itemid = 50806 and valuenum > 10000 THEN null -- mEq/L 'CHLORIDE'
       WHEN itemid = 50902 and valuenum > 10000 THEN null -- mEq/L 'CHLORIDE'
       WHEN itemid = 50912 and valuenum >   150 THEN null -- mg/dL 'CREATININE'
       WHEN itemid = 50809 and valuenum > 10000 THEN null -- mg/dL 'GLUCOSE'
       WHEN itemid = 50931 and valuenum > 10000 THEN null -- mg/dL 'GLUCOSE'
       WHEN itemid = 50810 and valuenum >   100 THEN null -- % 'HEMATOCRIT'
       WHEN itemid = 51221 and valuenum >   100 THEN null -- % 'HEMATOCRIT'
       WHEN itemid = 50811 and valuenum >    50 THEN null -- g/dL 'HEMOGLOBIN'
       WHEN itemid = 51222 and valuenum >    50 THEN null -- g/dL 'HEMOGLOBIN'
       WHEN itemid = 50813 and valuenum >    50 THEN null -- mmol/L 'LACTATE'
       WHEN itemid = 51265 and valuenum > 10000 THEN null -- K/uL 'PLATELET'
       WHEN itemid = 50822 and valuenum >    30 THEN null -- mEq/L 'POTASSIUM'
       WHEN itemid = 50971 and valuenum >    30 THEN null -- mEq/L 'POTASSIUM'
       WHEN itemid = 51275 and valuenum >   150 THEN null -- sec 'PTT'
       WHEN itemid = 51237 and valuenum >    50 THEN null -- 'INR'
       WHEN itemid = 51274 and valuenum >   150 THEN null -- sec 'PT'
       WHEN itemid = 50824 and valuenum >   200 THEN null -- mEq/L == mmol/L 'SODIUM'
       WHEN itemid = 50983 and valuenum >   200 THEN null -- mEq/L == mmol/L 'SODIUM'
       WHEN itemid = 51006 and valuenum >   300 THEN null -- 'BUN'
       WHEN itemid = 51300 and valuenum >  1000 THEN null -- 'WBC'
       WHEN itemid = 51301 and valuenum >  1000 THEN null -- 'WBC'
       ELSE labevent.valuenum END AS valuenum
  from physionet-data.mimiciii_clinical.labevents labevent inner join cohort
  on labevent.hadm_id = cohort.hadm_id and cohort.excluded = 0
  where labevent.ITEMID in
   (50868, -- ANION GAP | CHEMISTRY | BLOOD | 769895
    50862, -- ALBUMIN | CHEMISTRY | BLOOD | 146697
    51144, -- BANDS - hematology
    50882, -- BICARBONATE | CHEMISTRY | BLOOD | 780733
    50885, -- BILIRUBIN, TOTAL | CHEMISTRY | BLOOD | 238277
    50912, -- CREATININE | CHEMISTRY | BLOOD | 797476
    50902, -- CHLORIDE | CHEMISTRY | BLOOD | 795568
    50931, -- GLUCOSE | CHEMISTRY | BLOOD | 748981
    51221, -- HEMATOCRIT | HEMATOLOGY | BLOOD | 881846
    51222, -- HEMOGLOBIN | HEMATOLOGY | BLOOD | 752523
    50813, -- LACTATE | BLOOD GAS | BLOOD | 187124
    51265, -- PLATELET COUNT | HEMATOLOGY | BLOOD | 778444
    50971, -- POTASSIUM | CHEMISTRY | BLOOD | 845825
    51275, -- PTT | HEMATOLOGY | BLOOD | 474937
    51237, -- INR(PT) | HEMATOLOGY | BLOOD | 471183
    51274, -- PT | HEMATOLOGY | BLOOD | 469090
    50983, -- SODIUM | CHEMISTRY | BLOOD | 808489
    51006, -- UREA NITROGEN | CHEMISTRY | BLOOD | 791925
    51301, -- WHITE BLOOD CELLS | HEMATOLOGY | BLOOD | 753301
    51300  -- WBC COUNT | HEMATOLOGY | BLOOD | 2371
    ) and valuenum is not null and valuenum > 0 ) lab_data
GROUP BY lab_data.hadm_id, lab_data.hr
ORDER BY lab_data.hadm_id, lab_data.hr),

uno as (select icustay_id, hr, sum(UrineOutput) as UrineOutput
from(select cohort.icustay_id, datetime_diff(outputevent.charttime,cohort.intime,hour) as hr, 
  case when outputevent.itemid = 227489 then -1*outputevent.value
    else outputevent.value end as UrineOutput
  from cohort inner join physionet-data.mimiciii_clinical.outputevents outputevent
  on cohort.icustay_id = outputevent.icustay_id where cohort.excluded = 0
  and itemid in
 (40055, 43175, 40069, 40094, 40715, 40473, 40085, 40057, 40056, 40405,
  40428, 40086, 40096, 40651, 226559, 226560, 226561, 226584, 226563,
  226564, 226565, 226567, 226557, 226558, 227488, 227489)) uno_data
group by uno_data.icustay_id, uno_data.hr
order by uno_data.icustay_id, uno_data.hr),

vital_ce as
(select cohort.icustay_id, datetime_diff(chartevent.charttime, cohort.intime,hour) as hr,
  case when itemid in (211,220045) and valuenum > 0 and valuenum < 300 then valuenum else null end as HeartRate,
  case when itemid in (51,442,455,6701,220179,220050) and valuenum > 0 and valuenum < 400 then valuenum else null end as SysBP,
  case when itemid in (8368,8440,8441,8555,220180,220051) and valuenum > 0 and valuenum < 300 then valuenum else null end as DiasBP,
  case when itemid in (456,52,6702,443,220052,220181,225312) and valuenum > 0 and valuenum < 300 then valuenum else null end as MeanBP,
  case when itemid in (615,618,220210,224690) and valuenum > 0 and valuenum < 70 then valuenum else null end as RespRate,
  case when itemid in (223761,678) and valuenum > 70 and valuenum < 120 then (valuenum-32)/1.8
    when itemid in (223762,676) and valuenum > 10 and valuenum < 50  then valuenum else null end as TempC,
  case when itemid in (646,220277) and valuenum > 0 and valuenum <= 100 then valuenum else null end as SpO2,
  case when itemid in (807,811,1529,3745,3744,225664,220621,226537) and valuenum > 0 then valuenum else null end as Glucose
  from cohort inner join physionet-data.mimiciii_clinical.chartevents chartevent
  on cohort.icustay_id = chartevent.icustay_id and cohort.excluded = 0
  where chartevent.error != 1 and chartevent.itemid in
  (211, 220045, 51, 442, 455, 6701, 220179, 220050, 8368, 8440, 8441, 8555,
  220180, 220051, 456, 52, 6702, 443, 220052, 220181, 225312, 618, 615,
  220210, 224690, 646, 220277, 807, 811, 1529, 3745, 3744, 
  225664, 220621, 226537, 223762, 676, 223761, 678)),
  
vitals as (select vital_ce.icustay_id, vital_ce.hr, avg(HeartRate) as HeartRate, avg(SysBP) as SysBP, avg(DiasBP) as DiasBP,
  avg(MeanBP) as MeanBP, avg(RespRate) as RespRate, avg(TempC) as TempC, avg(SpO2) as SpO2, avg(Glucose) as Glucose
from vital_ce
group by vital_ce.icustay_id, vital_ce.hr
order by vital_ce.icustay_id, vital_ce.hr),

data_all as( select
  cohort_hour.subject_id, cohort_hour.hadm_id, cohort_hour.icustay_id, cohort_hour.hr,
  vitals.HeartRate, vitals.SysBP, vitals.DiasBP, vitals.MeanBP, vitals.RespRate,
  coalesce(art_bg.TEMPERATURE, vitals.TempC) as tempc, 
  coalesce(art_bg.SO2, vitals.SpO2) as spo2, 
  coalesce(lab.GLUCOSE,art_bg.GLUCOSE,vitals.Glucose) as glucose, 
  gcs.GCS, gcs.GCSMotor, gcs.GCSVerbal, gcs.GCSEyes, gcs.EndoTrachFlag,
  art_bg.PO2 as bg_PO2, art_bg.PCO2 as bg_PCO2, art_bg.PaO2FiO2Ratio as bg_PaO2FiO2Ratio,
  art_bg.PH as bg_PH, art_bg.BASEEXCESS as bg_BASEEXCESS, art_bg.TOTALCO2 as bg_TOTALCO2,
  art_bg.CARBOXYHEMOGLOBIN as bg_CARBOXYHEMOGLOBIN, art_bg.METHEMOGLOBIN as bg_METHEMOGLOBIN,
  lab.ANIONGAP as ANIONGAP, lab.ALBUMIN as ALBUMIN, lab.BANDS as BANDS,
  coalesce(lab.BICARBONATE,art_bg.BICARBONATE) as BICARBONATE, 
  lab.BILIRUBIN as BILIRUBIN, art_bg.CALCIUM as CALCIUM, lab.CREATININE as CREATININE,
  coalesce(lab.CHLORIDE, art_bg.CHLORIDE) as CHLORIDE,
  coalesce(lab.HEMATOCRIT,art_bg.HEMATOCRIT) as HEMATOCRIT,
  coalesce(lab.HEMOGLOBIN,art_bg.HEMOGLOBIN) as HEMOGLOBIN,
  coalesce(lab.LACTATE,art_bg.LACTATE) as LACTATE, 
  lab.PLATELET as PLATELET,
  coalesce(lab.POTASSIUM, art_bg.POTASSIUM) as POTASSIUM,
  lab.PTT as PTT, lab.INR as INR, lab.BUN as BUN, lab.WBC as WBC,
  coalesce(lab.SODIUM, art_bg.SODIUM) as SODIUM,
  uno.UrineOutput
from cohort_hour left join vitals on cohort_hour.icustay_id = vitals.icustay_id and cohort_hour.hr = vitals.hr
left join gcs on cohort_hour.icustay_id = gcs.icustay_id and cohort_hour.hr = gcs.hr
left join uno on cohort_hour.icustay_id = uno.icustay_id and cohort_hour.hr = uno.hr
left join art_bg on cohort_hour.hadm_id = art_bg.hadm_id and cohort_hour.hr = art_bg.hr
left join lab on cohort_hour.hadm_id = lab.hadm_id and cohort_hour.hr = lab.hr
order by cohort_hour.subject_id, cohort_hour.hadm_id, cohort_hour.icustay_id, cohort_hour.hr)


select icustay_id, hr, heartrate, sysbp, diasbp, meanbp, resprate, spo2, hospital_expire_flag
        from data_all inner join cohort
        using (icustay_id)
        where hr>=0 and hr<=6
        order by icustay_id, hr


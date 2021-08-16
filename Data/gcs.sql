with 
Phase_1 as
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
    inner join `bd4h-project.test.New_cohort` cohort
    on chartevent.icustay_id = cohort.icustay_id and cohort.excluded = 0
    where chartevent.ITEMID in (184, 454, 723, 223900, 223901, 220739) and chartevent.error!= 1) basics
    group by basics.ICUSTAY_ID, basics.charttime), 
  
Phase_2 as (
  select a.*, b.GCSVerbal as GCSVerbalPrev, b.GCSMotor as GCSMotorPrev, b.GCSEyes as GCSEyesPrev, 
  case when a.GCSVerbal = 0 or (a.GCSVerbal is null and b.GCSVerbal = 0) then 15
    when b.GCSVerbal = 0 then coalesce(a.GCSMotor,6) + coalesce(a.GCSVerbal,5) + coalesce(a.GCSEyes,4)
    else coalesce(a.GCSMotor,coalesce(b.GCSMotor,6)) + coalesce(a.GCSVerbal,coalesce(b.GCSVerbal,5))
      + coalesce(a.GCSEyes,coalesce(b.GCSEyes,4)) end as GCS
  from Phase_1 a left join Phase_1 b on a.ICUSTAY_ID = b.ICUSTAY_ID and a.rn = b.rn+1
  and b.charttime > datetime_sub(a.charttime, interval 6 hour)),
    
Phase_3 as
(select gs.icustay_id
  , charttime
  , datetime_diff(gs.charttime,co.intime,hour) as hr
  , GCS
  , coalesce(GCSMotor,GCSMotorPrev) as GCSMotor
  , coalesce(GCSVerbal,GCSVerbalPrev) as GCSVerbal
  , coalesce(GCSEyes,GCSEyesPrev) as GCSEyes
  , case when coalesce(GCSMotor,GCSMotorPrev) is null then 0 else 1 end
  + case when coalesce(GCSVerbal,GCSVerbalPrev) is null then 0 else 1 end
  + case when coalesce(GCSEyes,GCSEyesPrev) is null then 0 else 1 end
    as components_measured
  , EndoTrachFlag as EndoTrachFlag
  from Phase_1
  inner join `bd4h-project.test.New_cohort` cohort
    on Phase_1.icustay_id = cohort.icustay_id
    and cohort.excluded = 0), 

gcs_priority as
(
  select icustay_id
    , hr
    , GCS
    , GCSMotor
    , GCSVerbal
    , GCSEyes
    , EndoTrachFlag
    , ROW_NUMBER() over
      (
        PARTITION BY icustay_id, hr
        ORDER BY components_measured DESC, endotrachflag, gcs, charttime desc
      ) as rn
  from gcs_stg
)


select icustay_id, hr, GCS, GCSMotor, GCSVerbal, GCSEyes, EndoTrachFlag
from gcs_priority gs where rn = 1
order by icustay_id, hr;
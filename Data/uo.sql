select icustay_id, hr, sum(UrineOutput) as UrineOutput
from
(select cohort.icustay_id, datetime_diff(outputevent.charttime,cohort.intime,hour) as hr, 
  case when outputevent.itemid = 227489 then -1*outputevent.value
    else outputevent.value end as UrineOutput
  from bd4h-project.test.mp_cohort cohort
  inner join physionet-data.mimiciii_clinical.outputevents outputevent
  on cohort.icustay_id = outputevent.icustay_id where cohort.excluded = 0
  and itemid in
 (40055, -- "Urine Out Foley"
  43175, -- "Urine ."
  40069, -- "Urine Out Void"
  40094, -- "Urine Out Condom Cath"
  40715, -- "Urine Out Suprapubic"
  40473, -- "Urine Out IleoConduit"
  40085, -- "Urine Out Incontinent"
  40057, -- "Urine Out Rt Nephrostomy"
  40056, -- "Urine Out Lt Nephrostomy"
  40405, -- "Urine Out Other"
  40428, -- "Urine Out Straight Cath"
  40086,--	Urine Out Incontinent
  40096, -- "Urine Out Ureteral Stent #1"
  40651, -- "Urine Out Ureteral Stent #2"
  226559, -- "Foley"
  226560, -- "Void"
  226561, -- "Condom Cath"
  226584, -- "Ileoconduit"
  226563, -- "Suprapubic"
  226564, -- "R Nephrostomy"
  226565, -- "L Nephrostomy"
  226567, --	Straight Cath
  226557, -- R Ureteral Stent
  226558, -- L Ureteral Stent
  227488, -- GU Irrigant Volume In
  227489  -- GU Irrigant/Urine Volume Out
  )) uno_data
group by uno_data.icustay_id, uno_data.hr
order by uno_data.icustay_id, uno_data.hr;
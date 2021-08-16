-- this combines all views to get all features at all time points
select
  cohort_hour.subject_id, cohort_hour.hadm_id, cohort_hour.icustay_id, cohort_hour.hr,
  vitals.HeartRate, vitals.SysBP, vitals.DiasBP, vitals.MeanBP, vitals.RespRate.
  coalesce(bg.TEMPERATURE, vitals.TempC) as tempc, 
  coalesce(bg.SO2, vitals.SpO2) as spo2, 
  coalesce(lab.GLUCOSE,bg.GLUCOSE,vitals.Glucose) as glucose, 
  gcs.GCS, gcs.GCSMotor, gcs.GCSVerbal, gcs.GCSEyes, gcs.EndoTrachFlag,
  bg.PO2 as bg_PO2, bg.PCO2 as bg_PCO2, bg.PaO2FiO2Ratio as bg_PaO2FiO2Ratio,
  bg.PH as bg_PH, bg.BASEEXCESS as bg_BASEEXCESS, bg.TOTALCO2 as bg_TOTALCO2,
  bg.CARBOXYHEMOGLOBIN as bg_CARBOXYHEMOGLOBIN, bg.METHEMOGLOBIN as bg_METHEMOGLOBIN,
  lab.ANIONGAP as ANIONGAP, lab.ALBUMIN as ALBUMIN, lab.BANDS as BANDS,
  coalesce(lab.BICARBONATE,bg.BICARBONATE) as BICARBONATE, 
  lab.BILIRUBIN as BILIRUBIN, bg.CALCIUM as CALCIUM, lab.CREATININE as CREATININE,
  coalesce(lab.CHLORIDE, bg.CHLORIDE) as CHLORIDE,
  coalesce(lab.HEMATOCRIT,bg.HEMATOCRIT) as HEMATOCRIT,
  coalesce(lab.HEMOGLOBIN,bg.HEMOGLOBIN) as HEMOGLOBIN,
  coalesce(lab.LACTATE,bg.LACTATE) as LACTATE, 
  lab.PLATELET as PLATELET,
  coalesce(lab.POTASSIUM, bg.POTASSIUM) as POTASSIUM,
  lab.PTT as PTT, lab.INR as INR, lab.BUN as BUN, lab.WBC as WBC,
  coalesce(lab.SODIUM, bg.SODIUM) as SODIUM,
  uno.UrineOutput

from bd4h-project.test.New_cohort_hour cohort_hour
left join bd4h-project.test.New_vitals vitals
on cohort_hour.icustay_id = vitals.icustay_id and cohort_hour.hr = vitals.hr
left join bd4h-project.test.New_gcs gcs
on cohort_hour.icustay_id = gcs.icustay_id and cohort_hour.hr = gcs.hr
left join bd4h-project.test.new_uno uno 
on cohort_hour.icustay_id = uno.icustay_id and cohort_hour.hr = uno.hr
left join bd4h-project.test.New_bg_art bg
on  cohort_hour.hadm_id = bg.hadm_id and cohort_hour.hr = bg.hr
left join bd4h-project.test.New_lab lab
on cohort_hour.hadm_id = lab.hadm_id and cohort_hour.hr = lab.hr
order by cohort_hour.subject_id, cohort_hour.hadm_id, cohort_hour.icustay_id, cohort_hour.hr;
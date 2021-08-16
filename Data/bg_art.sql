--CREATE TABLE mp_bg_art AS
with stg_spo2 as
(
  select HADM_ID, CHARTTIME
    , avg(valuenum) as SpO2
  from physionet-data.mimiciii_clinical.chartevents
  where ITEMID in
  (
    646 -- SpO2
  , 220277 -- O2 saturation pulseoxymetry
  )
  and valuenum > 0 and valuenum <= 100
  group by HADM_ID, CHARTTIME
)
, stg_fio2 as
(
  select HADM_ID, CHARTTIME
    , max(
        case
          when itemid = 223835
            then case
              when valuenum > 0 and valuenum <= 1
                then valuenum * 100
              when valuenum > 1 and valuenum < 21
                then null
              when valuenum >= 21 and valuenum <= 100
                then valuenum
              else null end
        when itemid in (3420, 3422)
            then valuenum
        when itemid = 190 and valuenum > 0.20 and valuenum < 1
            then valuenum * 100
      else null end
    ) as fio2_chartevents
  from physionet-data.mimiciii_clinical.chartevents
  where ITEMID in
  (
    3420 -- FiO2
  , 190 -- FiO2 set
  , 223835 -- Inspired O2 Fraction (FiO2)
  , 3422 -- FiO2 [measured]
  )
  and valuenum > 0 and valuenum < 100
  and error != 1
  group by HADM_ID, CHARTTIME
)
, stg2 as
(
select bg.*
  , datetime_diff(bg.charttime,co.intime,hour) as hr
  , ROW_NUMBER() OVER (partition by bg.hadm_id, bg.charttime order by s1.charttime DESC) as lastRowSpO2
  , s1.spo2
from bd4h-project.test.mp_bg bg
inner join bd4h-project.test.mp_cohort co
  on bg.hadm_id = co.hadm_id
  and co.excluded = 0
left join stg_spo2 s1
  on  bg.hadm_id = s1.hadm_id
  and s1.charttime between datetime_sub(bg.charttime,interval 2 hour) and bg.charttime
where bg.po2 is not null
)
, stg3 as
(
select bg.*
  , ROW_NUMBER() OVER (partition by bg.hadm_id, bg.charttime order by s2.charttime DESC) as lastRowFiO2
  , ROW_NUMBER() over (partition by bg.hadm_id, bg.hr order by bg.charttime DESC) as lastRowInHour
  , s2.fio2_chartevents
  ,  1/(1+exp(-(-0.02544
  +    0.04598 * po2
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
from stg2 bg
left join stg_fio2 s2
  on  bg.hadm_id = s2.hadm_id
  and s2.charttime between datetime_sub(bg.charttime,interval 4 hour) and bg.charttime
  and s2.fio2_chartevents > 0
where bg.lastRowSpO2 = 1
)

select
  stg3.hadm_id
  , stg3.charttime
  , stg3.hr
  , SPECIMEN
  , case
        when SPECIMEN is not null then SPECIMEN
        when SPECIMEN_PROB > 0.75 then 'ART'
      else null end as SPECIMEN_PRED
  , SPECIMEN_PROB
  , SO2, spo2
  , PO2, PCO2
  , fio2_chartevents, FIO2
  , AADO2
  , case
      when  PO2 is not null
        and pco2 is not null
        and coalesce(FIO2, fio2_chartevents) is not null
        then (coalesce(FIO2, fio2_chartevents)/100) * (760 - 47) - (pco2/0.8) - po2
      else null
    end as AADO2_calc
  , case
      when PO2 is not null and coalesce(FIO2, fio2_chartevents) is not null
        then 100*PO2/(coalesce(FIO2, fio2_chartevents))
      else null
    end as PaO2FiO2Ratio
  , PH, BASEEXCESS
  , BICARBONATE, TOTALCO2
  , HEMATOCRIT
  , HEMOGLOBIN
  , CARBOXYHEMOGLOBIN
  , METHEMOGLOBIN
  , CHLORIDE, CALCIUM
  , TEMPERATURE
  , POTASSIUM, SODIUM
  , LACTATE
  , GLUCOSE
  , INTUBATED, TIDALVOLUME, VENTILATIONRATE, VENTILATOR
  , PEEP, O2Flow
  , REQUIREDO2
from stg3
where lastRowFiO2 = 1
and lastRowInHour = 1
and (SPECIMEN = 'ART' or SPECIMEN_PROB > 0.75)
order by hadm_id, hr;
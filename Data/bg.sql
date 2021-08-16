--CREATE TABLE mp_bg as
-- blood gases and chemistry values which were found in LABEVENTS

select le.hadm_id, le.charttime
, max(case when itemid = 50800 then value else null end) as SPECIMEN
, avg(case when itemid = 50801 and valuenum > 0 then valuenum else null end) as AADO2
, avg(case when itemid = 50802 and valuenum > 0 then valuenum else null end) as BASEEXCESS
, avg(case when itemid = 50803 and valuenum > 0 then valuenum else null end) as BICARBONATE
, avg(case when itemid = 50804 and valuenum > 0 then valuenum else null end) as TOTALCO2
, avg(case when itemid = 50805 and valuenum > 0 then valuenum else null end) as CARBOXYHEMOGLOBIN
, avg(case when itemid = 50806 and valuenum > 0 then valuenum else null end) as CHLORIDE
, avg(case when itemid = 50808 and valuenum > 0 then valuenum else null end) as CALCIUM
, avg(case when itemid = 50809 and valuenum > 0 then valuenum else null end) as GLUCOSE
, avg(case when itemid = 50810 and valuenum <= 100 then valuenum else null end) as HEMATOCRIT
, avg(case when itemid = 50811 and valuenum > 0 then valuenum else null end) as HEMOGLOBIN
, max(case when itemid = 50812 then value else null end) as INTUBATED
, avg(case when itemid = 50813 and valuenum > 0 then valuenum else null end) as LACTATE
, avg(case when itemid = 50814 and valuenum > 0 then valuenum else null end) as METHEMOGLOBIN
, avg(case when itemid = 50815 and valuenum > 0 and valuenum <=  70 then valuenum else null end) as O2FLOW
, avg(case when itemid = 50816 and valuenum > 0 and valuenum <= 100 then valuenum else null end) as FIO2
, avg(case when itemid = 50817 and valuenum > 0 and valuenum <= 100 then valuenum else null end) as SO2 -- OXYGENSATURATION
, avg(case when itemid = 50818 and valuenum > 0 then valuenum else null end) as PCO2
, avg(case when itemid = 50819 and valuenum > 0 then valuenum else null end) as PEEP
, avg(case when itemid = 50820 and valuenum > 0 then valuenum else null end) as PH
, avg(case when itemid = 50821 and valuenum <= 800 then valuenum else null end) as PO2
, avg(case when itemid = 50822 and valuenum > 0 then valuenum else null end) as POTASSIUM
, avg(case when itemid = 50823 and valuenum > 0 then valuenum else null end) as REQUIREDO2
, avg(case when itemid = 50824 and valuenum > 0 then valuenum else null end) as SODIUM
, avg(case when itemid = 50825 and valuenum > 0 then valuenum else null end) as TEMPERATURE
, avg(case when itemid = 50826 and valuenum > 0 then valuenum else null end) as TIDALVOLUME
, avg(case when itemid = 50827 and valuenum > 0 then valuenum else null end) as VENTILATIONRATE
, avg(case when itemid = 50828 and valuenum > 0 then valuenum else null end) as VENTILATOR
from physionet-data.mimiciii_clinical.labevents le
where le.ITEMID in
-- blood gases
(
    50800, 50801, 50802, 50803, 50804, 50805, 50806, 50807, 50808, 50809
  , 50810, 50811, 50812, 50813, 50814, 50815, 50816, 50817, 50818, 50819
  , 50820, 50821, 50822, 50823, 50824, 50825, 50826, 50827, 50828
  , 51545
)
group by le.hadm_id, le.charttime
having count(case when itemid = 50800 then value else null end)<2;
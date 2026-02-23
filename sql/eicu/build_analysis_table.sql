-- eICU-CRD template: build analysis-ready table for SUP PPI vs H2RA.
--
-- Output: dlfx_eicu_sup_ppi_h2ra
--
-- NOTE: eICU schemas differ across installs. Treat this as a mapping guide.

drop table if exists dlfx_eicu_sup_ppi_h2ra;

create table dlfx_eicu_sup_ppi_h2ra as
with
base as (
  select
    p.patientunitstayid as stay_id,
    p.uniquepid as patient_id,
    p.unitadmittime24 as icu_intime_text,
    p.unitdischargetime24 as icu_outtime_text,
    p.unitdischargeoffset as unitdischargeoffset,
    p.unitdischargestatus as unitdischargestatus,
    -- Many eICU tables store offsets in minutes from ICU admission.
    1440 as landmark_minutes,
    (case
      when p.age ~ '^[0-9]+$' then p.age::numeric
      when p.age ilike '%>%' then 90::numeric
      else null::numeric
    end) as age_years,
    p.gender as sex,
    p.ethnicity as race
  from patient p
  where (case
    when p.age ~ '^[0-9]+$' then p.age::int
    when p.age ilike '%>%' then 90
    else null
  end) >= 18
),
labs_24h as (
  -- TODO: verify lab table/fields in your eICU schema.
  -- Common pattern: lab.labresultoffset (minutes), lab.labname, lab.labresult
  select
    b.stay_id,
    min(case when l.labname ilike '%hemoglobin%' then l.labresult::numeric end) as hgb_min_24h,
    min(case when l.labname ilike '%platelet%' then l.labresult::numeric end) as platelet_min_24h,
    max(case when l.labname ilike '%inr%' then l.labresult::numeric end) as inr_max_24h
  from base b
  left join lab l
    on l.patientunitstayid = b.stay_id
   and l.labresultoffset >= 0
   and l.labresultoffset < 1440
  group by b.stay_id
),
coagulopathy as (
  select
    b.stay_id,
    l.platelet_min_24h,
    l.inr_max_24h,
    (case
      when l.platelet_min_24h is not null and l.platelet_min_24h < 50 then 1
      when l.inr_max_24h is not null and l.inr_max_24h > 1.5 then 1
      else 0
    end) as sup_indication_coagulopathy_24h
  from base b
  left join labs_24h l
    on l.stay_id = b.stay_id
),
mv_24h as (
  select
    b.stay_id,
    (case when max(case when rc.patientunitstayid is not null then 1 else 0 end) = 1 then 1 else 0 end) as sup_indication_mv_24h
  from base b
  left join respiratoryCare rc
    on rc.patientunitstayid = b.stay_id
   and rc.ventstartoffset is not null
   and rc.ventstartoffset < b.landmark_minutes
   and coalesce(nullif(rc.ventendoffset, 0), 999999) > 0
  group by b.stay_id
),
sup_flags as (
  select
    b.*,
    c.platelet_min_24h,
    c.inr_max_24h,
    mv.sup_indication_mv_24h,
    c.sup_indication_coagulopathy_24h,
    (case when mv.sup_indication_mv_24h = 1 or c.sup_indication_coagulopathy_24h = 1 then 1 else 0 end) as eligible_sup_high_risk
  from base b
  left join coagulopathy c on c.stay_id = b.stay_id
  left join mv_24h mv on mv.stay_id = b.stay_id
),
rx_24h as (
  -- TODO: map medication table/fields (often medication.medicationoffset, medication.drugname).
  select
    s.stay_id,
    max(case
      when m.drugname ilike '%omeprazole%' or m.drugname ilike '%pantoprazole%' or m.drugname ilike '%esomeprazole%'
        or m.drugname ilike '%lansoprazole%' or m.drugname ilike '%rabeprazole%'
      then 1 else 0 end) as ppi_any_24h,
    max(case
      when m.drugname ilike '%famotidine%' or m.drugname ilike '%ranitidine%'
        or m.drugname ilike '%cimetidine%' or m.drugname ilike '%nizatidine%'
      then 1 else 0 end) as h2ra_any_24h
  from sup_flags s
  left join medication m
    on m.patientunitstayid = s.stay_id
   and m.drugstartoffset >= 0
   and m.drugstartoffset < 1440
  group by s.stay_id
),
exposure as (
  select
    s.*,
    coalesce(r.ppi_any_24h, 0) as ppi_any_24h,
    coalesce(r.h2ra_any_24h, 0) as h2ra_any_24h,
    (case when coalesce(r.ppi_any_24h,0)=1 and coalesce(r.h2ra_any_24h,0)=1 then 1 else 0 end) as dual_ppi_h2ra_24h,
    (case
      when coalesce(r.ppi_any_24h,0)=1 and coalesce(r.h2ra_any_24h,0)=0 then 'ppi'
      when coalesce(r.ppi_any_24h,0)=0 and coalesce(r.h2ra_any_24h,0)=1 then 'h2ra'
      else null
    end) as treatment
  from sup_flags s
  left join rx_24h r on r.stay_id = s.stay_id
),
final as (
  select
    'eicu' as dataset,
    e.stay_id,
    e.patient_id,
    null::bigint as hadm_id,
    null::timestamp as icu_intime,
    null::timestamp as icu_outtime,
    null::timestamp as index_time,
    e.age_years,
    e.sex,
    e.race,
    1 as is_first_icu_stay,
    (case when e.unitdischargeoffset >= 1440 then 1 else 0 end) as alive_in_icu_at_landmark,
    (case when e.unitdischargeoffset >= 1440 then 1 else 0 end) as alive_at_landmark,
    e.sup_indication_mv_24h,
    e.sup_indication_coagulopathy_24h,
    e.eligible_sup_high_risk,
    coalesce(liver.liver_disease, 0) as liver_disease,
    coalesce(a24.antithrombotic_any_24h, 0) as antithrombotic_any_24h,
    e.platelet_min_24h,
    e.inr_max_24h,
    l.hgb_min_24h,
    null::numeric as hgb_max_24h,
    null::numeric as creatinine_min_24h,
    null::numeric as creatinine_max_24h,
    null::numeric as lactate_min_24h,
    null::numeric as lactate_max_24h,
    null::numeric as sofa_24h,
    e.treatment,
    e.ppi_any_24h,
    e.h2ra_any_24h,
    e.dual_ppi_h2ra_24h,
    (case when dx.ugib_time_days is not null then coalesce(dx.ugib_event, 0) else 0 end) as cigib_strict_event,
    coalesce(dx.ugib_time_days, f.followup_time_14d) as cigib_strict_time_days,
    (case when dx.ugib_time_days is not null then coalesce(dx.ugib_event, 0) else 0 end) as ugib_broad_event,
    coalesce(dx.ugib_time_days, f.followup_time_14d) as ugib_broad_time_days,
    coalesce(dx.cdi_event, 0) as cdi_event,
    dx.cdi_time_days as cdi_time_days,
    coalesce(d.death_event_28d, 0) as death_event_28d,
    coalesce(d.death_time_days, f.followup_time_28d) as death_time_days
  from exposure e
  left join labs_24h l on l.stay_id = e.stay_id
  left join (
    select
      e2.stay_id,
      max(case
        when d.icd9code like '571%' then 1
        when d.icd9code like '572%' then 1
        when d.diagnosisstring ilike '%cirrhosis%' then 1
        when d.diagnosisstring ilike '%liver failure%' then 1
        when d.diagnosisstring ilike '%hepatic failure%' then 1
        when d.diagnosisstring ilike '%portal hypertension%' then 1
        else 0 end) as liver_disease
    from exposure e2
    left join diagnosis d on d.patientunitstayid = e2.stay_id
    group by e2.stay_id
  ) liver on liver.stay_id = e.stay_id
  left join (
    select
      e3.stay_id,
      max(case
        when m.drugname ilike '%warfarin%' then 1
        when m.drugname ilike '%heparin%' then 1
        when m.drugname ilike '%enoxaparin%' then 1
        when m.drugname ilike '%dalteparin%' then 1
        when m.drugname ilike '%fondaparinux%' then 1
        when m.drugname ilike '%apixaban%' then 1
        when m.drugname ilike '%rivaroxaban%' then 1
        when m.drugname ilike '%dabigatran%' then 1
        when m.drugname ilike '%edoxaban%' then 1
        when m.drugname ilike '%clopidogrel%' then 1
        when m.drugname ilike '%ticagrelor%' then 1
        when m.drugname ilike '%prasugrel%' then 1
        when m.drugname ilike '%aspirin%' then 1
        else 0 end) as antithrombotic_any_24h
    from exposure e3
    left join medication m
      on m.patientunitstayid = e3.stay_id
     and m.drugstartoffset >= 0
     and m.drugstartoffset < 1440
    group by e3.stay_id
  ) a24 on a24.stay_id = e.stay_id
  left join (
    -- Diagnosis-based outcomes with offset timing (minutes since ICU admission).
    select
      p.patientunitstayid as stay_id,
      max(case
        when d.icd9code like '578%' then 1
        when d.diagnosisstring ilike '%gastrointestinal%hemorrhage%' then 1
        when d.diagnosisstring ilike '%gi%bleed%' then 1
        when d.diagnosisstring ilike '%upper%gi%bleed%' then 1
        else 0
      end) as ugib_event,
      min(case
        when (
          d.icd9code like '578%'
          or d.diagnosisstring ilike '%gastrointestinal%hemorrhage%'
          or d.diagnosisstring ilike '%gi%bleed%'
          or d.diagnosisstring ilike '%upper%gi%bleed%'
        )
        and d.diagnosisoffset >= 1440
        then d.diagnosisoffset
        else null
      end)::numeric / 1440.0 - 1.0 as ugib_time_days,
      max(case
        when d.icd9code like '00845%' then 1
        when d.diagnosisstring ilike '%clostrid%' then 1
        else 0
      end) as cdi_event,
      min(case
        when (d.icd9code like '00845%' or d.diagnosisstring ilike '%clostrid%')
         and d.diagnosisoffset >= 1440
        then d.diagnosisoffset
        else null
      end)::numeric / 1440.0 - 1.0 as cdi_time_days
    from patient p
    left join diagnosis d
      on d.patientunitstayid = p.patientunitstayid
    group by p.patientunitstayid
  ) dx
    on dx.stay_id = e.stay_id
  left join (
    select
      p.patientunitstayid as stay_id,
      (case
        when p.unitdischargestatus ilike 'Expired' and p.unitdischargeoffset <= (1440 + 28*1440)
        then 1 else 0 end) as death_event_28d,
      (case
        when p.unitdischargestatus ilike 'Expired'
         and p.unitdischargeoffset >= 1440
        then (p.unitdischargeoffset::numeric / 1440.0) - 1.0
        else null::numeric
      end) as death_time_days
    from patient p
  ) d
    on d.stay_id = e.stay_id
  left join (
    select
      p.patientunitstayid as stay_id,
      (least(coalesce(p.unitdischargeoffset, (1440 + 14*1440)), (1440 + 14*1440))::numeric / 1440.0) - 1.0 as followup_time_14d,
      (least(coalesce(p.unitdischargeoffset, (1440 + 28*1440)), (1440 + 28*1440))::numeric / 1440.0) - 1.0 as followup_time_28d
    from patient p
  ) f
    on f.stay_id = e.stay_id
)
select *
from final
where
  alive_in_icu_at_landmark = 1
  eligible_sup_high_risk = 1
  and dual_ppi_h2ra_24h = 0
  and treatment in ('ppi','h2ra');

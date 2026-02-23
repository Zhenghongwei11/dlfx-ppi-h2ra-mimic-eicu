-- Build analysis-ready table for:
-- Adult ICU, SUP high-risk, PPI vs H2RA initiators (0-24h), landmark at 24h.
--
-- Output table: dlfx_mimic_sup_ppi_h2ra
--
-- Notes:
-- - This template is written for PostgreSQL.
-- - Adjust schema names if your local install differs.
-- - Outcome definitions here start conservative (ICD-based) and should be refined.

drop table if exists dlfx_mimic_sup_ppi_h2ra;

create table dlfx_mimic_sup_ppi_h2ra as
with
icu_stays as (
  select
    ie.subject_id,
    ie.hadm_id,
    ie.stay_id,
    ie.intime as icu_intime,
    ie.outtime as icu_outtime,
    row_number() over (partition by ie.subject_id order by ie.intime) as rn_icu
  from mimiciv_icu.icustays ie
),
base as (
  select
    s.subject_id as patient_id,
    s.hadm_id,
    s.stay_id,
    s.icu_intime,
    s.icu_outtime,
    (s.icu_intime + interval '24 hour') as index_time,
    (case when s.rn_icu = 1 then 1 else 0 end) as is_first_icu_stay,
    -- Still in ICU at landmark:
    (case when s.icu_outtime >= (s.icu_intime + interval '24 hour') then 1 else 0 end) as in_icu_at_24h
  from icu_stays s
  where s.rn_icu = 1
),
demo as (
  select
    b.*,
    p.gender as sex,
    a.race,
    -- Approximate age at admission using anchor variables.
    -- If you have mimiciv_derived.age, use that instead (preferred).
    (p.anchor_age + (date_part('year', a.admittime) - p.anchor_year))::numeric as age_years,
    p.dod as dod_date,
    a.dischtime as hosp_dischtime,
    a.deathtime as hosp_deathtime
  from base b
  join mimiciv_hosp.admissions a
    on a.hadm_id = b.hadm_id
  join mimiciv_hosp.patients p
    on p.subject_id = b.patient_id
),
alive_at_landmark as (
  select
    d.*,
    -- "Alive at landmark" based on available death fields.
    (case
      when d.hosp_deathtime is not null and d.hosp_deathtime < d.index_time then 0
      when d.dod_date is not null and d.dod_date < d.index_time::date then 0
      else 1
    end) as alive_at_24h
  from demo d
),
labs_24h as (
  -- Pull key labs within [icu_intime, icu_intime+24h).
  -- Use explicit itemid mapping to avoid label collisions (e.g., LDH vs lactate).
  --
  -- Itemids (MIMIC-IV):
  -- - Hemoglobin: 50811, 51222, 51640
  -- - Platelet: 51265, 53189
  -- - INR: 51237, 51675
  -- - Creatinine: 50912, 52546
  -- - Lactate: 50813, 52442, 53154
  select
    a.stay_id,

    min(case
      when le.itemid in (50811, 51222, 51640) and le.valuenum between 0.1 and 30
      then le.valuenum end) as hgb_min_24h,
    max(case
      when le.itemid in (50811, 51222, 51640) and le.valuenum between 0.1 and 30
      then le.valuenum end) as hgb_max_24h,

    min(case
      when le.itemid in (51265, 53189) and le.valuenum between 1 and 2000
      then le.valuenum end) as platelet_min_24h,
    max(case
      when le.itemid in (51265, 53189) and le.valuenum between 1 and 2000
      then le.valuenum end) as platelet_max_24h,

    max(case
      when le.itemid in (51237, 51675) and le.valuenum between 0.1 and 20
      then le.valuenum end) as inr_max_24h,
    min(case
      when le.itemid in (51237, 51675) and le.valuenum between 0.1 and 20
      then le.valuenum end) as inr_min_24h,

    min(case
      when le.itemid in (50912, 52546) and le.valuenum between 0.01 and 50
      then le.valuenum end) as creatinine_min_24h,
    max(case
      when le.itemid in (50912, 52546) and le.valuenum between 0.01 and 50
      then le.valuenum end) as creatinine_max_24h,

    min(case
      when le.itemid in (50813, 52442, 53154) and le.valuenum between 0.01 and 50
      then le.valuenum end) as lactate_min_24h,
    max(case
      when le.itemid in (50813, 52442, 53154) and le.valuenum between 0.01 and 50
      then le.valuenum end) as lactate_max_24h
  from alive_at_landmark a
  join mimiciv_hosp.labevents le
    on le.hadm_id = a.hadm_id
   and le.charttime >= a.icu_intime
   and le.charttime < (a.icu_intime + interval '24 hour')
  where le.valuenum is not null
    and le.itemid in (
      50811, 51222, 51640, -- hemoglobin
      51265, 53189,        -- platelet
      51237, 51675,        -- INR
      50912, 52546,        -- creatinine
      50813, 52442, 53154  -- lactate
    )
  group by a.stay_id
),
coagulopathy as (
  select
    a.stay_id,
    l.platelet_min_24h,
    l.inr_max_24h,
    (case
      when l.platelet_min_24h is not null and l.platelet_min_24h < 50 then 1
      when l.inr_max_24h is not null and l.inr_max_24h > 1.5 then 1
      else 0
    end) as sup_indication_coagulopathy_24h
  from alive_at_landmark a
  left join labs_24h l
    on l.stay_id = a.stay_id
),
mv_24h as (
  -- Practical MV proxy using ventilator settings documentation in chartevents.
  -- If you have a validated ventilation table (e.g., mimiciv_derived.ventilation from mimic-code),
  -- replace this CTE with that definition.
  --
  -- Heuristic: any documented ventilator mode/type within [icu_intime, icu_intime+24h).
  -- Item IDs are from `icu/d_items.csv.gz` (examples: Ventilator Type/Mode).
  select
    a.stay_id,
    (case when count(*) > 0 then 1 else 0 end) as sup_indication_mv_24h
  from alive_at_landmark a
  join mimiciv_icu.chartevents ce
    on ce.stay_id = a.stay_id
   and ce.charttime >= a.icu_intime
   and ce.charttime < (a.icu_intime + interval '24 hour')
   and ce.itemid in (223848, 223849, 229314)
  where ce.value is not null
  group by a.stay_id
),
sup_flags as (
  select
    a.*,
    c.platelet_min_24h,
    c.inr_max_24h,
    coalesce(mv.sup_indication_mv_24h, 0) as sup_indication_mv_24h,
    c.sup_indication_coagulopathy_24h,
    (case when coalesce(mv.sup_indication_mv_24h, 0) = 1 or c.sup_indication_coagulopathy_24h = 1 then 1 else 0 end) as eligible_sup_high_risk
  from alive_at_landmark a
  left join coagulopathy c
    on c.stay_id = a.stay_id
  left join mv_24h mv
    on mv.stay_id = a.stay_id
),
rx_24h as (
  -- Exposure uses prescriptions as a portable default.
  select
    s.stay_id,
    max(case
      when pr.drug ilike '%omeprazole%' or pr.drug ilike '%pantoprazole%' or pr.drug ilike '%esomeprazole%'
        or pr.drug ilike '%lansoprazole%' or pr.drug ilike '%rabeprazole%'
      then 1 else 0 end) as ppi_any_24h,
    max(case
      when pr.drug ilike '%famotidine%' or pr.drug ilike '%ranitidine%'
        or pr.drug ilike '%cimetidine%' or pr.drug ilike '%nizatidine%'
      then 1 else 0 end) as h2ra_any_24h
  from sup_flags s
  join mimiciv_hosp.prescriptions pr
    on pr.hadm_id = s.hadm_id
   and pr.starttime < (s.icu_intime + interval '24 hour')
   and (pr.stoptime is null or pr.stoptime >= s.icu_intime)
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
  left join rx_24h r
    on r.stay_id = s.stay_id
),
icd_flags as (
  -- ICD-coded outcome flags (no precise timestamps in diagnoses_icd).
  -- Use as starting point; refine strict CIGIB using time-stamped evidence when possible.
  select
    e.stay_id,
    max(case
      when (di.icd_version = 10 and (
        di.icd_code like 'K920%' or di.icd_code like 'K921%' or di.icd_code like 'K922%'
        or di.icd_code like 'K25%' or di.icd_code like 'K26%' or di.icd_code like 'K27%' or di.icd_code like 'K28%'
      )) then 1
      when (di.icd_version = 9 and (
        di.icd_code like '578%' or di.icd_code like '531%' or di.icd_code like '532%' or di.icd_code like '533%' or di.icd_code like '534%'
      )) then 1
      else 0 end) as ugib_any_hosp,
    max(case
      when (di.icd_version = 10 and di.icd_code like 'A047%') then 1
      when (di.icd_version = 9 and di.icd_code like '00845%') then 1
      else 0 end) as cdi_any_hosp
    ,
    -- Baseline comorbidity proxy: liver disease (hospital-coded).
    max(case
      when (di.icd_version = 10 and (
        di.icd_code like 'K70%' or di.icd_code like 'K71%' or di.icd_code like 'K72%'
        or di.icd_code like 'K73%' or di.icd_code like 'K74%' or di.icd_code like 'K75%' or di.icd_code like 'K76%' or di.icd_code like 'K77%'
      )) then 1
      when (di.icd_version = 9 and (
        di.icd_code like '571%' or di.icd_code like '572%'
      )) then 1
      else 0 end) as liver_disease_any_hosp
  from exposure e
  left join mimiciv_hosp.diagnoses_icd di
    on di.hadm_id = e.hadm_id
  group by e.stay_id
),
antithrombotic_24h as (
  -- Baseline-window antithrombotic exposure proxy (portable string matching).
  select
    s.stay_id,
    max(case
      when pr.drug ilike '%warfarin%' then 1
      when pr.drug ilike '%heparin%' then 1
      when pr.drug ilike '%enoxaparin%' then 1
      when pr.drug ilike '%dalteparin%' then 1
      when pr.drug ilike '%fondaparinux%' then 1
      when pr.drug ilike '%apixaban%' then 1
      when pr.drug ilike '%rivaroxaban%' then 1
      when pr.drug ilike '%dabigatran%' then 1
      when pr.drug ilike '%edoxaban%' then 1
      when pr.drug ilike '%clopidogrel%' then 1
      when pr.drug ilike '%ticagrelor%' then 1
      when pr.drug ilike '%prasugrel%' then 1
      when pr.drug ilike '%aspirin%' then 1
      else 0 end) as antithrombotic_any_24h
  from sup_flags s
  join mimiciv_hosp.prescriptions pr
    on pr.hadm_id = s.hadm_id
   and pr.starttime < (s.icu_intime + interval '24 hour')
   and (pr.stoptime is null or pr.stoptime >= s.icu_intime)
  group by s.stay_id
),
outcomes as (
  select
    e.stay_id,

    -- Broad ICD-coded outcomes (no diagnosis timestamps in diagnoses_icd).
    coalesce(f.ugib_any_hosp, 0) as ugib_broad_event,
    null::numeric as ugib_broad_time_days,

    coalesce(f.cdi_any_hosp, 0) as cdi_event,
    null::numeric as cdi_time_days,

    -- Censoring time for 14-day outcomes: earliest of ICU discharge, hospital discharge, death, or 14 days.
    (case
      when x.followup_end_14 is not null then extract(epoch from (x.followup_end_14 - e.index_time))/86400.0
      else 14.0
    end) as followup_time_14d,

    -- PRBC transfusion evidence (time-stamped) used as a strictness signal for CIGIB.
    (case
      when coalesce(f.ugib_any_hosp,0) = 1 and p.prbc_starttime is not null and p.prbc_starttime <= x.followup_end_14 then 1
      else 0
    end) as cigib_strict_event,
    (case
      when coalesce(f.ugib_any_hosp,0) = 1 and p.prbc_starttime is not null and p.prbc_starttime <= x.followup_end_14
      then extract(epoch from (p.prbc_starttime - e.index_time))/86400.0
      else (case
        when x.followup_end_14 is not null then extract(epoch from (x.followup_end_14 - e.index_time))/86400.0
        else 14.0
      end)
    end) as cigib_strict_time_days,

    -- Mortality: prefer hospital deathtime if available, else patients.dod date.
    (case
      when d.death_time is not null
       and d.death_time >= e.index_time
       and d.death_time < (e.index_time + interval '28 day')
      then 1 else 0 end) as death_event_28d,
    (case
      when d.death_time is not null
       and d.death_time >= e.index_time
       and d.death_time < (e.index_time + interval '28 day')
      then extract(epoch from (d.death_time - e.index_time))/86400.0
      else 28.0
    end) as death_time_days
  from exposure e
  left join icd_flags f
    on f.stay_id = e.stay_id
  left join (
    -- Earliest PRBC transfusion after index within 14 days.
    select
      ie.stay_id,
      min(ie.starttime) as prbc_starttime
    from exposure e2
    join mimiciv_icu.inputevents ie
      on ie.stay_id = e2.stay_id
     and ie.starttime >= e2.index_time
     and ie.starttime < (e2.index_time + interval '14 day')
     and ie.itemid in (220996, 225168, 226368, 227070)
    group by ie.stay_id
  ) p
    on p.stay_id = e.stay_id
  left join (
    select
      e3.stay_id,
      coalesce(e3.hosp_deathtime, e3.dod_date::timestamp) as death_time
    from exposure e3
  ) d
    on d.stay_id = e.stay_id
  left join (
    select
      e4.stay_id,
      least(
        e4.index_time + interval '14 day',
        coalesce((case when e4.icu_outtime >= e4.index_time then e4.icu_outtime else null end), e4.index_time + interval '14 day'),
        -- Some admissions.dischtime timestamps can be earlier than the ICU timeline (data-quality artifact).
        -- For follow-up censoring, ignore discharge times that occur before index.
        coalesce((case when e4.hosp_dischtime >= e4.index_time then e4.hosp_dischtime else null end), e4.index_time + interval '14 day'),
        coalesce((case when e4.hosp_deathtime >= e4.index_time then e4.hosp_deathtime else null end), e4.index_time + interval '14 day')
      ) as followup_end_14
    from exposure e4
  ) x
    on x.stay_id = e.stay_id
),
final as (
  select
    'mimic' as dataset,
    e.stay_id,
    e.patient_id,
    e.hadm_id,
    e.icu_intime,
    e.icu_outtime,
    e.index_time,
    e.age_years,
    e.sex,
    e.race,
    e.is_first_icu_stay,
    e.in_icu_at_24h as alive_in_icu_at_landmark,
    e.alive_at_24h as alive_at_landmark,
    e.sup_indication_mv_24h,
    e.sup_indication_coagulopathy_24h,
    e.eligible_sup_high_risk,
    coalesce(f.liver_disease_any_hosp, 0) as liver_disease,
    coalesce(a.antithrombotic_any_24h, 0) as antithrombotic_any_24h,
    e.platelet_min_24h,
    e.inr_max_24h,
    l.hgb_min_24h,
    l.hgb_max_24h,
    l.creatinine_min_24h,
    l.creatinine_max_24h,
    l.lactate_min_24h,
    l.lactate_max_24h,
    -- TODO: add severity scores (SOFA/SAPS) and comorbidities (Charlson/Elixhauser).
    null::numeric as sofa_24h,
    e.treatment,
    e.ppi_any_24h,
    e.h2ra_any_24h,
    e.dual_ppi_h2ra_24h,
    o.cigib_strict_event,
    o.cigib_strict_time_days,
    o.ugib_broad_event,
    o.ugib_broad_time_days,
    o.cdi_event,
    o.cdi_time_days,
    o.death_event_28d,
    o.death_time_days
  from exposure e
  left join labs_24h l
    on l.stay_id = e.stay_id
  left join outcomes o
    on o.stay_id = e.stay_id
  left join icd_flags f
    on f.stay_id = e.stay_id
  left join antithrombotic_24h a
    on a.stay_id = e.stay_id
)
select *
from final
where
  is_first_icu_stay = 1
  and alive_in_icu_at_landmark = 1
  and alive_at_landmark = 1
  and eligible_sup_high_risk = 1
  and dual_ppi_h2ra_24h = 0
  and treatment in ('ppi', 'h2ra');

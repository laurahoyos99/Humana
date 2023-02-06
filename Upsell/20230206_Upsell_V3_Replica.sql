--PVP_PLAN_RECO1	PVP_PLAN_ACTUAL
with
base_inicial as(
select *
,case when tipo_plan="MH 15.000" then "MH 150.000" else tipo_plan end as tipo_plan_adj
from(
SELECT * except(tipo_plan,prima_actual)
,case when tipo_plan is not null then concat(left(replace(tipo_plan,'-',' '),5),'.','000') else null end as tipo_plan 
--,prima_actual/dependientes as prima_actual
,PVP_PLAN_RECO1 as prima_estimada,	PVP_PLAN_ACTUAL as prima_actual
FROM `bustling-psyche-375313.humana_oval_2023.20230206_Base_Upsell_vf` )
)

,flag_modelo as(
select distinct *
,case when tipo_plan_adj<>recomendaci__n_1 and prima_actual>prima_estimada then '1. Prima superior'
 when tipo_plan_adj=recomendaci__n_1 then '2. Igual' 
 when tipo_plan_adj<>recomendaci__n_1 and prima_actual<prima_estimada then '3. Prima inferior'
 else null end as Comp_flag_prima
,case when tipo_plan_adj like '%PH%' then 'PH' when tipo_plan_adj like '%MH%' then 'MH' else null end as MH_PH
,case when tipo_plan_adj=recomendaci__n_1 then '2. Mismo plan'
 when (tipo_plan_adj like '%MH%' and recomendaci__n_1 like '%PH%') or (tipo_plan_adj='MH 150.000' and tipo_plan_adj<>recomendaci__n_1) then '1. Plan superior'
 when (tipo_plan_adj like '%PH%' and recomendaci__n_1 like '%MH%') or (recomendaci__n_1='MH 150.000' and tipo_plan_adj<>recomendaci__n_1) then '3. Plan inferior'
 when (tipo_plan_adj like '%MH%' and recomendaci__n_1 like '%MH%' and safe_cast(right(tipo_plan_adj,6) as float64)>safe_cast(right(recomendaci__n_1,6) as float64)) or (tipo_plan_adj like '%PH%' and recomendaci__n_1 like '%PH%' and safe_cast(right(tipo_plan_adj,6) as float64)>safe_cast(right(recomendaci__n_1,6) as float64)) then '1. Plan superior'
 when (tipo_plan_adj like '%MH%' and recomendaci__n_1 like '%MH%' and safe_cast(right(tipo_plan_adj,6) as float64)<safe_cast(right(recomendaci__n_1,6) as float64)) or (tipo_plan_adj like '%PH%' and recomendaci__n_1 like '%PH%' and safe_cast(right(tipo_plan_adj,6) as float64)<safe_cast(right(recomendaci__n_1,6) as float64)) then '3. Plan inferior'
else null end as comp_flag_plan
from base_inicial
)

,matriz as(
select distinct Tipo_plan_adj,recomendaci__n_1 as plan_rec1,recomendaci__n_2 as plan_rec2,recomendaci__n_3 as plan_rec3,count(distinct contrato) as contratos,sum(dependientes) as afiliados,sum(prima_actual) as primas_reales, sum(prima_estimada) as primas_estimadas
from flag_modelo
group by 1,2,3,4 order by 1,2,3,4
)

select distinct --*
comp_flag_plan
--,comp_flag_prima
--,MH_PH,Tipo_plan_adj,Calificaci__n_Riesgo
,count(distinct contrato) as contratos,sum(dependientes) as afiliados,sum(prima_actual) as primas_reales, sum(prima_estimada) as primas_estimadas
from flag_modelo
--where left(comp_flag_plan,1)<>left(comp_flag_prima,1)
group by 1--,2,3,4--,5 
order by 1--,2,3,4--,5

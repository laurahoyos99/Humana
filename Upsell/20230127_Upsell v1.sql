with
base_inicial as(
SELECT *
,case when tipo_plan is not null then concat(left(replace(tipo_plan,'-',' '),5),'.','000') else null end as tipo_plan_adj
FROM `bustling-psyche-375313.humana_oval_2023.20230127_Base_Upsell` 
)
,flag_modelo as(
select distinct *
,case when tipo_plan_adj<>recomendaci__n_1 and prima_actual>prima_estimada then '1. Prima superior'
 when tipo_plan_adj=recomendaci__n_1 then '2. Igual' 
 when tipo_plan_adj<>recomendaci__n_1 and prima_actual<prima_estimada then '3. Prima inferior'
 else null end as Comp_flag
from base_inicial
)
select distinct comp_flag,count(distinct contrato) as contratos,sum(dependientes) as afiliados,sum(prima_actual) as primas_reales, sum(prima_estimada) as primas_estimadas
from flag_modelo
--where comp_flag is null
group by 1 order by 1

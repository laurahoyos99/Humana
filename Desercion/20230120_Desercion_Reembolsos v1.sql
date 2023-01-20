with 

reembolsos as(
SELECT distinct date_trunc(date(est_fecha_f),month) as month_r, date(est_fecha_f) as fecha_reembolso, nom_broker,estado_reembolso, contrato as contrato_r, rango_etario, genero, descconcep, transito
--date_trunc(date(est_fecha_f),month) as month_r, nom_broker,estado_reembolso,count(distinct contrato) as contratos 
FROM `bustling-psyche-375313.humana_oval_2023.20230120_Reembolsos_2020_22`
--where nom_broker="TECNISEGUROS S.A." and estado_reembolso <> "REEMBOLSO LIQUIDADO"
--group by 1,2,3 order by 1,2,3}
)

,desafiliacion as(
SELECT distinct contrato as contrato_d, Fecha_Generaci__n_Movimiento as fecha_gen_churn, Fecha_Aplicaci__n_del_Movimiento as fecha_churn, Clasificacion_Motivo as churn_type, 	Motivo_Desafiliacion
FROM `bustling-psyche-375313.humana_oval_2023.20230120_desercion_preliminar` 
where Clasificacion_Motivo="VOLUNTARIO"
)
,cruce as(
select r.*
,case when date_diff(fecha_churn,fecha_reembolso,day)<=180 and fecha_churn> fecha_reembolso then 1 else 0 end as churn_flag
,case when date_diff(fecha_churn,fecha_reembolso,day)<=180 and fecha_churn> fecha_reembolso then r.contrato_r else null end as churner
,case when date_diff(fecha_churn,fecha_reembolso,day)<=180 and fecha_churn> fecha_reembolso then fecha_churn else null end as fecha_churn
from reembolsos r left join desafiliacion d on r.contrato_r=d.contrato_d
)
select distinct month_r,estado_reembolso,count(distinct contrato_r) as contratos,count(distinct churner) as churners
from cruce
--where fecha_churn is not null
group by 1,2 order by 1,2
--limit 100

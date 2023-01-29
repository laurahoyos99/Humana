with
base_reembolsos as(
SELECT date_trunc(date(reem_fecha_gen),month) as month_r,date_trunc(date(reem_fecha_inc),month) as month_inc, date(reem_fecha_inc) as fecha_inc,* 
FROM --`bustling-psyche-375313.humana_oval_2023.20230127_Base_Reembolsos_vfvf` 
`bustling-psyche-375313.humana_oval_2023.20230129_Base_Reembolsos_w_Fecha_Incurrencia`
)
--select distinct month_inc, count(distinct contrato)
--from base_reembolsos
--where fecha_inc<date('2020-01-01') -- order by 1
--group by 1 order by 1

,desafiliacion as(
SELECT distinct contrato as contrato_d, Fecha_Generaci__n_Movimiento as fecha_gen_churn, Fecha_Aplicaci__n_del_Movimiento as fecha_ap_churn, Clasificacion_Motivo as churn_type, 	Motivo_Desafiliacion
,case when Fecha_Generaci__n_Movimiento<=Fecha_Aplicaci__n_del_Movimiento then Fecha_Generaci__n_Movimiento when Fecha_Aplicaci__n_del_Movimiento<Fecha_Generaci__n_Movimiento then Fecha_Aplicaci__n_del_Movimiento else null end as fecha_churn
FROM `bustling-psyche-375313.humana_oval_2023.20230120_desercion_preliminar` 
where Clasificacion_Motivo="VOLUNTARIO"
)
,union_desafiliacion as(
select distinct r.*
,case when date_diff(fecha_churn,fecha_inc,day)<=180 and fecha_churn> fecha_inc then 1 else 0 end as churn_flag
,case when date_diff(fecha_churn,fecha_inc,day)<=180 and fecha_churn> fecha_inc then d.contrato_d else null end as churner
,case when date_diff(fecha_churn,fecha_inc,day)<=180 and fecha_churn> fecha_inc then fecha_churn else null end as fecha_churn
from base_reembolsos r left join desafiliacion d on r.contrato=d.contrato_d
)
select distinct month_inc,count(distinct contrato) as contratos,count(distinct churner) as churners
from union_desafiliacion
group by 1 order by 1

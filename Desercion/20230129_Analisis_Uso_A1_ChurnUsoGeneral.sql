with
base_inicial as(
select distinct Ano_Generacion,Mes_Generacion
,case mes_generacion when 'ENERO' then '01'when 'FEBRERO' then '02'when 'MARZO' then '03'when 'ABRIL' then '04' when 'MAYO' then '05' when 'JUNIO' then '06' when 'JULIO' then '07'when 'AGOSTO' then '08' when 'SEPTIEMBRE' then '09' when 'OCTUBRE' then '10' when 'NOVIEMBRE' then '11' when 'DICIEMBRE' then '12'
else null end as mes_generacion_
,clasificacion_contrato,ciudad_contrato, sucursal, provincia_contrato,producto_principal,codigo_plan,forma_pago_contrato,canal_venta,segmentacion_cartera
,safe_cast(case when edad_actual_contratante='-' then null else edad_actual_contratante end as int64) as edad_actual_contratante,contrato
,afiliado
,count(distinct Identificacion_Afiliado) as afiliados,sum(primas) as total_primas
FROM `bustling-psyche-375313.humana_oval_2023.20230124_Base_Clientes_v1` 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
)
,base_adj as(
select distinct date(concat(ano_generacion,'-',mes_generacion_,'-','01')) as base_month,* except(mes_generacion,mes_generacion_)
from base_inicial
)
,desafiliacion as(
SELECT distinct contrato as contrato_d, Fecha_Generaci__n_Movimiento as fecha_gen_churn, Fecha_Aplicaci__n_del_Movimiento as fecha_ap_churn, Clasificacion_Motivo as churn_type, 	Motivo_Desafiliacion
,case when Fecha_Generaci__n_Movimiento<=Fecha_Aplicaci__n_del_Movimiento then Fecha_Generaci__n_Movimiento when Fecha_Aplicaci__n_del_Movimiento<Fecha_Generaci__n_Movimiento then Fecha_Aplicaci__n_del_Movimiento else null end as fecha_churn
FROM `bustling-psyche-375313.humana_oval_2023.20230120_desercion_preliminar` 
where Clasificacion_Motivo="VOLUNTARIO"
)
,union_desafiliacion as(
select distinct b.*
,case when date_diff(fecha_churn,base_month,day)<=90 and fecha_churn> base_month then 1 else 0 end as churn_flag
,case when date_diff(fecha_churn,base_month,day)<=90 and fecha_churn> base_month then d.contrato_d else null end as churner
,case when date_diff(fecha_churn,base_month,day)<=90 and fecha_churn> base_month then fecha_churn else null end as fecha_churn
from base_adj b left join desafiliacion d on b.contrato=safe_cast(d.contrato_d as string)
)
select distinct base_month,count(distinct contrato) as contratos,count(distinct churner) as churners
from union_desafiliacion
group by 1 order by 1

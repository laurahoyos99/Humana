--Union ya con regla de involuntario
with
base_fact as(
SELECT distinct date(concat(ano_generacion,'-',mes_generacion,'-','01')) as base_month,* except(mes_generacion,ano_generacion)
from `bustling-psyche-375313.humana_oval_2023.20230131_Base_Individual_Completa_2020_2022` -- limit 10
)
,base_clean as(
select distinct base_month,sucursal,grupo_negocio,producto_principal,codigo_plan,contrato,numero_renovacion_contrato,Fecha_Inicio_Contrato,Fecha_Fin_Contrato,Vendedor_Contrato,Broker_Contrato,Forma_Pago_Contrato,Clasificacion_Contrato,Segmentacion_Cliente,Canal_Venta,edad_actual_contratante,Genero_Contratante,Segmentacion_Cartera,cuota,tipo_documento,Calificacion_Riesgo,Sala,Supervisor,categoria_vendedor
,count(distinct identificacion_afiliado) as afiliados, sum(primas) as primas_ag
from base_fact
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
)

,desafiliacion as(
SELECT distinct contrato as contrato_d, Fecha_Generaci__n_Movimiento as fecha_gen_churn, Fecha_Aplicaci__n_del_Movimiento as fecha_ap_churn, Clasificacion_Motivo as churn_type, 	Motivo_Desafiliacion
,case when Fecha_Generaci__n_Movimiento<=Fecha_Aplicaci__n_del_Movimiento and Clasificacion_Motivo="VOLUNTARIO" then Fecha_Generaci__n_Movimiento 
      when Fecha_Aplicaci__n_del_Movimiento<Fecha_Generaci__n_Movimiento and Clasificacion_Motivo="VOLUNTARIO" then Fecha_Aplicaci__n_del_Movimiento 
      when Clasificacion_Motivo="CARTERA" then date_sub(Fecha_Generaci__n_Movimiento, interval 105 day)
else null end as fecha_churn
FROM `bustling-psyche-375313.humana_oval_2023.20230120_desercion_preliminar` 
--where Clasificacion_Motivo="VOLUNTARIO"
)
,union_churn as(
select distinct b.*
,case when contrato_d is not null then contrato_d else null end as churner
,date_trunc(fecha_churn,month) as churn_month
,fecha_churn,contrato_d,churn_type
--,fecha_gen_churn,fecha_ap_churn
from base_clean b left join desafiliacion d on b.contrato=safe_cast(d.contrato_d as string) 
and date_diff(fecha_churn,base_month,day)<=60 and fecha_churn>=base_month
)
,churners_clean as(
select * except(base_month) from(
select distinct *
,first_value(base_month) over(partition by contrato order by base_month desc) as base_month_churn
from union_churn
where churner is not null 
) where base_month_churn=base_month
--order by contrato,base_month
)
,union_churn_clean as(
select distinct b.*
,case when contrato_d is not null then contrato_d else null end as churner
,date_trunc(fecha_churn,month) as churn_month
,fecha_churn,churn_type
--,fecha_gen_churn,fecha_ap_churn,contratante,identificacion_contratante
from base_clean b left join churners_clean c on b.contrato=safe_cast(c.contrato_d as string) and base_month=base_month_churn
)
select distinct churn_month,count(distinct churner)
from union_churn_clean
where churn_type='VOLUNTARIO'
group by 1 order by 1




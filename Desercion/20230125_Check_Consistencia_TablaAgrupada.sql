with
base_inicial as(
select distinct Ano_Generacion,Mes_Generacion
,case mes_generacion when 'ENERO' then '01'when 'FEBRERO' then '02'when 'MARZO' then '03'when 'ABRIL' then '04' when 'MAYO' then '05' when 'JUNIO' then '06' when 'JULIO' then '07'when 'AGOSTO' then '08' when 'SEPTIEMBRE' then '09' when 'OCTUBRE' then '10' when 'NOVIEMBRE' then '11' when 'DICIEMBRE' then '12'
else null end as mes_generacion_
,clasificacion_contrato,ciudad_contrato, sucursal, producto_principal,codigo_plan,forma_pago_contrato,canal_venta,segmentacion_cartera
,safe_cast(case when edad_actual_contratante='-' then null else edad_actual_contratante end as int64) as edad_actual_contratante,contrato
,count(distinct Identificacion_Afiliado) as afiliados,sum(primas) as total_primas
FROM `bustling-psyche-375313.humana_oval_2023.20230124_Base_Clientes_v1` 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
)
,base_adj as(
select distinct date(concat(ano_generacion,'-',mes_generacion_,'-','01')) as base_month,* except(mes_generacion,mes_generacion_)
from base_inicial
)
/*select distinct base_month,clasificacion_contrato,count(distinct contrato)
from base_adj
group by 1,2
order by 1,2*/
/*TEMPORAL - REEMPLAZAR*/
,desafiliacion_temp as(
SELECT distinct contrato as contrato_d, Fecha_Generaci__n_Movimiento as fecha_gen_churn, Fecha_Aplicaci__n_del_Movimiento as fecha_ap_churn, Clasificacion_Motivo as churn_type, 	Motivo_Desafiliacion
--, date_trunc(Fecha_Aplicaci__n_del_Movimiento,month) as churn_month
, contratante, Identificacion_Contratante
,case when Fecha_Generaci__n_Movimiento<=Fecha_Aplicaci__n_del_Movimiento then Fecha_Generaci__n_Movimiento when Fecha_Aplicaci__n_del_Movimiento<Fecha_Generaci__n_Movimiento then Fecha_Aplicaci__n_del_Movimiento else null end as fecha_churn
FROM `bustling-psyche-375313.humana_oval_2023.20230120_desercion_preliminar` 
where Clasificacion_Motivo="VOLUNTARIO"
)
,union_churn as(
select distinct b.*
,case when contrato_d is not null then contrato_d else null end as churner
,date_trunc(fecha_churn,month) as churn_month
,fecha_churn,contrato_d
--,fecha_gen_churn,fecha_ap_churn
,contratante,identificacion_contratante
from base_adj b left join desafiliacion_temp d on b.contrato=safe_cast(d.contrato_d as string) 
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
,fecha_churn,contrato_d
--,fecha_gen_churn,fecha_ap_churn,contratante,identificacion_contratante
from base_adj b left join churners_clean c on b.contrato=safe_cast(c.contrato_d as string) and base_month=base_month_churn
)
,clasificacion_etario as(
select distinct *
,case when edad_actual_contratante < 2 then '0 a 23 meses'
when edad_actual_contratante between 2 and 17 then '2 a 17 años'
when edad_actual_contratante between 18 and 23 then '18 a 23 años'
when edad_actual_contratante between 24 and 35 then '24 a 35 años'
when edad_actual_contratante between 36 and 44 then '36 a 44 años'
when edad_actual_contratante between 45 and 55 then '45 a 55 años'
when edad_actual_contratante between 56 and 59 then '56 a 59 años'
when edad_actual_contratante between 56 and 23 then '60 a 65 años'
when edad_actual_contratante >65 then '65+ años' 
else null end  as rango_etario
from union_churn_clean
--limit 20
)
,prueba as(
select distinct base_month,clasificacion_contrato,ciudad_contrato, sucursal, producto_principal,codigo_plan,forma_pago_contrato,canal_venta,segmentacion_cartera--,
,contrato
--cast(clasificacion_etario as string)
--,count(distinct contrato) as contratos, count(distinct churner) as churners
from clasificacion_etario
--group by 1,2,3,4,5,6,7,8,9--,10
--order by 1,2,3,4,5,6,7,8,9
--order by contrato,1
--limit 10
)
select * --distinct base_month,contrato,count(*)
from prueba 
where contrato in('45688','275751','266989','205862','83710')
--group by 1,2 order by 3 desc
order by contrato, base_month

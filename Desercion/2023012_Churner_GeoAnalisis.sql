with
base_inicial as(
select distinct Ano_Generacion,Mes_Generacion
,case mes_generacion when 'ENERO' then '01'when 'FEBRERO' then '02'when 'MARZO' then '03'when 'ABRIL' then '04' when 'MAYO' then '05' when 'JUNIO' then '06' when 'JULIO' then '07'when 'AGOSTO' then '08' when 'SEPTIEMBRE' then '09' when 'OCTUBRE' then '10' when 'NOVIEMBRE' then '11' when 'DICIEMBRE' then '12'
else null end as mes_generacion_
,clasificacion_contrato,ciudad_contrato, sucursal, provincia_contrato,producto_principal,codigo_plan,forma_pago_contrato,canal_venta,segmentacion_cartera
,safe_cast(case when edad_actual_contratante='-' then null else edad_actual_contratante end as int64) as edad_actual_contratante,contrato
,count(distinct Identificacion_Afiliado) as afiliados,sum(primas) as total_primas
FROM `bustling-psyche-375313.humana_oval_2023.20230124_Base_Clientes_v1` 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
)
,base_geo as(
SELECT string_field_0 as provincia,case when string_field_1 like '%AGUA SANTA%' then 'BAÑOS DE AGUA SANTA' when string_field_1='PIÃ‘AS' then 'PIÑAS' when string_field_1='RUMIÃ‘AHUI' then 'RUMIÑAHUI' when string_field_1='CORONEL MARCELINO MARIDUEÃ‘A' then 'CORONEL MARCELINO MARIDUEÑA' else string_field_1 end as ciudad
,string_field_2 as Region, string_field_3 as Agrup_de_ciudades
FROM `bustling-psyche-375313.humana_oval_2023.20230127_Base_Geografica` 
where string_field_0 <> 'provincia_contrato'
)
,base_adj as(
select distinct date(concat(ano_generacion,'-',mes_generacion_,'-','01')) as base_month, b.* except(mes_generacion,mes_generacion_), g.region,g.Agrup_de_ciudades
from base_inicial b left join base_geo g on provincia_contrato=provincia and ciudad_contrato=ciudad
)

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

select distinct base_month,provincia_contrato,ciudad_contrato,region,agrup_de_ciudades
,count(distinct contrato) as contratos, count(distinct churner) as churners
from union_churn_clean 
group by 1,2,3,4,5 order by 1,2,3,4,5


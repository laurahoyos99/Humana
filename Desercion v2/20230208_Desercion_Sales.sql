with
base_fact as(
SELECT distinct date(concat(ano_generacion,'-',mes_generacion,'-','01')) as base_month,* except(mes_generacion,ano_generacion)
from `bustling-psyche-375313.humana_oval_2023.20230131_Base_Individual_Completa_2020_2022` -- limit 10
)
,base_f_adj as(
select distinct base_month,sucursal,grupo_negocio,producto_principal,codigo_plan,contrato,numero_renovacion_contrato,Fecha_Inicio_Contrato,Fecha_Fin_Contrato,Vendedor_Contrato,Broker_Contrato,Forma_Pago_Contrato,Clasificacion_Contrato,Segmentacion_Cliente,Canal_Venta,safe_cast(case when edad_actual_contratante='-' then null else edad_actual_contratante end as int64) as edad_actual_contratante,Genero_Contratante,Segmentacion_Cartera,cuota,tipo_documento,Calificacion_Riesgo,Sala,Supervisor,categoria_vendedor
,count(distinct identificacion_afiliado) as afiliados, sum(primas) as primas_ag
from base_fact
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
)
,base_contratos as(
SELECT distinct contrato,numrenov,tipo_facturacion
,date_trunc(date(vigdesde_contrato),month) as inicio_contrato_mes
,date(vigdesde_contrato) as inicio_vig_contrato
,date(vighasta_contrato) as fin_vig_contrato
,estado_contrato
,date(fecha_cancela_contrato) as cancela_contrato
,provincia,region,ciudad,banco_asociado,tarjeta_asociada
,case when fecha_cancela_contrato is not null then date(fecha_cancela_contrato) else date(vighasta_contrato) end as Fin_Contrato
--select *
FROM `bustling-psyche-375313.humana_oval_2023.20230202_Dimension_Contratos_2019_2022` --limit 10
--como lo extraje:
/*where date_trunc('month',date(vigdesde_contrato))>=date('2019-01-01') and tipo_contrato='INDIVIDUAL' 
and estado_contrato IN('CANCELADO','RENOVADO','FACTURADO')*/
)
,dias_mes as(
SELECT distinct dias_2019_2022 as meses_totales
FROM `bustling-psyche-375313.humana_oval_2023.20230202_Dias_anos_2019_2023` 
where date_trunc(dias_2019_2022,month) =dias_2019_2022
)
,meses_vigencia as(
select distinct b.* ,m.meses_totales as mes_vigente
from base_contratos b cross join dias_mes m 
where meses_totales between b.inicio_contrato_mes and b.fin_contrato
)
,cartera_limpia_total as(
select *
,case when numrenov=1 and cuota=1 then 'VENTAS NUEVAS'
      when numrenov=1 and cuota between 2 and 11 then 'V1'
      when cuota=12 then 'CARTERA EN RENOVACION'
      when numrenov>1 and cuota<12 then 'CARTERA HISTORICA'
      when numrenov=1 and cuota>12 then 'V1'
      when numrenov>1 and cuota>12 then 'CARTERA HISTORICA'
else 'SIN DEFINIR' end as segmentacion_cartera
,case when numrenov=1 and cuota=1 then '1. Venta Nueva'
      when numrenov=1 and cuota between 2 and 6 then '2. Entre 2 y 6 meses'
      when numrenov=1 and cuota between 7 and 12 then '3. Entre 7 y 12 meses'
      when numrenov>1 then '4. Mas de 1 año'
else '5. Sin definir' end as tenure
from(
select *,date_diff(mes_vigente,inicio_contrato_mes,month)+1 as cuota
from meses_vigencia
)
where cuota<=12
)
,cartera_acotada as(
select distinct *
from cartera_limpia_total
where mes_vigente>='2020-01-01'
)
,base_super_clean as(
select distinct 
mes_vigente,b.* ,c.tenure,banco_asociado,tarjeta_asociada
,case when edad_actual_contratante < 2 then 'R1. 0 a 23 meses'
when edad_actual_contratante between 2 and 17 then 'R.2 2 a 17 años'
when edad_actual_contratante between 18 and 23 then 'R.3 18 a 23 años'
when edad_actual_contratante between 24 and 35 then 'R.4 24 a 35 años'
when edad_actual_contratante between 36 and 44 then 'R.5 36 a 44 años'
when edad_actual_contratante between 45 and 55 then 'R.6 45 a 55 años'
when edad_actual_contratante between 56 and 59 then 'R.7 56 a 59 años'
when edad_actual_contratante between 60 and 65 then 'R.8 60 a 65 años'
when edad_actual_contratante >65 then 'R.9 65+ años' 
else null end  as rango_etario
from cartera_acotada c inner join base_f_adj b on safe_cast(c.contrato as string)=b.contrato and safe_cast(numrenov as string)=Numero_Renovacion_Contrato and safe_cast(c.cuota as string)=b.cuota
where tipo_documento='FACTURA'
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
from base_super_clean b left join desafiliacion d on b.contrato=safe_cast(d.contrato_d as string) 
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
from base_super_clean b left join churners_clean c on b.contrato=safe_cast(c.contrato_d as string) and base_month=base_month_churn
)
select distinct mes_vigente
--,producto_principal,tenure
,canal_venta
,sala
--,Supervisor,vendedor_contrato,categoria_vendedor,broker_contrato,forma_pago_contrato,banco_asociado,tarjeta_asociada
,count(distinct contrato) as contratos,count(distinct churner) as churners,count(distinct case when churn_type='VOLUNTARIO' then churner else null end) as vol,count(distinct case when churn_type='CARTERA' then churner else null end) as invol
,sum(afiliados) as total_afiliados,sum(primas_ag) as total_primas
from union_churn_clean
--where churn_type='VOLUNTARIO'
group by 1,2,3--,4--,5,6,7,8,9,10,11
order by 1,2,3--,4--,5,6,7,8,9,10,11

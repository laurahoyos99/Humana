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
SELECT distinct contrato,numrenov
,tipo_facturacion
,date_trunc(date(vigdesde_contrato),month) as inicio_contrato_mes
,date(vigdesde_contrato) as inicio_vig_contrato
,date(vighasta_contrato) as fin_vig_contrato
,estado_contrato
,date(fecha_cancela_contrato) as cancela_contrato
,provincia,region,ciudad
,case when fecha_cancela_contrato is not null then date(fecha_cancela_contrato) else date(vighasta_contrato) end as Fin_Contrato
FROM `bustling-psyche-375313.humana_oval_2023.20230202_Dimension_Contratos_2019_2022` 
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
mes_vigente,b.* ,c.tenure
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
where tipo_documento='FACTURA' and tenure='1. Venta Nueva' and extract(year from mes_vigente)=2022
)
######################################################
,base_upsell as(
select *
,case when tipo_plan="MH 15.000" then "MH 150.000" else tipo_plan end as tipo_plan_adj
from(
SELECT * except(tipo_plan,prima_estimada)
,case when tipo_plan is not null then concat(left(replace(tipo_plan,'-',' '),5),'.','000') else null end as tipo_plan 
--,prima_actual/dependientes as prima_actual
,PVP_PLAN_RECO_1 as prima_estimada,	PVP_PLAN_ACTUAL as prima_actual
FROM --`bustling-psyche-375313.humana_oval_2023.20230206_Base_Upsell_vf` )
`bustling-psyche-375313.humana_oval_2023.20230208_Base_Upsell_vfvf` )
)
,union_base_upsell as(
select distinct f.*,afiliados
--,b.sala
from base_upsell f inner join base_super_clean b on safe_cast(f.contrato as string)=b.contrato 
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
from union_base_upsell
)

,matriz as(
select distinct Tipo_plan_adj,recomendaci__n_1 as plan_rec1,recomendaci__n_2 as plan_rec2,recomendaci__n_3 as plan_rec3,count(distinct contrato) as contratos,sum(afiliados) as afiliados
--,null as afiliados
,sum(prima_actual) as primas_reales, sum(prima_estimada) as primas_estimadas
from flag_modelo
group by 1,2,3,4 order by 1,2,3,4
)

select distinct --*
comp_flag_plan
--,comp_flag_prima
,MH_PH,Tipo_plan_adj,Calificaci__n_Riesgo
,count(distinct contrato) as contratos,sum(afiliados) as afiliados
--,null as afiliados
,sum(prima_actual) as primas_reales, sum(prima_estimada) as primas_estimadas
from flag_modelo
--where left(comp_flag_plan,1)<>left(comp_flag_prima,1)
group by 1,2,3,4--,5 
order by 1,2,3,4--,5


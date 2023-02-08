with
base_fact as(
SELECT distinct date(concat(ano_generacion,'-',mes_generacion,'-','01')) as base_month,* except(mes_generacion,ano_generacion)
from `bustling-psyche-375313.humana_oval_2023.20230131_Base_Individual_Completa_2020_2022` -- limit 10
)
,base_geo as(
SELECT string_field_0 as provincia,case when string_field_1 like '%AGUA SANTA%' then 'BAÑOS DE AGUA SANTA' when string_field_1='PIÃ‘AS' then 'PIÑAS' when string_field_1='RUMIÃ‘AHUI' then 'RUMIÑAHUI' when string_field_1='CORONEL MARCELINO MARIDUEÃ‘A' then 'CORONEL MARCELINO MARIDUEÑA' else string_field_1 end as ciudad
,string_field_2 as Region, string_field_3 as Agrup_de_ciudades
FROM `bustling-psyche-375313.humana_oval_2023.20230127_Base_Geografica` 
where string_field_0 <> 'provincia_contrato'
)
,base_f_adj as(
select distinct base_month,sucursal,grupo_negocio,producto_principal,codigo_plan,contrato,numero_renovacion_contrato,Fecha_Inicio_Contrato,Fecha_Fin_Contrato,Vendedor_Contrato,Broker_Contrato,Forma_Pago_Contrato,Clasificacion_Contrato,Segmentacion_Cliente,Canal_Venta,safe_cast(case when edad_actual_contratante='-' then null else edad_actual_contratante end as int64) as edad_actual_contratante,Genero_Contratante,Segmentacion_Cartera,cuota,tipo_documento,Calificacion_Riesgo,Sala,Supervisor,categoria_vendedor
,identificacion_afiliado,primas
--,count(distinct identificacion_afiliado) as afiliados, sum(primas) as primas_ag
from base_fact 
--group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
)
,reembolsos as(
SELECT distinct date_trunc(date(reem_fecha_gen),month) as month_r,date_trunc(date(reem_fecha_inc),month) as month_inc,contrato,transito,tipo_reembolso,estado_reembolso,valor_pagado_neto,tiempo_reembolso,valor_presentado,valor_recorte,valor_copago,valor_deducible
FROM  `bustling-psyche-375313.humana_oval_2023.20230207_Base_Reembolsos_Con_Pagos_vf_inc` 
)
,tabla_adj_reembolsos as(
select distinct  month_inc,contrato,tipo_reembolso,estado_reembolso,count(distinct transito) as transitos,sum(valor_presentado) as total_presentado, sum(pagado_neto) as total_pagado,avg(pagado_neto) as pagado_por_transito,avg(avg_tiempo_servicio) as avg_tiempo_transito,sum(valor_recorte) as total_recorte,sum(valor_copago) as total_copago,sum(valor_deducible) as total_deducible
from(
select distinct month_inc,contrato,transito,tipo_reembolso,estado_reembolso,sum(valor_pagado_neto) as pagado_neto,avg(tiempo_reembolso) as avg_tiempo_servicio,sum(valor_presentado) as valor_presentado,sum(valor_recorte) as valor_recorte,sum(valor_copago) as valor_copago,sum(valor_deducible)as valor_deducible
from reembolsos
group by 1,2,3,4,5 --order by 1,2,3
)
group by 1,2,3,4 --order by 1,2
)
,categorizacion_reembolsos as(
select distinct month_inc,contrato,tipo_reembolso,estado_reembolso,transitos,total_presentado,total_pagado,total_copago,total_deducible,total_recorte,pagado_por_transito,avg_tiempo_transito
,case when avg_tiempo_transito=0 then '1. 0 dias'
      when avg_tiempo_transito between 1 and 3 then '2. 1-3 dias'
      when avg_tiempo_transito between 4 and 7 then '3. 4-7 dias'
      when avg_tiempo_transito between 8 and 15 then '4. 8-15 dias'
      when avg_tiempo_transito >15 then '5. >15 dias'
else null end as Tier_Tiempos
from tabla_adj_reembolsos
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
select distinct c.*,g.Agrup_de_ciudades,g.region as region_manual
from cartera_limpia_total c left join base_geo g on c.provincia=g.provincia and c.ciudad=g.ciudad
where mes_vigente>='2020-01-01'
)
,base_super_clean as(
select distinct 
mes_vigente,b.* ,c.tenure,c.provincia,c.region,c.region_manual,c.Agrup_de_ciudades,c.ciudad
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
,cruce_reembolsos as(
select distinct b.*
,r.contrato as contratos_uso, month_inc
,case when r.contrato is null then b.contrato else null end as contrato_s
,tipo_reembolso,estado_reembolso,transitos, total_presentado,total_pagado,total_copago,total_deducible,total_recorte
,pagado_por_transito,avg_tiempo_transito,tier_tiempos
from base_super_clean b left join categorizacion_reembolsos r on b.contrato=cast(r.contrato as string) and mes_vigente=month_inc
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
,union_desafiliacion as(
select distinct r.*
-- general
,case when date_diff(fecha_churn,mes_vigente,day)<=180 and fecha_churn> mes_vigente then 1 else 0 end as churn_flag
,case when date_diff(fecha_churn,mes_vigente,day)<=180 and fecha_churn> mes_vigente then d.contrato_d else null end as churner
,case when date_diff(fecha_churn,mes_vigente,day)<=180 and fecha_churn> mes_vigente then fecha_churn else null end as fecha_churn
,churn_type
-- con reembolsos
,case when date_diff(fecha_churn,mes_vigente,day)<=180 and fecha_churn> mes_vigente and contratos_uso is not null then 1 else 0 end as churn_flag_r
,case when date_diff(fecha_churn,mes_vigente,day)<=180 and fecha_churn> mes_vigente and contratos_uso is not null then  d.contrato_d else null end as churner_R
,case when date_diff(fecha_churn,mes_vigente,day)<=180 and fecha_churn> mes_vigente and contratos_uso is not null then fecha_churn else null end as fecha_churn_r
--
,case when date_diff(fecha_churn,mes_vigente,day)<=180 and fecha_churn> mes_vigente and contratos_uso is null then 1 else 0 end as churn_flag_s
,case when date_diff(fecha_churn,mes_vigente,day)<=180 and fecha_churn> mes_vigente and contratos_uso is null then d.contrato_d else null end as churner_s
,case when date_diff(fecha_churn,mes_vigente,day)<=180 and fecha_churn> mes_vigente and contratos_uso is null then fecha_churn else null end as fecha_churn_S
from cruce_reembolsos r left join desafiliacion d on r.contrato=cast(d.contrato_d as string)
)
select distinct mes_vigente,producto_principal,tenure,churn_type
,segmentacion_cartera
--,tipo_reembolso,estado_reembolso,transitos
--,tier_tiempos
,count(distinct contrato) as contratos
,count(distinct churner) as churners,count(distinct contratos_uso) as contratos_uso,count(distinct churner_r) as churners_uso,count(distinct contrato_s) as contratos_sin_uso,count(distinct churner_s) as churners_sin_uso
--month_r,count(distinct contratos_uso)
from union_desafiliacion
--cruce_reembolsos
group by 1,2,3,4,5--,6,7,8,9
order by 1,2,3,4,5--,6,7,8,9


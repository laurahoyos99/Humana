with
base_fact as(
SELECT distinct date(concat(ano_generacion,'-',mes_generacion,'-','01')) as base_month,* except(mes_generacion,ano_generacion)
from `bustling-psyche-375313.humana_oval_2023.20230131_Base_Individual_Completa_2020_2022` -- limit 10
)
,base_f_adj as(
select distinct base_month,sucursal,grupo_negocio,producto_principal,codigo_plan,contrato,numero_renovacion_contrato,Fecha_Inicio_Contrato,Fecha_Fin_Contrato,Vendedor_Contrato,Broker_Contrato,Forma_Pago_Contrato,Clasificacion_Contrato,Segmentacion_Cliente,Canal_Venta,edad_actual_contratante,Genero_Contratante,Segmentacion_Cartera,cuota,tipo_documento,Calificacion_Riesgo,Sala,Supervisor,categoria_vendedor
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
,case when fecha_cancela_contrato is not null then date(fecha_cancela_contrato) else date(vighasta_contrato) end as Fin_Contrato
FROM `bustling-psyche-375313.humana_oval_2023.20230202_Dimension_Contratos_2019_2022` 
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
where mes_vigente>='2020-01-01' --and safe_cast(contrato as string) in(select contrato from `bustling-psyche-375313.humana_oval_2023.20230131_Base_Individual_Completa_2020_2022`)
)
select distinct --mes_vigente,b.* --
c.mes_vigente--,producto_principal
,count(distinct c.contrato),sum(afiliados),sum(primas_ag)
from cartera_acotada c inner join base_f_adj b on safe_cast(c.contrato as string)=b.contrato and safe_cast(numrenov as string)=Numero_Renovacion_Contrato and safe_cast(c.cuota as string)=b.cuota
where tipo_documento='FACTURA'
--AND b.contrato='266081'
--order by contrato,c.mes_vigente
group by 1--,2 
order by 1--,2

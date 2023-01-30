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
,reembolsos as(
SELECT distinct date_trunc(date(est_fecha_f),month) as month_r, date(est_fecha_f) as fecha_reembolso, nom_broker,estado_reembolso, contrato as contrato_r, rango_etario, genero, descconcep, transito
FROM `bustling-psyche-375313.humana_oval_2023.20230126_Base_Reembolsos_vf` 
)
,cruce_reembolsos as(
select distinct b.*
,contrato_r as contratos_uso
,r.fecha_reembolso, estado_reembolso,month_r
from base_adj b right join reembolsos r on b.contrato=cast(r.contrato_r as string) and base_month=month_r
)
,gap as(
select distinct  contratos_uso
,fecha_reembolso, estado_reembolso,month_r
--month_r--,base_month
--,count(distinct contrato) as contratos, count(distinct contratos_uso) as contratos_uso
from cruce_reembolsos
where base_month is null
--group by 1--,2 
--order by 1--,2
)
select distinct contratos_uso,month_r,b.*
--month_r,count(distinct contratos_uso)
from base_adj b right join gap g on b.contrato=cast(g.contratos_uso as string)
--where contrato is null
--group by 1 order by 1
order by contratos_uso,month_r,base_month

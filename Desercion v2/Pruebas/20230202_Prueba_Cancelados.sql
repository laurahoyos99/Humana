with
base_contratos as(
SELECT distinct contrato,numrenov
,date_trunc(date(vigdesde_contrato),month) as start_month
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
select distinct b.* --,m.meses_totales as mes_vigente
from base_contratos b cross join dias_mes m 
where meses_totales between b.start_month and b.fin_contrato
)
,prueba as(
select distinct * --except(mes_vigente)
,lag(estado_contrato) over(partition by contrato order by numrenov desc) as sgte_estado
from meses_vigencia
--order by contrato,inicio_vig_contrato
--limit 1000
)
select *
from prueba
--where estado_contrato='CANCELADO' and sgte_estado is not null and sgte_estado<>'CANCELADO'
where contrato in(240379,268616,257402)
order by contrato,numrenov
/*select distinct *
,case when numrenov=1 and 
from meses_vigencia
--where contrato=150001
--group by 1,2 order by 3 desc
order by contrato,mes_vigente
limit 100
--solo se repiten 2 veces lo cual esta ok
*/

with base as(
SELECT distinct --date_trunc(date(reem_fecha_gen),month) as month_r, date(reem_fecha_gen) as fecha_reembolso--, nom_broker,estado_reembolso, contrato as contrato_r, rango_etario, genero, descconcep, transito
*
--from `bustling-psyche-375313.humana_oval_2023.20230210_Fresa_Reembolsos_2019_2`
--FROM `bustling-psyche-375313.humana_oval_2023.20230127_Base_Reembolsos_vfvf` 
--select *
from `bustling-psyche-375313.humana_oval_2023.20230210_Fresa_Reembolsos_2022_2` --limit 10
)
,segmentacion as(
select distinct *
from(
select distinct transito,prestador as prestador_real
,case when lower(prestador) like '%hospi%' or lower(prestador) like '%clínica%' or lower(prestador) like '%clinica%' or lower(prestador) like '%ClÃ­nica%' or lower(prestador) like '%centro medico%' or lower(prestador) like '%metrored%' or lower(prestador) like '%medilink%' or lower(prestador) like '%conclina%' or lower(prestador) like '%veris%' or lower(prestador) like '%red hosp%' or prestador ='Sistemas Medicos De La USFQ' or lower(prestador) like '%axxiscan%' or prestador = 'JUNTA DE BENEFICENCIA DE GUAYAQUIL' or lower(prestador) like '%mediglobal%'
then 1 else 0 end as prestador_flag
from base
)
where prestador_flag=1
)
--select * from segmentacion
--where transito=9125274
/*
select distinct --nomconserv --
prestador,prestador_real,count(distinct contrato),sum(valor_pagado_neto)
from segmentacion
where month_r>='2022-01-01' --and prestador='FYBECA (ABF)'
group by 1,2 order by 4 desc
--order by transito
*/
,base_adj as(
select 
case when prestador_real is null then prestador else prestador_real end as prestador_real
,b.*
,case when prestador_real is not null and prestador_real<>prestador then 1 else 0 end as No_Institucional
from base b left join segmentacion s on b.transito=s.transito
--where month_r in('2022-12-01','2022-11-01','2022-10-01')
)
select distinct * --except(prestador) --transito,month_r,prestador_real,sum(valor_pagado_neto) as pagado,count(distinct contrato) as contratos
from base_adj
--where transito=9229531
--limit 100

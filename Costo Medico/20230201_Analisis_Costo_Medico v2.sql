with base as(
SELECT distinct date_trunc(date(reem_fecha_gen),month) as month_r, date(reem_fecha_gen) as fecha_reembolso--, nom_broker,estado_reembolso, contrato as contrato_r, rango_etario, genero, descconcep, transito
,*
FROM `bustling-psyche-375313.humana_oval_2023.20230127_Base_Reembolsos_vfvf` 
)
,segmentacion as(
select distinct *
from(
select distinct transito,nom_prestador as prestador_real
,case when lower(nom_prestador) like '%hospi%' or lower(nom_prestador) like '%clínica%' or lower(nom_prestador) like '%clinica%' or lower(nom_prestador) like '%ClÃ­nica%' or lower(nom_prestador) like '%centro medico%' or lower(nom_prestador) like '%metrored%' or lower(nom_prestador) like '%medilink%' or lower(nom_prestador) like '%conclina%' or lower(nom_prestador) like '%veris%' or lower(nom_prestador) like '%red hosp%' or nom_prestador ='Sistemas Medicos De La USFQ' or lower(nom_prestador) like '%axxiscan%' or nom_prestador = 'JUNTA DE BENEFICENCIA DE GUAYAQUIL' or lower(nom_prestador) like '%mediglobal%'
then 1 else 0 end as prestador_flag
from base
)
where prestador_flag=1
)
--select * from segmentacion
--where transito=9125274
/*
select distinct --nomconserv --
nom_prestador,prestador_real,count(distinct contrato),sum(valor_pagado_neto)
from segmentacion
where month_r>='2022-01-01' --and nom_prestador='FYBECA (ABF)'
group by 1,2 order by 4 desc
--order by transito
*/
,base_adj as(
select 
case when prestador_real is null then nom_prestador else prestador_real end as prestador_real
,b.*
,case when prestador_real is not null and prestador_real<>nom_prestador then 1 else 0 end as No_Institucional
from base b left join segmentacion s on b.transito=s.transito
where month_r in('2022-12-01','2022-11-01','2022-10-01')
)
select distinct --transito,
month_r,prestador_real,sum(valor_pagado_neto) as pagado,count(distinct contrato) as contratos
from base_adj
--where --prestador_real is not null
--transito=9125274
group by 1,2 order by 1,2
--where No_Institucional=1
--order by transito

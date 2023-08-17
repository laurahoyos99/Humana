with 
dim_contrato as(
SELECT distinct contrato,numrenov,tipo_facturacion
,date_trunc('month',date(vigdesde_contrato)) as inicio_contrato_mes
,date(vigdesde_contrato) as inicio_vig_contrato
,date(vighasta_contrato) as fin_vig_contrato
,estado_contrato,segmentacion_contrato
,date(fecha_cancela_contrato) as cancela_contrato
,provincia,region,ciudad
,case when fecha_cancela_contrato is not null then date(fecha_cancela_contrato) else date(vighasta_contrato) end as Fin_Contrato
FROM "humana_prodl_raw"."prodl_raw_dwh_dim_contrato"
where date_trunc('month',date(vigdesde_contrato))>=date('2021-01-01')  and estado_contrato IN('CANCELADO','RENOVADO','FACTURADO') 
)
,months_years as (
select date(concat(year_number,'-',month_number,'-01')) as meses_totales from(
  select '2023' as year_number, '01' as month_number union all select '2023','02' union all select '2023','03' union all select '2023','04' union all select '2023','05' union all select '2023','06' union all   select '2023','07' union all select '2023','08' union all select '2023','09' union all select '2023','10' union all select '2023','11' union all select '2023','12'
 union all select '2022','01' union all select '2022','02' union all select '2022','03' union all select '2022','04' union all select '2022','05' union all select '2022','06' union all select '2022','07' union all select '2022','08' union all select '2022','09' union all select '2022','10' union all select '2022','11' union all select '2022','12'
  union all select '2021','01' union all select '2021','02' union all select '2021','03' union all select '2021','04' union all select '2021','05' union all select '2021','06' union all select '2021','07' union all select '2021','08' union all select '2021','09' union all select '2021','10' union all select '2021','11' union all select '2021','12'
))
,meses_vigencia as(
select distinct b.* ,m.meses_totales as mes_vigente
from dim_contrato b cross join months_years m 
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
select *,date_diff('month',inicio_contrato_mes,mes_vigente)+1 as cuota
from meses_vigencia
)
where cuota<=12
)
----------
,base_facturacion as(
SELECT s.sucursal
    , i.linea_negocio 
    , i.negocio
    , p.producto_principal
    , h.plan_corto as Codigo_Plan
    , a.contrato
    , a.numrenov 
    , a.vigdesde_contrato as Fecha_Inicio_Contrato
    , a.vighasta_contrato as Fecha_Fin_Contrato
    , a.vendedor_contrato  
    , a.broker_contrato
    , a.forma_pago as Forma_Pago_Contrato
    , a.clasificacion as Clasificacion_Contrato
    , a.segmentacion_contrato as Segmentacion_Cliente
    , a.canal_venta
    ,cc.identificacion_contratante as Identificacion_Contratante
    ,cc.nombre_contratante as Contratante
    ,EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM cc.fecha_nacimiento) AS Edad_Actual_Contratante
    ,cc.genero as Genero_Contratante
    ------,Calificacion_Riesgo no carga a AWS pendiente a cargar
---, Email_Contratante no sube a AWS
---,Celular_Contratante no sube a AWS
---,Segmentacion_Cartera calculado en Qlik

,CASE
           WHEN a.numrenov = 1 AND b.cuota = 1 THEN 'VENTAS NUEVAS'
           WHEN a.numrenov = 1 AND b.cuota > 1 AND b.cuota < 12 THEN 'V1'
           WHEN b.cuota = 12 THEN 'CARTERA EN RENOVACION'
           WHEN a.numrenov > 1 AND b.cuota < 12 THEN 'CARTERA HISTORICA'
           WHEN a.numrenov = 1 AND b.cuota > 12 THEN 'V1'
           WHEN a.numrenov > 1 AND b.cuota > 12 THEN 'CARTERA HISTORICA'
           ELSE 'SIN DEFINIR'
       END AS Segmentacion_Cartera
,da.identificacion_afiliado as Identificacion_Afiliado
,da.nombres_afiliado as Afiliado
,da.parentesco as Parentesco_Afiliado
----,Rango_Edad_Afiliado campo calculado
,CASE
           WHEN DATE_DIFF('year', da.fecha_nacimiento, a.vigdesde_contrato) < 2 THEN 'R1. 0 meses - 23 Meses'
           WHEN DATE_DIFF('year', da.fecha_nacimiento, a.vigdesde_contrato) >= 2 AND DATE_DIFF('year', da.fecha_nacimiento, a.vigdesde_contrato) < 18 THEN 'R2. 24 meses - 17 Años'
           WHEN DATE_DIFF('year', da.fecha_nacimiento, a.vigdesde_contrato) >= 18 AND DATE_DIFF('year', da.fecha_nacimiento, a.vigdesde_contrato) < 24 THEN 'R3. 18 Años - 23 Años'
           WHEN DATE_DIFF('year', da.fecha_nacimiento, a.vigdesde_contrato) >= 24 AND DATE_DIFF('year', da.fecha_nacimiento, a.vigdesde_contrato) < 36 THEN 'R4. 24 Años - 35 Años'
           WHEN DATE_DIFF('year', da.fecha_nacimiento, a.vigdesde_contrato) >= 36 AND DATE_DIFF('year', da.fecha_nacimiento, a.vigdesde_contrato) < 45 THEN 'R5. 36 Años - 44 Años'
           WHEN DATE_DIFF('year', da.fecha_nacimiento, a.vigdesde_contrato) >= 45 AND DATE_DIFF('year', da.fecha_nacimiento, a.vigdesde_contrato) < 56 THEN 'R6. 45 Años - 55 Años'
           WHEN DATE_DIFF('year', da.fecha_nacimiento, a.vigdesde_contrato) >= 56 AND DATE_DIFF('year', da.fecha_nacimiento, a.vigdesde_contrato) < 60 THEN 'R7. 56 Años - 59 Años'
           WHEN DATE_DIFF('year', da.fecha_nacimiento, a.vigdesde_contrato) >= 60 AND DATE_DIFF('year', da.fecha_nacimiento, a.vigdesde_contrato) < 66 THEN 'R8. 60 Años - 65 Años'
           WHEN DATE_DIFF('year', da.fecha_nacimiento, a.vigdesde_contrato) >= 66 THEN 'R9. 66 Años o más'
           ELSE 'Error Rangos de Edad'
       END AS Rango_Edad_Afiliado
---,Arquetipo_Afiliado campo calculado
,b.cuota as Cuota
,CASE
           WHEN b.id_tipo_documento = 1 THEN 'FACTURA'
           WHEN b.id_tipo_documento = 4 THEN 'NOTA DE CREDITO'
           WHEN b.id_tipo_documento = 5 THEN 'NOTA DE DEBITO'
           WHEN b.id_tipo_documento = 2 THEN 'NOTA VENTA'
           WHEN b.id_tipo_documento = 3 THEN 'LIQUIDACION DE COMPRA'
           WHEN b.id_tipo_documento = 6 THEN 'RECIBOS'
           WHEN b.id_tipo_documento = 7 THEN 'COMPROBANTES DE RETENCION'
           ELSE 'Otro Tipo de Documento'
       END AS Tipo_Documento
    ,a.sala as Sala
    ,a.supervisor_sala as Supervisor
    , b.valor_prima
    , a.provincia
    , a.ciudad
    ,date_trunc('month',date(fecha_generacion_f)) as base_month
   -- ,EXTRACT(YEAR FROM b.fecha_generacion_f) AS anio_generacion
  --  , EXTRACT(MONTH FROM b.fecha_generacion_f) AS mes_generacion
FROM humana_prodl_landing.prodl_dwh_th_facturaciones_diferidas  b
            LEFT JOIN humana_prodl_raw.prodl_raw_dwh_dim_contrato a ON a.ID_CONTRATO=b.ID_CONTRATO
            left join humana_prodl_landing.prodl_dwh_dim_afiliado da on b.id_afiliado = da.id_afiliado
            LEFT JOIN humana_prodl_landing.prodl_dwh_dim_plan h on b.id_plan = h.id_plan 
            LEFT JOIN humana_prodl_consumption.prodl_cons_dim_negocio i on b.id_negocio = i.id_negocio 
            LEFT JOIN humana_prodl_consumption.prodl_cons_dim_producto p ON b.ID_PRODUCTO=p.ID_PRODUCTO
            LEFT JOIN humana_prodl_landing."prodl_dwh_dim_fecha"  d ON (b.ID_FECHA_GEN=d.ID_FECHA)
            LEFT JOIN "humana_prodl_consumption"."prodl_cons_dim_sucursal"  s ON (s.id_sucursal=b.id_sucursal)
            LEFT JOIN "humana_prodl_consumption"."prodl_cons_dim_contratante"  cc ON (cc.id_contratante=b.id_contratante)
            WHERE  b.id_estado_documento in (1,2,3)
            and b.id_concepto_documento in (1,5,6)
            and b.id_tipo_factura <> 2
            --CONDICIONES
            and i.linea_negocio ='INDIVIDUAL'
            and b.fecha_generacion_f between '2020-01-01' and '2022-12-31'
)
,facturacion_agrupada AS (
    SELECT 
        DISTINCT base_month,
        sucursal,
        linea_negocio
        ,producto_principal2 as producto_principal
,codigo_plan2 as codigo_plan
        ,contrato,
        --numero_renovacion_contrato,

        Vendedor_Contrato,
        Broker_Contrato,
        numrenov,
       Forma_Pago_Contrato,
       -- Clasificacion_Contrato,
       -- Segmentacion_Cliente,
        Canal_Venta,
       --TRY_CAST(edad_actual_contratante AS INT) AS edad_actual_contratante,
      --  Genero_Contratante,
       -- Segmentacion_Cartera,
        cuota,
        tipo_documento,
       -- Calificacion_Riesgo,
      Sala,
      Supervisor,
       --categoria_vendedor,
      COUNT(DISTINCT identificacion_afiliado) AS afiliados,
        SUM(valor_prima) AS primas_ag
    from (select distinct *,first_value(producto_principal) over(partition by contrato, base_month order by producto_principal) as producto_principal2
,first_value(codigo_plan) over(partition by contrato, base_month order by codigo_plan) as codigo_plan2
    FROM base_facturacion where tipo_documento='FACTURA')
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15--,16,17,18,19,20,21,22,23,24
)

,base_super_clean as(
select distinct 
mes_vigente,b.* ,c.tenure
from cartera_limpia_total c inner join facturacion_agrupada b on c.contrato=b.contrato and c.numrenov=b.numrenov 
and c.cuota=b.cuota
)
-----------
,base_desercion as(
SELECT distinct contrato as contrato_d, "fecha generación movimiento" as fecha_gen_churn, "fecha aplicación del movimiento" as fecha_ap_churn, 	"clasificacion motivo" as churn_type, 	"motivo desafiliacion" as Motivo_Desafiliacion
,case when "fecha generación movimiento"<="fecha aplicación del movimiento" and 	"clasificacion motivo"='VOLUNTARIO' then date("fecha generación movimiento")
      when "fecha aplicación del movimiento"<"fecha generación movimiento" and 	"clasificacion motivo"='VOLUNTARIO' then date(cast(fecha_app as timestamp))
      when 	"clasificacion motivo"='CARTERA'  then date_add('day',-105,date("fecha generación movimiento"))
else null end as fecha_churn
from(
select distinct contrato,"fecha generación movimiento","clasificacion motivo","motivo desafiliacion","fecha aplicación del movimiento"
,case when "fecha aplicación del movimiento" ='-' then null else "fecha aplicación del movimiento" end as  fecha_app
FROM "humana_prodl_consumption"."desercion_ov_desercion_oval" 
where "grupo negocio"='INDIVIDUAL'
)
)
--select * -- distinct date_trunc('month',fecha_churn) as mes,count(distinct contrato_d)
--from base_desercion
--where contrato_d in(277265,260377,279172,278151,269120)
--group by 1 order by 1


,union_churn as(
select distinct b.*
,case when contrato_d is not null then contrato_d else null end as churner
,date_trunc('month',fecha_churn) as churn_month
,fecha_churn,contrato_d,churn_type
--,fecha_gen_churn,fecha_ap_churn
from base_super_clean b left join base_desercion d on b.contrato=d.contrato_d
and date_diff('day',b.base_month,fecha_churn)<=60 and fecha_churn>=b.base_month
)
--select distinct base_month,count(distinct contrato_d)
--from union_churn
--group by 1 order by 1

--/*
,churners_clean as(
select distinct contrato_d,fecha_churn,churn_type,churn_month,churner,base_month_churn
--except(base_month) 
from(
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
,date_trunc('month',fecha_churn) as churn_month
,fecha_churn,churn_type
--,fecha_gen_churn,fecha_ap_churn,contratante,identificacion_contratante
from base_super_clean b left join churners_clean c on b.contrato=c.contrato_d and base_month=base_month_churn
)
,agrupacion_final as(
select distinct mes_vigente,  sucursal,
        linea_negocio
        , producto_principal
,codigo_plan
        ,contrato,
        Vendedor_Contrato,
        Broker_Contrato,
       Forma_Pago_Contrato,
        Canal_Venta,
       --Genero_Contratante,
        tipo_documento,
      Sala,
      Supervisor,
      churner,churn_month,fecha_churn,churn_type
     , sum(afiliados) AS afiliados,
        SUM(primas_ag) AS primas_ag
        from union_churn_clean
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
)
-------REEMBOLSOS
,base_reembolsos as(
sELECT
  r.transito
       ,r.valor_presentado
       ,r.valor_pagado_neto
       , da.cod_persona /*Beneficiarios*/ /*int*/
       , pres.cod_pres codpres /*integer*/
       , r.fecha_generacion est_fecha_f  /*string*/
       ,er.estado_reembolso
       , COALESCE(dn.linea_negocio, 'NO VALUE') as nombre /*Grupo de negocio*/  
       , COALESCE(ta.tipo_atencion, 'NO VALUE') as descconcep /*Tipo de Atencion*/  
       , COALESCE(pres.concepto_servicio, 'NO VALUE') as nomconserv /*Servicios*/  
       , COALESCE(dx.nivel_3, 'OTROS') as dgn_nomdiag
       , COALESCE(dpr.nombre_prestador, 'NO VALUE') as nom_prestador
       , COALESCE(dc.vendedor_contrato, 'NO VALUE') as nom_vendedor
       , COALESCE(dc.broker_contrato, 'NO VALUE') as nom_broker            
       , COALESCE(dx.agrupacion_diagnostico, 'NO APLICA') as aguda_cronica /*Tipo Diagnostico*/
       , COALESCE(CAST(r.rdo_codisecu as varchar), 'NO VALUE') as rdo_codisecu /*primer agregado*/
       , COALESCE(CAST(dc.contrato as varchar), 'NO VALUE') as contrato /*segundo agregado*/
       , COALESCE(CAST(dc.numrenov as varchar), 'NO VALUE') as numrenov /*tercer agregado*/
       , COALESCE(dx.cod_diagnostico_n3, 'NO VALUE') as dgn_coddiag /*Diag. Nivel 3*/  
       , COALESCE(CAST(dpr.id_prestador as varchar), 'NO VALUE') as codper /*Prestador*/
       , COALESCE(CAST(dc.codigo_broker_contrato as varchar), 'NO VALUE') as broker
       , COALESCE(CAST(dc.codigo_vendedor_contrato as varchar), 'NO VALUE') as vendedor /*Broker y Vendedor*/
       , COALESCE(case when dn.linea_negocio =  'CORPORATIVO' then '99999' else coalesce(cast(dc.sala as varchar), '99999') end) as sala_vendedor /*Sala vendedor*/
       , COALESCE(dc.grupo_empresarial, 'NO VALUE') as grupo_empresarial  /*Grupo empresarvendedorial*/
       , COALESCE(da.genero, 'NO VALUE') as genero /*Genero*/
       , COALESCE(dp.producto_principal, 'NO VALUE') as nompro /*Product*/
       , COALESCE(tr.tipo_reembolso, 'NO VALUE') as tipo_reembolso
       , COALESCE(CAST(CASE
                WHEN date_diff('year', fecha_nacimiento,dc.vigdesde_contrato) < 2                THEN '0 a 23 meses'
                WHEN date_diff('year', fecha_nacimiento,dc.vigdesde_contrato) between 2 and 17   THEN '24 meses a 17 años'
                WHEN date_diff('year', fecha_nacimiento,dc.vigdesde_contrato) between 18 and 23  THEN '18 años a 23 años'
                WHEN date_diff('year', fecha_nacimiento,dc.vigdesde_contrato) between 24 and 35  THEN '24 años a 35 años'
                WHEN date_diff('year', fecha_nacimiento,dc.vigdesde_contrato) between 36 and 44  THEN '36 años a 44 años'
                WHEN date_diff('year', fecha_nacimiento,dc.vigdesde_contrato) between 45 and 55  THEN '45 años a 55 años'
                WHEN date_diff('year', fecha_nacimiento,dc.vigdesde_contrato) between 56 and 59  THEN '56 años a 59 años'
                WHEN date_diff('year', fecha_nacimiento,dc.vigdesde_contrato) between 60 and 65  THEN '60 años a 65 años'
                WHEN date_diff('year', fecha_nacimiento,dc.vigdesde_contrato) >=66  THEN '>= 66 años'
                ELSE 'NO VALUE'  END as varchar), 'NO VALUE') as rango_etario /*Rango etario*/
FROM humana_prodl_landing.prodl_dwh_th_reembolsos r
left join humana_prodl_raw.prodl_raw_dwh_dim_contrato dc on r.id_contrato= dc.id_contrato
left join humana_prodl_landing.prodl_dwh_dim_afiliado da on r.id_afiliado = da.id_afiliado
left join humana_prodl_landing.prodl_dwh_dim_fecha df on r.id_fecha_gene= df.id_fecha
left join humana_prodl_consumption.prodl_cons_dim_negocio dn on r.id_negocio = dn.id_negocio
left join humana_prodl_consumption.prodl_cons_dim_producto dp on r.id_producto = dp.id_producto
left join humana_prodl_consumption.prodl_cons_dim_tipo_atencion ta on r.id_tipo_atencion = ta.id_tipo_atencion
left join humana_prodl_consumption.prodl_cons_dim_tipo_reembolsos tr on r.id_tipo_reembolso = tr.id_tipo_reembolso
left join humana_prodl_landing.prodl_dwh_dim_prestacion pres on r.id_prestacion = pres.id_prestacion
left join humana_prodl_landing.prodl_dwh_dim_diagnostico dx on r.id_diagnostico = dx.id_diagnostico
left join humana_prodl_consumption.prodl_cons_dim_prestador dpr on r.id_prestador = dpr.id_prestador
left join humana_prodl_consumption.prodl_cons_dim_estado_reembolso er on r.id_estado_reembolso=er.id_estado_reembolso
where r.id_estado_reembolso=9
AND r.fecha_generacion_f >= '2020-01-01' AND r.fecha_generacion_f <= '2023-12-31'
)
,reembolsos as(
SELECT distinct date_trunc('month',date(est_fecha_f)) as month_r,contrato,transito,tipo_reembolso,estado_reembolso,valor_pagado_neto--,tiempo_reembolso
--,valor_presentado,valor_recorte,valor_copago,valor_deducible
FROM base_reembolsos
)
,reembolsos_agrup as(
select distinct month_r,contrato,sum(valor_pagado_neto) as siniestralidad
from reembolsos
group by 1,2 order by 1,2
)
,cruce_reembolsos as(
select distinct u.*
,siniestralidad
from agrupacion_final u left join reembolsos_agrup r on mes_vigente=month_r and cast(u.contrato as varchar)=r.contrato
)
,duplicados as(
select distinct mes_vigente,contrato,count(*) as dup
from cruce_reembolsos
group by 1,2 order by 3 desc
)
/*select distinct --a.*,dup
--,first_value(producto_principal) over(partition by a.contrato, a.mes_vigente order by producto_principal) as producto_principal2
--,first_value(codigo_plan) over(partition by a.contrato, a.mes_vigente order by codigo_plan) as codigo_plan2

a.mes_vigente,count(distinct a.contrato),sum(dup)
from cruce_reembolsos a left join duplicados b on a.contrato=b.contrato
where dup>1
--order by contrato,mes_vigente
group by 1 order by 1 asc

*/

-- pendiente quitar duplicados y poner columnas faltantes




--/*
select distinct mes_vigente
--,rango_etario
,count(distinct contrato) as contratos,count(distinct churner) as churners
,sum(primas_ag) as total_primas
,sum(siniestralidad) as siniestralidad
from cruce_reembolsos
group by 1--,2
order by 1--,2
--*/

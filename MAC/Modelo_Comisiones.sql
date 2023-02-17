with 
cumplimiento_vendedor as(
SELECT * ,A__O as year
FROM `bustling-psyche-375313.humana_oval_2023.20230215_Comisiones_Cumplimiento_Vendedores`
where a__o=2022
)
select distinct year,categoria, count(distinct identificaci__n)
from cumplimiento_vendedor
group by 1,2 order by 1,2

,nominas_vendedor as(
SELECT * ,Ano as year--, concat(year,mes) as month
FROM `bustling-psyche-375313.humana_oval_2023.20230215_Comisiones_Nominas_Vendedores`
--where mes=11
)
,nomina_por_vendedor as(
select distinct year--,concat(year,month)
--,mes
,cedula
,sum(REMUNERACION_UNIFICADA) +sum(HORAS_EXTRAS_100_) + sum(HORAS_NOCTURNAS_25_) + sum(HORAS_SUPLEMENTARIAS_50_)+sum(BONO_REGIONAL) + sum(BONO_TEMPORAL) +sum(AJUSTE_PROPORCIONAL_SUELDO) + sum(PROVISION_DECIMO_TERCERO_) + sum(PROVISI__N_FONDOS_DE_RESERVA_) + sum(PROVISI__N_D__CIMO_CUARTO) + sum(PROVISION_DE_VACACIONES) as fijo
,sum(COMISIONES_EN_VENTAS) + sum(COMISION_COPORATIVA) + sum(COMISION_SERVICIO_AL_CLIENTE) as comisiones
,sum(BONO_DE_DESEMPENO) + sum(BONO_DE_CATEGORIA) + sum(BONIFICACI__N_ESPECIALISTAS_DE_PRODUCTO) + sum(BONO_REFERIDOS) + sum(BONO_ALIMENTACION) + sum(MOVILIZACION) + sum(GIFT_CARD) as otros_variable
,sum(REMUNERACION_UNIFICADA) +sum(HORAS_EXTRAS_100_) + sum(HORAS_NOCTURNAS_25_) + sum(HORAS_SUPLEMENTARIAS_50_)+sum(BONO_REGIONAL) + sum(BONO_TEMPORAL) +sum(AJUSTE_PROPORCIONAL_SUELDO) + sum(PROVISION_DECIMO_TERCERO_) + sum(PROVISI__N_FONDOS_DE_RESERVA_) + sum(PROVISI__N_D__CIMO_CUARTO) + sum(PROVISION_DE_VACACIONES)+sum(COMISIONES_EN_VENTAS) + sum(COMISION_COPORATIVA) + sum(COMISION_SERVICIO_AL_CLIENTE) + sum(BONO_DE_DESEMPENO) + sum(BONO_DE_CATEGORIA) + sum(BONIFICACI__N_ESPECIALISTAS_DE_PRODUCTO) + sum(BONO_REFERIDOS) + sum(BONO_ALIMENTACION) + sum(MOVILIZACION) + sum(GIFT_CARD) as total_salario
from nominas_vendedor
group by 1,2--,3 
order by 1,2--,3
)
select distinct --mes,
sum(fijo),sum(comisiones),sum(otros_variable),sum(total_salario)
from nomina_por_vendedor
--group by 1 order by 1

select distinct c.year, cumplimiento,fijo,comisiones,otros_variable,total_salario
,count(distinct identificaci__n) as id_vendedor
--,count(distinct cedula)
from cumplimiento_vendedor c inner join 
nomina_por_vendedor n on c.identificaci__n=n.cedula and c.mes_dat=n.mes
group by 1,2,3,4,5,6 order by 1,2,3,4,5,6



with params as (
SELECT
'tagname' as "tagname", --tagname que será utilizada na consulta aos Bancos de Dados
'00:00'::time as "start", --Hora inicial do dia
'23:59'::time as "end", --Hora final do dia
20::int as "intervalo", --intervalo dos dados usados nos cálculos
0::float as "outorga", --Valor Outorga m³/dia | período de 20h
0::float as "anomalia", --Valor acima/abaixo considerados anômalos
	/*Conversores*/
3.6::float as "converhs", --conversor m³/h p/ L/s(multiplica) | L/s p/ m³/h(divide)
86400::float as "converds",--conversor L/dia p/ L/s (divide) | L/s p/ L/dia (multiplica)
1000::float as "converlm"--conversor L/dia p/ m³/dia (divide) | m³/dia p/ L/dia (multiplica)
)

,medicoes as(
	SELECT
	"value",
	"timestamp" as "time",
  date_trunc('minute',"timestamp") as "minute"

	/*chama o DB*/
	FROM openiot_json.historian_new_data_extra hnde
	WHERE tagname = (SELECT tagname FROM params) 
	and "timestamp" > now() - interval (SELECT intervalo FROM params) --intervalo entre agora e XX horas antes
	and ("timestamp" at time zone 'America/Sao_Paulo')::time BETWEEN (SELECT start FROM params) and (SELECT "end" FROM params)
  ORDER BY "time"
)

,calcula as (
	SELECT

    distinct on (minute) minute,

	case
	when "value" < (SELECT anomalia FROM params) --valores anômalos acima de 200
	then (sum("value") over w) --soma todas as medições no período de XX horas ignorando valores anômalos
	else null
	end as outor

	FROM medicoes
	window w as (partition by minute)
)

,calculaoutor as (
	SELECT

	(avg(outor) over w * (SELECT converds FROM params)) / (SELECT converlm FROM params) as resuoutorga --calcula a outorga e converte L/dia para m³/dia

	FROM calcula
	window w as (partition by minute)
)

SELECT

minute as "time",
--outor as "Valor Outorga", --valores da outorga ignorando valores anômalos
resuoutorga as "Valor Outorga Diário(20h)",

case
--when resuoutorga > (SELECT outorga FROM params) * 1000 --alerta em L/dia
when resuoutorga > (SELECT outorga FROM params) --alerta em m³/dia
then 1
else 0
end as "Alerta Outorga"

FROM calculaoutor, calcula

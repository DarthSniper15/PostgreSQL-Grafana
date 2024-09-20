with params as (
SELECT
'CEFSP12_FT_DAEE' as "tagname",
'00:00'::time as "start",
'23:59'::time as "end",
--0.95::float as "alert_threshold",
1200::float as "outorga", --1200 m³/dia | período de 20h
3.6::float as "converhs", --conversor m³/h > L/s(multiplica) | L/s > m³/h(divide)
86400::float as "converds",--conversor L/dia > L/s (divide) | L/s > L/dia (multiplica)
1000::float as "converlm"--conversor L/dia > m³/dia (divide) | m³/dia > L/dia (multiplica)
)

,medicoes as(
	SELECT
	"value",
	"timestamp" as "time",
  date_trunc('minute',"timestamp") as "minute"

	/*chama o DB*/
	FROM openiot_json.historian_new_data_extra hnde
	WHERE tagname = (SELECT tagname FROM params) 
	and "timestamp" > now() - interval '20 hours' --intervalo entre agora e 20 horas antes
	and ("timestamp" at time zone 'America/Sao_Paulo')::time BETWEEN (SELECT start from params) and (select "end" from params)
  ORDER BY "time"
)

,calcula as (
	SELECT

    distinct on (minute) minute,

	case
	when "value" < 200 --valores anômalos acima de 200
	then (sum("value") over w) --soma todas as medições no período de 20 horas ignorando valores anômalos
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
--Funcional Minuto em Minuto

select

  /*Filtra o período de 24h*/
  case when date_part('hour', "timestamp" at time zone 'America/Sao_Paulo') BETWEEN 0 and 23

  /*Filtra a cada 6 minutos*/
  and cast(extract(minute from timestamp at time zone 'America/Sao_Paulo') as numeric) % cast(extract(minute from interval '6 minutes') as numeric) = 0

  /*Filtra os minutos*/
  and date_part('second', "timestamp" at time zone 'America/Sao_Paulo') = 0/*BETWEEN 30 and 60*/
  
  /*filtra valores = 0
  and value > 0*/ then value

  else null
  end

  as "Teste", $__time("timestamp")

/*chama o DB*/
from openiot_json.historian_new_data_extra hnde
where tagname = 'tagname' and $__timeFilter("timestamp")
ORDER by "time"
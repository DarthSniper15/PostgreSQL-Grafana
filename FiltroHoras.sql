--Funcional Hora em Hora

select

  /*Filtra o período de 24h*/
  case when date_part('hour', "timestamp" at time zone 'America/Sao_Paulo') BETWEEN 0 and 23

  /*Filtra a cada 1 hora*/
  and cast(extract(hour from timestamp at time zone 'America/Sao_Paulo') as numeric) % cast(extract(hour from interval '1 hours') as numeric) = 0

  /*Filtra os minutos*/
  and date_part('minute', "timestamp" at time zone 'America/Sao_Paulo') = 0/*BETWEEN 30 and 60*/
  
  /*filtra valores = 0
  and value > 0*/ then value

  else null
  end

  as "Teste", $__time("timestamp")

/*chama o DB*/
from "DataBase" hnde
where tagname = 'tagname' and $__timeFilter("timestamp")

ORDER by "time"

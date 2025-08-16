-- Query para Monitorar o alerta em tempos específicos --

-- Define as variáveis
WITH parametros AS (
  SELECT    
    '08:00'::time AS hora_inicio, -- Inicio do Intervalo de monitoramento
    '17:00'::time AS hora_fim, --  Fim do Intervalo de monitoramento
    INTERVAL '6 minutes' AS limite_tolerado, -- Limite tolerado sem dados
    'tagname' AS tag -- Tagname do sistema
    
),

-- Pegas os dados brutos
ultimo_dado AS (
  SELECT
    "timestamp", 
    value
    FROM databse, parametros p
    WHERE tagname = p.tag AND $__timeFilter("timestamp")
    ORDER BY "timestamp" DESC -- Ordena do mais recente para o mais antigo
    LIMIT 1 -- Pega apenas o primeiro (o mais recente)
)

-- Responsável pelo alerta sem dados | pega o alerta do período mostrado no gráfico
SELECT
  $__timeTo()::timestamptz AS time,
  CASE
  
    -- Caso valores sejam nulos dentro do intervalo, alerta será definido como 1
    WHEN ($__timeTo()::timestamptz AT TIME ZONE 'America/Sao_Paulo')::time NOT BETWEEN p.hora_inicio AND p.hora_fim THEN
            CASE
                -- Condição de Alerta:
                -- 1. Se não houver nenhum dado (último timestamp é NULL)
                -- 2. OU se o último dado for NULL E o tempo for mais antigo que o limite de tolerado
                WHEN u."timestamp" IS NULL 
                OR ($__timeTo()::timestamptz - u."timestamp" > p.limite_tolerado)
                THEN 1
                
                -- Há dados dentro do limite ou do intervalo, alerta definido como 0
                ELSE 0
            END
            
        -- Fora do horário de monitoramento
        ELSE NULL

  END AS "alerta acompanhamento"
  FROM parametros p
  LEFT JOIN ultimo_dado u ON TRUE;

# Ponto de entrada das Azure Functions (modelo Python v2).
#   pipeline_semanal: roda Silver e Gold toda segunda as 06:00 de Brasilia (09:00 UTC)
#   consumidor_eventos: roda sempre que chegam mensagens novas no Event Hubs
# Se qualquer uma der erro, a falha vai para o Application Insights e o
# alerta do Azure Monitor manda e-mail.
# A fonte muda poucas vezes por ano, por isso a pipeline roda so uma vez por
# semana. Depois da data em DATA_FIM_AGENDAMENTO ela para de processar.

import logging
import os
import sys
from datetime import date
from typing import List

import azure.functions as func

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

app = func.FunctionApp()


@app.timer_trigger(schedule="0 0 9 * * 1", arg_name="timer", run_on_startup=False)
def pipeline_semanal(timer: func.TimerRequest):
    data_fim = os.getenv("DATA_FIM_AGENDAMENTO")
    if data_fim and date.today() > date.fromisoformat(data_fim):
        logging.info(f"Agendamento encerrado em {data_fim}, nada foi processado")
        return

    import rodar_pipeline
    rodar_pipeline.rodar()


@app.event_hub_message_trigger(arg_name="mensagens", event_hub_name="%EVENTHUB_NAME%",
                               connection="EVENTHUB_CONNECTION_STRING", cardinality="many")
def consumidor_eventos(mensagens: List[func.EventHubEvent]):
    from consumidor import processar_mensagens
    processar_mensagens([m.get_body().decode("utf-8") for m in mensagens])

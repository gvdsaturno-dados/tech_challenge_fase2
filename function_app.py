# Ponto de entrada das Azure Functions (modelo Python v2).
#   pipeline_diaria: roda Silver e Gold todo dia as 06:00 de Brasilia (09:00 UTC)
#   consumidor_eventos: roda sempre que chegam mensagens novas no Event Hubs
# Se qualquer uma der erro, a falha vai para o Application Insights e o
# alerta do Azure Monitor manda e-mail.

import os
import sys
from typing import List

import azure.functions as func

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

app = func.FunctionApp()


@app.timer_trigger(schedule="0 0 9 * * *", arg_name="timer", run_on_startup=False)
def pipeline_diaria(timer: func.TimerRequest):
    import rodar_pipeline
    rodar_pipeline.rodar()


@app.event_hub_message_trigger(arg_name="mensagens", event_hub_name="%EVENTHUB_NAME%",
                               connection="EVENTHUB_CONNECTION_STRING", cardinality="many")
def consumidor_eventos(mensagens: List[func.EventHubEvent]):
    from consumidor import processar_mensagens
    processar_mensagens([m.get_body().decode("utf-8") for m in mensagens])

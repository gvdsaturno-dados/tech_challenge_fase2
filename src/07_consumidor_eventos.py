# Consumidor do streaming para rodar no computador: ouve o Event Hubs por
# alguns segundos e processa cada micro-lote com a mesma funcao que a Azure
# Function usa (src/consumidor.py). Le o hub desde o inicio, mas os
# duplicados sao descartados, entao rodar de novo nao duplica nada.

import threading

from azure.eventhub import EventHubConsumerClient

from config import EVENTHUB_CONNECTION_STRING, EVENTHUB_NAME
from consumidor import processar_mensagens

TEMPO_ESCUTA = 90  # segundos ouvindo o hub antes de encerrar


def processar_lote(contexto, mensagens):
    processar_mensagens([m.body_as_str() for m in mensagens])


consumidor = EventHubConsumerClient.from_connection_string(
    EVENTHUB_CONNECTION_STRING, consumer_group="$Default", eventhub_name=EVENTHUB_NAME)

# depois de TEMPO_ESCUTA segundos o consumidor e fechado e o script termina
threading.Timer(TEMPO_ESCUTA, consumidor.close).start()
print(f"Ouvindo o Event Hubs por {TEMPO_ESCUTA} segundos...\n")
with consumidor:
    consumidor.receive_batch(on_event_batch=processar_lote, starting_position="-1",
                             max_batch_size=10, max_wait_time=5)
print("\nConsumidor encerrado")

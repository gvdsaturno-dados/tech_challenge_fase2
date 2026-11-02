# Produtor de eventos simulados: "o indicador de um municipio foi atualizado".
# Os eventos partem de municipios reais da Gold, com a taxa alterada um pouco.
# Para testar o consumidor, alguns eventos sao reenviados (duplicados, como
# acontece quando a mensageria entrega "pelo menos uma vez") e alguns sao
# invalidos de proposito.

import json
import random
import time
import uuid
from datetime import datetime, timezone

import pandas as pd
from azure.eventhub import EventData, EventHubProducerClient

from config import EVENTHUB_CONNECTION_STRING, EVENTHUB_NAME, STORAGE_OPTIONS

QTD_EVENTOS = 30

indicador = pd.read_parquet("abfs://gold/indicador_municipio/indicador_municipio.parquet",
                            storage_options=STORAGE_OPTIONS)
base = indicador[(indicador["origem"] == "oficial") & (indicador["ano"] == 2024) &
                 (indicador["rede"] == "3") & indicador["taxa_alfabetizacao"].notna()]
amostra = base.sample(QTD_EVENTOS)

eventos = []
for _, linha in amostra.iterrows():
    nova_taxa = min(100, max(0, linha["taxa_alfabetizacao"] + random.uniform(-3, 3)))
    eventos.append({
        "id_evento": str(uuid.uuid4()),
        "tipo": "atualizacao_indicador_municipio",
        "ano": int(linha["ano"]),
        "id_municipio": linha["id_municipio"],
        "rede": linha["rede"],
        "taxa_alfabetizacao": round(nova_taxa, 2),
    })

# tres eventos invalidos: taxa impossivel, municipio inexistente e taxa vazia
for alteracao in [{"taxa_alfabetizacao": 150.0}, {"id_municipio": "9999999"}, {"taxa_alfabetizacao": None}]:
    invalido = dict(random.choice(eventos))
    invalido["id_evento"] = str(uuid.uuid4())
    invalido.update(alteracao)
    eventos.append(invalido)

produtor = EventHubProducerClient.from_connection_string(EVENTHUB_CONNECTION_STRING, eventhub_name=EVENTHUB_NAME)
enviados = 0
with produtor:
    for i, evento in enumerate(eventos):
        # o produtor anota quando o evento aconteceu e o consumidor anota
        # quando recebeu. A diferenca entre os dois e a latencia
        evento["momento_evento"] = datetime.now(timezone.utc).isoformat()
        lote = produtor.create_batch()
        lote.add(EventData(json.dumps(evento)))
        produtor.send_batch(lote)
        enviados += 1

        # a cada 10 eventos, reenvia o mesmo (simula uma retentativa)
        if i % 10 == 0:
            produtor.send_batch(lote)
            enviados += 1

        time.sleep(0.5)

print(f"{len(eventos)} eventos diferentes, {enviados} mensagens enviadas "
      f"({enviados - len(eventos)} duplicadas, 3 invalidas)")

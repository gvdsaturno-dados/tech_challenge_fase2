# Processamento de um micro-lote de eventos do streaming. Usado pelo
# 07_consumidor_eventos.py (execucao local) e pela Azure Function
# consumidor_eventos (execucao na nuvem, acionada a cada mensagem nova).
#   1. carimba o momento de ingestao e mede a latencia (ingestao - evento)
#   2. grava o lote cru na Bronze (historico, nada e descartado)
#   3. manda os invalidos para a quarentena e descarta duplicados pelo id_evento
#   4. grava os validos na Silver e acrescenta na Gold com origem = "simulado"
# Como o descarte de duplicados olha tudo o que ja foi processado, receber a
# mesma mensagem de novo nao duplica nada (consumidor idempotente).

import json
import time
from datetime import datetime, timezone

import pandas as pd

from config import STORAGE_OPTIONS
from utils_pipeline import checar_regra, classificar, registrar_execucao

CAMINHO_GOLD = "abfs://gold/indicador_municipio/indicador_municipio.parquet"


def ler_ou_vazio(caminho):
    try:
        return pd.read_parquet(caminho, storage_options=STORAGE_OPTIONS)
    except FileNotFoundError:
        return pd.DataFrame(columns=["id_evento"])


def processar_mensagens(textos):
    """textos: lista com o corpo (JSON) de cada mensagem do lote."""
    if not textos:
        return
    inicio = time.time()
    agora = datetime.now(timezone.utc)
    id_lote = agora.strftime("%Y%m%d_%H%M%S_%f")

    lote = pd.DataFrame([json.loads(texto) for texto in textos])
    lote["momento_ingestao"] = agora
    lote["latencia_segundos"] = (agora - pd.to_datetime(lote["momento_evento"], utc=True)).dt.total_seconds()

    # Bronze: o lote como chegou
    lote.to_parquet(f"abfs://bronze/eventos/dt_ingestao={agora:%Y-%m-%d}/{id_lote}.parquet",
                    index=False, storage_options=STORAGE_OPTIONS)

    # invalidos vao para a quarentena (o lote inteiro continua preservado na Bronze)
    gold = pd.read_parquet(CAMINHO_GOLD, storage_options=STORAGE_OPTIONS)
    valido = (lote["taxa_alfabetizacao"].between(0, 100) &
              lote["id_municipio"].isin(gold["id_municipio"]) &
              lote["ano"].notna())
    invalidos = lote[~valido]
    if len(invalidos) > 0:
        invalidos.to_parquet(f"abfs://silver/eventos_quarentena/{id_lote}.parquet",
                             index=False, storage_options=STORAGE_OPTIONS)

    # duplicados: repetidos dentro do lote ou ja processados antes
    validos = lote[valido].drop_duplicates("id_evento")
    ja_processados = ler_ou_vazio("abfs://silver/eventos")
    validos = validos[~validos["id_evento"].isin(ja_processados["id_evento"])]
    duplicados = valido.sum() - len(validos)

    if len(validos) > 0:
        validos.to_parquet(f"abfs://silver/eventos/{id_lote}.parquet", index=False, storage_options=STORAGE_OPTIONS)

        # Gold: mesma linha do municipio oficial, com a taxa nova e origem simulado
        oficial = gold[gold["origem"] == "oficial"].drop(
            columns=["taxa_alfabetizacao", "situacao_meta", "distancia_meta_2030", "origem", "id_evento"])
        novos = validos[["id_evento", "ano", "id_municipio", "rede", "taxa_alfabetizacao"]].merge(
            oficial, on=["ano", "id_municipio", "rede"], how="left")
        taxa = novos["meta_2030"].notna().mean() * 100
        checar_regra("streaming: join evento x linha oficial do municipio", "informativa", taxa == 100,
                     f"{taxa:.0f}% casaram")
        novos["situacao_meta"] = novos.apply(classificar, axis=1)
        novos["distancia_meta_2030"] = novos["meta_2030"] - novos["taxa_alfabetizacao"]
        novos["origem"] = "simulado"
        gold = pd.concat([gold, novos], ignore_index=True)
        gold.to_parquet(CAMINHO_GOLD, index=False, storage_options=STORAGE_OPTIONS)

    latencia_media = lote["latencia_segundos"].mean()
    registrar_execucao("streaming", "eventos", len(lote), len(validos), round(time.time() - inicio, 1), "sucesso",
                       detalhe=f"invalidos={len(invalidos)} duplicados={duplicados} "
                               f"latencia_media_s={latencia_media:.2f} latencia_max_s={lote['latencia_segundos'].max():.2f}")
    checar_regra("streaming: lote sem eventos invalidos", "informativa", len(invalidos) == 0,
                 f"{len(invalidos)} para a quarentena")
    checar_regra("streaming: lote sem eventos duplicados", "informativa", duplicados == 0,
                 f"{duplicados} descartados")
    print(f"lote {id_lote}: {len(lote)} recebidos, {len(validos)} novos na Gold, "
          f"{len(invalidos)} invalidos, {duplicados} duplicados, latencia media {latencia_media:.2f} s")

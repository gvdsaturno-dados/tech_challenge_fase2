# Extrai as 7 tabelas da fonte e grava a camada Bronze em Parquet.
# Politica: cada execucao gera um arquivo novo com nome unico, entao
# nenhuma execucao apaga a anterior (historico preservado).
# As metricas de cada tabela sao acumuladas em data/ops/extracao.csv.

import os
import time
from datetime import datetime

import pandas as pd
from google.cloud import bigquery

from config import GCP_PROJECT_ID, DATASET_FONTE

TABELAS = [
    "alunos",
    "municipio",
    "uf",
    "meta_alfabetizacao_brasil",
    "meta_alfabetizacao_uf",
    "meta_alfabetizacao_municipio",
    "dicionario",
]
ARQUIVO_METRICAS = "data/ops/extracao.csv"

client = bigquery.Client(project=GCP_PROJECT_ID)
config_dry_run = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)

agora = datetime.now()
dt_ingestao = agora.strftime("%Y-%m-%d")
id_execucao = agora.strftime("%Y%m%d_%H%M%S")
os.makedirs("data/ops", exist_ok=True)
print(f"Execucao {id_execucao}\n")

for tabela in TABELAS:
    inicio = time.time()
    consulta = f"SELECT * FROM `{DATASET_FONTE}.{tabela}`"

    dry_run = client.query(consulta, job_config=config_dry_run)
    job = client.query(consulta)
    df = job.to_dataframe(create_bqstorage_client=False)

    df["dt_ingestao"] = dt_ingestao
    df["id_execucao"] = id_execucao

    pasta = f"data/bronze/{tabela}/dt_ingestao={dt_ingestao}"
    os.makedirs(pasta, exist_ok=True)
    df.to_parquet(f"{pasta}/{id_execucao}.parquet", index=False)

    duracao = round(time.time() - inicio, 1)
    metrica = pd.DataFrame([{
        "id_execucao": id_execucao,
        "tabela": tabela,
        "status": "sucesso",
        "linhas": len(df),
        "bytes_processados": dry_run.total_bytes_processed,
        "id_job_bigquery": job.job_id,
        "duracao_segundos": duracao,
    }])
    metrica.to_csv(ARQUIVO_METRICAS, mode="a", index=False, header=not os.path.exists(ARQUIVO_METRICAS))

    print(f"{tabela:<32} {len(df):>10} linhas  {duracao:>7} s")

print(f"\nBronze gravada em data/bronze/ e metricas em {ARQUIVO_METRICAS}")

# Extrai as 7 tabelas da fonte e grava a camada Bronze em Parquet na Azure.
# Politica: cada execucao gera um arquivo novo com nome unico, entao
# nenhuma execucao apaga a anterior (historico preservado).
# As metricas de cada tabela vao para ops/execucoes.csv.

import time
from datetime import datetime

from google.cloud import bigquery

from config import GCP_PROJECT_ID, DATASET_FONTE, STORAGE_OPTIONS
from utils_pipeline import registrar_execucao

TABELAS = [
    "alunos",
    "municipio",
    "uf",
    "meta_alfabetizacao_brasil",
    "meta_alfabetizacao_uf",
    "meta_alfabetizacao_municipio",
    "dicionario",
]

client = bigquery.Client(project=GCP_PROJECT_ID)
config_dry_run = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)

agora = datetime.now()
dt_ingestao = agora.strftime("%Y-%m-%d")
id_execucao = agora.strftime("%Y%m%d_%H%M%S")
print(f"Execucao {id_execucao}\n")

for tabela in TABELAS:
    inicio = time.time()
    consulta = f"SELECT * FROM `{DATASET_FONTE}.{tabela}`"

    dry_run = client.query(consulta, job_config=config_dry_run)
    job = client.query(consulta)
    df = job.to_dataframe(create_bqstorage_client=False)
    linhas_fonte = len(df)

    df["dt_ingestao"] = dt_ingestao
    df["id_execucao"] = id_execucao

    caminho = f"abfs://bronze/{tabela}/dt_ingestao={dt_ingestao}/{id_execucao}.parquet"
    df.to_parquet(caminho, index=False, storage_options=STORAGE_OPTIONS)

    duracao = round(time.time() - inicio, 1)
    registrar_execucao("bronze", tabela, linhas_fonte, len(df), duracao, "sucesso",
                       bytes_processados=dry_run.total_bytes_processed,
                       detalhe=f"id_execucao={id_execucao} job_bigquery={job.job_id}")

    print(f"{tabela:<32} {len(df):>10} linhas  {duracao:>7} s")

print("\nBronze gravada no container bronze e metricas em ops/execucoes.csv")

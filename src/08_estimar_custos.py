# Estimativa de quanto a pipeline custaria por mes em producao.
# As quantidades foram medidas no projeto (bytes lidos no BigQuery, tamanho
# dos containers e duracao das Functions) e os precos sao os publicos.
# O projeto rodou de graca (BigQuery sandbox e credito do Azure for Students),
# entao a tabela mostra quanto o mesmo desenho custaria se fosse pago.

import pandas as pd
from adlfs import AzureBlobFileSystem

from config import AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY, STORAGE_OPTIONS

# quantas vezes cada parte roda por mes em producao
EXTRACOES_POR_MES = 1          # a fonte e atualizada poucas vezes por ano
PIPELINES_POR_MES = 4          # Silver e Gold uma vez por semana
EVENTOS_POR_MES = 10_000       # volume imaginado para o streaming
EVENTOS_POR_LOTE = 10          # media de eventos por lote no teste
HORAS_NO_MES = 730

# duracoes medidas no Application Insights (02/11/2026)
SEGUNDOS_PIPELINE = 22.4       # maior duracao de uma execucao da pipeline (Silver e Gold)
SEGUNDOS_LOTE_STREAMING = 1.6  # duracao de um lote na consumidor_eventos
MEMORIA_GB = 4                 # memoria da instancia da Function

# precos publicos em US$
PRECO_BIGQUERY_TIB = 6.25      # por TiB lido (1 TiB gratis por mes)
PRECO_ADLS_GB_MES = 0.033      # Hot LRS com namespace hierarquico
PRECO_FUNCTIONS_GB_S = 0.000037  # Flex Consumption (100.000 GB-s gratis por mes)
PRECO_EVENTHUBS_HORA = 0.015   # Basic, por unidade de throughput
PRECO_EVENTHUBS_MILHAO = 0.028  # Basic, por milhao de eventos recebidos

# quantidades medidas
execucoes = pd.read_csv("abfs://ops/execucoes.csv", storage_options=STORAGE_OPTIONS)
bronze = execucoes[(execucoes["etapa"] == "bronze") & (execucoes["status"] == "sucesso")]
ultima_extracao = bronze[bronze["data_hora"] == bronze["data_hora"].max()]
bytes_por_extracao = ultima_extracao["bytes_processados"].sum()

fs = AzureBlobFileSystem(account_name=AZURE_STORAGE_ACCOUNT, account_key=AZURE_STORAGE_KEY)
gb_por_container = {c: fs.du(c) / 1024**3 for c in ["bronze", "silver", "gold", "ops"]}
gb_armazenados = sum(gb_por_container.values())

print("Medicoes:")
print(f"  bytes lidos por extracao no BigQuery: {bytes_por_extracao / 1024**2:.1f} MB")
for container, gb in gb_por_container.items():
    print(f"  {container:<7} {gb * 1024:.1f} MB")
print()

# custo de um mes
tib_lidos = bytes_por_extracao * EXTRACOES_POR_MES / 1024**4
gb_s = MEMORIA_GB * (PIPELINES_POR_MES * SEGUNDOS_PIPELINE +
                     EVENTOS_POR_MES / EVENTOS_POR_LOTE * SEGUNDOS_LOTE_STREAMING)

linhas = [
    ["BigQuery (leitura da fonte)", "TiB lido", tib_lidos, PRECO_BIGQUERY_TIB],
    ["ADLS Gen2 (Bronze, Silver, Gold, ops)", "GB/mes", gb_armazenados, PRECO_ADLS_GB_MES],
    ["Azure Functions Flex (batch + streaming)", "GB-segundo", gb_s, PRECO_FUNCTIONS_GB_S],
    ["Event Hubs Basic (unidade ligada)", "hora", HORAS_NO_MES, PRECO_EVENTHUBS_HORA],
    ["Event Hubs Basic (eventos recebidos)", "milhao de eventos", EVENTOS_POR_MES / 1e6, PRECO_EVENTHUBS_MILHAO],
]
tabela = pd.DataFrame(linhas, columns=["servico", "unidade", "quantidade_mes", "preco_unitario_usd"])
tabela["custo_mensal_usd"] = tabela["quantidade_mes"] * tabela["preco_unitario_usd"]

# cotas gratuitas que valem para qualquer conta paga
cota = {"BigQuery (leitura da fonte)": 1, "Azure Functions Flex (batch + streaming)": 100_000}
tabela["custo_com_cota_gratis_usd"] = [
    max(0, linha.quantidade_mes - cota.get(linha.servico, 0)) * linha.preco_unitario_usd
    for linha in tabela.itertuples()
]

pd.set_option("display.float_format", lambda x: f"{x:,.6f}")
print(tabela.to_string(index=False))
print(f"\nTotal sem cotas gratuitas: US$ {tabela['custo_mensal_usd'].sum():.2f} por mes")
print(f"Total com cotas gratuitas: US$ {tabela['custo_com_cota_gratis_usd'].sum():.2f} por mes")

maior = tabela.loc[tabela["custo_com_cota_gratis_usd"].idxmax()]
print(f"Maior custo: {maior['servico']} ({maior['custo_com_cota_gratis_usd'] / tabela['custo_com_cota_gratis_usd'].sum():.0%} do total)")
print(f"Custo por execucao da pipeline (sem cota): US$ {MEMORIA_GB * SEGUNDOS_PIPELINE * PRECO_FUNCTIONS_GB_S:.5f}")

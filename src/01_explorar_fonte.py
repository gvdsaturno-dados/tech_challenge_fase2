# Primeiro contato com a fonte: quanto custa ler cada tabela (dry run),
# quais colunas e tipos ela tem, e como o dicionario traduz os codigos.
# O dry run nao executa a consulta, so devolve quantos bytes ela leria.

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

client = bigquery.Client(project=GCP_PROJECT_ID)

# use_query_cache=False para medir a leitura real, sem aproveitar cache
config_dry_run = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)

print("Bytes que cada leitura completa processaria:\n")
total_bytes = 0
for tabela in TABELAS:
    consulta = f"SELECT * FROM `{DATASET_FONTE}.{tabela}`"
    job = client.query(consulta, job_config=config_dry_run)
    total_bytes += job.total_bytes_processed
    print(f"{tabela:<32} {job.total_bytes_processed / 1024**2:>10.2f} MB")

print(f"{'TOTAL':<32} {total_bytes / 1024**2:>10.2f} MB")
print(f"Cota gratuita mensal usada: {total_bytes / 1024**4 * 100:.3f}% de 1 TiB")

print("\nColunas e tipos de cada tabela:")
for tabela in TABELAS:
    info = client.get_table(f"{DATASET_FONTE}.{tabela}")
    print(f"\n{tabela} ({info.num_rows} linhas)")
    for coluna in info.schema:
        print(f"  {coluna.name:<28} {coluna.field_type}")

# o dicionario tem poucas linhas, entao vale ler de verdade
dicionario = client.query(f"SELECT * FROM `{DATASET_FONTE}.dicionario`").to_dataframe()
print("\nConteudo do dicionario:")
print(dicionario.to_string(index=False))

# as tabelas de meta nao aparecem no dicionario, entao e preciso ver
# como a rede esta escrita em cada tabela antes de qualquer join
print("\nValores de ano e rede em cada tabela (com quantidade de linhas):")
for tabela in TABELAS:
    if tabela == "dicionario":
        continue
    consulta = f"""
        SELECT ano, rede, COUNT(*) AS linhas
        FROM `{DATASET_FONTE}.{tabela}`
        GROUP BY ano, rede
        ORDER BY ano, rede
    """
    valores = client.query(consulta).to_dataframe()
    print(f"\n{tabela}")
    print(valores.to_string(index=False))

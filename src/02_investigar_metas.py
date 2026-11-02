# As tabelas de meta escrevem a rede por extenso ("Publica", "Municipal"),
# enquanto as de resultado usam codigo. Este script descobre a qual codigo cada
# texto corresponde comparando a taxa_alfabetizacao que as duas tabelas trazem.
# Tambem mostra o que significam ano, nivel_alfabetizacao e percentual_participacao.

from google.cloud import bigquery

from config import GCP_PROJECT_ID, DATASET_FONTE

client = bigquery.Client(project=GCP_PROJECT_ID)


def ler(tabela):
    return client.query(f"SELECT * FROM `{DATASET_FONTE}.{tabela}`").to_dataframe()


uf = ler("uf")
municipio = ler("municipio")
meta_brasil = ler("meta_alfabetizacao_brasil")
meta_uf = ler("meta_alfabetizacao_uf")
meta_municipio = ler("meta_alfabetizacao_municipio")

# se a taxa da meta for igual a taxa de um codigo de rede, esse e o par
print("Meta UF ('Publica') comparada com a tabela uf, por codigo de rede:")
for codigo in ["2", "3", "5"]:
    resultado = uf[uf["rede"] == codigo][["ano", "sigla_uf", "taxa_alfabetizacao"]]
    comparacao = meta_uf.merge(resultado, on=["ano", "sigla_uf"], how="left", suffixes=("_meta", "_resultado"))
    casaram = comparacao["taxa_alfabetizacao_resultado"].notna().sum()
    diferenca = (comparacao["taxa_alfabetizacao_meta"] - comparacao["taxa_alfabetizacao_resultado"]).abs()
    print(f"  rede {codigo}: {casaram} de {len(comparacao)} linhas casaram, diferenca media {diferenca.mean():.4f}")

print("\nMeta municipio ('Municipal') comparada com a tabela municipio, por codigo de rede:")
for codigo in ["2", "3", "5"]:
    resultado = municipio[municipio["rede"] == codigo][["ano", "id_municipio", "taxa_alfabetizacao"]]
    comparacao = meta_municipio.merge(resultado, on=["ano", "id_municipio"], how="left", suffixes=("_meta", "_resultado"))
    casaram = comparacao["taxa_alfabetizacao_resultado"].notna().sum()
    diferenca = (comparacao["taxa_alfabetizacao_meta"] - comparacao["taxa_alfabetizacao_resultado"]).abs()
    print(f"  rede {codigo}: {casaram} de {len(comparacao)} linhas casaram, diferenca media {diferenca.mean():.4f}")

# a meta Brasil tem so 3 linhas, entao da para ver inteira
print("\nMeta Brasil completa:")
print(meta_brasil.to_string(index=False))

print("\nMeta UF de uma UF como exemplo (SP):")
print(meta_uf[meta_uf["sigla_uf"] == "SP"].to_string(index=False))

print("\nnivel_alfabetizacao (meta municipio): quantidade por valor")
print(meta_municipio["nivel_alfabetizacao"].value_counts(dropna=False).sort_index())

print("\npercentual_participacao: resumo por tabela")
for nome, tabela in [("brasil", meta_brasil), ("uf", meta_uf), ("municipio", meta_municipio)]:
    print(f"  {nome}: min {tabela['percentual_participacao'].min()}, "
          f"max {tabela['percentual_participacao'].max()}, "
          f"nulos {tabela['percentual_participacao'].isna().sum()}")

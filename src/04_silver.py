# Camada Silver: le a execucao mais recente da Bronze, padroniza, traduz os
# codigos pelo dicionario, integra resultados com metas e checa as regras de
# qualidade. Todas as regras rodam ANTES de gravar: se uma bloqueante falhar,
# nada da Silver e publicado. A Silver e sobrescrita a cada execucao porque
# pode ser recriada a partir da Bronze, que guarda o historico.

import os
import time

import pandas as pd

from config import STORAGE_OPTIONS
from utils_pipeline import checar_regra, registrar_execucao

inicio = time.time()


def ler_bronze(tabela):
    df = pd.read_parquet(f"abfs://bronze/{tabela}", storage_options=STORAGE_OPTIONS, partitioning=None)
    return df[df["id_execucao"] == df["id_execucao"].max()].reset_index(drop=True)


alunos = ler_bronze("alunos")
municipio = ler_bronze("municipio")
uf = ler_bronze("uf")
meta_brasil = ler_bronze("meta_alfabetizacao_brasil")
meta_uf = ler_bronze("meta_alfabetizacao_uf")
meta_municipio = ler_bronze("meta_alfabetizacao_municipio")
dicionario = ler_bronze("dicionario")
linhas_entrada = {"alunos": len(alunos), "municipio": len(municipio), "uf": len(uf)}
print("Bronze lida\n")

# teste de falha: com SIMULAR_FALHA=1 a rede da meta municipal chega em
# maiusculas, imitando a fonte mudando o formato de uma chave
if os.getenv("SIMULAR_FALHA") == "1":
    print("ATENCAO: simulando falha (rede da meta municipal em maiusculas)\n")
    meta_municipio["rede"] = meta_municipio["rede"].str.upper()

# ---------------------------------------------------------------------------
# 1. Traduzir os codigos pelo dicionario
# codigo que nao existe no dicionario vira "desconhecido" (nunca um valor valido)
# ---------------------------------------------------------------------------

def traduzir(df, tabela, coluna):
    filtro = (dicionario["id_tabela"] == tabela) & (dicionario["nome_coluna"] == coluna)
    mapa = dicionario[filtro].set_index("chave")["valor"]
    return df[coluna].map(mapa).fillna("desconhecido")


for coluna in ["rede", "serie", "presenca", "preenchimento_caderno", "alfabetizado"]:
    alunos[f"{coluna}_nome"] = traduzir(alunos, "alunos", coluna)
for coluna in ["rede", "serie"]:
    municipio[f"{coluna}_nome"] = traduzir(municipio, "municipio", coluna)
    uf[f"{coluna}_nome"] = traduzir(uf, "uf", coluna)

# as metas escrevem a rede por extenso. A correspondencia foi provada
# comparando as taxas (ver src/02_investigar_metas.py)
REDE_DAS_METAS = {"Pública": "5", "Municipal": "3"}
for meta in [meta_brasil, meta_uf, meta_municipio]:
    meta["rede"] = meta["rede"].map(REDE_DAS_METAS).fillna("desconhecido")

# ---------------------------------------------------------------------------
# 2. Padronizar tipos e chaves
# ---------------------------------------------------------------------------

# 0/1 viram inteiros para facilitar as contas na Gold. Nulos continuam nulos.
for coluna in ["presenca", "preenchimento_caderno", "alfabetizado"]:
    alunos[coluna] = alunos[coluna].astype("Int64")

# os 2 primeiros digitos do municipio sao o codigo IBGE da UF
CODIGO_IBGE_UF = {
    "11": "RO", "12": "AC", "13": "AM", "14": "RR", "15": "PA", "16": "AP", "17": "TO",
    "21": "MA", "22": "PI", "23": "CE", "24": "RN", "25": "PB", "26": "PE", "27": "AL",
    "28": "SE", "29": "BA", "31": "MG", "32": "ES", "33": "RJ", "35": "SP", "41": "PR",
    "42": "SC", "43": "RS", "50": "MS", "51": "MT", "52": "GO", "53": "DF",
}
for df in [alunos, municipio, meta_municipio]:
    df["sigla_uf"] = df["id_municipio"].str[:2].map(CODIGO_IBGE_UF)

# as metas vem com uma coluna por ano (meta_alfabetizacao_2024 ... 2030).
# Aqui viram linhas: (ano_meta, meta), mais faceis de comparar com o resultado
colunas_meta = [f"meta_alfabetizacao_{ano}" for ano in range(2024, 2031)]


def desempilhar_metas(meta, chaves):
    longo = meta.melt(id_vars=chaves, value_vars=colunas_meta, var_name="ano_meta", value_name="meta")
    longo["ano_meta"] = longo["ano_meta"].str[-4:].astype(int)
    return longo


meta_brasil_longo = desempilhar_metas(meta_brasil, ["ano", "rede", "taxa_alfabetizacao", "percentual_participacao"])
meta_uf_longo = desempilhar_metas(meta_uf, ["ano", "sigla_uf", "rede", "taxa_alfabetizacao", "percentual_participacao"])
meta_municipio_longo = desempilhar_metas(meta_municipio, ["ano", "id_municipio", "sigla_uf", "rede", "taxa_alfabetizacao",
                                                          "nivel_alfabetizacao", "percentual_participacao"])

# ---------------------------------------------------------------------------
# 3. Integrar resultado com meta (mesmo ano, mesmo lugar, mesma rede)
# a base e a meta: cada linha de meta procura o resultado correspondente.
# So entram os anos que tem resultado publicado (a meta tem 2025, o resultado nao)
# ---------------------------------------------------------------------------

colunas_resultado = ["taxa_alfabetizacao", "media_portugues"]

meta_mun_com_resultado = meta_municipio[meta_municipio["ano"].isin(municipio["ano"].unique())]
municipio_meta = meta_mun_com_resultado.merge(
    municipio[["ano", "id_municipio", "rede"] + colunas_resultado],
    on=["ano", "id_municipio", "rede"], how="left", suffixes=("_tabela_meta", "_resultado"),
)

meta_uf_com_resultado = meta_uf[meta_uf["ano"].isin(uf["ano"].unique())]
uf_meta = meta_uf_com_resultado.merge(
    uf[["ano", "sigla_uf", "rede"] + colunas_resultado],
    on=["ano", "sigla_uf", "rede"], how="left", suffixes=("_tabela_meta", "_resultado"),
)

# ---------------------------------------------------------------------------
# 4. Regras de qualidade (bloqueantes param o script antes de gravar)
# ---------------------------------------------------------------------------
print("Regras de qualidade:")

# chaves sem nulo
for nome, df, chaves in [("alunos", alunos, ["ano", "id_municipio", "rede"]),
                         ("municipio", municipio, ["ano", "id_municipio", "rede"]),
                         ("uf", uf, ["ano", "sigla_uf", "rede"])]:
    nulos = df[chaves].isna().sum().sum()
    checar_regra(f"{nome}: chaves sem nulo", "bloqueante", nulos == 0, f"{nulos} nulos")

# duplicidade
duplicados = municipio.duplicated(["ano", "id_municipio", "serie", "rede"]).sum()
checar_regra("municipio: sem duplicidade de chave", "bloqueante", duplicados == 0, f"{duplicados} duplicados")
duplicados = uf.duplicated(["ano", "sigla_uf", "serie", "rede"]).sum()
checar_regra("uf: sem duplicidade de chave", "bloqueante", duplicados == 0, f"{duplicados} duplicados")
duplicados = alunos.duplicated(["ano", "id_aluno"]).sum()
checar_regra("alunos: sem aluno repetido no mesmo ano", "bloqueante", duplicados == 0, f"{duplicados} duplicados")

# todo codigo foi traduzido (nenhum "desconhecido")
for nome, df in [("alunos", alunos), ("municipio", municipio), ("uf", uf)]:
    desconhecidos = (df.filter(like="_nome") == "desconhecido").sum().sum()
    checar_regra(f"{nome}: codigos todos no dicionario", "bloqueante", desconhecidos == 0, f"{desconhecidos} desconhecidos")
for nome, df in [("meta_brasil", meta_brasil), ("meta_uf", meta_uf), ("meta_municipio", meta_municipio)]:
    desconhecidos = (df["rede"] == "desconhecido").sum()
    checar_regra(f"{nome}: rede traduzida para codigo", "bloqueante", desconhecidos == 0, f"{desconhecidos} desconhecidos")

# chave de municipio no formato certo e com UF identificada
sem_uf = alunos["sigla_uf"].isna().sum() + municipio["sigla_uf"].isna().sum()
checar_regra("id_municipio com codigo de UF valido", "bloqueante", sem_uf == 0, f"{sem_uf} sem UF")

# chave de relacionamento: municipios dos alunos existem na tabela municipio
mun_alunos = pd.Series(alunos["id_municipio"].unique())
taxa = mun_alunos.isin(municipio["id_municipio"]).mean() * 100
checar_regra("alunos -> municipio: municipios encontrados >= 99%", "bloqueante", taxa >= 99,
             f"{taxa:.2f}% de {len(mun_alunos)} municipios")

# taxa de correspondencia dos joins (obrigatoria no PDF)
taxa = municipio_meta["taxa_alfabetizacao_resultado"].notna().mean() * 100
checar_regra("join meta_municipio x municipio >= 95%", "bloqueante", taxa >= 95,
             f"{taxa:.2f}% de {len(municipio_meta)} linhas")
taxa = uf_meta["taxa_alfabetizacao_resultado"].notna().mean() * 100
checar_regra("join meta_uf x uf >= 85%", "bloqueante", taxa >= 85, f"{taxa:.2f}% de {len(uf_meta)} linhas")

# consistencia entre tabelas: a taxa que vem na meta deve bater com a do resultado
diferenca = (municipio_meta["taxa_alfabetizacao_tabela_meta"] - municipio_meta["taxa_alfabetizacao_resultado"]).abs().mean()
checar_regra("taxa da meta municipal = taxa do resultado (dif. media < 0,1)", "informativa", diferenca < 0.1,
             f"diferenca media {diferenca:.4f}")

# informativas: lacunas da propria fonte, registradas mas sem parar a pipeline
for ano in sorted(uf["ano"].unique()):
    faltando = 27 - uf[(uf["ano"] == ano) & (uf["rede"] == "5")]["sigla_uf"].nunique()
    checar_regra(f"uf {ano}: 27 UFs na rede publica", "informativa", faltando == 0, f"{faltando} UFs ausentes")

sem_nivel = meta_municipio["nivel_alfabetizacao"].isna().sum()
checar_regra("meta_municipio: nivel_alfabetizacao preenchido", "informativa", sem_nivel == 0, f"{sem_nivel} nulos")

privada = (alunos["rede"] == "4").sum()
checar_regra("alunos: sem rede privada (fora do indicador)", "informativa", privada == 0, f"{privada} alunos da rede privada")

presentes = alunos[alunos["presenca"] == 1]
sem_nota = presentes["proficiencia"].isna().mean() * 100
checar_regra("alunos presentes com proficiencia preenchida", "informativa", sem_nota == 0,
             f"{sem_nota:.2f}% dos presentes sem nota")

# ---------------------------------------------------------------------------
# 5. Gravar a Silver (so chega aqui se nenhuma regra bloqueante falhou)
# ---------------------------------------------------------------------------
tabelas_silver = {
    "alunos": alunos,
    "municipio": municipio,
    "uf": uf,
    "meta_brasil": meta_brasil_longo,
    "meta_uf": meta_uf_longo,
    "meta_municipio": meta_municipio_longo,
    "municipio_meta": municipio_meta,
    "uf_meta": uf_meta,
}
duracao = round(time.time() - inicio, 1)
print("\nGravando a Silver:")
for nome, df in tabelas_silver.items():
    df.to_parquet(f"abfs://silver/{nome}/{nome}.parquet", index=False, storage_options=STORAGE_OPTIONS)
    registrar_execucao("silver", nome, linhas_entrada.get(nome, len(df)), len(df), duracao, "sucesso")
    print(f"  {nome:<16} {len(df):>9} linhas")

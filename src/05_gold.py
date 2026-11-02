# Camada Gold: as tabelas prontas para analise e para o modelo da Fase 3.
# Gera o indicador por municipio, por UF e Brasil comparado com a meta, a
# evolucao ano a ano, a validacao cruzada com o microdado e a base da Fase 3.
#
# O resultado de cada ano e comparado com a meta daquele ano, publicada no
# mesmo ano. Como as metas comecam em 2024, 2023 fica "sem meta". A distancia
# ate 2030 usa a meta pactuada da fonte. Os 100% aparecem em outra coluna,
# como cenario.

import time

import pandas as pd

from config import STORAGE_OPTIONS
from utils_pipeline import checar_regra, classificar, registrar_execucao

inicio = time.time()


def ler_silver(tabela, colunas=None):
    return pd.read_parquet(f"abfs://silver/{tabela}/{tabela}.parquet", columns=colunas, storage_options=STORAGE_OPTIONS)


# da tabela de alunos, so as colunas usadas aqui (le menos e ocupa menos memoria)
alunos = ler_silver("alunos", ["ano", "id_municipio", "sigla_uf", "id_escola", "id_aluno", "rede",
                               "presenca", "alfabetizado", "peso_aluno"])
municipio = ler_silver("municipio")
uf = ler_silver("uf")
meta_brasil = ler_silver("meta_brasil")
meta_uf = ler_silver("meta_uf")
meta_municipio = ler_silver("meta_municipio")
print("Silver lida\n")


def metas_da_edicao(meta, chaves):
    """Meta do proprio ano e meta de 2030, ambas da edicao do ano avaliado."""
    do_ano = meta[meta["ano_meta"] == meta["ano"]][chaves + ["meta"]].rename(columns={"meta": "meta_do_ano"})
    de_2030 = meta[meta["ano_meta"] == 2030][chaves + ["meta"]].rename(columns={"meta": "meta_2030"})
    return do_ano.merge(de_2030, on=chaves, how="outer")


# ---------------------------------------------------------------------------
# 1. Indicador por municipio
# ---------------------------------------------------------------------------
chaves_mun = ["ano", "id_municipio", "rede"]
indicador_municipio = municipio[chaves_mun + ["sigla_uf", "rede_nome", "taxa_alfabetizacao"]].merge(
    metas_da_edicao(meta_municipio, chaves_mun), on=chaves_mun, how="left")
indicador_municipio["situacao_meta"] = indicador_municipio.apply(classificar, axis=1)
indicador_municipio["distancia_meta_2030"] = indicador_municipio["meta_2030"] - indicador_municipio["taxa_alfabetizacao"]
# o streaming acrescenta linhas "simulado" nesta tabela e a coluna separa os dois
indicador_municipio["origem"] = "oficial"
indicador_municipio["id_evento"] = None

# o batch reescreve a tabela inteira, entao as linhas que vieram do
# streaming (origem = "simulado") sao lidas da versao anterior e mantidas
try:
    anterior = pd.read_parquet("abfs://gold/indicador_municipio/indicador_municipio.parquet",
                               storage_options=STORAGE_OPTIONS)
    simulados = anterior[anterior["origem"] == "simulado"]
    indicador_municipio = pd.concat([indicador_municipio, simulados], ignore_index=True)
except FileNotFoundError:
    pass

# ---------------------------------------------------------------------------
# 2. Indicador por UF e Brasil (a taxa vem da tabela de meta, que inclui 2025)
# ---------------------------------------------------------------------------
taxa_uf = meta_uf[meta_uf["ano_meta"] == 2030][["ano", "sigla_uf", "rede", "taxa_alfabetizacao"]]
indicador_uf = taxa_uf.merge(metas_da_edicao(meta_uf, ["ano", "sigla_uf", "rede"]),
                             on=["ano", "sigla_uf", "rede"], how="left")
indicador_uf = indicador_uf.rename(columns={"sigla_uf": "local"})
indicador_uf["nivel"] = "UF"

taxa_br = meta_brasil[meta_brasil["ano_meta"] == 2030][["ano", "rede", "taxa_alfabetizacao"]]
indicador_br = taxa_br.merge(metas_da_edicao(meta_brasil, ["ano", "rede"]), on=["ano", "rede"], how="left")
indicador_br["local"] = "BR"
indicador_br["nivel"] = "Brasil"

indicador_uf_brasil = pd.concat([indicador_br, indicador_uf], ignore_index=True)
indicador_uf_brasil["situacao_meta"] = indicador_uf_brasil.apply(classificar, axis=1)
indicador_uf_brasil["distancia_meta_2030"] = indicador_uf_brasil["meta_2030"] - indicador_uf_brasil["taxa_alfabetizacao"]
indicador_uf_brasil["distancia_universalizacao_cenario"] = 100 - indicador_uf_brasil["taxa_alfabetizacao"]

# ---------------------------------------------------------------------------
# 3. Evolucao temporal (rede publica = codigo 5, que existe em todos os anos)
# ---------------------------------------------------------------------------
evolucao_mun = municipio[municipio["rede"] == "5"][["ano", "id_municipio", "taxa_alfabetizacao"]]
evolucao_mun = evolucao_mun.rename(columns={"id_municipio": "local"})
evolucao_mun["nivel"] = "Municipio"
evolucao = pd.concat([indicador_uf_brasil[["nivel", "local", "ano", "taxa_alfabetizacao"]], evolucao_mun],
                     ignore_index=True).sort_values(["nivel", "local", "ano"])
evolucao["variacao_pp"] = evolucao.groupby(["nivel", "local"])["taxa_alfabetizacao"].diff()

# ---------------------------------------------------------------------------
# 4. Validacao cruzada: recalcular a taxa a partir dos microdados
# denominador = alunos presentes, ponderados pelo peso amostral
# ---------------------------------------------------------------------------
presentes = alunos[(alunos["presenca"] == 1) & alunos["alfabetizado"].notna() & alunos["peso_aluno"].notna()].copy()
presentes["peso_alfabetizado"] = presentes["peso_aluno"] * presentes["alfabetizado"]


def taxa_ponderada(df, chaves):
    soma = df.groupby(chaves)[["peso_alfabetizado", "peso_aluno"]].sum().reset_index()
    soma["taxa_microdado"] = soma["peso_alfabetizado"] / soma["peso_aluno"] * 100
    return soma.drop(columns=["peso_alfabetizado", "peso_aluno"])


publica = presentes[presentes["rede"].isin(["2", "3"])]

micro_uf = pd.concat([
    taxa_ponderada(presentes[presentes["rede"].isin(["2", "3"])], ["ano", "sigla_uf", "rede"]),
    taxa_ponderada(publica, ["ano", "sigla_uf"]).assign(rede="5"),
])
micro_uf = micro_uf.merge(uf[["ano", "sigla_uf", "rede", "taxa_alfabetizacao"]], on=["ano", "sigla_uf", "rede"], how="left")
micro_uf["local"] = micro_uf["sigla_uf"]
micro_uf["nivel"] = "UF"

micro_br = taxa_ponderada(publica, ["ano"]).assign(rede="5", local="BR", nivel="Brasil")
micro_br = micro_br.merge(taxa_br[["ano", "taxa_alfabetizacao"]], on="ano", how="left")

validacao_cruzada = pd.concat([micro_br, micro_uf.drop(columns=["sigla_uf"])], ignore_index=True)
validacao_cruzada = validacao_cruzada.rename(columns={"taxa_alfabetizacao": "taxa_oficial"})
validacao_cruzada["diferenca_pp"] = validacao_cruzada["taxa_microdado"] - validacao_cruzada["taxa_oficial"]

print("Validacao cruzada - Brasil, rede publica:")
print(micro_br[["ano", "taxa_microdado", "taxa_alfabetizacao"]].round(2).to_string(index=False))
print()

# ---------------------------------------------------------------------------
# 5. Base para a Fase 3 (uma linha por municipio x ano x rede)
# alvo: taxa_alfabetizacao. Ficam fora as colunas calculadas a partir das
# notas do mesmo ano (media_portugues, proporcao_aluno_nivel_*), a taxa que
# vem na tabela de meta (e o proprio alvo) e o nivel_alfabetizacao (origem
# desconhecida, pode derivar da taxa). Features so com informacao que ja
# existia antes do resultado: participacao, tamanho e o ano anterior.
# ---------------------------------------------------------------------------


def perfil_participacao(df, chaves):
    perfil = df.groupby(chaves).agg(
        alunos_previstos=("id_aluno", "count"),
        escolas=("id_escola", "nunique"),
        percentual_presenca=("presenca", "mean"),
    ).reset_index()
    perfil["percentual_presenca"] = perfil["percentual_presenca"] * 100
    return perfil


perfil = pd.concat([
    perfil_participacao(alunos[alunos["rede"].isin(["2", "3"])], ["ano", "id_municipio", "rede"]),
    perfil_participacao(alunos[alunos["rede"].isin(["2", "3"])], ["ano", "id_municipio"]).assign(rede="5"),
])

base_fase3 = municipio[municipio["rede"].isin(["2", "3", "5"])][
    ["ano", "id_municipio", "sigla_uf", "rede", "taxa_alfabetizacao", "media_portugues"]].copy()
base_fase3 = base_fase3.merge(perfil, on=["ano", "id_municipio", "rede"], how="left")

# resultado do ano anterior: e passado, entao pode ser usado como feature
ano_anterior = base_fase3[["ano", "id_municipio", "rede", "taxa_alfabetizacao", "media_portugues"]].copy()
ano_anterior["ano"] = ano_anterior["ano"] + 1
ano_anterior = ano_anterior.rename(columns={"taxa_alfabetizacao": "taxa_ano_anterior",
                                            "media_portugues": "media_portugues_ano_anterior"})
base_fase3 = base_fase3.drop(columns=["media_portugues"]).merge(ano_anterior, on=["ano", "id_municipio", "rede"], how="left")

# ---------------------------------------------------------------------------
# 6. Regras de qualidade da Gold
# ---------------------------------------------------------------------------
print("Regras de qualidade:")

colunas_proibidas = ["media_portugues", "nivel_alfabetizacao"] + [c for c in base_fase3.columns if c.startswith("proporcao_aluno_nivel")]
vazando = [c for c in colunas_proibidas if c in base_fase3.columns]
checar_regra("base_fase3: sem colunas derivadas do alvo", "bloqueante", len(vazando) == 0, f"colunas: {vazando}")

for nome, df in [("indicador_municipio", indicador_municipio), ("indicador_uf_brasil", indicador_uf_brasil)]:
    estados = set(df["situacao_meta"].unique())
    invalidos = estados - {"atingiu", "nao atingiu", "sem meta", "sem resultado"}
    checar_regra(f"{nome}: situacao_meta so com estados previstos", "bloqueante", len(invalidos) == 0, f"{sorted(estados)}")

duplicados = base_fase3.duplicated(["ano", "id_municipio", "rede"]).sum()
checar_regra("base_fase3: uma linha por municipio x ano x rede", "bloqueante", duplicados == 0, f"{duplicados} duplicados")

dif_brasil = validacao_cruzada[validacao_cruzada["nivel"] == "Brasil"]["diferenca_pp"].abs().max()
checar_regra("validacao cruzada Brasil: microdado x oficial ate 1 pp", "informativa", dif_brasil <= 1,
             f"maior diferenca {dif_brasil:.2f} pp")
dif_uf = validacao_cruzada[validacao_cruzada["nivel"] == "UF"]["diferenca_pp"].abs()
checar_regra("validacao cruzada UF: diferenca media ate 1 pp", "informativa", dif_uf.mean() <= 1,
             f"media {dif_uf.mean():.2f} pp, maxima {dif_uf.max():.2f} pp")

# taxa de correspondencia de cada juncao da Gold
oficial_municipal = indicador_municipio[(indicador_municipio["origem"] == "oficial") & (indicador_municipio["rede"] == "3")]
taxa = oficial_municipal["meta_2030"].notna().mean() * 100
checar_regra("join municipio x meta municipal (rede municipal) >= 95%", "bloqueante", taxa >= 95, f"{taxa:.2f}% casaram")

taxa = indicador_uf_brasil["meta_2030"].notna().mean() * 100
checar_regra("join taxa x metas (UF e Brasil) >= 95%", "bloqueante", taxa >= 95, f"{taxa:.2f}% casaram")

taxa = validacao_cruzada["taxa_oficial"].notna().mean() * 100
checar_regra("join microdado x taxa oficial >= 85%", "bloqueante", taxa >= 85, f"{taxa:.2f}% casaram")

taxa = base_fase3["alunos_previstos"].notna().mean() * 100
checar_regra("join municipio x perfil dos alunos >= 95%", "bloqueante", taxa >= 95, f"{taxa:.2f}% casaram")

# o ano anterior so existe para 2024 em diante, entao a taxa e medida so nesses anos
com_anterior = base_fase3[base_fase3["ano"] > base_fase3["ano"].min()]
taxa = com_anterior["taxa_ano_anterior"].notna().mean() * 100
checar_regra("join base_fase3 x ano anterior (anos com historico)", "informativa", taxa >= 90, f"{taxa:.2f}% casaram")

# ---------------------------------------------------------------------------
# 7. Gravar a Gold
# ---------------------------------------------------------------------------
tabelas_gold = {
    "indicador_municipio": indicador_municipio,
    "indicador_uf_brasil": indicador_uf_brasil,
    "evolucao_temporal": evolucao,
    "validacao_cruzada": validacao_cruzada,
    "base_fase3": base_fase3,
}
duracao = round(time.time() - inicio, 1)
print("\nGravando a Gold:")
for nome, df in tabelas_gold.items():
    df.to_parquet(f"abfs://gold/{nome}/{nome}.parquet", index=False, storage_options=STORAGE_OPTIONS)
    registrar_execucao("gold", nome, len(df), len(df), duracao, "sucesso")
    print(f"  {nome:<22} {len(df):>7} linhas")

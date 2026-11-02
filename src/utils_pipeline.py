# Funcoes usadas por todas as etapas da pipeline: registrar cada execucao
# e checar as regras de qualidade. Os dois registros ficam no container ops
# da Azure e acumulam as execucoes, uma linha nova a cada chamada.

from datetime import datetime

import pandas as pd

from config import STORAGE_OPTIONS


def acrescentar_linha(arquivo, linha):
    """Le o CSV do container ops, junta a linha nova e grava de volta."""
    caminho = f"abfs://ops/{arquivo}"
    try:
        historico = pd.read_csv(caminho, storage_options=STORAGE_OPTIONS)
    except FileNotFoundError:
        historico = pd.DataFrame()

    historico = pd.concat([historico, pd.DataFrame([linha])], ignore_index=True)
    historico.to_csv(caminho, index=False, storage_options=STORAGE_OPTIONS)


def registrar_execucao(etapa, tabela, linhas_entrada, linhas_saida, duracao, status,
                       bytes_processados=None, detalhe=""):
    """Uma linha em ops/execucoes.csv para cada tabela processada."""
    acrescentar_linha("execucoes.csv", {
        "data_hora": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "etapa": etapa,
        "tabela": tabela,
        "linhas_entrada": linhas_entrada,
        "linhas_saida": linhas_saida,
        "duracao_segundos": duracao,
        "status": status,
        "bytes_processados": bytes_processados,
        "detalhe": detalhe,
    })


def checar_regra(nome, tipo, passou, detalhe=""):
    """Registra a regra em ops/qualidade.csv.

    tipo: "bloqueante" ou "informativa". Se uma regra bloqueante falhar,
    o script para aqui, antes de gravar a proxima camada.
    """
    acrescentar_linha("qualidade.csv", {
        "data_hora": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "regra": nome,
        "tipo": tipo,
        "passou": bool(passou),
        "detalhe": detalhe,
    })
    print(f"[{tipo}] {nome}: {'OK' if passou else 'FALHOU'} ({detalhe})")

    if tipo == "bloqueante" and not passou:
        raise Exception(f"Regra bloqueante falhou: {nome} ({detalhe})")

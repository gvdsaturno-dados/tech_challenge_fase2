# Roda as etapas batch em ordem. Se uma etapa quebrar, a falha e registrada
# em ops/execucoes.csv e o erro continua subindo: as etapas seguintes nao
# rodam e quem chamou (o terminal ou a Azure Function) fica sabendo que falhou.
# A extracao (03_extrair_bronze.py) depende do login do Google e roda no
# computador quando a fonte e atualizada: python src/rodar_pipeline.py extracao

import os
import subprocess
import sys
import time

from utils_pipeline import registrar_execucao

PASTA = os.path.dirname(os.path.abspath(__file__))
ETAPAS = ["04_silver.py", "05_gold.py"]


def rodar(etapas=ETAPAS):
    for script in etapas:
        inicio = time.time()
        print(f"\n=== {script} ===")
        # cada etapa roda como um programa separado: quando termina, a memoria
        # que ela usou e devolvida antes da proxima comecar
        # PYTHONPATH: o programa novo precisa saber onde estao as bibliotecas
        # (na Azure elas ficam numa pasta que so o processo principal conhece)
        ambiente = dict(os.environ, PYTHONPATH=os.pathsep.join(sys.path))
        resultado = subprocess.run([sys.executable, os.path.join(PASTA, script)],
                                   capture_output=True, text=True, env=ambiente)
        print(resultado.stdout)
        if resultado.returncode != 0:
            erro = resultado.stderr.strip().splitlines()[-1] if resultado.stderr.strip() else f"codigo {resultado.returncode}"
            registrar_execucao(script.replace(".py", ""), "-", None, None,
                               round(time.time() - inicio, 1), "falha", detalhe=erro)
            raise Exception(f"Etapa {script} falhou: {erro}")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "extracao":
        rodar(["03_extrair_bronze.py"])
    else:
        rodar()

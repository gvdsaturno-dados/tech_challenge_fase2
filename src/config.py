# Le as configuracoes do arquivo .env. Os outros scripts importam daqui,
# assim o ID do projeto fica escrito em um lugar so.

import os
from dotenv import load_dotenv

load_dotenv()

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")

# dataset publico da Base dos Dados (nome fixo da fonte, nao e configuracao)
DATASET_FONTE = "basedosdados.br_inep_avaliacao_alfabetizacao"

# Le as configuracoes do arquivo .env. Os outros scripts importam daqui,
# assim o ID do projeto fica escrito em um lugar so.

import os
from dotenv import load_dotenv

load_dotenv()

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")

# conta de armazenamento ADLS Gen2 da Azure (camadas bronze, silver, gold e ops)
AZURE_STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT")
AZURE_STORAGE_KEY = os.getenv("AZURE_STORAGE_KEY")

# o pandas usa isso para ler e gravar caminhos "abfs://container/arquivo"
STORAGE_OPTIONS = {"account_name": AZURE_STORAGE_ACCOUNT, "account_key": AZURE_STORAGE_KEY}

# dataset publico da Base dos Dados (nome fixo da fonte, nao e configuracao)
DATASET_FONTE = "basedosdados.br_inep_avaliacao_alfabetizacao"

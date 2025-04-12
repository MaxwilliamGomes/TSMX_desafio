# import
import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from urllib.parse import quote


# import das minhas variáveis de ambiente

load_dotenv()

DB_HOST = os.getenv('DB_HOST_PROD')
DB_PORT = os.getenv('DB_PORT_PROD')
DB_NAME = os.getenv('DB_NAME_PROD')
DB_USER = os.getenv('DB_USER_PROD')
DB_PASS = os.getenv('DB_PASS_PROD')
DB_SCHEMA = os.getenv('DB_SCHEMA_PROD')

DB_PASS_ENCODED = quote(DB_PASS)
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS_ENCODED}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)


# Função para carregar dados do Excel
def load_excel_data(file_path):
    """Carrega os dados do arquivo Excel"""
    try:
        df = pd.read_excel(file_path)
        print(f"Dados do Excel carregados: {len(df)} linhas")
        return df
    except Exception as e:
        print(f"Erro ao carregar arquivo Excel: {e}")
        return None

# Execução principal
if __name__ == "__main__":
    path = 'C:/Users/PICHAU/Documents/TSMX/dataset/dados_importacao.xlsx'
    excel_data = load_excel_data(path)




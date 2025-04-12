import os
import pandas as pd
import numpy as np
import re
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from urllib.parse import quote

# Carrega variáveis de ambiente
load_dotenv()

DB_HOST = os.getenv('DB_HOST_PROD')
DB_PORT = os.getenv('DB_PORT_PROD')
DB_NAME = os.getenv('DB_NAME_PROD')
DB_USER = os.getenv('DB_USER_PROD')
DB_PASS = os.getenv('DB_PASS_PROD')
DB_SCHEMA = os.getenv('DB_SCHEMA_PROD')

DB_PASS_ENCODED = quote(DB_PASS)
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS_ENCODED}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Mapeamento de estados para siglas
ESTADO_MAP = {
    'Acre': 'AC', 'Alagoas': 'AL', 'Amapá': 'AP', 'Amazonas': 'AM',
    'Bahia': 'BA', 'Ceará': 'CE', 'Distrito Federal': 'DF', 'Espírito Santo': 'ES',
    'Goiás': 'GO', 'Maranhão': 'MA', 'Mato Grosso': 'MT', 'Mato Grosso do Sul': 'MS',
    'Minas Gerais': 'MG', 'Pará': 'PA', 'Paraíba': 'PB', 'Paraná': 'PR',
    'Pernambuco': 'PE', 'Piauí': 'PI', 'Rio de Janeiro': 'RJ', 'Rio Grande do Norte': 'RN',
    'Rio Grande do Sul': 'RS', 'Rondônia': 'RO', 'Roraima': 'RR', 'Santa Catarina': 'SC',
    'São Paulo': 'SP', 'Sergipe': 'SE', 'Tocantins': 'TO'
}

class ETLPipeline:
    def __init__(self, excel_path):
        self.excel_path = excel_path
        self.engine = None
        self.df = None
        self.imported_records = 0
        self.not_imported_records = []
        self.tipos_contato = {}
        self.planos = {}
        self.status_contratos = {}

    def connect_to_database(self):
        try:
            self.engine = create_engine(DATABASE_URL)
            print("Conexão com o banco de dados estabelecida com sucesso.")
            return True
        except Exception as e:
            print(f"Erro ao conectar ao banco de dados: {e}")
            return False

    def load_excel_data(self):
        try:
            self.df = pd.read_excel(self.excel_path)
            print(f"Dados do Excel carregados: {len(self.df)} linhas")
            return True
        except Exception as e:
            print(f"Erro ao carregar arquivo Excel: {e}")
            return False

    def clean_data(self):
        # Padroniza colunas
        self.df.columns = [col.strip() for col in self.df.columns]
        # Substitui NaN por None
        self.df = self.df.replace({np.nan: None})
        # Limpa CPF/CNPJ
        if 'CPF/CNPJ' in self.df.columns:
            self.df['CPF/CNPJ'] = self.df['CPF/CNPJ'].apply(
                lambda x: re.sub(r'[^\d]', '', str(x)) if x else None
            )
        # Converte datas e trata NaT -> None
        date_columns = ['Data Nasc.', 'Data Cadastro']
        for col in date_columns:
            if col in self.df.columns:
                # Converte para datetime e depois para objeto Python datetime ou None
                self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
                # Importante: substitui explicitamente NaT por None
                self.df[col] = self.df[col].where(pd.notna(self.df[col]), None)
        # Converte numéricos
        if 'Plano Valor' in self.df.columns:
            self.df['Plano Valor'] = pd.to_numeric(self.df['Plano Valor'], errors='coerce')
            self.df['Plano Valor'] = self.df['Plano Valor'].where(pd.notna(self.df['Plano Valor']), None)
        if 'Vencimento' in self.df.columns:
            self.df['Vencimento'] = pd.to_numeric(self.df['Vencimento'], errors='coerce')
            self.df['Vencimento'] = self.df['Vencimento'].where(pd.notna(self.df['Vencimento']), None)
        if 'Isento' in self.df.columns:
            self.df['Isento'] = self.df['Isento'].apply(
                lambda x: True if x == 1 or str(x).lower() == 'true' else False
            )
        # Converte nomes de estados para siglas
        if 'UF' in self.df.columns:
            self.df['UF'] = self.df['UF'].map(ESTADO_MAP)
        print("Dados limpos e preparados para importação")
        return True

    def setup_database_lookups(self):
        with self.engine.connect() as conn:
            # tipos_contato
            result = conn.execute(text("SELECT id, tipo_contato FROM tbl_tipos_contato"))
            self.tipos_contato = {}
            for id_, tipo_raw in result:
                key = re.sub(r'[^a-z]', '', tipo_raw.lower())
                self.tipos_contato[key] = id_
            for tipo in ['celular', 'telefone', 'email']:
                if tipo not in self.tipos_contato:
                    novo_id = conn.execute(
                        text("INSERT INTO tbl_tipos_contato (tipo_contato) VALUES (:tipo) RETURNING id"),
                        {"tipo": tipo.capitalize()}
                    ).scalar()
                    self.tipos_contato[tipo] = novo_id
            # planos
            result = conn.execute(text("SELECT id, descricao FROM tbl_planos"))
            self.planos = {row[1]: row[0] for row in result}
            # status_contrato
            result = conn.execute(text("SELECT id, status FROM tbl_status_contrato"))
            self.status_contratos = {row[1]: row[0] for row in result}
        print("Configuração de lookups concluída")

    def get_or_create_status(self, status):
        if not status:
            status = "Desconhecido"
        if status in self.status_contratos:
            return self.status_contratos[status]
        with self.engine.connect() as conn:
            new = conn.execute(
                text("INSERT INTO tbl_status_contrato (status) VALUES (:status) RETURNING id"),
                {"status": status}
            ).scalar()
            conn.commit()
            self.status_contratos[status] = new
            return new

    def get_or_create_plano(self, descricao, valor):
        if not descricao:
            descricao = "Plano Padrão"
        if descricao in self.planos:
            return self.planos[descricao]
        with self.engine.connect() as conn:
            new = conn.execute(
                text("INSERT INTO tbl_planos (descricao, valor) VALUES (:descricao, :valor) RETURNING id"),
                {"descricao": descricao, "valor": valor or 0}
            ).scalar()
            conn.commit()
            self.planos[descricao] = new
            return new

    def get_cliente_by_cpf_cnpj(self, cpf_cnpj):
        if not cpf_cnpj:
            return None
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT id FROM tbl_clientes WHERE cpf_cnpj = :cpf_cnpj"),
                {"cpf_cnpj": cpf_cnpj}
            ).fetchone()
            return row[0] if row else None

    def add_contato(self, cliente_id, tipo, contato):
        if not cliente_id or not contato:
            return
        tipo_id = self.tipos_contato.get(tipo)
        if not tipo_id:
            print(f"Tipo de contato desconhecido: {tipo}")
            return
        with self.engine.connect() as conn:
            exists = conn.execute(
                text("SELECT 1 FROM tbl_cliente_contatos WHERE cliente_id=:cliente_id AND tipo_contato_id=:tipo_id AND contato=:contato"),
                {"cliente_id": cliente_id, "tipo_id": tipo_id, "contato": contato}
            ).fetchone()
            if not exists:
                conn.execute(
                    text("INSERT INTO tbl_cliente_contatos (cliente_id, tipo_contato_id, contato) VALUES (:cliente_id, :tipo_id, :contato)"),
                    {"cliente_id": cliente_id, "tipo_id": tipo_id, "contato": contato}
                )
                conn.commit()

    def process_data(self):
        if self.df is None or self.df.empty:
            print("Não há dados para processar")
            return
        self.setup_database_lookups()
        for idx, row in self.df.iterrows():
            try:
                cpf_cnpj = row.get('CPF/CNPJ')
                if not cpf_cnpj:
                    raise ValueError("CPF/CNPJ ausente ou inválido")
                cliente_id = self.get_cliente_by_cpf_cnpj(cpf_cnpj)
                if not cliente_id:
                    nome = row.get('Nome/Razão Social') or row.get('Nome Razão Social')
                    if not nome:
                        raise ValueError("Nome/Razão Social ausente")
                    
                    # Garantir que data_nasc seja None se for NaT ou inválido
                    data_nasc = row.get('Data Nasc.')
                    if pd.isna(data_nasc) or data_nasc is None:
                        data_nasc = None
                    
                    # Garantir que data_cad seja uma data válida ou use o momento atual
                    data_cad = row.get('Data Cadastro')
                    if pd.isna(data_cad) or data_cad is None:
                        data_cad = datetime.now()
                    
                    with self.engine.connect() as conn:
                        cliente_id = conn.execute(
                            text("""
                                INSERT INTO tbl_clientes
                                (nome_razao_social, nome_fantasia, cpf_cnpj, data_nascimento, data_cadastro)
                                VALUES (:nome, :fantasia, :cpf_cnpj, :data_nasc, :data_cad)
                                RETURNING id
                            """),
                            {
                                "nome": nome,
                                "fantasia": row.get('Nome Fantasia'),
                                "cpf_cnpj": cpf_cnpj,
                                "data_nasc": data_nasc,
                                "data_cad": data_cad
                            }
                        ).scalar()
                        conn.commit()
                # contatos
                for campo, tipo in [('Celulares', 'celular'), ('Telefones', 'telefone'), ('Emails', 'email')]:
                    if row.get(campo):
                        for item in str(row[campo]).split(','):
                            contato_valor = item.strip()
                            if contato_valor:  # Verificar se o contato não está vazio
                                self.add_contato(cliente_id, tipo, contato_valor)
                
                # Verificar valores antes de criar contratos
                plano_valor = row.get('Plano Valor')
                if pd.isna(plano_valor):
                    plano_valor = None
                
                dia_vencimento = row.get('Vencimento')
                if pd.isna(dia_vencimento):
                    dia_vencimento = None
                
                # contratos
                plano_id = self.get_or_create_plano(row.get('Plano'), plano_valor)
                status_id = self.get_or_create_status(row.get('Status'))
                cep = row.get('CEP') or ''
                params = {
                    "cliente_id": cliente_id,
                    "plano_id": plano_id,
                    "dia_vencimento": dia_vencimento,
                    "isento": row.get('Isento'),
                    "endereco": row.get('Endereço'),
                    "numero": row.get('Número'),
                    "complemento": row.get('Complemento'),
                    "bairro": row.get('Bairro'),
                    "cep": cep,
                    "cidade": row.get('Cidade'),
                    "uf": row.get('UF'),
                    "status_id": status_id
                }
                with self.engine.connect() as conn:
                    existing = conn.execute(
                        text("SELECT id FROM tbl_cliente_contratos WHERE cliente_id=:cliente_id AND plano_id=:plano_id"),
                        {"cliente_id": cliente_id, "plano_id": plano_id}
                    ).fetchone()
                    if existing:
                        params["id"] = existing[0]
                        conn.execute(
                            text("""
                                UPDATE tbl_cliente_contratos
                                SET dia_vencimento=:dia_vencimento, isento=:isento,
                                    endereco_logradouro=:endereco, endereco_numero=:numero,
                                    endereco_complemento=:complemento, endereco_bairro=:bairro,
                                    endereco_cep=:cep, endereco_cidade=:cidade, endereco_uf=:uf,
                                    status_id=:status_id
                                WHERE id=:id
                            """), params)
                    else:
                        conn.execute(
                            text("""
                                INSERT INTO tbl_cliente_contratos
                                (cliente_id, plano_id, dia_vencimento, isento,
                                 endereco_logradouro, endereco_numero, endereco_complemento,
                                 endereco_bairro, endereco_cep, endereco_cidade, endereco_uf,
                                 status_id)
                                VALUES
                                (:cliente_id, :plano_id, :dia_vencimento, :isento,
                                 :endereco, :numero, :complemento,
                                 :bairro, :cep, :cidade, :uf,
                                 :status_id)
                            """), params)
                    conn.commit()
                self.imported_records += 1
                print(f"Linha {idx+2} importada com sucesso.")
            except Exception as e:
                self.not_imported_records.append({"row": idx+2, "reason": str(e), "data": row.to_dict()})
                print(f"Erro na linha {idx+2}: {e}")

    def generate_report(self):
        print("\n" + "="*40)
        print("RELATÓRIO DE IMPORTAÇÃO")
        print("="*40)
        total = self.imported_records + len(self.not_imported_records)
        print(f"Total: {total}, Sucesso: {self.imported_records}, Falha: {len(self.not_imported_records)}")
        if self.not_imported_records:
            print("Registros não importados:")
            for r in self.not_imported_records:
                print(f"  Linha {r['row']}: {r['reason']}")
        with open("relatorio_importacao.txt", "w", encoding="utf-8") as f:
            f.write("RELATÓRIO DE IMPORTAÇÃO\n")
            f.write(f"Total: {total}, Sucesso: {self.imported_records}, Falha: {len(self.not_imported_records)}\n")
            for r in self.not_imported_records:
                f.write(f"Linha {r['row']}: {r['reason']}\n")
        print("Relatório gerado.")

    def run(self):
        if not self.connect_to_database(): return False
        if not self.load_excel_data(): return False
        if not self.clean_data(): return False
        self.process_data()
        self.generate_report()
        return True

if __name__ == "__main__":
    excel_path = os.getenv('EXCEL_PATH', 'C:/Users/PICHAU/Documents/TSMX/dataset/dados_importacao.xlsx')
    etl = ETLPipeline(excel_path)
    result = etl.run()
    if result:
        print("Processo de ETL concluído com sucesso")
    else:
        print("Processo de ETL falhou")
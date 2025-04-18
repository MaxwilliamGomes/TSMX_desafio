# 🛠️ TSMX - Pipeline ETL

Este projeto é um pipeline de ETL (Extração, Transformação e Carga) desenvolvido em Python. Ele realiza a leitura de uma planilha Excel contendo dados de clientes e contratos e insere ou atualiza essas informações em um banco de dados PostgreSQL.

## 📌 O que esse projeto faz?

- Carrega dados de uma planilha Excel
- Realiza limpeza e padronização dos dados (datas, números, strings)
- Valida e insere informações nas tabelas:
  - `tbl_clientes`
  - `tbl_cliente_contratos`
  - `tbl_tipos_contato`, `tbl_planos`, `tbl_status_contrato`
- Gera um relatório com erros de importação, se houver

## 🧰 Como instalar o projeto

1. Clone o repositório:
   ```bash
   git clone https://github.com/MaxwilliamGomes/TSMX_desafio.git
   cd TSMX_desafio


2. Crie e ative um ambiente virtual

```bash
python -m venv .venv
# Linux/Mac
source .venv/bin/activate
# Windows
.venv\Scripts\activate
```


3. Instale as dependências: 

```bash
pip install -r requirements.txt
```

4. Crie o banco de dados e as tabelas no PostgreSQL

```
psql -U seuusuario -d seubanco -f dataset/ScriptTabelas.sql
```

5. Crie um arquivo .env com suas credenciais

```DB_HOST_PROD=localhost
DB_PORT_PROD=5432
DB_NAME_PROD=seubanco
DB_USER_PROD=seuusuario
DB_PASS_PROD=suasenha
DB_SCHEMA_PROD=public
EXCEL_PATH=C:/caminho/para/seuarquivo.xlsx
```

6. Execute o script principal

```
python src/etl_pipeline.py
```



7. Fluxo do ETL
```mermaid
flowchart TD
  subgraph EXTRAÇÃO
    A[Início] --> B[Conectar ao PostgreSQL]
    B --> B1[Extrair planilha]
  end

  subgraph TRANSFORMAÇÃO
    B1 --> C[Limpar dados]
    C --> D[Carregar tabelas de referência]
    D --> E{Cliente existe?}
    E -->|Sim| F[Atualizar contratos]
    E -->|Não| G[Criar novo cliente]
  end

  subgraph CARGA
    F --> H[Carregar dados no banco]
    G --> H
  end

  subgraph RELATÓRIO
    H --> I[Gerar relatórios da execução]
    I --> J[Encerrar processo]
  end

```



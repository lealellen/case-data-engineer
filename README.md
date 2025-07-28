# Assignment 1 

Solução desenvolvida em PySpark para processamento e agregação de dados.

---

## **Como executar o projeto no Databricks Community Edition**

### 1. Faça upload dos dados

- No menu à esquerda, clique em **New**
- Clique em **Add or Upload Data** > **Upload files to a volume** > **All** > **default**
- Crie um volume com o nome "assignment-1"
- Faça upload do arquivo **info_transportes.csv**  
  - Caminho típico: `/Volumes/workspace/default/assignment-1/info_transportes.csv`

### 3. Importe e execute o notebook

- Importe o notebook `pipeline_case_data_engineer.ipynb` para seu workspace Databricks
  - (Use o arquivo .ipynb disponível neste repositório, na pasta "assignment-1")

 
  
## Acesse o notebook no Databricks

Você pode visualizar ou clonar o notebook diretamente pela plataforma Databricks:

[Clique aqui para abrir o notebook no Databricks](https://dbc-dbfffb09-9678.cloud.databricks.com/editor/notebooks/2929827828570792?o=3068775036321311)

---

# Assignment 2

Pipeline para ingestão, validação e armazenamento de dados de sensores IoT, utilizando Kafka, Spark e PostgreSQL com deployment via Docker Compose.

![Arquitetura do Projeto](arq.jpg)

---

## Como executar o projeto

### 1. Pré-requisitos

- Docker instalado

### 2. Clone o repositório

```bash
cd assignment-2
cd scripts
```

### 3. Build e execução dos containers

```bash
docker compose up
```

### 4. Saídas

Serão gerados arquivos parquet em data/bronze e data/logs

- data/bronze: Dados brutos, salvos após o envio pelo Kafka
- data/logs: Dados com campos inválidos (Simulando dados corrompidos). 

A camada silver é salva em um banco de dados Postgre. 

Para visualizar os dados das camadas basta executar o notebook **testes.ipynb**

### 5. Observações

Após a execução local, mantive os dados gerados na pasta assignment-2/scripts (após execução) para eventuais consultas das saídas sem a necessidade de execução.

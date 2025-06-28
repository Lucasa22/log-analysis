# Log Analysis Challenge

> Solução completa para análise de logs web usando Apache Spark, respondendo às questões do desafio de engenhar5. **Visualize os Resultados:**
   
   - **Jupyter (Análises e Visualizações)**: [http://localhost:8888](http://localhost:8888) (token: easy)
   - **Airflow (Orquestração)**: [http://localhost:8080](http://localhost:8080) (user: admin / pass: admin)
   - **Spark UI (Monitoramento)**: [http://localhost:8081](http://localhost:8081) dados.

![Versão](https://img.shields.io/badge/versão-1.0.0-blue)
![Python](https://img.shields.io/badge/python-3.9+-green)
![Spark](https://img.shields.io/badge/spark-3.5.0-orange)
![Licença](https://img.shields.io/badge/licença-MIT-yellow)

---

## Sumário

- [Log Analysis Challenge](#log-analysis)
  - [Sumário](#sumário)
  - [Respostas ao Desafio](#respostas-ao-desafio)
  - [Visão Geral da Solução](#visão-geral-da-solução)
  - [Arquitetura](#arquitetura)
    - [Design Simplificado do Código](#design-simplificado-do-código)
  - [Pré-requisitos](#pré-requisitos)
  - [Instalação e Execução](#instalação-e-execução)
  - [Como Utilizar o Código para Resolver o Desafio](#como-utilizar-o-código-para-resolver-o-desafio)
  - [Estrutura do Projeto](#estrutura-do-projeto)
  - [Armazenamento Persistente](#armazenamento-persistente-seção-opcional-do-desafio)
  - [Como Executar o Pipeline Completo](#como-executar-o-pipeline-completo)
  - [Licença](#licença)

---

## Respostas ao Desafio

Esta solução responde às seis perguntas propostas no desafio de análise de logs web:

1. **10 maiores origens de acesso (Client IP)**: Implementado na função `analyze_logs()` que retorna o DataFrame `top_ips`.
2. **6 endpoints mais acessados**: Implementado na função `analyze_logs()` que retorna o DataFrame `top_endpoints`.
3. **Quantidade de Client IPs distintos**: Calculado e disponibilizado no DataFrame `summary_metrics`.
4. **Quantidade de dias nos dados**: Calculado e disponibilizado no DataFrame `summary_metrics`.
5. **Volume de dados nas respostas**:
   - Volume total: Calculado e armazenado no DataFrame `summary_metrics` como `total_data_volume_bytes`.
   - Maior, menor e média: Disponível através da análise em `notebook/explore.ipynb`.
6. **Dia da semana com mais erros do tipo HTTP Client Error**: Implementado no DataFrame `peak_error`.

Os resultados detalhados podem ser visualizados no Jupyter Notebook `notebook/explore.ipynb` após a execução da pipeline.

## Visão Geral da Solução

Esta solução implementa uma pipeline completa de engenharia de dados para processamento e análise de logs web, respondendo às questões do desafio com:

**Diferenciais:**

- **Interface simplificada** e intuitiva para processamento de logs
- Processamento distribuído e escalável com **Apache Spark**
- Análises visuais e interativas com **Jupyter Notebook**
- Arquitetura de medalhas (Bronze → Silver → Gold) para organização dos dados
- Armazenamento persistente em PostgreSQL e Parquet
- **Deploy completo** com Docker Compose

## Arquitetura

O projeto implementa a arquitetura medalhão para organizar o processamento dos logs de acesso:

**Bronze:**

- Ingestão bruta dos logs (`data/bronze/`)
- Extração de campos básicos (timestamp, IP, URL, status code)
- Formato: Parquet para processamento eficiente

**Silver:**

- Limpeza e enriquecimento dos dados (`data/silver/`)
- Conversão de tipos, parsing de timestamp, normalização de URLs
- Armazenamento em PostgreSQL para consultas ad-hoc

**Gold:**

- Métricas e relatórios prontos para análise (`data/gold/`)
- Geração das respostas às perguntas do desafio
- Visualizações e insights em notebooks Jupyter

### Design Simplificado do Código

O código foi projetado para resolver o desafio de forma:

**Componentes principais:**

```python
log_analyzer/
  etl/
    extractor.py     # Extração dos logs no formato Apache Common/Combined
    transformer.py   # Transformação e enriquecimento dos dados
    analyzer.py      # Implementação das análises solicitadas no desafio
    load.py          # Persistência em Parquet e PostgreSQL
    simple_pipeline.py # Pipeline completo: extração → transformação → análise
```

## Pré-requisitos

Para executar a solução do desafio você precisará de:

- Docker (recomendado: versão 20+)
- Docker Compose (v2+)
- 4GB+ RAM livre para containers Spark/Airflow
- (Opcional) Python 3.9+ para execuções locais/testes

## Instalação e Execução

### Opção 1: Docker (Recomendada)

1. **Clone o Repositório:**

   ```bash
   git clone https://github.com/seu-usuario/log-analysis.git
   cd log-analysis
   ```

2. **Setup Inicial:**

   ```bash
   # Executa o script de setup que baixa o driver JDBC e prepara o ambiente
   ./scripts/setup.sh
   
   # Ou use make:
   make setup
   ```

3. **Inicie os Serviços e Execute a Análise:**

   ```bash
   # Inicia os serviços e processa automaticamente os logs
   docker-compose up -d
   ```

4. **Visualize os Resultados:**
   
   * **Jupyter (Análises e Visualizações)**: [http://localhost:8888](http://localhost:8888) (token: easy)
   * **Airflow (Orquestração)**: [http://localhost:8080](http://localhost:8080) (user: admin / pass: admin)
   * **Spark UI (Monitoramento)**: [http://localhost:8081](http://localhost:8081)

5. **Para ver as respostas ao desafio:**
   
   Abra no Jupyter Notebook o arquivo `notebook/explore.ipynb` para visualizar as respostas às questões do desafio com visualizações.

## Como Utilizar o Código para Resolver o Desafio

O código foi projetado para responder diretamente às perguntas do desafio. Veja como usá-lo:

```python
# Pipeline completo para processar logs e responder às perguntas
from log_analyzer.etl import run_pipeline

result = run_pipeline(
    input_path="data/logs.txt",
    output_path="data/output",
    log_format="combined",  # Formato Apache Combined Log
    save_to_db=True  # Salvar no PostgreSQL (opcional)
)

# Ver as métricas gold geradas (respostas ao desafio)
for metric_name, df in result['gold_dfs'].items():
    print(f"\n=== {metric_name.upper()} ===")
    df.show(truncate=False)

# Respostas específicas às perguntas do desafio:
# 1. Top 10 IPs por quantidade de acessos
result['gold_dfs']['top_ips'].show(10)

# 2. Top 6 endpoints mais acessados (excluindo arquivos)
result['gold_dfs']['top_endpoints'].show(6)

# 3. Quantidade de IPs distintos
distinct_ips = result['gold_dfs']['summary'].filter("metric_name = 'distinct_client_ips'").first()['metric_value']
print(f"IPs distintos: {distinct_ips}")

# 4-5. Volume de dados e outras métricas
summary_df = result['gold_dfs']['summary']
summary_df.show()

# 6. Dia da semana com mais erros HTTP Client Error
result['gold_dfs']['peak_error'].show()
```

### Execução pela Linha de Comando

Para executar via linha de comando e ver os resultados imediatamente:

```bash
# Processa os logs e imprime os resultados no terminal
python src/log_analyzer/main.py

# Versão sem persistência no banco de dados
python src/log_analyzer/main.py --no-db
```

## Interface Jupyter para Análise do Desafio

O projeto inclui dois notebooks Jupyter para análise dos logs:

- `notebook/explore.ipynb` - **Responde às perguntas do desafio** com visualizações
- `notebook/01_run_pipeline.ipynb` - Demonstração do uso do código e validação dos resultados

## Estrutura do Projeto

```text
.
├── dags/                # DAGs do Airflow para agendamento da análise 
├── data/                # Dados brutos e processados (bronze, silver, gold)
│   └── sample/          # Dados de exemplo para testes
├── docker/              # Configurações dos serviços Docker 
├── docs/                # Documentação sobre JDBC e outros componentes
├── notebook/            # Jupyter notebooks com as respostas ao desafio
├── src/                 # Código fonte 
│   └── log_analyzer/    # Implementação da solução do desafio
├── tests/               # Testes automatizados garantindo a corretude
├── docker-compose.yml   # Configuração dos serviços (Spark, PostgreSQL, Jupyter)
├── Dockerfile           # Imagem Docker da aplicação
└── README.md            # Esta documentação
```

## Armazenamento Persistente (Seção Opcional do Desafio)

Esta solução implementa dois métodos de armazenamento persistente:

1. **Formato Parquet**:
   - Eficiente para análises em lote
   - Suporte a particionamento e compressão
   - Integração nativa com Spark

2. **PostgreSQL**:
   - Tabelas para bronze (logs brutos), silver (dados limpos) e gold (métricas)
   - Integração via JDBC
   - Suporte a consultas SQL ad-hoc
   - Modelagem relacional adequada para logs web

### Justificativa

A escolha do PostgreSQL se deve à:

- Facilidade de integração com ferramentas de BI
- Capacidade de realizar consultas complexas
- Funcionalidade para indexar e otimizar consultas frequentes
- Flexibilidade para servir APIs e dashboards em tempo real

## Como Executar o Pipeline Completo

Execute a pipeline que responde ao desafio com um único comando:

```bash
# Via Docker (recomendado)
docker-compose up -d

# Ou localmente via Python
python -m log_analyzer.cli

# Ou com a ferramenta CLI
log-analyzer medalhao
```

Por padrão, os resultados são processados em múltiplas camadas:

- Bronze: `data/bronze/logs.parquet` - Dados brutos extraídos
- Silver: `data/silver/logs.parquet` - Dados limpos e normalizados
- Gold: `data/gold/metrics.json` - Respostas às perguntas do desafio

## Licença

Este projeto está licenciado sob a Licença MIT. Veja o arquivo [`LICENSE`](LICENSE) para mais detalhes.
# Projeto AWS Spotify Data Pipeline

## Visão Geral

Este projeto implementa um pipeline de dados para processar informações do Spotify utilizando serviços AWS, como S3, Glue, Athena e QuickSight. O objetivo é extrair, transformar e carregar (ETL) dados de artistas, álbuns e faixas para análise e visualização.

## Arquitetura

A arquitetura do pipeline segue as seguintes etapas:

1. **Coleta de Dados:** Dados brutos do Spotify são armazenados no Amazon S3 (camadas Bronze, Silver, Gold).
2. **Processamento ETL:** AWS Glue extrai e transforma os dados armazenados no S3.
3. **Catálogo de Dados:** AWS Glue Crawlers catalogam os dados processados.
4. **Consulta e Análise:** Amazon Athena permite consultas SQL sobre os dados no S3.
5. **Visualização:** Amazon QuickSight é usado para criar dashboards e relatórios.

## Tecnologias Utilizadas

- **AWS S3:** Armazena os dados em diferentes camadas
- **AWS Glue:** Realiza o processamento ETL
- **AWS Glue Crawlers:** Catalogam os dados processados
- **AWS Athena:** Consulta os dados usando SQL
- **AWS QuickSight:** Visualização de dados
- **PySpark:** Processamento distribuído dos dados

## Estrutura do Repositório

```
AWS_Spotify/
|-- scripts/
|   |-- glue_job.py  # Script principal do AWS Glue
|-- data/
|-- README.md        # Documentação do projeto
```

## Descrição do Script AWS Glue

O script `glue_job.py` realiza as seguintes operações:

1. **Leitura dos dados:** Carrega os arquivos CSV do S3 para DynamicFrames.
2. **Junção de tabelas:**
   - Une artistas com álbuns pelo campo `artist_id`.
   - Une o resultado com as faixas (tracks) pelo campo `track_id`.
3. **Avaliação da Qualidade dos Dados:**
   - Verifica se a contagem de colunas é maior que zero.
4. **Escrita no Data Warehouse (S3):**
   - Os dados são gravados em formato Parquet no S3.

## Como Executar o Job

Para executar o job Glue:

1. Faça upload do script `glue_job.py` no AWS Glue.
2. Crie um job no AWS Glue e configure as permissões adequadas.
3. Execute o job pelo console da AWS ou via AWS CLI:
   
   ```bash
   aws glue start-job-run --job-name "AWS_Spotify_Job"
   ```

## Estrutura do S3

```
s3://project-spt-biss/
|-- staging/
|   |-- spotify_artist_data_2023.csv
|   |-- spotify_albums_data_2023.csv
|   |-- spotify_tracks_data_2023.csv
|-- datawarehouse/  # Dados transformados
```
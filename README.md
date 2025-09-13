# Case Stone – Medallion Orchestration (GCP)

Arquitetura proposta para ingestão e processamento do CNPJ (RFB) em 3 camadas (Bronze → Silver → Gold) usando **Cloud Workflows** + **Dataproc Serverless** e três modos de ingestão intercambiáveis:
- **Cloud Run (HTTP)** – serviço `ingest-rfb` (container) baixa os arquivos e grava em `raw/<run_id>/`.
- **Storage Transfer Service (STS)** – job gerenciado copia direto para o bucket.
- **VM** – alternativa de contingência que escreve um *marker* em GCS quando conclui.

O **Workflows** orquestra: escolhe o modo de ingestão, aguarda *markers/arquivos*, dispara lotes do Dataproc (bronze/silver/gold/load) e grava *markers* por etapa.

## Componentes

- **Bucket GCS** (`case-stone-medallion-<proj>`):  

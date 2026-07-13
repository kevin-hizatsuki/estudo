# Step Function genérica – Orquestrador de ingestão incremental

Este pacote generaliza a state machine original, removendo qualquer vínculo
com a conta AWS `568622306342` e com o projeto `EngDadosTF_prod_DataLake`.

## Arquivos

- `statemachine.generic.json` — definição da Step Function (ASL) com
  placeholders `<<...>>` no lugar de tudo que era específico da conta antiga.
- `deploy.sh` — script que substitui os placeholders e cria/atualiza a state
  machine na conta e região atuais do seu AWS CLI.

## Placeholders usados no template

| Placeholder                | O que é                                             |
|-----------------------------|------------------------------------------------------|
| `<<ACCOUNT_ID>>`            | Conta AWS (detectada automaticamente via `aws sts`)  |
| `<<REGION>>`                | Região AWS                                           |
| `<<PREFIX>>`                | Prefixo comum dos recursos (Lambda, Glue, Batch, Dynamo, state machine EMR) |
| `<<BUCKET_NAME>>`           | Bucket S3 com bootstrap/scripts do EMR               |
| `<<BATCH_JOBDEF_REVISION>>` | Revisão da Job Definition do AWS Batch                |
| `<<TAG_PROJETO>>`           | Tag "Projeto" propagada nos jobs do Batch             |
| `<<TAG_OWNER>>`             | Tag/owner usada na chamada ao EMR                     |

## Pré-requisitos na conta nova

O script só cria a Step Function. Antes de rodar, você precisa ter (ou criar)
na conta de teste:

1. **Lambda**: `<PREFIX>_Listar_tabela` — lista as tabelas incrementais a
   processar (lê da tabela DynamoDB de parâmetros).
2. **DynamoDB**: `<PREFIX>_Parametros` — parâmetros de cada tabela/ingestão.
3. **Glue Job**: `<PREFIX>_Ingestion` — job Spark de ingestão incremental.
4. **AWS Batch**:
   - Job Queue: `<PREFIX>_fila-batch`
   - Job Definition: `<PREFIX>_task-lake` (imagem/container que roda a ingestão)
5. **Outra Step Function** para orquestração via EMR:
   `<PREFIX>_OrquestracaoEMR_v1` (cria cluster EMR efêmero e roda o
   spark-submit).
6. **Bucket S3** com o bootstrap (`emr/bootstrap_emr.sh`) e os scripts/jars
   usados no `spark-submit` (delta-core jar, `engdados_helper.py`,
   `ingestion.py`).
7. **IAM Role** para a própria Step Function, com permissão para:
   `lambda:InvokeFunction`, `batch:SubmitJob`/`DescribeJobs`/`TerminateJob`,
   `glue:StartJobRun`/`GetJobRun`, `states:StartExecution`/`DescribeExecution`
   (para a state machine do EMR), e `events:PutTargets`/`PutRule`/
   `RemoveTargets` (exigido pelos padrões `.sync`).

## Como rodar

```bash
export AWS_REGION=us-east-1
export PROJECT_PREFIX=MeuProjeto_DataLake
export BUCKET_NAME=meu-bucket-datalake
export BATCH_JOBDEF_REVISION=1
export TAG_PROJETO=MeuProjeto
export TAG_OWNER=MeuTime
export STATE_MACHINE_ROLE_ARN=arn:aws:iam::<conta>:role/MinhaRoleStepFunctions

./deploy.sh
```

O script:
1. Detecta automaticamente o `ACCOUNT_ID` via `aws sts get-caller-identity`.
2. Gera `statemachine.rendered.json` com todos os placeholders substituídos.
3. Valida o JSON.
4. Cria a state machine (ou atualiza, se já existir uma com o mesmo nome).

## Observações sobre a lógica original (preservada)

- Faz o **Map** distribuído (`DISTRIBUTED`, `MaxConcurrency: 70`) sobre a
  lista de tabelas retornada pela Lambda.
- Decide a engine por tabela: `BATCH`, `EMR` ou Spark via Glue
  (`Categoria_carga`).
- No Batch, se faltar `table_name` ou `ARN_SECRETS_MANAGER`, cai para EMR.
- Em erro de capacidade do Batch, espera com jitter e tenta de novo até
  2 vezes; esgotado o limite, cai para EMR.
- Em erro de timeout/estado inválido do Batch, vai direto para EMR.
- Se a orquestração via EMR também falhar, cai como último recurso para o
  Glue Job Spark incremental.

Nenhuma dessa lógica foi alterada — só os nomes/ARNs/bucket que amarravam o
processo à conta antiga.

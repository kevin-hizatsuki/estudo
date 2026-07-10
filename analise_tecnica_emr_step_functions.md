# Análise Técnica: Orquestrador EMR via AWS Step Functions

Este documento apresenta uma análise técnica detalhada da máquina de estados do **AWS Step Functions** definida no arquivo [step_functions_emr.tf](file:///Users/kevinhizatsuki/Documents/GitHub/oncar_lake/infra/modules/main/step_functions_emr.tf). Ele descreve o fluxo de execução, as otimizações configuradas para os clusters EMR e realiza uma comparação profunda de custo e performance com o **AWS Glue**.

---

## 1. Visão Geral da Arquitetura

O Terraform implanta um orquestrador serverless baseado em Step Functions (`aws_sfn_state_machine.orchestrator`) e um grupo de logs no CloudWatch (`aws_cloudwatch_log_group.OrquestracaoEMR`) com retenção de 14 dias. 

A máquina de estados foi projetada para gerenciar o ciclo de vida completo de clusters EMR transitórios (ou reaproveitar clusters existentes), submeter jobs Spark em lote e em paralelo, e tratar falhas de infraestrutura de forma resiliente (como interrupção de instâncias Spot ou falta de IPs na VPC).

```mermaid
graph TD
    Start([Início]) --> Input[Modelo de Input]
    Input --> Defaults[Parâmetros Default]
    Defaults --> Merge[Unir Parâmetros]
    Merge --> Length[Consulta Qtd de Steps]
    Length --> ChoiceCluster{Criar Cluster?}
    
    %% Fluxo de Reaproveitamento de Cluster
    ChoiceCluster -- Não (cluster_id informado) --> StepsMap[Executar Steps em Paralelo (Map)]
    
    %% Fluxo de Criação de Cluster
    ChoiceCluster -- Sim (cluster_id nulo) --> ValidateParams{Validarprojeto/etapa/owner?}
    ValidateParams -- Inválido --> FailParams[Falha: Parâmetro Inválido]
    ValidateParams -- Válido --> ValidateOwner{Validar Owner?}
    ValidateOwner -- Inválido --> FailOwner[Falha: Owner Inválido]
    ValidateOwner -- Válido --> GetCategory[Lambda: Consultar EMRCategorias]
    
    GetCategory --> ValidateCategory{Categoria Existe?}
    ValidateCategory -- Não (404) --> FailCategory[Falha: Categoria Inexistente]
    ValidateCategory -- Sim --> ChoiceResource{Step Único?}
    
    ChoiceResource -- Sim --> EnableResource[Ativar MaximizeResourceAllocation]
    ChoiceResource -- Não --> DisableResource[Desativar MaximizeResourceAllocation]
    
    EnableResource --> CreateCluster[Criar Cluster EMR]
    DisableResource --> CreateCluster[Criar Cluster EMR]
    
    %% Criação de Cluster e Tratamento de Erros
    CreateCluster -->|Erro na Criação| CheckError{Erro de Spot, IP ou Throttling?}
    CheckError -- Sim --> WaitRetry[Esperar 30s] --> CreateCluster
    CheckError -- Não --> NotifySNS[SNS: Alertar Engenharia] --> FailCluster[Falha na Criação]
    
    CreateCluster -->|Sucesso| StepsMap
    
    %% Processamento de Steps (Map)
    subgraph MapState [Map: Executar Steps]
        StepsMap --> ChoiceArgs{Overwrite Args?}
        ChoiceArgs -- Sim --> PrepNoDefault[Preparo sem Args Default]
        ChoiceArgs -- Não --> PrepWithDefault[Preparo com Args Default]
        PrepNoDefault --> AddStep[Adicionar Step EMR]
        PrepWithDefault --> AddStep[Adicionar Step EMR]
        AddStep -->|Erro| CheckThrottling{Throttling?}
        CheckThrottling -- Sim --> WaitStep[Esperar 5s] --> AddStep
        CheckThrottling -- Não --> FailStep[Falha no Step]
    end
    
    MapState -->|Sucesso| Success([Sucesso])
    MapState -->|Erro Geral| WaitStatus[Aguardar EMR definir status real - 240s]
    
    WaitStatus --> DescribeCluster[Describe Cluster]
    DescribeCluster --> CheckFailReason{Falha por Spot ou IP?}
    CheckFailReason -- Não --> FailWorkflow[Falha do Workflow]
    CheckFailReason -- Sim --> CheckTransiente{Foi criado nesta Step?}
    CheckTransiente -- Sim --> CreateCluster
    CheckTransiente -- Não --> StepsMap
```

---

## 2. Funcionamento Detalhado da State Machine

### A. Preparação de Parâmetros e Validação
1. **Modelo de Input (Pass):** Um estado ilustrativo que documenta o esquema de entrada esperado (ex: `cluster_id`, `owner`, `projeto`, `etapa`, `categoria`, `jobs_paralelas`, `steps`).
2. **Parâmetros Default (Pass):** Define valores padrão caso o usuário não informe (como `emr_version = 6.5.0`, `categoria = leve`, `jobs_paralelas = 5`, além de subnets e security groups específicos do ambiente).
3. **Unir Parâmetros (Pass):** Utiliza a função intrínseca `States.JsonMerge` para mesclar o input do usuário com as configurações default, garantindo que o input prevaleça.
4. **Validações de Segurança e Governança:**
   - Verifica se os parâmetros obrigatórios (`projeto`, `etapa`, `owner`) foram preenchidos.
   - Valida se o `owner` pertence a um dos grupos autorizados (`EngenhariaDados`, `CienciaDados`, `Risco`, `DW`).

### B. Provisionamento Dinâmico via DynamoDB
* **Lambda (`ConsultaParametrosCategoriaEMR`):** A máquina de estados faz uma chamada síncrona a uma Lambda que consulta a tabela DynamoDB `EMRCategorias`.
* Esta tabela retorna a configuração de infraestrutura específica para a categoria de carga de trabalho (`leve`, `medio`, `pesado`, etc.), contendo:
  - **`InstanceFleets`:** A definição dos tipos de instância EC2 para Master, Core e Task nodes.
  - **`ManagedScalingPolicy`:** Regras para escalabilidade automática do cluster.

### C. Alocação Inteligente de Recursos do Spark
* **Maximize Resource Allocation:** O estado `Step único?` analisa se haverá execução paralela.
  - Se houver apenas **1 step** configurado no pipeline, a propriedade `maximizeResourceAllocation` é definida como `true`, fazendo com que o Spark aloque automaticamente 100% dos recursos de CPU e memória de cada nó para aquele executor único.
  - Se houver **múltiplos steps paralelos**, essa propriedade é desativada (`false`), permitindo que o YARN faça o compartilhamento granular de recursos entre os múltiplos jobs concorrentes (definido por `StepConcurrencyLevel`).

### D. Criação do Cluster EMR (`elasticmapreduce:createCluster.sync`)
O cluster EMR é criado com configurações altamente otimizadas:
* **AutoTerminationPolicy:** Configurado com `IdleTimeout = 240` segundos (4 minutos). Se o cluster ficar ocioso após processar os steps, ele é deletado automaticamente para evitar custos residuais.
* **Integração com AWS Glue Data Catalog:** A configuração em `spark-hive-site` aponta o Spark diretamente para o catálogo do Glue:
  ```json
  "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
  ```
  Isso permite consultar tabelas catalogadas diretamente do Spark EMR sem precisar de um Metastore externo (como MySQL/Hive).
* **Otimização do EMRFS (S3 Connection Pooling):** O arquivo de configuração `emrfs-site` expande as conexões máximas do S3 e as threads (ex: `fs.s3.connection.maximum = 6000`, `fs.s3.threads.max = 5000`) para evitar erros de `HTTP 503 Slow Down / Throttling` ao ler/escrever grandes conjuntos de dados Delta Lake concorrentemente.
* **Bootstrap Actions:** Executa um script de inicialização (`bootstrap_emr.sh`) para carregar pacotes Python adicionais em todos os nós do cluster.

### E. Execução de Steps via Map State (`Map`)
* O orquestrador itera sobre a lista de `steps` em paralelo (`MaxConcurrency: 500`).
* **Argumentos Padrão:** Cada job Spark executa um `spark-submit` configurado com:
  - `--deploy-mode=cluster` (o driver Spark roda no próprio cluster EMR, e não na máquina de estados).
  - Drivers JDBC do SQL Server (`sqljdbc42.jar`) e jars do Delta Lake (`delta-core_2.12-1.0.0.jar`).
* **ActionOnFailure:** Configurado como `CONTINUE`. Se um step individual falhar, os demais continuam rodando, e a Step Functions tratará o erro ao final do loop.

### F. Resiliência e Auto-recuperação (Tratamento de Falhas)
1. **Falha na criação do cluster:** Captura erros de API Throttling ou falta de instâncias Spot / IPs na VPC. Nesses casos, aguarda 30 segundos e tenta criar novamente. Se falhar por outro motivo, dispara um alerta SNS para o time de dados.
2. **Interrupção de Spot durante a execução:**
   - Se o cluster cair no meio da execução devido à perda de instâncias Spot (erro clássico em ambientes de baixo custo) ou falta de IPs, o orquestrador entra em estado de espera de **240 segundos** para que o EMR finalize e consolide o status do cluster.
   - Em seguida, executa um `describeCluster` no SDK da AWS para identificar o motivo da falha.
   - Caso seja confirmada a interrupção por Spot/IP, e o cluster tenha sido criado de forma transiente dentro desta execução, **o workflow volta automaticamente para o estado "Criar Cluster"**, criando um novo cluster limpo para reprocessar os steps pendentes.

---

## 3. Análise Comparativa: EMR Orchestrator vs. AWS Glue

Abaixo, detalhamos os fatores que influenciam na escolha entre a arquitetura atual (EMR orquestrado por Step Functions) e o uso direto do AWS Glue.

### A. Tabela Comparativa Geral

| Característica | EMR (Orquestrado por Step Functions) | AWS Glue (Jobs Spark serverless) |
| :--- | :--- | :--- |
| **Infraestrutura** | Provisionado/Transitório (EC2 / Spot / Instance Fleets) | Totalmente Serverless (DPUs) |
| **Tempo de Inicialização (Cold Start)** | Alto (3 a 8 minutos para subir e configurar o cluster) | Baixo (15 a 45 segundos) |
| **Preço Base** | Cobrança por EC2 + Taxa EMR (~25%). Suporta Spot (até 90% desconto) | Cobrança fixa por DPU-hora (~$0.44/DPU) |
| **Performance (Jobs Longos/Pesados)** | Excelente (customização fina de memória, CPU e YARN) | Muito boa (porém com pouca flexibilidade de ajuste JVM) |
| **Performance (Concorrência)** | Excelente (roda múltiplos steps concorrentes no mesmo cluster) | Média (cada Job Glue inicia uma JVM separada) |
| **Complexidade de Configuração** | Alta (YARN, Spark, Bootstrap, Terraform complexo) | Baixa (Escrever código PySpark e configurar DPUs) |
| **Integrações** | Requer scripts ou jars customizados (Delta, JDBC, etc.) | Nativas (Glue Catalogs, Conectores Nativo de Data Sources) |

---

### B. Análise de Custo (Cost Perspective)

A economia gerada pelo modelo EMR atual depende fortemente do perfil do job:

#### 1. Quando o EMR (com Step Functions) é mais barato:
* **Cargas de Trabalho Massivas e Longas:** Para pipelines que demoram horas para rodar (ex: processamentos semanais ou mensais que processam terabytes), o custo das instâncias Spot do EC2 (como a família `r5` ou `c5`) é drasticamente menor do que o Glue. 
* **Processamento Paralelo no mesmo Cluster:** O orquestrador permite submeter até `jobs_paralelas` de forma concorrente no mesmo EMR. No Glue, rodar 10 jobs paralelos significa pagar por 10 execuções separadas (multiplicando a quantidade de DPUs mínimas). No EMR, você divide a capacidade computacional já paga de um único cluster robusto.
* **Exemplo Prático de Custo:**
  Considere processar 1 TB de dados utilizando uma capacidade equivalente a **32 vCPUs e 128 GB de RAM** por 4 horas:
  * **AWS Glue (8 DPUs / 32 vCPUs):**
    $$\text{Custo} = 8 \text{ DPUs} \times \$0.44/\text{DPU-hora} \times 4 \text{ horas} = \$14.08$$
  * **AWS EMR (1 Master m5.xlarge + 4 Workers r5.xlarge Spot):**
    * *Master (On-Demand):* $(\$0.192 \text{ EC2} + \$0.048 \text{ EMR}) \times 4 \text{ hrs} = \$0.96$
    * *Workers (Spot - ~70% desc):* $4 \text{ instâncias} \times (\$0.075 \text{ EC2 Spot} + \$0.048 \text{ EMR}) \times 4 \text{ hrs} = \$1.97$
    * *Total EMR:* $\$0.96 + \$1.97 = \$2.93$
  * **Economia do EMR neste cenário: ~79% de redução de custo.**

#### 2. Quando o AWS Glue é mais barato:
* **Jobs Muito Curtos (Micro-lotes / < 10 minutos):** Como o EMR leva de 3 a 8 minutos apenas para iniciar o cluster e instalar as dependências via Bootstrap, você paga por esse tempo de inicialização (embora a cobrança seja por segundo). Se o processamento de fato leva apenas 2 minutos, o Glue é muito mais econômico devido ao seu cold start de poucos segundos.

---

### C. Análise de Performance (Performance Perspective)

#### 1. Vantagens de Performance do EMR (State Machine):
* **Reaproveitamento de Cluster (`cluster_id`):** O workflow permite passar um `cluster_id` já existente. Se um cluster "quente" for reutilizado para rodar novos steps, a execução é imediata, eliminando o cold start.
* **Customização do YARN e Spark Executor Memory:** O EMR permite definir variáveis críticas como `spark.executor.memory`, `spark.driver.memory`, e configurações do YARN no `Configurations`. O Glue possui presets de DPU (G.1X, G.2X, G.025X) e não permite alterar livremente a relação CPU/Memória por executor.
* **Otimização de IO com S3:** Através do `emrfs-site`, o pipeline define conexões de rede muito superiores aos limites padrão do Spark. Isso garante uma taxa de transferência de I/O em leitura/escrita no S3 que o AWS Glue (em suas configurações padrão) não consegue atingir sem tuning avançado de Spark.

#### 2. Vantagens de Performance do AWS Glue:
* **Warm Pools:** O Glue oferece recursos como Warm Pools que reduzem quase a zero o tempo de alocação de containers Spark.
* **Auto Scaling Nativo:** O Auto Scaling do Glue 3.0+ é extremamente rápido em responder a picos de consumo de CPU no meio da execução do script, enquanto o EMR depende de políticas de Managed Scaling que levam alguns minutos para requisitar novas instâncias EC2 à AWS.

---

## 4. Conclusão e Recomendação para o Oncar Lake

A arquitetura implantada com **Step Functions + EMR Transitório** é ideal para o **Oncar Lake** devido aos seguintes fatores:

1. **Eficiência no Uso de Spot:** A lógica embutida na máquina de estados para capturar erros de Spot e re-criar o cluster automaticamente remove o principal risco da utilização de instâncias Spot (a instabilidade), gerando economia máxima sem quebra de pipelines.
2. **Consolidação de Steps:** Como o repositório possui múltiplos scripts PySpark (como [Cubo_Vendas.py](file:///Users/kevinhizatsuki/Documents/GitHub/oncar_lake/infra/modules/main/src/scripts/Cubo_Vendas.py) e [Api_estoque_erp.py](file:///Users/kevinhizatsuki/Documents/GitHub/oncar_lake/infra/modules/main/src/scripts/Api_estoque_erp.py)), a arquitetura permite criar um único cluster EMR de tamanho adequado ("médio" ou "pesado") via DynamoDB e disparar todos esses jobs em paralelo, reduzindo o tempo total do pipeline diário de dados.

### Diretriz de Decisão:
* **Use a Step Functions + EMR (Arquitetura Atual) se:**
  - O pipeline envolve múltiplos scripts Spark que podem rodar concorrentemente.
  - O volume de dados exige tuning específico de Spark (tamanho de executor, portas de rede, drivers JDBC específicos como SQL Server).
  - O tempo total de execução ultrapassa 15-20 minutos, diluindo o impacto do cold start de boot do cluster.
* **Use o AWS Glue se:**
  - For um script isolado de curta duração (menos de 10 minutos) que consome poucos recursos.
  - O pipeline precisar rodar sob gatilhos event-driven em tempo real (onde o cold start do EMR seria impeditivo).
  - Houver restrição de governança na rede para criação/deleção dinâmica de instâncias EC2/Spot.

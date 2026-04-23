# 🔵 Databand + Databricks — Observabilidade de Dados em Pipelines de Distribuição

> Demo técnica completa de monitoramento de qualidade de dados, detecção de anomalias e rastreamento de KPIs usando **IBM Databand** e **Databricks**.

---

## 📋 Índice

1. [Visão Geral](#1-visão-geral)
2. [Arquitetura da Solução](#2-arquitetura-da-solução)
3. [Pré-requisitos](#3-pré-requisitos)
4. [Configuração do Ambiente](#4-configuração-do-ambiente)
5. [Estrutura da Pipeline](#5-estrutura-da-pipeline)
6. [Código Completo](#6-código-completo)
7. [Painel de Controle — Flags](#7-painel-de-controle--flags)
8. [Cenários de Demo](#8-cenários-de-demo)
9. [Visualizando no Databand](#9-visualizando-no-databand)
10. [Referências](#10-referências)

---

## 1. Visão Geral

Este projeto demonstra como instrumentar uma pipeline **PySpark no Databricks** com o **IBM Databand** para monitoramento contínuo em tempo real.

### Problema que resolve

Sem observabilidade, problemas de qualidade de dados chegam silenciosamente em relatórios executivos — nulos não detectados, duplicatas inflando métricas, valores negativos corrompendo receita. O Databand atua como um **smoke detector for data**: detecta, alerta e aborta antes que o problema cause impacto.

### O que esta demo mostra

| Capacidade | Descrição |
|---|---|
| **DQ Score automático** | Calcula qualidade dos dados em cada estágio |
| **Abort inteligente** | Pipeline aborta se score cai abaixo do threshold |
| **Métricas por stage** | Rastreamento de nulos, duplicatas, SLA e receita |
| **Simulação de falhas** | Flags de controle para quebrar o pipeline ao vivo |
| **Gargalos de performance** | Modo lento simulado para visualização gráfica |

---

## 2. Arquitetura da Solução

```
┌──────────────────────────────────────────────────────────────────┐
│                        Databricks Notebook                       │
│                                                                  │
│  ┌─────────┐   ┌─────────┐   ┌─────────┐   ┌─────────┐        │
│  │ Stage 1 │──▶│ Stage 2 │──▶│ Stage 3 │──▶│ Stage 4 │        │
│  │  Raw    │   │   DQ    │   │ Silver  │   │  Stock  │        │
│  └─────────┘   └─────────┘   └─────────┘   └─────────┘        │
│       │             │              │              │              │
│       ▼             ▼              ▼              ▼              │
│  ┌─────────┐   ┌─────────┐                                     │
│  │ Stage 5 │──▶│ Stage 6 │                                     │
│  │   SLA   │   │  Gold   │                                     │
│  └─────────┘   └─────────┘                                     │
└──────────────────────────────────────────────────────────────────┘
                           │
                    dbnd SDK (API)
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│                        IBM Databand                              │
│                                                                  │
│   Pipelines │ Execuções │ Métricas │ Alertas │ Datasets         │
└──────────────────────────────────────────────────────────────────┘
```

### Fluxo de dados

```
Pedidos Brutos (Raw)
        │
        ▼
  DQ Validation ──── dq_score < threshold ──▶ ABORT + ALERTA
        │
        ▼
  Limpeza Silver ──── remove nulos, negativos, duplicatas
        │
        ▼
  Stock Check ──── detecta risco de ruptura de estoque
        │
        ▼
  SLA Check ──── monitora cumprimento de prazo de entrega
        │
        ▼
  KPIs Gold ──── receita, volume, SLA médio por região
```

---

## 3. Pré-requisitos

- ✅ Conta **Databricks** (Free Edition ou superior)
- ✅ Conta **IBM Databand** com tenant ativo
- ✅ **Access Token** do Databand
  - Caminho: `Settings (⚙️) → Access Tokens → Create Token`
  - ⚠️ Use o token pessoal, **não** o token da Custom Integration

---

## 4. Configuração do Ambiente

### Instalação do SDK

```python
%pip install dbnd==1.0.34.1 --quiet
dbutils.library.restartPython()
```

### Variáveis de Ambiente

```python
import os

os.environ["DBND__CORE__DATABAND_URL"]          = "https://<seu-tenant>.databand.ai"
os.environ["DBND__CORE__DATABAND_ACCESS_TOKEN"] = "<seu-access-token>"
os.environ["DBND__TRACKING"]                    = "True"
os.environ["DBND__CORE__TRACKER"]               = "api"

print("✅ Databand configurado!")
```

> 💡 **Dica:** Após `restartPython()`, sempre reexecute a célula do Painel de Controle antes de rodar a pipeline.

---

## 5. Estrutura da Pipeline

| Stage | Nome | Responsabilidade | Métricas Enviadas |
|---|---|---|---|
| 1 | `stage1_ingest_raw` | Ingestão de pedidos brutos | `raw_total_pedidos`, `raw_regioes` |
| 2 | `stage2_dq_validation` | Validação de qualidade | `dq_score`, `dq_nulos`, `dq_alerta_critico` |
| 3 | `stage3_clean_silver` | Limpeza e enriquecimento | `silver_total_pedidos`, `silver_removidos_*` |
| 4 | `stage4_stock_validation` | Verificação de estoque | `pct_risco_ruptura`, `risco_ruptura_estoque` |
| 5 | `stage5_sla_check` | Análise de SLA de entrega | `entregas_no_prazo_pct`, `violacoes_sla` |
| 6 | `stage6_gold_kpis` | Agregação de KPIs finais | `gold_receita_total_R$`, `gold_sla_medio_pct` |

---

## 6. Código Completo

### Célula 1 — Painel de Controle

```python
# ╔══════════════════════════════════════════════════════════╗
# ║           🎛️  PAINEL DE CONTROLE DA PIPELINE            ║
# ║   ⚠️  EXECUTE ESTA CÉLULA PRIMEIRO APÓS QUALQUER RESTART ║
# ╚══════════════════════════════════════════════════════════╝

ENABLE_NULL_FILTER       = True   # False → nulos chegam no Silver e corrompem KPIs
ENABLE_DEDUP             = True   # False → duplicatas inflam volume em ~30%
ENABLE_NEG_FILTER        = True   # False → valores negativos corrompem receita
ENABLE_SLA_CHECK         = True   # False → violações de SLA passam invisíveis
ENABLE_STOCK_VALIDATION  = True   # False → risco de ruptura não monitorado
DQ_THRESHOLD             = 80     # Abaixo disso → pipeline aborta com alerta crítico
FAST_MODE                = True   # False → simula gargalos de performance por stage

print("🎛️  Painel carregado!")
print(f"   DQ Threshold : {DQ_THRESHOLD}")
print(f"   Fast Mode    : {FAST_MODE}")
```

### Célula 2 — Imports e Helpers

```python
import os, random, time
from datetime import datetime, timedelta
from pyspark.sql import functions as F, DataFrame
from pyspark.sql.types import *
from dbnd import dbnd_tracking, task, log_metric, log_dataframe

os.environ["DBND__CORE__DATABAND_URL"]          = "https://<seu-tenant>.databand.ai"
os.environ["DBND__CORE__DATABAND_ACCESS_TOKEN"] = "<seu-access-token>"
os.environ["DBND__TRACKING"]                    = "True"
os.environ["DBND__CORE__TRACKER"]               = "api"

def simulate_load(stage_name: str, fast_seconds: float, slow_seconds: float):
    """Simula tempo de processamento baseado no FAST_MODE."""
    duration = fast_seconds if FAST_MODE else slow_seconds
    if not FAST_MODE:
        print(f"   ⏳ {stage_name}: modo lento ativado ({slow_seconds}s)...")
    time.sleep(duration)
    log_metric(f"duration_seg_{stage_name}", duration)

print("✅ Setup concluído!")
```

### Célula 3 — Stage 1: Ingestão Raw

```python
@task
def stage1_ingest_raw() -> DataFrame:
    """Ingere pedidos de distribuição de múltiplas regiões."""
    random.seed(7)
    regioes        = ["Regiao-A","Regiao-B","Regiao-C","Regiao-D",
                      "Regiao-E","Regiao-F","Regiao-G","Regiao-H"]
    distribuidoras = ["Dist-Norte","Dist-Sul","Dist-Leste","Dist-Oeste","Dist-Centro"]
    produtos       = ["Produto-P1","Produto-P2","Produto-P3","Produto-Industrial"]
    motoristas     = [f"Motorista-{i}" for i in range(1, 51)]

    data = []
    for i in range(3000):
        duplicata = random.random() < 0.06
        id_pedido = i if not duplicata else random.randint(0, i)
        data.append({
            "id_pedido":           id_pedido,
            "id_cliente":          random.randint(1, 800),
            "distribuidora":       random.choice(distribuidoras),
            "tipo_produto":        random.choice(produtos + [None, None]),
            "quantidade":          random.randint(1, 50),
            "valor_unitario":      round(random.uniform(-10, 120), 2),
            "regiao":              random.choice(regioes),
            "motorista":           random.choice(motoristas),
            "prazo_entrega_h":     random.randint(2, 48),
            "tempo_entrega_h":     random.randint(1, 72),
            "estoque_disponivel":  random.randint(0, 200),
            "data_pedido":         (datetime(2024,1,1) + timedelta(
                                    days=random.randint(0,365))).strftime("%Y-%m-%d"),
            "status":              random.choice(["entregue","entregue","entregue",
                                                  "pendente","cancelado"])
        })

    schema = StructType([
        StructField("id_pedido",           IntegerType()),
        StructField("id_cliente",          IntegerType()),
        StructField("distribuidora",       StringType()),
        StructField("tipo_produto",        StringType()),
        StructField("quantidade",          IntegerType()),
        StructField("valor_unitario",      DoubleType()),
        StructField("regiao",              StringType()),
        StructField("motorista",           StringType()),
        StructField("prazo_entrega_h",     IntegerType()),
        StructField("tempo_entrega_h",     IntegerType()),
        StructField("estoque_disponivel",  IntegerType()),
        StructField("data_pedido",         StringType()),
        StructField("status",              StringType())
    ])

    df = spark.createDataFrame(data, schema=schema)

    log_metric("raw_total_pedidos",  df.count())
    log_metric("raw_distribuidoras", df.select("distribuidora").distinct().count())
    log_metric("raw_regioes",        df.select("regiao").distinct().count())
    log_dataframe("raw_pedidos", df, with_histograms=True)
    simulate_load("ingest", fast_seconds=1, slow_seconds=12)

    print(f"📦 Stage 1 — {df.count()} pedidos | {df.select('regiao').distinct().count()} regiões")
    return df
```

### Célula 4 — Stage 2: Validação DQ

```python
@task
def stage2_dq_validation(df: DataFrame) -> DataFrame:
    """Valida qualidade dos dados. Aborta se score abaixo do threshold."""
    total      = df.count()
    nulos      = df.filter(F.col("tipo_produto").isNull()).count()
    negativos  = df.filter(F.col("valor_unitario") < 0).count()
    duplicatas = total - df.dropDuplicates(["id_pedido"]).count()

    pct_nulos      = round(nulos / total * 100, 2)
    pct_negativos  = round(negativos / total * 100, 2)
    pct_duplicatas = round(duplicatas / total * 100, 2)
    dq_score       = round(100 - pct_nulos - pct_negativos - pct_duplicatas, 1)

    log_metric("dq_total",          total)
    log_metric("dq_nulos",          nulos)
    log_metric("dq_pct_nulos",      pct_nulos)
    log_metric("dq_negativos",      negativos)
    log_metric("dq_pct_negativos",  pct_negativos)
    log_metric("dq_duplicatas",     duplicatas)
    log_metric("dq_pct_duplicatas", pct_duplicatas)
    log_metric("dq_score",          dq_score)
    simulate_load("dq_validation", fast_seconds=1, slow_seconds=18)

    print(f"🔍 Stage 2 — DQ Score: {dq_score}/100")
    print(f"   Nulos: {nulos} ({pct_nulos}%) | Negativos: {negativos} | Duplicatas: {duplicatas}")

    if dq_score < DQ_THRESHOLD:
        log_metric("dq_alerta_critico", 1)
        raise ValueError(
            f"❌ PIPELINE ABORTADA — DQ Score {dq_score} abaixo do threshold {DQ_THRESHOLD}! "
            f"Revise os dados de origem."
        )

    log_metric("dq_alerta_critico", 0)
    return df
```

### Célula 5 — Stage 3: Limpeza Silver

```python
@task
def stage3_clean_silver(df: DataFrame) -> DataFrame:
    """Aplica filtros conforme flags do painel de controle."""
    df_clean = df

    if ENABLE_NULL_FILTER:
        antes = df_clean.count()
        df_clean = df_clean.filter(F.col("tipo_produto").isNotNull())
        log_metric("silver_removidos_nulos", antes - df_clean.count())
    else:
        log_metric("silver_removidos_nulos", 0)
        print("⚠️  NULL filter DESATIVADO — nulos passando para Silver!")

    if ENABLE_NEG_FILTER:
        antes = df_clean.count()
        df_clean = df_clean.filter(F.col("valor_unitario") > 0)
        log_metric("silver_removidos_negativos", antes - df_clean.count())
    else:
        log_metric("silver_removidos_negativos", 0)
        print("⚠️  Filtro negativos DESATIVADO — valores corrompidos no Silver!")

    if ENABLE_DEDUP:
        antes = df_clean.count()
        df_clean = df_clean.dropDuplicates(["id_pedido"])
        log_metric("silver_removidos_duplicatas", antes - df_clean.count())
    else:
        log_metric("silver_removidos_duplicatas", 0)
        print("⚠️  Deduplicação DESATIVADA — volume inflado artificialmente!")

    df_clean = (df_clean
        .filter(F.col("status") == "entregue")
        .withColumn("valor_total",      F.col("valor_unitario") * F.col("quantidade"))
        .withColumn("data_pedido",      F.to_date("data_pedido", "yyyy-MM-dd"))
        .withColumn("mes",              F.month("data_pedido"))
        .withColumn("trimestre",        F.quarter("data_pedido"))
        .withColumn("entrega_no_prazo", F.col("tempo_entrega_h") <= F.col("prazo_entrega_h"))
    )

    log_metric("silver_total_pedidos", df_clean.count())
    log_dataframe("silver_pedidos", df_clean, with_histograms=True)
    simulate_load("clean_silver", fast_seconds=1, slow_seconds=25)

    print(f"🥈 Stage 3 — {df_clean.count()} pedidos limpos")
    return df_clean
```

### Célula 6 — Stage 4: Validação de Estoque

```python
@task
def stage4_stock_validation(df: DataFrame) -> DataFrame:
    """Detecta pedidos que excedem capacidade de estoque."""
    if not ENABLE_STOCK_VALIDATION:
        log_metric("pedidos_acima_estoque", 0)
        log_metric("risco_ruptura_estoque", 0)
        print("⚠️  Stock Validation DESATIVADO — ruptura de estoque não monitorada!")
        return df

    df_risco  = df.filter(F.col("quantidade") > F.col("estoque_disponivel"))
    df_ok     = df.filter(F.col("quantidade") <= F.col("estoque_disponivel"))
    pct_risco = round(df_risco.count() / df.count() * 100, 1)

    log_metric("pedidos_acima_estoque", df_risco.count())
    log_metric("pedidos_estoque_ok",    df_ok.count())
    log_metric("pct_risco_ruptura",     pct_risco)
    log_metric("risco_ruptura_estoque", 1 if pct_risco > 10 else 0)
    simulate_load("stock_validation", fast_seconds=1, slow_seconds=10)

    print(f"📦 Stage 4 — {df_risco.count()} pedidos com risco de ruptura ({pct_risco}%)")
    return df_ok
```

### Célula 7 — Stage 5: SLA de Entrega

```python
@task
def stage5_sla_check(df: DataFrame) -> DataFrame:
    """Analisa cumprimento de SLA por motorista e região."""
    if not ENABLE_SLA_CHECK:
        log_metric("entregas_no_prazo_pct", 0.0)
        log_metric("violacoes_sla",         0)
        print("⚠️  SLA Check DESATIVADO — atrasos de entrega não monitorados!")
        return df

    total        = df.count()
    no_prazo     = df.filter(F.col("entrega_no_prazo") == True).count()
    atrasados    = total - no_prazo
    pct_no_prazo = round(no_prazo / total * 100, 1)

    df_motor = (df.groupBy("motorista")
                  .agg(F.avg(F.col("tempo_entrega_h") - F.col("prazo_entrega_h"))
                  .alias("atraso_medio"))
                  .orderBy(F.desc("atraso_medio")))
    pior = df_motor.first()

    log_metric("entregas_no_prazo",     no_prazo)
    log_metric("entregas_atrasadas",    atrasados)
    log_metric("entregas_no_prazo_pct", pct_no_prazo)
    log_metric("violacoes_sla",         atrasados)
    log_metric("sla_abaixo_meta",       1 if pct_no_prazo < 85 else 0)
    simulate_load("sla_check", fast_seconds=1, slow_seconds=15)

    print(f"🚚 Stage 5 — SLA: {pct_no_prazo}% no prazo | {atrasados} atrasos")
    print(f"   Motorista com mais atrasos: {pior['motorista']} ({round(pior['atraso_medio'],1)}h de desvio)")
    return df
```

### Célula 8 — Stage 6: KPIs Gold

```python
@task
def stage6_gold_kpis(df: DataFrame) -> DataFrame:
    """Calcula KPIs finais por região, distribuidora e produto."""
    df_gold = (df
        .groupBy("regiao", "distribuidora", "tipo_produto", "trimestre")
        .agg(
            F.sum("valor_total").alias("receita_total"),
            F.sum("quantidade").alias("unidades_entregues"),
            F.avg("valor_unitario").alias("preco_medio"),
            F.count("id_pedido").alias("qtd_pedidos"),
            F.countDistinct("id_cliente").alias("clientes_atendidos"),
            F.avg((F.col("entrega_no_prazo")).cast("integer") * 100).alias("sla_pct"),
            F.avg(F.col("tempo_entrega_h") - F.col("prazo_entrega_h")).alias("desvio_sla_h")
        )
        .orderBy(F.desc("receita_total"))
    )

    receita   = df_gold.agg(F.sum("receita_total")).collect()[0][0] or 0
    unidades  = df_gold.agg(F.sum("unidades_entregues")).collect()[0][0] or 0
    sla_medio = df_gold.agg(F.avg("sla_pct")).collect()[0][0] or 0

    log_metric("gold_receita_total_R$",   round(receita, 2))
    log_metric("gold_unidades_entregues", int(unidades))
    log_metric("gold_sla_medio_pct",      round(sla_medio, 1))
    log_metric("gold_regioes_atendidas",  df_gold.select("regiao").distinct().count())
    log_metric("gold_distribuidoras",     df_gold.select("distribuidora").distinct().count())
    log_dataframe("gold_kpis_distribuicao", df_gold)
    simulate_load("gold_kpis", fast_seconds=1, slow_seconds=20)

    print(f"🥇 Stage 6 — Receita: R$ {receita:,.2f} | {int(unidades):,} unidades | SLA: {round(sla_medio,1)}%")
    return df_gold
```

### Célula 9 — Execução do Pipeline

```python
with dbnd_tracking(job_name="pipeline-distribuicao-completo"):
    raw    = stage1_ingest_raw()
    _      = stage2_dq_validation(raw)
    silver = stage3_clean_silver(raw)
    stock  = stage4_stock_validation(silver)
    _      = stage5_sla_check(stock)
    gold   = stage6_gold_kpis(stock)
    gold.show(10, truncate=False)
    print("\n✅ Pipeline concluída! Acesse: Databand → Pipelines → pipeline-distribuicao-completo")
```

---

## 7. Painel de Controle — Flags

| Flag | Padrão | Efeito quando `False` | Impacto no Databand |
|---|---|---|---|
| `ENABLE_NULL_FILTER` | `True` | Nulos passam para Silver | `silver_removidos_nulos = 0` |
| `ENABLE_DEDUP` | `True` | Duplicatas inflam volume ~30% | `silver_removidos_duplicatas = 0` |
| `ENABLE_NEG_FILTER` | `True` | Valores negativos corrompem receita | `gold_receita_total_R$` fica negativo |
| `ENABLE_SLA_CHECK` | `True` | Atrasos de entrega invisíveis | `violacoes_sla = 0` |
| `ENABLE_STOCK_VALIDATION` | `True` | Ruptura de estoque não detectada | `risco_ruptura_estoque = 0` |
| `FAST_MODE` | `True` | Gargalos de performance por stage | Duração aumenta 10-25x por stage |
| `DQ_THRESHOLD` | `80` | Threshold de abort do pipeline | Pipeline aborta no Stage 2 |

---

## 8. Cenários de Demo

### ✅ Cenário 1 — Pipeline Saudável
```python
DQ_THRESHOLD = 50
FAST_MODE    = True
# todos os demais True
```
**O que mostrar:** 6 stages verdes, métricas saudáveis, receita positiva.

---

### ❌ Cenário 2 — Dados com Baixa Qualidade
```python
DQ_THRESHOLD = 80
```
**O que mostrar:** Pipeline abortada no Stage 2, `dq_alerta_critico = 1`, `dq_score = 53.6`.

---

### ⏳ Cenário 3 — Gargalo de Performance
```python
FAST_MODE = False
DQ_THRESHOLD = 50
```
**O que mostrar:** Comparar duração dos stages — ~6s vs ~100s. Identificar Stage 3 como gargalo principal (25s).

---

### ⚠️ Cenário 4 — Dados Corrompidos Silenciosos (mais impactante)
```python
ENABLE_DEDUP            = False
ENABLE_SLA_CHECK        = False
ENABLE_STOCK_VALIDATION = False
DQ_THRESHOLD            = 50
```
**O que mostrar:** Pipeline passa, mas métricas estão erradas. Volume inflado, SLA zerado, ruptura invisível. Esse é o cenário mais comum em produção sem observabilidade.

---

### Narrative para o Cliente

> *"Às 2h da manhã a pipeline rodou normalmente — sem erros, sem alertas. Mas os dados tinham 8% de duplicatas e o SLA check estava desativado. O relatório do board das 9h mostrou receita 30% maior que o real e zero ocorrências de atraso de entrega. Com o Databand, esse cenário seria detectado no Stage 2 antes de qualquer impacto."*

---

## 9. Visualizando no Databand

### Onde encontrar os resultados

| Seção no Databand | O que observar |
|---|---|
| **Pipelines** | Histórico de runs — buscar `pipeline-distribuicao-completo` |
| **Execuções** | Status ✅/❌, duração por stage, timestamp de cada run |
| **Métricas** | `dq_score`, `gold_receita_total_R$`, `sla_pct` ao longo do tempo |
| **Conjuntos de dados** | Lineage: `raw_pedidos` → `silver_pedidos` → `gold_kpis_distribuicao` |
| **Alertas** | `dq_alerta_critico = 1` nas runs com falha de qualidade |

### Comparação de Runs no Databand

```
pipeline-distribuicao-completo
├── ✅ Run 1 — Cenário 1: 6 stages OK, receita real, SLA monitorado
├── ❌ Run 2 — Cenário 2: abortou Stage 2, dq_score=53.6, alerta crítico
├── ⏳ Run 3 — Cenário 3: 6 stages OK mas duração 100s vs 6s
└── ⚠️  Run 4 — Cenário 4: passou mas métricas corrompidas
```

---

## 10. Referências

- [IBM Databand Documentation](https://docs.databand.ai)
- [dbnd SDK — GitHub](https://github.com/databand-ai/dbnd)
- [IBM Databand + Spark Integration](https://www.ibm.com/products/databand/integrations/spark)
- [Databricks Free Edition](https://www.databricks.com/try-databricks)
- [OpenLineage Spec](https://openlineage.io)

---

*Desenvolvido como demo técnica de observabilidade de dados com IBM Databand + Databricks.*  
*Dados sintéticos — nenhuma informação real de clientes ou empresas.*

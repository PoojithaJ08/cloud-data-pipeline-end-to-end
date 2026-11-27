# Cloud Pipeline Notes — One Pipeline, Mapped to AWS and Azure

**Poojitha Krishna Jonnalagadda** — Data Engineer (Python, SQL, Spark/PySpark)

> **Demonstration note:** These notes map the end-to-end pipeline in this project (ingest → transform → validate → load → report, on SAP-style master and transactional data) to the cloud services on my AWS and Azure resumes, at the depth my project work supports. Service names are real; configurations are described generically — no client accounts, endpoints or data.

---

## The pipeline shape (platform-neutral)

```
Landing (bronze)     Transform (silver)        Governed target (gold)
raw extracts    →    cleanse · dedupe ·   →    validated, partitioned /
unchanged            validate · reconcile      merged tables for reporting
```

Every stage has one job and one failure mode I design for: landing must never lose a file, transform must never load a record silently, and the target must reconcile back to the source counts. The code in this project implements exactly that shape; the two mappings below are how I have run it on each platform.

## AWS mapping (from my AWS Data Engineer work)

| Stage | Service | How I use it |
|---|---|---|
| Landing | **Amazon S3** | Raw SAP extracts land in a bronze prefix by date/object; large inventory and transaction files stay in S3 and are checked in place |
| Transform | **AWS Glue** (PySpark jobs) / **Amazon EMR** | Glue jobs cleanse and reshape master data (the 25,000-record cleanses on my current programme follow this pattern); EMR for the heavier ad-hoc checks over large files during migration test cycles |
| Ad-hoc validation | **Amazon Athena** | SQL checks directly over S3 files — duplicates, gaps, count reconciliation — without standing up a cluster |
| Target | **Amazon Redshift** (and S3/Delta tables) | Reporting tables on top of the cleansed data; sales, inventory and production datasets for the dashboards |
| Orchestration & alerts | **Apache Airflow** | Daily scheduling with failure alerts, so a broken load surfaces the same morning instead of at month end (see `airflow_dag_sample.py`) |

## Azure mapping (from my Azure Data Engineer work)

| Stage | Service | How I use it |
|---|---|---|
| Landing | **Azure Data Lake / OneLake** (Microsoft Fabric) | Raw extracts land in the lakehouse bronze area |
| Orchestration | **Azure Data Factory** | Pipelines pull SAP sales, procurement, inventory and production data into the lake; **exception alerts flag failed loads the same day** — a pattern I set up so repeat errors dropped instead of accumulating |
| Transform | **Azure Databricks** (PySpark notebooks/jobs) | Clean and reshape master data before load — the same PySpark code shape as `pyspark_etl_sample.py` |
| Target / modeling | **Azure Synapse Analytics** + **Microsoft Fabric lakehouse** | Modeled tables (Delta format) for BOMs, routings, work centers and production versions, so planning and costing read the same data in every plant |
| Reporting | **Power BI** | Dashboards on the modeled data — 20 on sales, inventory and production in my current role |

The same pipeline also targets **Snowflake** where that is the warehouse: the SQL form of the transform lives in `snowflake-sql-samples.sql` (staging → dedupe → merge), and the **dbt** form of the staging/mart models lives under `dbt/`.

## A note on Apache Kafka (claimed on my Data Engineer / AWS resumes)

Kafka belongs to the streaming variant of this shape: instead of daily file extracts, SAP-side events (order created, goods movement posted, invoice issued) are published to topics and consumed into the landing zone continuously. The downstream stages do not change — the same cleansing, validation and reconciliation rules apply to streamed records, and the failure discipline matters *more*, because a bad record arrives in seconds, not in tomorrow's file. I have kept Kafka at this concept level in this pack deliberately: a meaningful streaming demonstration needs a running broker cluster, which a portable sample cannot honestly reproduce.

## Design rules I carry across platforms

1. **Bronze is sacred** — raw data lands unchanged; every correction happens in a re-runnable transform, so any result can be rebuilt and re-proved.
2. **Validation is a gate, not a report** — a run that fails reconciliation does not load the target (see the quality gate in `airflow_dag_sample.py`).
3. **Rejects go back with reasons** — per-rule failure counts and sample rows to the data owners; silent drops are how migrations fail quietly.
4. **Idempotent loads** — dedupe on natural keys and merge/upsert into targets, so re-running after a fix is safe.
5. **Alerts on the day, not the month** — pipeline failure and data-quality alerts fire on the run that failed.

---

*Prepared by Poojitha Krishna Jonnalagadda · pkj08.work@gmail.com · 757-804-7949 · linkedin.com/in/poojithaj11*

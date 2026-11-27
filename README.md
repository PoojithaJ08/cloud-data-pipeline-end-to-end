# Cloud Data Pipeline — End to End (Spark / Snowflake / AWS / Azure)

**Poojitha Krishna Jonnalagadda** — Data Engineer · Python, SQL, PySpark (9 yrs SQL/Python/ETL)

> **Demonstration note:** This is a complete, runnable sample built from my data engineering work — moving SAP master and transactional data into reporting platforms (25,000 master-data records cleansed and loaded in my current role; 35 data objects through an S/4HANA cutover across 8 plants). The input data is synthetic, written to look like real SAP-style extracts with a realistic mix of clean and flawed rows. No client data is used.

---

## End-to-end scope

This project is a **whole pipeline, not a fragment**. One flow runs through every stage:

```
sample_input/ ──► INGEST ──► TRANSFORM/CLEANSE ──► VALIDATE ──► LOAD ──► REPORT
 (3 CSV extracts)   land raw    trim · normalise      rule set    partitioned   reconciliation
                                 · dedupe                        output        per rule + totals
```

**Run it now (no dependencies, Python 3 standard library only):**

```bash
python3 run_pipeline_local.py
```

Real output from that run is saved in `output/validation-report.txt`: **42 input rows → 31 clean rows written**, with every rejection counted per rule and every entity reconciling (input = duplicates + rejected + clean). The cleansed output itself sits in `output/` partitioned by sales org / plant — the local stand-in for partitioned Parquet/Delta on S3 or Azure Data Lake.

The **PySpark version** (`pyspark_etl_sample.py`) implements the identical stages for a Spark platform (Amazon EMR, AWS Glue, Azure Databricks, Databricks). It requires PySpark (and optionally delta-spark for the Delta write path; it falls back to partitioned Parquet) — that requirement is stated in its header, and it is included so the cloud form of the pipeline can be read alongside the runnable local form.

## Files

| File | Contents |
|---|---|
| `sample_input/` | The 3 synthetic extracts: `customers.csv` (14 rows), `materials.csv` (12), `sales_orders.csv` (16) — with planted duplicates, missing fields, a bad email, a negative price, orphan references and a zero quantity |
| `run_pipeline_local.py` | The full pipeline in pure Python — **runs end to end today** and produces `output/` |
| `pyspark_etl_sample.py` | The same pipeline as a PySpark job (Delta write with Parquet fallback) |
| `output/validation-report.txt` | The **actual output** of the local run — per-entity, per-rule counts and reconciliation |
| `output/customers|materials|sales_orders/` | The actual cleansed output, partitioned (`sales_org=…`, `plant=…`) |
| `snowflake-sql-samples.sql` | The warehouse form of the transform: staging → dedupe (QUALIFY) → validation gates → MERGE → reconciliation |
| `dbt/models/` | The dbt form: `stg_customers` (cleanse + dedupe), `mart_customer_master` (validated mart), `schema.yml` (sources + tests as build-time validation) |
| `airflow_dag_sample.py` | Orchestration: daily DAG wrapping ingest → transform → quality gate → load, with same-day failure alerting (illustrative — needs an Airflow environment) |
| `cloud-pipeline-notes.md` | How this pipeline maps to **AWS** (Glue, S3, EMR, Athena, Redshift, Airflow) and **Azure** (Data Factory, Databricks, Synapse, Fabric/OneLake, Power BI), plus a concept note on Kafka |

## The validation rules (same set in every form)

- **Customers:** mandatory fields (customer_id, name, sales_org) · email present and well-formed
- **Materials:** mandatory fields (material_id, plant, material_group) · unit_price a positive number
- **Sales orders:** mandatory IDs · quantity positive · customer and material must exist in the *cleansed* sets (referential integrity against the target, not the raw extract — an order pointing at a rejected material is itself rejected and reported)

## Skills demonstrated

PySpark / Apache Spark · Python · SQL (Snowflake dialect) · dbt (models + tests) · Apache Airflow (orchestration + alerting) · Delta Lake / partitioned Parquet · Data lake → warehouse pipeline design (bronze/silver/gold) · ETL/ELT, data modeling, data quality & reconciliation · AWS and Azure service mapping at the depth of my project work (see `cloud-pipeline-notes.md`).

---

*Prepared by Poojitha Krishna Jonnalagadda · pkj08.work@gmail.com · 757-804-7949 · linkedin.com/in/poojithaj11*

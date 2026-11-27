#!/usr/bin/env python3
"""
End-to-End Cloud Data Pipeline — Local Fallback Runner
Poojitha Krishna Jonnalagadda - Data Engineer (Python, SQL, Spark/PySpark)

Demonstration sample built from my data engineering work: moving SAP-style
master and transactional data (customers, materials, sales orders) through
the same stages I run on cloud platforms -

    INGEST -> TRANSFORM/CLEANSE -> VALIDATE -> LOAD (partitioned) -> REPORT

This runner executes the identical pipeline logic as pyspark_etl_sample.py
using only the Python standard library, so the full flow can be run and
inspected anywhere. On a cloud platform the same steps run as a PySpark job
(EMR / Databricks / Glue) or an equivalent ADF + Databricks flow - see
cloud-pipeline-notes.md.

Usage:
    python3 run_pipeline_local.py [input_dir] [output_dir]

Defaults: input_dir = sample_input/, output_dir = output/
Exit code 0 on a completed run (data failures are reported, not raised).
"""

import csv
import os
import re
import sys
from collections import defaultdict

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


# ---------------------------------------------------------------------------
# INGEST
# ---------------------------------------------------------------------------
def read_csv(path):
    with open(path, newline="", encoding="utf-8") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


# ---------------------------------------------------------------------------
# TRANSFORM / CLEANSE helpers
# ---------------------------------------------------------------------------
def cleanse(rows, upper_fields=()):
    """Trim whitespace on all fields; normalise selected fields to upper case."""
    out = []
    for row in rows:
        r = {k: (v.strip() if isinstance(v, str) else v) for k, v in row.items()}
        for f in upper_fields:
            if r.get(f):
                r[f] = r[f].upper()
        out.append(r)
    return out


def dedupe(rows, key_fields):
    """Drop duplicate rows on the natural key, keeping the first occurrence."""
    seen, kept, dropped = set(), [], 0
    for row in rows:
        key = tuple(row.get(f, "") for f in key_fields)
        if key in seen:
            dropped += 1
        else:
            seen.add(key)
            kept.append(row)
    return kept, dropped


# ---------------------------------------------------------------------------
# VALIDATE - each rule returns the list of rows that FAILED it.
# A row that fails any rule is rejected from the load and reported, never
# silently dropped: the error-log-back-to-the-business pattern I use on
# migration programmes.
# ---------------------------------------------------------------------------
def rule_mandatory(rows, fields):
    return [r for r in rows if any(not r.get(f) for f in fields)]


def rule_email(rows):
    return [r for r in rows if not EMAIL_RE.match(r.get("email", "") or "")]


def rule_positive_price(rows):
    bad = []
    for r in rows:
        try:
            if float(r.get("unit_price", "")) <= 0:
                bad.append(r)
        except (TypeError, ValueError):
            bad.append(r)
    return bad


def rule_positive_qty(rows):
    bad = []
    for r in rows:
        try:
            if int(r.get("quantity", "")) <= 0:
                bad.append(r)
        except (TypeError, ValueError):
            bad.append(r)
    return bad


def rule_reference(rows, field, valid_keys):
    return [r for r in rows if r.get(field) not in valid_keys]


def apply_rules(rows, rules):
    """rules: list of (rule_name, failing_rows). Returns (clean_rows, failures)."""
    rejected_ids = set()
    failures = {}
    for name, failing in rules:
        failures[name] = len(failing)
        rejected_ids.update(id(r) for r in failing)
    clean = [r for r in rows if id(r) not in rejected_ids]
    return clean, failures, len(rows) - len(clean)


# ---------------------------------------------------------------------------
# LOAD - write clean rows partitioned by a key column, one CSV part per
# partition (the local stand-in for partitioned Parquet/Delta on S3/ADLS).
# ---------------------------------------------------------------------------
def write_partitioned(rows, base_dir, entity, partition_field, columns):
    counts = {}
    groups = defaultdict(list)
    for r in rows:
        groups[r.get(partition_field) or "UNKNOWN"].append(r)
    for part_value, part_rows in sorted(groups.items()):
        part_dir = os.path.join(base_dir, entity, f"{partition_field}={part_value}")
        os.makedirs(part_dir, exist_ok=True)
        with open(os.path.join(part_dir, "part-000.csv"), "w", newline="",
                  encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=columns)
            writer.writeheader()
            writer.writerows(part_rows)
        counts[part_value] = len(part_rows)
    return counts


# ---------------------------------------------------------------------------
# PIPELINE
# ---------------------------------------------------------------------------
def main(argv):
    input_dir = argv[1] if len(argv) > 1 else os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "sample_input")
    output_dir = argv[2] if len(argv) > 2 else os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "output")
    os.makedirs(output_dir, exist_ok=True)

    report = []
    report.append("=" * 72)
    report.append("END-TO-END PIPELINE RUN - customers / materials / sales_orders")
    report.append("Stages: INGEST -> TRANSFORM/CLEANSE -> VALIDATE -> LOAD -> REPORT")
    report.append("=" * 72)

    # ---- INGEST + TRANSFORM/CLEANSE -------------------------------------
    customers_raw = read_csv(os.path.join(input_dir, "customers.csv"))
    materials_raw = read_csv(os.path.join(input_dir, "materials.csv"))
    orders_raw = read_csv(os.path.join(input_dir, "sales_orders.csv"))

    customers, cust_dupes = dedupe(
        cleanse(customers_raw, upper_fields=("country",)),
        key_fields=("customer_id",))
    materials, mat_dupes = dedupe(
        cleanse(materials_raw, upper_fields=("currency",)),
        key_fields=("material_id", "plant"))
    orders = cleanse(orders_raw)

    # ---- VALIDATE ---------------------------------------------------------
    customers_clean, cust_fail, cust_rejected = apply_rules(customers, [
        ("mandatory fields (customer_id, name, sales_org)",
         rule_mandatory(customers, ("customer_id", "name", "sales_org"))),
        ("email present and valid format", rule_email(customers)),
    ])
    materials_clean, mat_fail, mat_rejected = apply_rules(materials, [
        ("mandatory fields (material_id, plant, material_group)",
         rule_mandatory(materials, ("material_id", "plant", "material_group"))),
        ("unit_price must be a positive number", rule_positive_price(materials)),
    ])
    valid_customers = {r["customer_id"] for r in customers_clean}
    valid_materials = {r["material_id"] for r in materials_clean}
    orders_clean, ord_fail, ord_rejected = apply_rules(orders, [
        ("mandatory fields (order_id, customer_id, material_id)",
         rule_mandatory(orders, ("order_id", "customer_id", "material_id"))),
        ("quantity must be positive", rule_positive_qty(orders)),
        ("customer_id must exist in cleansed customers",
         rule_reference(orders, "customer_id", valid_customers)),
        ("material_id must exist in cleansed materials",
         rule_reference(orders, "material_id", valid_materials)),
    ])

    # ---- LOAD (partitioned output) ---------------------------------------
    cust_parts = write_partitioned(
        customers_clean, output_dir, "customers", "sales_org",
        ["customer_id", "name", "sales_org", "country", "email", "credit_limit"])
    mat_parts = write_partitioned(
        materials_clean, output_dir, "materials", "plant",
        ["material_id", "description", "plant", "material_group", "base_uom",
         "unit_price", "currency"])
    ord_parts = write_partitioned(
        orders_clean, output_dir, "sales_orders", "sales_org",
        ["order_id", "customer_id", "material_id", "sales_org", "quantity",
         "order_date"])

    # ---- REPORT ------------------------------------------------------------
    def entity_block(title, raw, dupes, rejected, clean, failures, parts,
                     part_field):
        report.append("")
        report.append(f"[{title}]")
        report.append(f"  input rows:            {raw}")
        report.append(f"  duplicates removed:    {dupes}")
        report.append(f"  rows rejected:         {rejected}")
        for name, count in failures.items():
            report.append(f"    - failed rule: {name}: {count}")
        report.append(f"  clean rows written:    {clean}")
        report.append(f"  reconciliation:        {raw} input = {dupes} duplicates "
                      f"+ {rejected} rejected + {clean} clean "
                      f"-> {'OK' if raw == dupes + rejected + clean else 'MISMATCH'}")
        report.append(f"  partitions written ({part_field}): "
                      + ", ".join(f"{k}={v}" for k, v in parts.items()))

    entity_block("CUSTOMERS", len(customers_raw), cust_dupes, cust_rejected,
                 len(customers_clean), cust_fail, cust_parts, "sales_org")
    entity_block("MATERIALS", len(materials_raw), mat_dupes, mat_rejected,
                 len(materials_clean), mat_fail, mat_parts, "plant")
    entity_block("SALES ORDERS", len(orders_raw), 0, ord_rejected,
                 len(orders_clean), ord_fail, ord_parts, "sales_org")

    total_in = len(customers_raw) + len(materials_raw) + len(orders_raw)
    total_clean = len(customers_clean) + len(materials_clean) + len(orders_clean)
    report.append("")
    report.append(f"TOTAL: {total_in} input rows -> {total_clean} clean rows "
                  f"written to partitioned output under {output_dir}")
    report.append("Rejected rows are reported above per rule - in a live "
                  "engagement they go back to the data owners in an error "
                  "log for correction at source, never loaded silently.")

    text = "\n".join(report) + "\n"
    with open(os.path.join(output_dir, "validation-report.txt"), "w",
              encoding="utf-8") as fh:
        fh.write(text)
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))

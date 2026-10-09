"""Apache Spark (PySpark) job: katta hajmdagi to'lovlarni taqsimlangan tarzda agregatsiya qilish.

Natijalar:
  * marts/spark_supplier_year      — yetkazuvchi × yil kesimida shartnoma va to'lov summalari
  * marts/spark_overpaid_contracts — to'lovi shartnomadan oshgan shartnomalar

Pipeline bu natijalarni DuckDB hisob-kitoblari bilan solishtiradi (reconciliation):
ikki mustaqil dvigatel bir xil natija berishi — hisob-kitob to'g'riligining dalili.

Mustaqil ishga tushirish:
    python -m spark_jobs.supplier_spend data/silver data/marts
"""
from __future__ import annotations

import sys
from pathlib import Path


def run(silver_dir: Path, out_dir: Path, overpayment_tolerance: float = 0.01) -> dict[str, int]:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = (SparkSession.builder.master("local[*]").appName("xarid-audit-supplier-spend")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        contracts = spark.read.parquet(str(silver_dir / "contracts.parquet"))
        payments = spark.read.parquet(str(silver_dir / "payments.parquet"))
        # fact_payment bilan bir xil: faqat ma'lum shartnomaga bog'langan to'lovlar
        paid = (payments.join(contracts.select("contract_id"), "contract_id", "inner")
                .groupBy("contract_id").agg(F.sum("amount").alias("paid"), F.count("*").alias("n_payments")))
        enriched = contracts.join(paid, "contract_id", "left").fillna({"paid": 0.0, "n_payments": 0})

        supplier_year = (enriched.withColumn("year", F.year("contract_date"))
                         .groupBy("supplier_stir", "year")
                         .agg(F.count("*").alias("contracts"),
                              F.sum("amount").alias("contract_amount"),
                              F.sum("paid").alias("paid_amount")))
        overpaid = (enriched.where(F.col("paid") > F.col("amount") * (1 + overpayment_tolerance))
                    .select("contract_id", "buyer_id", "supplier_stir", "amount", "paid",
                            (F.col("paid") - F.col("amount")).alias("overpaid")))

        out_dir.mkdir(parents=True, exist_ok=True)
        supplier_year.write.mode("overwrite").parquet(str(out_dir / "spark_supplier_year"))
        overpaid.write.mode("overwrite").parquet(str(out_dir / "spark_overpaid_contracts"))
        return {"supplier_year_rows": supplier_year.count(), "overpaid_contracts": overpaid.count()}
    finally:
        spark.stop()


if __name__ == "__main__":
    silver, out = Path(sys.argv[1]), Path(sys.argv[2])
    print(run(silver, out))

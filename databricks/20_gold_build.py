# Databricks notebook source
from pyspark.sql import functions as F

SILVER = "abfss://silver@stuaebankdev417.dfs.core.windows.net"
GOLD = "abfss://gold@stuaebankdev417.dfs.core.windows.net/delta"
rd = lambda name: spark.read.format("delta").load(f"{SILVER}/delta/{name}")

def save(df, name):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{GOLD}/{name}")
    print(name, "rows:", spark.read.format("delta").load(f"{GOLD}/{name}").count())

# dim_date: every day from 1 Jan to 30 Sep 2026
dates = spark.sql("SELECT explode(sequence(to_date('2026-01-01'), to_date('2026-09-30'), interval 1 day)) AS date")
dim_date = dates.select(
    (F.year("date") * 10000 + F.month("date") * 100 + F.dayofmonth("date")).alias("date_key"),
    "date", F.year("date").alias("year"), F.month("date").alias("month"),
    F.date_format("date", "MMM").alias("month_name"), F.dayofmonth("date").alias("day"),
    F.date_format("date", "EEEE").alias("day_name"),
    F.dayofweek("date").isin(1, 7).alias("is_weekend"))
save(dim_date, "dim_date")

# dim_merchant: real merchants plus two special members
m = rd("merchants").select("merchant_id", "name", "mcc", "category", "risk_tier", "emirate", "city")
extra_m = spark.createDataFrame([
    ("UNKNOWN", "Unknown merchant", None, "Unknown", "Unknown", "Unknown", "Unknown"),
    ("NO_MERCHANT", "No merchant (transfer, ATM, top-up)", None, "None", "None", "None", "None")], m.schema)
save(m.unionByName(extra_m), "dim_merchant")

# dim_account: real accounts plus one unknown member
a = rd("accounts").select("account_id", "customer_id", "account_type", "currency", "open_date", "status", "branch_code")
extra_a = spark.createDataFrame([("UNKNOWN", None, "Unknown", None, None, "Unknown", None)], a.schema)
save(a.unionByName(extra_a), "dim_account")

# COMMAND ----------

from pyspark.sql import Window

c = spark.read.format("delta").load(f"{SILVER}/dim_customer_scd2")

# one surrogate key per customer VERSION, so a customer with two versions gets two keys
w = Window.orderBy("customer_id", "valid_from")
dim_customer = c.select(
    F.row_number().over(w).cast("int").alias("customer_sk"),
    "customer_id", "first_name", "last_name", "nationality", "emirate", "segment",
    "kyc_status", F.to_date("onboarding_date").alias("onboarding_date"),
    "valid_from", "valid_to", "is_current")

# special member for transactions whose account or customer cannot be found
unknown = spark.createDataFrame(
    [(-1, "UNKNOWN", "Unknown", "Unknown", "Unknown", "Unknown", "Unknown", "Unknown", None,
      None, None, True)],
    dim_customer.schema)

save(dim_customer.unionByName(unknown), "dim_customer")
spark.read.format("delta").load(f"{GOLD}/dim_customer").filter("customer_id = 'C000072'").orderBy("valid_from").show(truncate=False)

# COMMAND ----------

t = rd("transactions")
acc = rd("accounts").select("account_id", F.col("customer_id").alias("acct_customer_id"))
fx = rd("fx_rates").select(F.col("rate_date").alias("fx_date"), F.col("currency").alias("fx_ccy"), "rate_to_aed")
cust = (spark.read.format("delta").load(f"{GOLD}/dim_customer")
        .filter("customer_sk > 0")
        .select("customer_sk", F.col("customer_id").alias("dc_customer_id"),
                F.col("valid_from").alias("dc_from"), F.col("valid_to").alias("dc_to")))

j = (t.withColumn("txn_date", F.to_date("txn_ts"))
       .withColumn("ccy", F.upper(F.trim("currency")))
       .join(acc, "account_id", "left")
       .join(fx, (F.col("txn_date") == F.col("fx_date")) & (F.col("ccy") == F.col("fx_ccy")), "left")
       .join(cust, (F.col("acct_customer_id") == F.col("dc_customer_id"))
                   & (F.col("txn_date") >= F.col("dc_from")) & (F.col("txn_date") <= F.col("dc_to")), "left"))

no = lambda c: F.coalesce(F.col(c), F.lit(False))

fact = (j.select(
            "transaction_id",
            F.date_format("txn_date", "yyyyMMdd").cast("int").alias("date_key"),
            "txn_ts",
            F.coalesce("customer_sk", F.lit(-1)).alias("customer_sk"),
            F.when(no("dq_orphan_account"), "UNKNOWN").otherwise(F.col("account_id")).alias("account_id"),
            F.when(F.col("merchant_id").isNull(), "NO_MERCHANT")
             .when(no("dq_orphan_merchant"), "UNKNOWN").otherwise(F.col("merchant_id")).alias("merchant_id"),
            "txn_type", "channel", "status", "decline_reason", "fraud_flag", "country",
            F.col("ccy").alias("currency"), "amount",
            F.when(F.col("ccy") == "AED", F.lit(1).cast("decimal(18,6)")).otherwise(F.col("rate_to_aed")).alias("fx_rate_to_aed"),
            "dq_missing_amount", "dq_negative_purchase", "dq_orphan_account", "dq_orphan_merchant", "dq_bad_timestamp")
        .withColumn("amount_aed", F.round(F.col("amount") * F.col("fx_rate_to_aed"), 2).cast("decimal(18,2)"))
        .withColumn("is_valid", ~no("dq_missing_amount") & ~no("dq_negative_purchase")
                                & ~no("dq_bad_timestamp") & F.col("amount_aed").isNotNull()))

save(fact, "fact_transactions")
n = spark.read.format("delta").load(f"{GOLD}/fact_transactions").count()
print("fact rows:", n, "| silver rows:", t.count(), "| match:", n == t.count())
print("valid rows:", fact.filter("is_valid").count())
print("rows with unknown customer (-1):", fact.filter("customer_sk = -1").count())
print("non-AED rows with no fx rate:", fact.filter(F.col("fx_rate_to_aed").isNull() & F.col("amount").isNotNull()).count())

# COMMAND ----------

# 1) Independent total from SILVER: valid AED rows only (rate is 1, so no FX involved)
s = (t.filter(~no("dq_missing_amount") & ~no("dq_negative_purchase") & ~no("dq_bad_timestamp"))
      .filter(F.upper(F.trim("currency")) == "AED")
      .agg(F.sum("amount").alias("silver_total")).collect()[0]["silver_total"])

# 2) The same total from GOLD
g = (spark.read.format("delta").load(f"{GOLD}/fact_transactions")
      .filter("is_valid AND currency = 'AED'")
      .agg(F.sum("amount_aed").alias("gold_total")).collect()[0]["gold_total"])
print("silver AED total:", s, "| gold AED total:", g, "| match:", s == g)

# 3) One USD row, so you can check the multiplication yourself
fx_chk = (spark.read.format("delta").load(f"{GOLD}/fact_transactions")
          .filter("currency = 'USD' AND is_valid").limit(3)
          .select("transaction_id", "date_key", "amount", "fx_rate_to_aed", "amount_aed"))
fx_chk.show(truncate=False)
assert s == g, "AED reconciliation failed: silver and gold totals differ"

# COMMAND ----------

EXPORT = "abfss://gold@stuaebankdev417.dfs.core.windows.net/export"
for name in ["dim_date", "dim_merchant", "dim_account", "dim_customer", "fact_transactions"]:
    df = spark.read.format("delta").load(f"{GOLD}/{name}")
    df.coalesce(1).write.mode("overwrite").parquet(f"{EXPORT}/{name}")
    print(name, "exported rows:", spark.read.parquet(f"{EXPORT}/{name}").count())
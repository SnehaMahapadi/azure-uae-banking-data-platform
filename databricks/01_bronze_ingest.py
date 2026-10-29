# Databricks notebook source
from pyspark.sql import functions as F

LAKE = "abfss://bronze@stuaebankdev417.dfs.core.windows.net"
RAW = f"{LAKE}/raw"
DELTA = f"{LAKE}/delta"
EXPECTED = {"customers": 5000, "accounts": 7834, "merchants": 600, "fx_rates": 819}

def add_tracking(df):
    return (df.withColumn("_source_file", F.col("_metadata.file_path"))
              .withColumn("_ingested_at", F.current_timestamp()))

def write_delta(df, name):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{DELTA}/{name}")
    n = spark.read.format("delta").load(f"{DELTA}/{name}").count()
    exp = EXPECTED.get(name)
    print(f"{name}: rows in delta = {n} | expected = {exp} | match = {n == exp}")

csv = lambda p: spark.read.option("header", True).option("inferSchema", False).csv(p)
write_delta(add_tracking(csv(f"{RAW}/customers/load_date=2026-10-18/customers_initial.csv")), "customers")
write_delta(add_tracking(csv(f"{RAW}/accounts/load_date=2026-10-18/accounts.csv")), "accounts")
write_delta(add_tracking(csv(f"{RAW}/fx_rates/load_date=2026-10-18/fx_rates.csv")), "fx_rates")
write_delta(add_tracking(spark.read.option("primitivesAsString", True).json(f"{RAW}/merchants/load_date=2026-10-18/merchants.jsonl")), "merchants")

# COMMAND ----------

for item in dbutils.fs.ls(f"{RAW}/transactions/"):
    print(item.path, [f.name for f in dbutils.fs.ls(item.path)])

df = add_tracking(csv(f"{RAW}/transactions/*/*.csv"))
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{DELTA}/transactions")

t = spark.read.format("delta").load(f"{DELTA}/transactions")
print("transactions total rows:", t.count())
display(t.groupBy("_source_file").count().orderBy("_source_file"))

# COMMAND ----------

print(t.columns)
display(t.limit(10))

# COMMAND ----------

t = spark.read.format("delta").load(f"{DELTA}/transactions")
jan = t.filter(F.col("_source_file").contains("month=2026-01"))
print("Jan rows:", jan.count(), "| distinct transaction_id:", jan.select("transaction_id").distinct().count())
print("All rows:", t.count(), "| distinct transaction_id:", t.select("transaction_id").distinct().count())
display(t.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in t.columns]))

# COMMAND ----------

display(t.groupBy("currency").count().orderBy("currency"))
display(t.groupBy("txn_type").count().orderBy("txn_type"))
display(t.groupBy("status").count().orderBy("status"))

# COMMAND ----------

display(t.groupBy(F.concat(F.lit("["), F.col("currency"), F.lit("]")).alias("currency_exact")).count().orderBy("currency_exact"))

amt = t.withColumn("amount_num", F.col("amount").cast("double"))
display(amt.groupBy("txn_type").agg(
    F.count("*").alias("rows"),
    F.sum((F.col("amount_num") < 0).cast("int")).alias("negative"),
    F.sum(F.col("amount").isNull().cast("int")).alias("missing"),
    F.sum((F.col("amount").isNotNull() & F.col("amount_num").isNull()).cast("int")).alias("not_a_number"),
    F.min("amount_num").alias("min"),
    F.max("amount_num").alias("max")).orderBy("txn_type"))

# COMMAND ----------

acc = spark.read.format("delta").load(f"{DELTA}/accounts").select("account_id").distinct()
mer = spark.read.format("delta").load(f"{DELTA}/merchants").select("merchant_id").distinct()
print("transactions whose account_id is not in accounts:", t.join(acc, "account_id", "left_anti").count())
print("transactions whose merchant_id is not in merchants:", t.filter(F.col("merchant_id").isNotNull()).join(mer, "merchant_id", "left_anti").count())

iso = r"^\d{4}-\d{2}-\d{2}"
bad = t.filter(~F.col("txn_timestamp").rlike(iso))
print("timestamps not starting with yyyy-mm-dd:", bad.count())
display(bad.select("transaction_id", "txn_timestamp", "_source_file").limit(10))

# COMMAND ----------

# DBTITLE 1,Build silver transactions
from pyspark.sql import functions as F

BRONZE = "abfss://bronze@stuaebankdev417.dfs.core.windows.net/delta"
SILVER = "abfss://silver@stuaebankdev417.dfs.core.windows.net/delta"

b = spark.read.format("delta").load(f"{BRONZE}/transactions")
acc = spark.read.format("delta").load(f"{BRONZE}/accounts").select("account_id").distinct().withColumn("_acc_ok", F.lit(1))
mer = spark.read.format("delta").load(f"{BRONZE}/merchants").select("merchant_id").distinct().withColumn("_mer_ok", F.lit(1))

business_cols = [c for c in b.columns if c not in ("_source_file", "_ingested_at")]
s = (b.dropDuplicates(business_cols)
     .withColumn("currency", F.upper(F.trim("currency")))
     .withColumn("amount", F.expr("try_cast(amount as decimal(18,2))"))
     .withColumn("fraud_flag", F.expr("try_cast(fraud_flag as int)"))
     .withColumn("txn_ts", F.expr("coalesce(try_to_timestamp(txn_timestamp, 'dd/MM/yyyy HH:mm'), try_to_timestamp(txn_timestamp))"))
     .join(acc, "account_id", "left")
     .join(mer, "merchant_id", "left")
     .withColumn("dq_missing_amount", F.col("amount").isNull())
     .withColumn("dq_negative_purchase", (F.col("txn_type") == "PURCHASE") & (F.col("amount") < 0))
     .withColumn("dq_orphan_account", F.col("_acc_ok").isNull())
     .withColumn("dq_orphan_merchant", F.col("merchant_id").isNotNull() & F.col("_mer_ok").isNull())
     .withColumn("dq_bad_timestamp", F.col("txn_ts").isNull())
     .drop("_acc_ok", "_mer_ok"))

s.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{SILVER}/transactions")
r = spark.read.format("delta").load(f"{SILVER}/transactions")
print("bronze rows:", b.count(), "| silver rows:", r.count(), "| distinct transaction_id:", r.select("transaction_id").distinct().count())
display(r.agg(*[F.sum(F.col(c).cast("int")).alias(c) for c in r.columns if c.startswith("dq_")]))
display(r.groupBy("currency").count().orderBy("currency"))

# COMMAND ----------

from pyspark.sql import functions as F
BRONZE = "abfss://bronze@stuaebankdev417.dfs.core.windows.net/delta"
SILVER = "abfss://silver@stuaebankdev417.dfs.core.windows.net/delta"

def rd(name):
    return spark.read.format("delta").load(f"{BRONZE}/{name}")

def wr(df, name):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{SILVER}/{name}")
    return spark.read.format("delta").load(f"{SILVER}/{name}").count()

cust = (rd("customers").dropDuplicates(["customer_id"])
        .withColumn("date_of_birth", F.expr("try_cast(date_of_birth as date)"))
        .withColumn("onboarding_date", F.expr("try_cast(onboarding_date as date)")))
acct = (rd("accounts").dropDuplicates(["account_id"])
        .withColumn("currency", F.upper(F.trim("currency")))
        .withColumn("open_date", F.expr("try_cast(open_date as date)")))
fx = (rd("fx_rates")
      .withColumn("currency", F.upper(F.trim("currency")))
      .withColumn("rate_date", F.expr("try_cast(rate_date as date)"))
      .withColumn("rate_to_aed", F.expr("try_cast(rate_to_aed as decimal(18,6))")))
mer = (rd("merchants").dropDuplicates(["merchant_id"])
       .withColumn("emirate", F.col("location.emirate"))
       .withColumn("city", F.col("location.city"))
       .drop("location")
       .withColumn("onboarded_date", F.expr("try_cast(onboarded_date as date)")))

for name, df, exp in [("customers", cust, 5000), ("accounts", acct, 7834), ("fx_rates", fx, 819), ("merchants", mer, 600)]:
    n = wr(df, name)
    print(name, "silver rows:", n, "| expected:", exp, "| match:", n == exp)

print("null dates: customers", cust.filter(F.col("date_of_birth").isNull() | F.col("onboarding_date").isNull()).count(),
      "| accounts", acct.filter(F.col("open_date").isNull()).count(),
      "| fx_rates", fx.filter(F.col("rate_date").isNull() | F.col("rate_to_aed").isNull()).count(),
      "| merchants", mer.filter(F.col("onboarded_date").isNull()).count())
display(mer.limit(5))

# COMMAND ----------

from pyspark.sql import functions as F
SILVER = "abfss://silver@stuaebankdev417.dfs.core.windows.net/delta"

s = spark.read.format("delta").load(f"{SILVER}/transactions")
s2 = (s.withColumn("file_month", F.regexp_extract("_source_file", r"month=(\d{4}-\d{2})", 1))
       .withColumn("txn_month", F.date_format("txn_ts", "yyyy-MM"))
       .withColumn("is_late", F.col("txn_month") < F.col("file_month")))

display(s2.groupBy("file_month").agg(F.count("*").alias("rows"),
        F.sum(F.col("is_late").cast("int")).alias("late_rows")).orderBy("file_month"))
display(s2.filter("is_late").select("transaction_id", "txn_timestamp", "file_month", "txn_month").limit(10))

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

BRONZE = "abfss://bronze@stuaebankdev417.dfs.core.windows.net/delta"
SILVER = "abfss://silver@stuaebankdev417.dfs.core.windows.net/delta"
SILVER_TXN = f"{SILVER}/transactions"

def build_silver_txn(b):
    acc = spark.read.format("delta").load(f"{BRONZE}/accounts").select("account_id").distinct().withColumn("_acc_ok", F.lit(1))
    mer = spark.read.format("delta").load(f"{BRONZE}/merchants").select("merchant_id").distinct().withColumn("_mer_ok", F.lit(1))
    business_cols = [c for c in b.columns if c not in ("_source_file", "_ingested_at")]
    return (b.dropDuplicates(business_cols)
        .withColumn("currency", F.upper(F.trim("currency")))
        .withColumn("amount", F.expr("try_cast(amount as decimal(18,2))"))
        .withColumn("fraud_flag", F.expr("try_cast(fraud_flag as int)"))
        .withColumn("txn_ts", F.expr("coalesce(try_to_timestamp(txn_timestamp, 'dd/MM/yyyy HH:mm'), try_to_timestamp(txn_timestamp))"))
        .join(acc, "account_id", "left")
        .join(mer, "merchant_id", "left")
        .withColumn("dq_missing_amount", F.col("amount").isNull())
        .withColumn("dq_negative_purchase", (F.col("txn_type") == "PURCHASE") & (F.col("amount") < 0))
        .withColumn("dq_orphan_account", F.col("_acc_ok").isNull())
        .withColumn("dq_orphan_merchant", F.col("merchant_id").isNotNull() & F.col("_mer_ok").isNull())
        .withColumn("dq_bad_timestamp", F.col("txn_ts").isNull())
        .drop("_acc_ok", "_mer_ok"))

def merge_month(month):
    b = (spark.read.format("delta").load(f"{BRONZE}/transactions")
         .filter(F.col("_source_file").contains(f"month={month}/")))
    src = build_silver_txn(b)
    (DeltaTable.forPath(spark, SILVER_TXN).alias("t")
        .merge(src.alias("s"), "t.transaction_id = s.transaction_id")
        .whenNotMatchedInsertAll().execute())
    return spark.read.format("delta").load(SILVER_TXN).count()

print("merge function ready")

# COMMAND ----------

cnt = lambda: spark.read.format("delta").load(SILVER_TXN).count()
print("1. silver rows now:", cnt())
spark.sql(f"DELETE FROM delta.`{SILVER_TXN}` WHERE _source_file LIKE '%month=2026-09/%'")
print("2. after deleting month 09:", cnt())
print("3. after merge #1:", merge_month("2026-09"))
print("4. after merge #2 (same month again):", merge_month("2026-09"))

# COMMAND ----------

cnt = lambda: spark.read.format("delta").load(SILVER_TXN).count()
late = lambda: (spark.read.format("delta").load(SILVER_TXN)
    .filter(F.col("_source_file").contains("month=2026-02/") & (F.date_format("txn_ts", "yyyy-MM") < "2026-02")).count())

print("1. late rows (Jan-dated, in Feb file):", late(), "| silver rows:", cnt())
spark.sql(f"DELETE FROM delta.`{SILVER_TXN}` WHERE _source_file LIKE '%month=2026-02/%'")
print("2. after deleting month 02: late rows:", late(), "| silver rows:", cnt())
print("3. after merge #1: silver rows:", merge_month("2026-02"), "| late rows:", late())
print("4. after merge #2: silver rows:", merge_month("2026-02"), "| late rows:", late())

# COMMAND ----------

p = "abfss://bronze@stuaebankdev417.dfs.core.windows.net/raw/customers/load_date=2026-10-18/customer_changes.csv"
chg = spark.read.option("header", True).csv(p)
print(chg.count())
chg.show(5, truncate=False)

# COMMAND ----------

B = "abfss://bronze@stuaebankdev417.dfs.core.windows.net"
chg = spark.read.option("header", True).csv(f"{B}/raw/customers/load_date=2026-10-18/customer_changes.csv")
cust = spark.read.format("delta").load(f"{B}/delta/customers")

print("change rows:", chg.count(), "| distinct customers changed:", chg.select("customer_id").distinct().count())

# does old_value match the starting value in the customers table?
j = chg.join(cust.select("customer_id", "segment", "emirate"), "customer_id", "left")
bad = j.filter(
    ((j.attribute == "segment") & (j.old_value != j.segment)) |
    ((j.attribute == "emirate") & (j.old_value != j.emirate))
).count()
print("changes where old_value does not match the customer's start value:", bad)

# COMMAND ----------

from pyspark.sql import functions as F
S = "abfss://silver@stuaebankdev417.dfs.core.windows.net"

cols = ["customer_id","first_name","last_name","date_of_birth","gender",
        "nationality","emirate","segment","kyc_status","onboarding_date"]
base = cust.select(*cols)
c = chg.select("customer_id","attribute","new_value",
               F.to_date("effective_date","yyyy-MM-dd").alias("effective_date"))
j = base.join(c, "customer_id", "left")

END = F.lit("9999-12-31").cast("date")
START = F.lit("2026-01-01").cast("date")

unchanged = j.filter(F.col("attribute").isNull()).select(
    *cols, START.alias("valid_from"), END.alias("valid_to"), F.lit(True).alias("is_current"))

changed = j.filter(F.col("attribute").isNotNull())

old_rows = changed.select(
    *cols, START.alias("valid_from"),
    F.date_sub("effective_date", 1).alias("valid_to"), F.lit(False).alias("is_current"))

new_rows = changed.select(
    "customer_id","first_name","last_name","date_of_birth","gender","nationality",
    F.when(F.col("attribute") == "emirate", F.col("new_value")).otherwise(F.col("emirate")).alias("emirate"),
    F.when(F.col("attribute") == "segment", F.col("new_value")).otherwise(F.col("segment")).alias("segment"),
    "kyc_status","onboarding_date",
    F.col("effective_date").alias("valid_from"), END.alias("valid_to"), F.lit(True).alias("is_current"))

scd = unchanged.unionByName(old_rows).unionByName(new_rows)
scd.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(f"{S}/dim_customer_scd2")

d = spark.read.format("delta").load(f"{S}/dim_customer_scd2")
print("total rows:", d.count(), "| current rows:", d.filter("is_current").count())
print("customers with 2 rows:", d.groupBy("customer_id").count().filter("count = 2").count())
display(d.filter("customer_id = 'C000072'").orderBy("valid_from"))
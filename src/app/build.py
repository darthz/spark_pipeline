import os
import pandas as pd
from functions import get_app_version, camel_to_snake
from spark_utils.delta_spark import initialize_spark
from pyspark.sql.functions import lit, col, explode, to_date, trim, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, LongType, FloatType
from datetime import datetime


def save_to_delta_silver(spark, bronze_filename="perdcomp_bronze.parquet", silver_filename="perdcomp_silver"):
    # Read parquet from bronze
    bronze_path = os.path.join("storage", "bronze", bronze_filename)
    df = spark.read.parquet(bronze_path)
    print("Colunas disponíveis na bronze:", df.columns)
    # Salva como Delta na silver
    output_dir = os.path.join("storage", "silver")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, silver_filename)
    df.write.format("delta").mode("overwrite").save(output_path)
    print(f"Delta salvo em: {output_path}")

def save_to_delta_gold(spark, silver_filename="perdcomp_silver", gold_filename="perdcomp_gold"):
    """
    Lê os dados da silver, faz casts, seleciona e renomeia as colunas desejadas, adiciona data_processamento e salva na gold.
    """

    silver_path = os.path.join("storage", "silver", silver_filename)
    gold_path = os.path.join("storage", "gold", gold_filename)

    df = spark.read.format("delta").load(silver_path)

    # Limpeza: remove espaços e tudo que não for dígito (opcional)
    df = df.withColumn(
        "numero_perdcomp",
        regexp_replace(trim(col("numero_perdcomp")), "[^0-9]", "")
    )

    # Filtra linhas onde numero_perdcomp ficou vazio após limpeza
    df = df.filter(col("numero_perdcomp") != "")

    df_gold = df.select(
        col("numero_perdcomp"),  # Mantém como string
        col("tipo_documento"),
        col("tipo_credito"),
        col("situacao"),
        col("descricao_situacao"),
        col("contribuinte_ni").cast(LongType()).alias("cnpj"),
        col("contribuinte_nome").alias("razao_social"),
        to_date(col("data_transmissao")).alias("data_transmissao"),
    )

    # data_processamento as current date
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    df_gold = df_gold.withColumn("data_processamento", to_date(lit(data_hoje)))

    df_gold.write.format("delta").mode("overwrite").save(gold_path)
    print(f"Delta gold salvo em: {gold_path}")
    spark.stop()

def save_perdcomp_to_parquet_bronze(data, filename="perdcomp_bronze.parquet"):

    output_dir = os.path.join("storage", "bronze")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, filename)

    clean_data = [item for item in data if item is not None]
    expanded = []
    for item in clean_data:
        if isinstance(item, list):
            expanded.extend(item)
        else:
            expanded.append(item)

    if expanded:
        df = pd.json_normalize(expanded)
        # Padronize column names to snake_case
        df.columns = [camel_to_snake(col.replace('.', '_')) for col in df.columns]
    else:
        df = pd.DataFrame()

    df.to_parquet(output_path, index=False)
    print(f"Arquivo salvo em: {output_path}")


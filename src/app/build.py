import os, re, random, requests
import pandas as pd
from functions import get_app_version
from spark_utils.delta_spark import initialize_spark
from pyspark.sql.functions import lit, col, explode, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, LongType
from datetime import datetime
from time import sleep


SLEEP_TIME_RANGE = (1, 3)  


def get_perdcomp_for_cnpjs(cnpj_list, data_inicial, data_final):
    headers = {
        "versao_app": f'"{get_app_version()}"',
    }
    params = {
        "dataInicial": data_inicial,
        "dataFinal": data_final,
    }
    results = []
    total = len(cnpj_list)
    for idx, cnpj in enumerate(cnpj_list, start=1):
        cnpj_fix = str(cnpj).zfill(14)
        print(f"perdcomp cnpj: {cnpj_fix} ({idx} de {total})")
        try:
            response = requests.get(
                f"https://p-app-receita-federal.estaleiro.serpro.gov.br/servicos-rfb-apprfb-perdcomp/apprfb-perdcomp/consulta/ni/{cnpj_fix}",
                params=params,
                headers=headers,
                timeout=20,
            )
            response.raise_for_status()
            results.append(response.json())
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 412:
                print(f"get_perdcomp: {cnpj_fix} - 412 Client Error: Precondition Failed")
                results.append(None)
            else:
                print(f"get_perdcomp: {cnpj_fix} - {e}")
                results.append(None)
        except Exception as e:
            print(f"get_perdcomp: {cnpj_fix} - {e}")
            results.append(None)
        sleep_time = random.uniform(*SLEEP_TIME_RANGE)
        print(f"get_perdcomp: Sleeping for: {sleep_time:.2f} seconds")
        sleep(sleep_time)
    return results


def camel_to_snake(name):
    import re
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

def save_to_delta_silver(spark, bronze_filename="perdcomp_bronze.parquet", silver_filename="perdcomp_silver"):
    import os
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

    print("Colunas disponíveis na silver:", df.columns)

    # Casts and column selection
    df_gold = df.select(
        col("numero_perdcomp").cast(LongType()),
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

    def camel_to_snake(name):
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

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


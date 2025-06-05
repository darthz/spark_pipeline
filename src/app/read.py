from spark_utils.delta_spark import initialize_spark
import os

def read_delta_gold(gold_filename="perdcomp_gold"):
    """
    Lê os dados da camada gold e exibe no console.
    """
    spark = initialize_spark("ReadGold")
    gold_path = os.path.join("storage", "gold", gold_filename)
    try:
        df = spark.read.format("delta").load(gold_path)
        df.show(truncate=True)
        df.printSchema()
        print(df.count())
    except Exception as e:
        print(f"Erro ao ler a camada gold: {e}")
    finally:
        spark.stop()

# read_delta_gold()

# from spark_utils.delta_spark import initialize_spark
# import os

def read_delta_gold_and_save_excel(gold_filename="perdcomp_gold", excel_filename="perdcomp_gold.xlsx"):
    """
    Lê os dados da camada gold e salva em um arquivo Excel.
    """
    spark = initialize_spark("ReadGold")
    gold_path = os.path.join("storage", "gold", gold_filename)
    try:
        df = spark.read.format("delta").load(gold_path)
        pdf = df.toPandas()
        pdf.to_excel(excel_filename, index=False)
        print(f"Arquivo Excel salvo como: {excel_filename}")
    except Exception as e:
        print(f"Erro ao ler ou salvar a camada gold: {e}")
    finally:
        spark.stop()

# read_delta_gold_and_save_excel()

from spark_utils.delta_spark import initialize_spark
import os

def read_delta_silver(silver_filename="perdcomp_silver"):
    """
    Lê os dados da camada silver e exibe no console.
    """
    spark = initialize_spark("ReadSilver")
    silver_path = os.path.join("storage", "silver", silver_filename)
    try:
        df = spark.read.format("delta").load(silver_path)
        df.printSchema()  # Mostra o schema da tabela silver
        df.show(truncate=True)
    except Exception as e:
        print(f"Erro ao ler a camada silver: {e}")
    finally:
        spark.stop()

# Exemplo de uso:
read_delta_silver()

def show_distinct_perdcomps_from_silver(silver_filename="perdcomp_silver"):
    """
    Lê os dados da camada silver e mostra todos os numero_perdcomp distintos.
    """
    spark = initialize_spark("ReadSilverDistinct")
    silver_path = os.path.join("storage", "silver", silver_filename)
    try:
        df = spark.read.format("delta").load(silver_path)
        df.select("numero_perdcomp").distinct().show(truncate=False)
    except Exception as e:
        print(f"Erro ao ler ou mostrar os perdcomps distintos: {e}")
    finally:
        spark.stop()

# Exemplo de uso:
# show_distinct_perdcomps_from_silver()


from pyspark.sql.functions import col, trim

def show_invalid_perdcomps_from_silver(silver_filename="perdcomp_silver"):
    """
    Mostra os numero_perdcomp da camada silver que são nulos, vazios ou inválidos.
    """
    spark = initialize_spark("ReadSilverInvalid")
    silver_path = os.path.join("storage", "silver", silver_filename)
    try:
        df = spark.read.format("delta").load(silver_path)
        df.filter(
            col("numero_perdcomp").isNull() | 
            (trim(col("numero_perdcomp")) == "") |
            (col("numero_perdcomp") == "null")
        ).select("numero_perdcomp").distinct().show(truncate=False)
    except Exception as e:
        print(f"Erro ao ler ou mostrar perdcomps inválidos: {e}")
    finally:
        spark.stop()

# Exemplo de uso:
# show_invalid_perdcomps_from_silver()

def count_gold(gold_filename="perdcomp_gold"):
    """
    Conta o número de registros na camada gold.
    """
    spark = initialize_spark("CountGold")
    gold_path = os.path.join("storage", "gold", gold_filename)
    try:
        df = spark.read.format("delta").load(gold_path)
        print(f"Total de registros na gold: {df.count()}")
    except Exception as e:
        print(f"Erro ao contar registros na gold: {e}")
    finally:
        spark.stop()

# Exemplo de uso:
# count_gold()

import pandas as pd

def ler_parquet(filename="perdcomp_bronze.parquet"):
    """
    Lê um arquivo Parquet e retorna um DataFrame.
    
    Parâmetros:
        caminho_arquivo (str): Caminho para o arquivo .parquet
    
    Retorna:
        pd.DataFrame: DataFrame com os dados do parquet
    """
    bronze_path = os.path.join("storage", "bronze", filename)
    try:
        df = pd.read_parquet(bronze_path)
        print("✅ Arquivo lido com sucesso. Preview:")
        print(df.columns)
        return df
    except Exception as e:
        print(f"❌ Erro ao ler o arquivo: {e}")
        return None
    
# ler_parquet()
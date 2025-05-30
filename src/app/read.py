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
    except Exception as e:
        print(f"Erro ao ler a camada gold: {e}")
    finally:
        spark.stop()

read_delta_gold()


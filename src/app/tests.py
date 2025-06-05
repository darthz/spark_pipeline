from build import (
    save_perdcomp_to_parquet_bronze,
    save_to_delta_silver,
    save_to_delta_gold,
)
import time, os
from crawler import get_perdcomp_for_cnpjs
from datetime import date

# # 2. Datas para consulta
data_inicial = "2010-01-01"
data_final = date.today().strftime("%Y-%m-%d")

# # # 4. Consulta a API
empresas_ativas = ['ag', 'jvs']
for empresa in empresas_ativas:
    dados_api = get_perdcomp_for_cnpjs(data_inicial, filename=f"cnpjs_{empresa}.txt")

# from pandas import DataFrame

# df = DataFrame(dados_api)
# df.to_json(orient="records", force_ascii=False, indent=2, path_or_buf="dados_api.json")
# print(df)

# # # 5. Salva bronze
save_perdcomp_to_parquet_bronze(dados_api)

# # # 3. Inicializa a sessão Spark
# from spark_utils.delta_spark import initialize_spark
# spark = initialize_spark("MeuApp")

# # # # 6. Salva silver (lê bronze do disco)
# try:
#     save_to_delta_silver(spark)
# except Exception as e:
#     print(f"Erro ao salvar na camada silver: {e}")

# # Aguarda alguns segundos para garantir que os dados sejam gravados
# time.sleep(5)

# # # 7. Salva gold
# try:
#     save_to_delta_gold(spark)
# except Exception as e:
#     print(f"Erro ao salvar na camada gold: {e}")

# # spark.stop()

# # # 8. Lê e exibe a camada gold
# def read_delta_gold(gold_filename="perdcomp_gold"):
#     """
#     Lê os dados da camada gold e exibe no console.
#     """
#     spark = initialize_spark("ReadGold")
#     gold_path = os.path.join("storage", "gold", gold_filename)
#     try:
#         df = spark.read.format("delta").load(gold_path)
#         df.show(truncate=True)
#     except Exception as e:
#         print(f"Erro ao ler a camada gold: {e}")
#     finally:
#         spark.stop()

# read_delta_gold()



# def read_bronze_head(n=10, bronze_filename="perdcomp_bronze.parquet"):
#     """
#     Lê e exibe os primeiros n registros da camada bronze.
#     """
#     import pandas as pd
#     import os

#     bronze_path = os.path.join("storage", "bronze", bronze_filename)
#     try:
#         df = pd.read_parquet(bronze_path)
#         print(df.head(n))
#     except Exception as e:
#         print(f"Erro ao ler a camada bronze: {e}")

# # # Exemplo de uso:
# read_bronze_head()


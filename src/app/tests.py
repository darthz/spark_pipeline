import os, boto3

# Defina suas credenciais AWS aqui (NÃO SUBA ISSO PARA O GIT!)
os.environ["AWS_ACCESS_KEY_ID"] = "3bc0d05a0bef4bf2b318ee4c5469eb05"
os.environ["AWS_SECRET_ACCESS_KEY"] = "8676829b449d47289e50a3470dcbe647"


from build import (
    save_raw_to_s3,
    save_to_trusted,
    save_to_refined,
    save_to_enriched,
)
from crawler import get_perdcomp_for_cnpjs
from datetime import date, datetime
from spark_utils.delta_spark import initialize_spark

def get_today_str():
    return datetime.now().strftime("%Y%m%d")

# 1. Parâmetros
data_inicial = "2010-01-01"
data_final = date.today().strftime("%Y-%m-%d")
# raw_filename = f"perdcomp_raw_{get_today_str()}.parquet"
# trusted_filename = f"perdcomp_trusted_{get_today_str()}"
# refined_filename = f"perdcomp_refined_{get_today_str()}"
# enriched_filename = f"perdcomp_enriched_{get_today_str()}"

s3 = boto3.client(
    's3',
    endpoint_url='https://s3.bhs.io.cloud.ovh.net',
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
)

bucket = "drivalake"
prefix = "perdcomp/src/"
response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

print("Inicializando Spark...")
spark = initialize_spark("TestPerdcompPipeline")

print("Arquivos .txt em perdcomp/src:")
txt_files = []
for obj in response.get('Contents', []):
    key = obj['Key']
    if key.endswith('.txt'):
        txt_files.append(os.path.basename(key))

for file in txt_files:
    nome = file.replace("cnpjs_", "").replace(".txt", "")
    print(f"Consultando API para {file}...")
    dados_api = get_perdcomp_for_cnpjs(data_inicial, filename=file)
    raw_filename = f'raw_{nome}_{get_today_str()}.parquet'
    trusted_filename = f'trusted_{nome}_{get_today_str()}'

    save_raw_to_s3(dados_api, filename=raw_filename)
    save_to_trusted(spark, raw_filename=raw_filename, trusted_filename=trusted_filename)




# # Configure as credenciais e endpoint para S3-compatible service (OVH)
# hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
# hadoop_conf.set("fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
# hadoop_conf.set("fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])

# # Configurações específicas para OVH Object Storage
# hadoop_conf.set("fs.s3a.endpoint", "s3.bhs.io.cloud.ovh.net")  # Endpoint correto do OVH
# hadoop_conf.set("fs.s3a.path.style.access", "true")  # Necessário para OVH
# hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
# hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

# # Configurações adicionais para OVH
# hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
# hadoop_conf.set("fs.s3a.attempts.maximum", "3")
# hadoop_conf.set("fs.s3a.connection.establish.timeout", "5000")
# hadoop_conf.set("fs.s3a.connection.timeout", "200000")




# # 2. Consulta a API
# print("Consultando API...")
# dados_api = get_perdcomp_for_cnpjs(data_inicial, filename="cnpjs_test.txt")

# # 3. Salva camada raw no S3
# print("Salvando camada RAW...")
# save_raw_to_s3(dados_api, filename=raw_filename)


# print("Configurações S3A aplicadas para OVH Object Storage")

# # # 5. Salva camada trusted
# print("Salvando camada TRUSTED...")
# save_to_trusted(spark, raw_filename=raw_filename, trusted_filename=trusted_filename)

# # 6. Salva camada refined 
# print("Salvando camada REFINED...")
# # Corrigir: usar trusted_path em vez de trusted_filename
# trusted_path = f"s3a://drivalake/perdcomp/storage/trusted/{trusted_filename}"
# save_to_refined(spark, trusted_path=trusted_path, refined_filename=refined_filename)

# # 7. Salva camada enriched (com grupos de CNPJs)
# print("Salvando camada ENRICHED...")
# refined_path = f"s3a://drivalake/perdcomp/storage/refined/{refined_filename}"
# save_to_enriched(spark, refined_path=refined_path, enriched_base_filename=enriched_filename)

# spark.stop()
# print("Pipeline concluído!")


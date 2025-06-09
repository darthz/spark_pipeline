import boto3
import os
from spark_utils.delta_spark import initialize_spark
from datetime import datetime

os.environ["AWS_ACCESS_KEY_ID"] = "3bc0d05a0bef4bf2b318ee4c5469eb05"
os.environ["AWS_SECRET_ACCESS_KEY"] = "8676829b449d47289e50a3470dcbe647"

def get_today_str():
    return datetime.now().strftime("%Y%m%d")

# Inicializar Spark com Delta Lake
print("Inicializando Spark...")
spark = initialize_spark("ReadTrustedData")

# Configure as credenciais e endpoint para S3-compatible service (OVH)
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
hadoop_conf.set("fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
hadoop_conf.set("fs.s3a.endpoint", "s3.bhs.io.cloud.ovh.net")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")

print("Configurações S3A aplicadas para OVH Object Storage")

# Teste básico de conectividade com boto3
print("\n=== Teste de Conectividade ===")
s3 = boto3.client('s3',
    endpoint_url='https://s3.bhs.io.cloud.ovh.net',
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    region_name='bhs'
)

try:
    response = s3.list_objects_v2(Bucket='drivalake', Prefix='perdcomp/storage/raw/')
    print("✓ Credenciais funcionam - listando arquivos na trusted:")
    
    if 'Contents' in response:
        for obj in response['Contents']:
            print(f"  - {obj['Key']} (Size: {obj['Size']} bytes)")
    else:
        print("  Nenhum arquivo encontrado na camada trusted")
        
except Exception as e:
    print(f"✗ Erro de conectividade: {e}")

# Ler dados da camada trusted
print("\n=== Lendo Dados da Trusted ===")
try:
    # Caminho da tabela Delta na trusted (usando o nome de hoje)
    trusted_filename = f"perdcomp_trusted_{get_today_str()}"
    trusted_path = f"s3a://drivalake/perdcomp/storage/trusted/{trusted_filename}"
    
    print(f"Lendo Delta Lake de: {trusted_path}")
    
    # Ler a tabela Delta
    df_trusted = spark.read.format("delta").load(trusted_path)
    
    # Mostrar informações básicas
    print(f"✓ Dados carregados com sucesso!")
    print(f"  Número de registros: {df_trusted.count()}")
    print(f"  Número de colunas: {len(df_trusted.columns)}")
    
    # Mostrar schema
    print("\n=== Schema dos Dados ===")
    df_trusted.printSchema()
    
    # Mostrar algumas linhas de exemplo
    print("\n=== Primeiras 10 linhas ===")
    df_trusted.show(10, truncate=True)
    
    # Estatísticas básicas
    print("\n=== Estatísticas por Situação ===")
    df_trusted.groupBy("situacao").count().orderBy("count", ascending=False).show()
    
    print("\n=== Estatísticas por Tipo de Documento ===")
    df_trusted.groupBy("tipo_documento").count().orderBy("count", ascending=False).show()
    
    # Filtro de exemplo
    print("\n=== Exemplo: Registros com situação específica ===")
    df_filtrado = df_trusted.filter(df_trusted.situacao == "ATIVO")
    print(f"Registros ativos: {df_filtrado.count()}")
    df_filtrado.select("numero_perdcomp", "razao_social", "cnpj", "data_transmissao").show(5)
    
except Exception as e:
    print(f"✗ Erro ao ler dados da trusted: {e}")

# Ler dados da camada raw
print("\n=== Lendo Dados da Raw ===")
try:
    # Listar arquivos no prefixo raw
    response = s3.list_objects_v2(Bucket='drivalake', Prefix='perdcomp/storage/raw/')
    raw_files = []
    if 'Contents' in response:
        for obj in response['Contents']:
            key = obj['Key']
            # Ignorar "diretórios"
            if not key.endswith('/'):
                raw_files.append(f"s3a://drivalake/{key}")
    else:
        print("Nenhum arquivo encontrado na camada raw.")

    if raw_files:
        print(f"Arquivos encontrados na raw: {len(raw_files)}")
        for f in raw_files:
            print(f"  - {f}")

        # Exemplo: lendo todos como CSV (ajuste o formato conforme necessário)
        df_raw = spark.read.option("header", "true").csv(raw_files)
        print(f"✓ Dados raw carregados! Registros: {df_raw.count()}, Colunas: {len(df_raw.columns)}")
        df_raw.printSchema()
        df_raw.show(10, truncate=True)
    else:
        print("Nenhum arquivo raw para ler.")

except Exception as e:
    print(f"✗ Erro ao ler dados da raw: {e}")

# Fechar Spark
print("\n=== Finalizando ===")
spark.stop()
print("Spark finalizado")
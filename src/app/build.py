import os
import pandas as pd
import boto3
from datetime import datetime
from io import BytesIO
from pyspark.sql.functions import col, to_date, lit, trim, regexp_replace
from pyspark.sql.types import LongType
from spark_utils.delta_spark import initialize_spark
from sqlalchemy import create_engine
from dotenv import load_dotenv

def get_today_str():
    return datetime.now().strftime("%Y%m%d")

def save_raw_to_s3(data, filename=None):
    """
    Salva os dados crus do crawler em Parquet no S3, adicionando data_processamento.
    Caminho: s3://drivalake/perdcomp/storage/raw/
    """
    data_processamento = datetime.now().strftime("%Y-%m-%d")
    if filename is None:
        filename = f"perdcomp_raw_{get_today_str()}.parquet"
    clean_data = [item for item in data if item is not None]
    expanded = []
    for item in clean_data:
        if isinstance(item, list):
            expanded.extend(item)
        else:
            expanded.append(item)
    if expanded:
        df = pd.json_normalize(expanded)
        df["data_processamento"] = data_processamento
    else:
        df = pd.DataFrame()
    
    # Use o endpoint correto do OVH
    s3 = boto3.client('s3',
        endpoint_url='https://s3.bhs.io.cloud.ovh.net',
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name='bhs'
    )
    
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(
        Bucket="drivalake",
        Key=f"perdcomp/storage/raw/{filename}",
        Body=buffer.getvalue()
    )
    print(f"Arquivo salvo em: s3://drivalake/perdcomp/storage/raw/{filename}")

def save_to_trusted(spark, raw_filename=None, trusted_filename=None):
    """
    Lê Parquet da raw (via boto3 e Pandas), seleciona e tipa colunas, salva como Delta na trusted.
    Caminho: s3a://drivalake/perdcomp/storage/trusted/
    """
    if raw_filename is None:
        raw_filename = f"perdcomp_raw_{get_today_str()}.parquet"
    if trusted_filename is None:
        trusted_filename = f"perdcomp_trusted_{get_today_str()}"  # Faltava fechar as aspas aqui
    
    # Caminho do arquivo Parquet na camada raw no S3
    bucket_name = 'drivalake'
    raw_key = f"perdcomp/storage/raw/{raw_filename}"
    trusted_path = f"s3a://drivalake/perdcomp/storage/trusted/{trusted_filename}"

    print(f"Lendo arquivo raw do S3: s3://{bucket_name}/{raw_key}")

    try:
        # Configure boto3 para usar o endpoint correto do OVH
        s3 = boto3.client('s3',
            endpoint_url='https://s3.bhs.io.cloud.ovh.net',
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            region_name='bhs'
        )
        
        # Teste de conectividade
        print("Testando conectividade com o bucket...")
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix='perdcomp/storage/', MaxKeys=1)
        print("✓ Bucket acessível")
        
        # Agora tente ler o arquivo
        response = s3.get_object(Bucket=bucket_name, Key=raw_key)
        parquet_content = response['Body'].read()
        
        # Ler o conteúdo do Parquet em um DataFrame Pandas
        df_pandas = pd.read_parquet(BytesIO(parquet_content))
        
        # Converter DataFrame Pandas para DataFrame Spark
        if df_pandas.empty:
            print(f"Arquivo raw {raw_filename} está vazio ou não pôde ser lido como Parquet.")
            return
        df = spark.createDataFrame(df_pandas)
        
    except Exception as e:
        print(f"Erro ao ler o arquivo Parquet da camada raw (s3://{bucket_name}/{raw_key}) com boto3/pandas: {e}")
        return

    print(f"Arquivo raw {raw_filename} lido e convertido para DataFrame Spark com sucesso.")
    
    # Transformações para criar df_trusted
    df_trusted = df.select(
        col("numeroPerdcomp").alias("numero_perdcomp"),
        col("tipoDocumento").alias("tipo_documento"),
        col("tipoCredito").alias("tipo_credito"),
        col("situacao"), 
        col("descricaoSituacao").alias("descricao_situacao"),
        col("`contribuinte.ni`").cast(LongType()).alias("cnpj"),
        col("`contribuinte.nome`").alias("razao_social"),
        to_date(col("dataTransmissao")).alias("data_transmissao"),
        col("data_processamento")
    )
    
    print(f"DataFrame transformado: {df_trusted.schema}")
    
    try:
        # Salvar diretamente como Delta Lake
        df_trusted.write.format("delta").mode("overwrite").save(trusted_path)
        print(f"Delta trusted salvo em: {trusted_path}")
        
    except Exception as e:
        print(f"Erro ao salvar no S3-compatible storage: {e}")
        print("Verifique se o endpoint S3 está configurado corretamente no Spark")
        return

def save_to_refined(spark, trusted_path=None, refined_filename=None):
    """
    Junta todos os trusted em uma tabela única e salva como Delta na refined.
    Caminho: s3a://drivalake/perdcomp/storage/refined/
    """
    if trusted_path is None:
        trusted_path = f"s3a://drivalake/perdcomp/storage/trusted/perdcomp_trusted_{get_today_str()}" # Helper get_today_str()
    if refined_filename is None:
        refined_filename = f"perdcomp_refined_{get_today_str()}" # Helper get_today_str()
    
    refined_path = f"s3a://drivalake/perdcomp/storage/refined/{refined_filename}"
    # Spark reads the trusted Delta table
    df = spark.read.format("delta").load(trusted_path) 
    df.write.format("delta").mode("overwrite").save(refined_path)
    print(f"Delta refined salvo em: {refined_path}")

def upload_enriched_to_postgres(df_enriched, grupo_nome):
    """
    Faz upload dos dados enriched para PostgreSQL
    """
    try:
        # Load variables from .env
        load_dotenv()
        
        host = os.getenv("PG_HOST")
        port = os.getenv("PG_PORT", "5432")
        db = os.getenv("PG_DB")
        user = os.getenv("PG_USER")
        password = os.getenv("PG_PASS")
        schema = os.getenv("SCHEMA", "public")
        
        table_name = f"enriched_{grupo_nome}"
        
        print(f"📤 Iniciando upload para PostgreSQL...")
        print(f"  Host: {host}:{port}")
        print(f"  Database: {db}")
        print(f"  Schema: {schema}")
        print(f"  Tabela: {table_name}")
        
        # Converter Spark DataFrame para Pandas
        df_pandas = df_enriched.toPandas()
        print(f"  Registros para upload: {len(df_pandas)}")
        
        # Criar connection string
        conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
        
        # Conectar e fazer upload
        engine = create_engine(conn_str)
        df_pandas.to_sql(
            table_name, 
            engine, 
            if_exists="replace",  # Full load
            index=False, 
            schema=schema
        )
        
        print(f"✅ Upload concluído para {schema}.{table_name}")
        return True
        
    except Exception as e:
        print(f"❌ Erro no upload para PostgreSQL: {e}")
        return False

def save_to_enriched(spark, refined_path=None, enriched_base_filename=None):
    """
    Lê a refined e cria tabelas enriched filtradas por grupos de CNPJs dos arquivos .txt no S3.
    Para cada arquivo cnpjs_*.txt, cria uma tabela enriched_* correspondente.
    Também faz upload automático para PostgreSQL.
    Caminho: s3a://drivalake/perdcomp/storage/enriched/
    """
    if refined_path is None:
        refined_path = f"s3a://drivalake/perdcomp/storage/refined/perdcomp_refined_{get_today_str()}"
    if enriched_base_filename is None:
        enriched_base_filename = f"perdcomp_enriched_{get_today_str()}"
    
    print(f"Lendo dados da refined: {refined_path}")
    
    try:
        # Ler a tabela refined
        df_refined = spark.read.format("delta").load(refined_path)
        print(f"✓ Dados refined carregados: {df_refined.count()} registros")
        
        # Configure boto3 para listar e ler os arquivos de CNPJs
        s3 = boto3.client('s3',
            endpoint_url='https://s3.bhs.io.cloud.ovh.net',
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            region_name='bhs'
        )
        
        # Listar arquivos CNPJs no S3
        print("Procurando arquivos de CNPJs no S3...")
        bucket_name = 'drivalake'
        cnpj_files_prefix = 'perdcomp/src/'  # Prefixo onde os arquivos de CNPJs estão armazenados
        
        try:
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=cnpj_files_prefix)
            
            if 'Contents' not in response:
                print("❌ Nenhum arquivo de CNPJs encontrado no S3")
                return
                
            cnpj_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.txt') and 'cnpjs_' in obj['Key']]
            
            if not cnpj_files:
                print("❌ Nenhum arquivo .txt de CNPJs encontrado")
                return
                
            print(f"✓ Encontrados {len(cnpj_files)} arquivos de CNPJs:")
            for file in cnpj_files:
                print(f"  - {file}")
            
            # Debug: mostrar quais grupos serão processados
            grupos_encontrados = []
            for file in cnpj_files:
                file_name = file.split('/')[-1]
                if file_name.startswith('cnpjs_') and file_name.endswith('.txt'):
                    grupo = file_name.replace('cnpjs_', '').replace('.txt', '')
                    grupos_encontrados.append(grupo)
            
            print(f"\n🎯 Grupos que serão processados: {grupos_encontrados}")
            print(f"📊 Esperado criar tabelas: {[f'enriched_{g}' for g in grupos_encontrados]}")
                
        except Exception as e:
            print(f"❌ Erro ao listar arquivos de CNPJs: {e}")
            return
        
        # Contador de sucessos
        sucessos = 0
        total_arquivos = len(cnpj_files)
        
        # Processar cada arquivo de CNPJs
        for cnpj_file_key in cnpj_files:
            try:
                # Extrair o nome do grupo do arquivo (ex: cnpjs_jvs.txt -> jvs)
                file_name = cnpj_file_key.split('/')[-1]  # Pega apenas o nome do arquivo
                if not file_name.startswith('cnpjs_') or not file_name.endswith('.txt'):
                    print(f"⚠️  Ignorando arquivo {file_name} (formato incorreto)")
                    continue
                    
                grupo_nome = file_name.replace('cnpjs_', '').replace('.txt', '')
                
                print(f"\n{'='*50}")
                print(f"🔄 Processando grupo: {grupo_nome.upper()}")
                print(f"{'='*50}")
                
                # Ler o arquivo de CNPJs do S3
                response = s3.get_object(Bucket=bucket_name, Key=cnpj_file_key)
                cnpj_content = response['Body'].read().decode('utf-8')
                
                # Processar CNPJs (remover espaços, quebras de linha, etc.)
                cnpjs_list = []
                for line in cnpj_content.strip().split('\n'):
                    cnpj = line.strip()
                    if cnpj and cnpj.isdigit():  # Só aceita CNPJs numéricos
                        cnpjs_list.append(int(cnpj))  # Converter para int para match com a coluna
                
                if not cnpjs_list:
                    print(f"⚠️  Nenhum CNPJ válido encontrado em {file_name}")
                    continue
                    
                print(f"📋 Carregados {len(cnpjs_list)} CNPJs do arquivo {file_name}")
                print(f"   Primeiros 5 CNPJs: {cnpjs_list[:5]}")
                
                # Filtrar dados da refined pelos CNPJs do grupo
                df_enriched = df_refined.filter(df_refined.cnpj.isin(cnpjs_list))
                
                registros_encontrados = df_enriched.count()
                print(f"🔍 Encontrados {registros_encontrados} registros para o grupo {grupo_nome}")
                
                if registros_encontrados == 0:
                    print(f"⚠️  Nenhum registro encontrado para os CNPJs do grupo {grupo_nome}")
                    continue
                
                # Adicionar coluna identificando o grupo
                df_enriched = df_enriched.withColumn("grupo", lit(grupo_nome))
                
                # Definir caminho de saída para este grupo
                enriched_path = f"s3a://drivalake/perdcomp/storage/enriched/perdcomp_enriched_{grupo_nome}_{get_today_str()}"
                
                # Salvar como Delta Lake
                print(f"💾 Salvando em Delta Lake...")
                df_enriched.write.format("delta").mode("overwrite").save(enriched_path)
                print(f"✅ Delta enriched salvo em: {enriched_path}")
                
                # Upload para PostgreSQL
                print(f"🚀 Iniciando upload para PostgreSQL...")
                upload_success = upload_enriched_to_postgres(df_enriched, grupo_nome)
                
                if upload_success:
                    sucessos += 1
                    print(f"✅ Grupo {grupo_nome} processado com sucesso!")
                else:
                    print(f"❌ Falha no upload do grupo {grupo_nome}")
                
                # Mostrar algumas estatísticas do grupo
                print(f"\n📊 Estatísticas do grupo {grupo_nome}:")
                print(f"   - Total de registros: {registros_encontrados}")
                print(f"   - CNPJs únicos: {df_enriched.select('cnpj').distinct().count()}")
                
                # Mostrar distribuição por situação
                situacoes = df_enriched.groupBy("situacao").count().collect()
                print(f"   - Distribuição por situação:")
                for row in situacoes:
                    print(f"     • {row.situacao}: {row.count} registros")
                
            except Exception as e:
                print(f"❌ Erro ao processar arquivo {cnpj_file_key}: {e}")
                continue
                
        print(f"\n{'='*50}")
        print(f"🎉 PROCESSAMENTO CONCLUÍDO!")
        print(f"{'='*50}")
        print(f"✅ Grupos processados com sucesso: {sucessos}/{total_arquivos}")
        print(f"📊 Tabelas criadas no PostgreSQL:")
        
        # Listar as tabelas que deveriam ter sido criadas
        grupos_esperados = ['jvs', 'ag', 'grupo_bahia']
        for grupo in grupos_esperados:
            if any(grupo in file for file in cnpj_files):
                print(f"   - perdcomp.enriched_{grupo}")
        
    except Exception as e:
        print(f"❌ Erro geral na função save_to_enriched: {e}")
        return

if __name__ == "__main__":
    # Inicializa Spark
    spark = initialize_spark()
    
    # Exemplo de execução sequencial das etapas
    # 1. Trusted
    save_to_trusted(spark)
    # 2. Refined
    save_to_refined(spark)
    # 3. Enriched + upload
    save_to_enriched(spark)
    
    spark.stop()

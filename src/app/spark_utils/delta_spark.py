from pyspark.sql import SparkSession
import pyspark
# import delta # Comentado ou removido, pois delta.__version__ está causando erro.
              # A versão será obtida via importlib.metadata.
import importlib.metadata # Adicionado para obter a versão do pacote

def initialize_spark(app_name: str = 'DrivaPerdcomp-SparkDelta'):
    """
    Inicializa uma SparkSession com suporte a Delta Lake e Catalyst Optimizer.

    Args:
        app_name (str): Nome da aplicação Spark.

    Returns:
        SparkSession: Objeto SparkSession configurado.
    """
    print("=" * 100)
    print(f"{'INICIANDO SPARK'.center(100, '=')}")
    print("=" * 100)

    # Constrói a string completa de pacotes: Delta Lake + S3
    
    # Tenta obter a versão do pacote 'delta-spark' instalado via pip.
    try:
        delta_lib_version = importlib.metadata.version('delta-spark')
    except importlib.metadata.PackageNotFoundError:
        # Se o pacote não for encontrado, usa a versão do log do Ivy como fallback.
        # Isso garante que estamos alinhados com o JAR que o Spark tentará usar.
        delta_lib_version = "3.3.2" # Baseado no log: found io.delta#delta-spark_2.12;3.3.2
        print(f"Aviso: Pacote 'delta-spark' não encontrado por importlib.metadata. Usando a versão {delta_lib_version} (do log do Ivy) para a coordenada do Spark.")
        # Considere verificar se o pacote 'delta-spark' está corretamente instalado no seu ambiente virtual.

    delta_package_coordinate = f"io.delta:delta-spark_2.12:{delta_lib_version}"
    s3_hadoop_packages = "org.apache.hadoop:hadoop-aws:3.3.4"
    s3_sdk_packages = "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    
    all_packages_str = f"{delta_package_coordinate},{s3_hadoop_packages},{s3_sdk_packages}"

    print(f"Configurando Spark com os seguintes pacotes: {all_packages_str}")

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", all_packages_str)  # Define a lista completa de pacotes
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') # Configuração para Delta Lake
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') # Configuração para Delta Lake
        # Catalyst Optimizer já vem ativado por padrão
        # Outras configs de performance podem ser adicionadas aqui
    )

    # Ao definir explicitamente o pacote Delta e suas configurações SQL acima,
    # a chamada a configure_spark_with_delta_pip(builder) torna-se redundante para essas configurações.
    # Se ela fizer outras configurações essenciais não cobertas, precisaria ser mantida e investigada,
    # mas para o problema dos pacotes, a configuração manual e explícita é mais segura.
    # Vamos remover a chamada a configure_spark_with_delta_pip para evitar conflitos.
    # spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')
    spark.conf.set("spark.sql.shuffle.partitions", 8)

    print('SPARK | DELTA | INICIALIZADO')
    print(f'PySpark Version: {pyspark.__version__}')
    try:
        # Tenta imprimir a versão obtida para confirmação
        print(f'Delta Lake Version (for Spark package): {delta_lib_version}')
    except Exception:
        print('Delta Lake Version: não encontrada')

    return spark

# spark = (
#     SparkSession.builder
#     .appName("TestPerdcompPipeline")
#     .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
#     .getOrCreate()
# )
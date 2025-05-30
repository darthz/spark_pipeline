from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
import pyspark
import delta

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

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
        # Catalyst Optimizer já vem ativado por padrão
        # Outras configs de performance podem ser adicionadas aqui
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    spark.conf.set("spark.sql.shuffle.partitions", 8)

    print('SPARK | DELTA | INICIALIZADO')
    print(f'PySpark Version: {pyspark.__version__}')
    try:
        print(f'Delta Lake Version: {delta.__version__}')
    except Exception:
        print('Delta Lake Version: não encontrada')

    return spark

# spark = initialize_spark("MeuApp")
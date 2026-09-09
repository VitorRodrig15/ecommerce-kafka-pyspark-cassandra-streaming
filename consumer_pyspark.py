import subprocess

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, explode, expr, round as _round, to_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType


def validar_java():
    """Valida se a versão do Java instalada é compatível com o PySpark 3.5.1."""
    try:
        resultado = subprocess.run(
            ["java", "-version"], capture_output=True, text=True, check=False
        )
        versao = resultado.stderr or resultado.stdout
        primeira_linha = versao.splitlines()[0] if versao else "versão desconhecida"
        versao_maior = int(primeira_linha.split('"')[1].split('.')[0])
        
        if versao_maior > 17:
            raise RuntimeError(
                f"Java incompatível ({primeira_linha}). "
                "PySpark 3.5.1 deve ser executado com Java 17 ou anterior."
            )
    except (IndexError, ValueError):
        pass

def criar_sessao_spark():
    """Inicializa a sessão do PySpark com os conectores Kafka e Cassandra."""
    return SparkSession.builder \
        .appName("EcommerceSalesStreamingProcessor") \
        .master("local[*]") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
        .config("spark.cassandra.connection.host", "127.0.0.1") \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.sql.shuffle.partitions", "3") \
        .getOrCreate()

def gravar_no_cassandra(target_df, batch_id):
    """Grava cada micro-batch de streaming diretamente na tabela do Cassandra."""
    target_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "e_commerce") \
        .option("table", "compras_por_cliente") \
        .mode("append") \
        .save()

def main():
    validar_java()
    spark = criar_sessao_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("⚡ Inicializando Consumer PySpark -> Cassandra (Structured Streaming)...")

    # Schemas alinhados com o Producer
    schema_produto = StructType([
        StructField("nome_produto", StringType(), True),
        StructField("quantidade", IntegerType(), True),
        StructField("preco_unitario", DoubleType(), True),
        StructField("subtotal", DoubleType(), True)
    ])

    schema_venda = StructType([
        StructField("id_ordem", StringType(), True),
        StructField("documento_cliente", StringType(), True),
        StructField("produtos_comprados", ArrayType(schema_produto), True),
        StructField("quantidade_total_itens", IntegerType(), True),
        StructField("valor_total_venda", DoubleType(), True),
        StructField("data_hora_venda", StringType(), True)
    ])

    # 1. Leitura do Stream do Kafka
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "e-commerce-vendas") \
        .option("startingOffsets", "latest") \
        .load()

    # 2. Parse do JSON e Mapeamento para o Schema do Cassandra
    # As colunas precisam corresponder à tabela: cliente_id, data_compra, compra_id, valor_total, status, produtos
    df_cassandra = df_kafka.selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), schema_venda).alias("data")) \
        .select(
            expr("uuid()").alias("cliente_id"),  # Gera o UUID para Partition Key
            to_timestamp(col("data.data_hora_venda"), "dd/MM/yyyy HH:mm:ss").alias("data_compra"),
            expr("uuid()").alias("compra_id"),   # Gera o UUID para Clustering Key
            col("data.valor_total_venda").cast("decimal(10,2)").alias("valor_total"),
            expr("'PAGO'").alias("status"),
            expr("transform(data.produtos_comprados, x -> x.nome_produto)").alias("produtos")
        )

    # 3. Persistência Contínua no Cassandra
    query = df_cassandra.writeStream \
        .foreachBatch(gravar_no_cassandra) \
        .option("checkpointLocation", "./checkpoints/kafka_to_cassandra") \
        .start()

    print("📊 Ingestão em Tempo Real Kafka -> Cassandra Iniciada com Sucesso!\n")
    query.awaitTermination()

if __name__ == "__main__":
    main()
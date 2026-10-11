
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window,
    sum as spark_sum, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType
)

spark = (
    SparkSession.builder
    .appName("StreamingInventarioKafka")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

esquema = StructType([
    StructField("fecha", StringType(), True),
    StructField("producto", StringType(), True),
    StructField("tipo", StringType(), True),
    StructField("cantidad", IntegerType(), True),
    StructField("precio_unitario", DoubleType(), True),
])

mensajes = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "movimientos_inventario")
    .option("startingOffsets", "latest")
    .load()
)

eventos = (
    mensajes
    .selectExpr("CAST(value AS STRING) AS contenido")
    .select(from_json(col("contenido"), esquema).alias("dato"))
    .select("dato.*")
    .withColumn("fecha_evento", to_timestamp(col("fecha")))
    .filter(
        col("producto").isNotNull()
        & col("tipo").isin("ENTRADA", "VENTA")
        & (col("cantidad") > 0)
    )
)

resumen = (
    eventos
    .withWatermark("fecha_evento", "1 minute")
    .groupBy(
        window(col("fecha_evento"), "30 seconds"),
        col("producto")
    )
    .agg(
        spark_sum(
            when(col("tipo") == "ENTRADA", col("cantidad"))
            .otherwise(0)
        ).alias("unidades_entrada"),
        spark_sum(
            when(col("tipo") == "VENTA", col("cantidad"))
            .otherwise(0)
        ).alias("unidades_vendidas"),
        spark_sum(
            when(
                col("tipo") == "VENTA",
                col("cantidad") * col("precio_unitario")
            ).otherwise(0)
        ).alias("ingresos_ventas")
    )
)

consulta = (
    resumen.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation",
            "/home/vboxuser/proyecto_bigdata_inventarios/checkpoints/streaming")
    .start()
)

print("Spark Streaming conectado a Kafka.")
consulta.awaitTermination()

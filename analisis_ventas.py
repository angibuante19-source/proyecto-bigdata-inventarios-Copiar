from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    count,
    month,
    year,
    round
)

# =====================================================
# 1. Crear la sesión de Apache Spark
# =====================================================
spark = (
    SparkSession.builder
    .appName("AnalisisInteligenteInventarios")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Rutas del proyecto
ruta = "/home/vboxuser/proyecto_bigdata_inventarios/datos/online_retail.csv"
salida = "/home/vboxuser/proyecto_bigdata_inventarios/resultados"

print("=" * 60)
print("ANÁLISIS INTELIGENTE DE INVENTARIOS Y VENTAS")
print("=" * 60)

# =====================================================
# 2. Cargar los datos
# =====================================================
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(ruta)
)

registros_originales = df.count()
print("REGISTROS ORIGINALES:", registros_originales)

# =====================================================
# 3. Limpiar registros incompletos y ventas no válidas
# =====================================================
datos_limpios = df.dropna(
    subset=[
        "InvoiceNo",
        "StockCode",
        "Description",
        "Quantity",
        "InvoiceDate",
        "UnitPrice",
        "Country"
    ]
)

ventas = datos_limpios.filter(
    (col("Quantity") > 0) &
    (col("UnitPrice") > 0)
)

# =====================================================
# 4. Calcular ingresos por línea de venta
# =====================================================
ventas = ventas.withColumn(
    "Ingresos",
    round(col("Quantity") * col("UnitPrice"), 2)
)

registros_validos = ventas.count()

print("REGISTROS DESPUÉS DE LIMPIAR:", registros_validos)
print("REGISTROS EXCLUIDOS:", registros_originales - registros_validos)

# =====================================================
# 5. Resumen general
# =====================================================
resumen = ventas.agg(
    count("*").alias("Lineas_Venta"),
    spark_sum("Quantity").alias("Unidades_Vendidas"),
    round(spark_sum("Ingresos"), 2).alias("Ingresos_Totales")
)

print("\nRESUMEN GENERAL:")
resumen.show(truncate=False)

# =====================================================
# 6. Diez productos con mayores ingresos
# =====================================================
print("\nTOP 10 PRODUCTOS POR INGRESOS:")

top_productos = (
    ventas.groupBy("StockCode", "Description")
    .agg(
        round(spark_sum("Ingresos"), 2)
        .alias("Ingresos_Totales")
    )
    .orderBy(col("Ingresos_Totales").desc())
    .limit(10)
)

top_productos.show(truncate=False)

# =====================================================
# 7. Diez países con mayores ingresos
# =====================================================
print("\nTOP 10 PAISES POR INGRESOS:")

paises = (
    ventas.groupBy("Country")
    .agg(
        round(spark_sum("Ingresos"), 2)
        .alias("Ingresos_Totales")
    )
    .orderBy(col("Ingresos_Totales").desc())
    .limit(10)
)

paises.show(truncate=False)

# =====================================================
# 8. Ingresos por año y mes
# =====================================================
print("\nINGRESOS POR MES:")

ingresos_mensuales = (
    ventas.groupBy(
        year("InvoiceDate").alias("Anio"),
        month("InvoiceDate").alias("Mes")
    )
    .agg(
        round(spark_sum("Ingresos"), 2)
        .alias("Ingresos_Totales")
    )
    .orderBy("Anio", "Mes")
)

ingresos_mensuales.show(100, truncate=False)

# =====================================================
# 9. Guardar los resultados para el informe
# =====================================================
resumen.coalesce(1).write.mode("overwrite").option(
    "header", True
).csv(salida + "/resumen")

top_productos.coalesce(1).write.mode("overwrite").option(
    "header", True
).csv(salida + "/top_productos")

paises.coalesce(1).write.mode("overwrite").option(
    "header", True
).csv(salida + "/paises")

ingresos_mensuales.coalesce(1).write.mode("overwrite").option(
    "header", True
).csv(salida + "/ingresos_mensuales")

print("\n" + "=" * 60)
print("ANALISIS TERMINADO CORRECTAMENTE")
print("Resultados guardados en:", salida)
print("=" * 60)

spark.stop()
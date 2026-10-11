
import csv
from pyspark.sql import SparkSession

RUTA = (
    "/home/vboxuser/proyecto_bigdata_inventarios/"
    "datos/online_retail.csv"
)

spark = (
    SparkSession.builder
    .appName("AnalisisRDDInventarios")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
sc = spark.sparkContext


def leer_fila(linea):
    return next(csv.reader([linea]))


lineas = sc.textFile(RUTA)
cabecera = lineas.first()

datos = (
    lineas
    .filter(lambda linea: linea != cabecera)
    .map(leer_fila)
    .filter(lambda fila: len(fila) == 8)
)


def convertir_venta(fila):
    try:
        pais = fila[7].strip()
        cantidad = int(fila[3])
        precio = float(fila[5])

        if cantidad > 0 and precio > 0:
            return [(pais, cantidad * precio)]
        return []
    except (ValueError, IndexError):
        return []


ventas_por_pais = (
    datos
    .flatMap(convertir_venta)
    .reduceByKey(lambda a, b: a + b)
    .sortBy(lambda dato: dato[1], ascending=False)
)

print("\nTOP 10 PAISES POR INGRESOS - ANALISIS RDD")

for pais, ingresos in ventas_por_pais.take(10):
    print(f"{pais}: {ingresos:,.2f}")

spark.stop()

# Análisis inteligente de inventarios y ventas con Big Data

## 1. Descripción del proyecto

Este proyecto utiliza Apache Spark para analizar datos históricos de ventas y Apache Kafka con Spark Structured Streaming para demostrar el procesamiento de movimientos simulados en tiempo real.

El objetivo es identificar patrones de ingresos por país, productos y periodos, y procesar eventos de entrada y venta conforme llegan al sistema.

## 2. Problema

El análisis de grandes volúmenes de transacciones permite identificar productos y mercados que generan mayores ingresos. El procesamiento por lotes permite estudiar datos históricos, mientras que el procesamiento en tiempo real permite analizar nuevos eventos de manera continua.

## 3. Conjunto de datos

Se utiliza Online Retail del UCI Machine Learning Repository.

- Registros: 541.909.
- Columnas: 8.
- Campos principales: InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID y Country.
- Fuente: https://archive.ics.uci.edu/dataset/352/online+retail

## 4. Tecnologías utilizadas

- Python 3.10.
- Apache Spark 3.5.8 y PySpark.
- Apache Kafka 3.9.2.
- Java 11.
- pandas y matplotlib.

## 5. Archivos del proyecto

- `analisis_ventas.py`: limpieza, transformación y análisis batch mediante Spark DataFrames.
- `analisis_rdd.py`: análisis de ingresos por país mediante RDD.
- `generador_movimientos.py`: genera movimientos simulados y los envía a Kafka.
- `streaming_inventario.py`: procesa eventos con Spark Structured Streaming.
- `graficos_ventas.py`: genera gráficos de productos, países e ingresos mensuales.

## 6. Preparación de los datos

Descargar Online Retail desde la fuente oficial. Colocar `Online Retail.xlsx` en la carpeta `datos` y convertirlo a CSV mediante `convertir_datos.py`.

El archivo esperado es `datos/online_retail.csv`.

## 7. Ejecución batch con Spark

Desde la carpeta del proyecto, ejecutar:

```bash
/opt/spark/bin/spark-submit analisis_ventas.py
```

Para ejecutar el análisis RDD:

```bash
/opt/spark/bin/spark-submit analisis_rdd.py
```

El procesamiento permite analizar los ingresos por país, los productos principales y la evolución mensual de las ventas.

## 8. Procesamiento en tiempo real

Se utiliza el tema de Kafka `movimientos_inventario`, disponible en `localhost:9092`.

Iniciar Spark Streaming con el conector compatible con Spark 3.5.8:

```bash
/opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 \
  streaming_inventario.py
```

En otra terminal, ejecutar el generador:

```bash
.venv/bin/python generador_movimientos.py
```

Spark procesa los eventos recibidos y muestra las unidades de entrada, las unidades vendidas y los ingresos por producto.

## 9. Resultados

El análisis batch permitió identificar los países con mayores ingresos y estudiar los resultados por productos y meses. El análisis RDD calculó los ingresos por país utilizando operaciones distribuidas de Spark.

La demostración de Kafka y Spark Streaming procesó movimientos simulados en ventanas de 30 segundos.

## 10. Limitaciones

Los movimientos generados para el streaming son simulados y no representan el inventario real de una empresa. Los resultados históricos corresponden al conjunto Online Retail.

## 11. Autoría

Proyecto académico de Big Data.

Estudiante: [Escribe tu nombre completo]

Curso: [Escribe el nombre del curso]

Institución: [Escribe el nombre de la institución]

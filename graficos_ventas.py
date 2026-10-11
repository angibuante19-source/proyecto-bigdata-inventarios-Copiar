import os
import glob
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

BASE = "/home/vboxuser/proyecto_bigdata_inventarios/resultados"
SALIDA = "/home/vboxuser/proyecto_bigdata_inventarios/graficos"
os.makedirs(SALIDA, exist_ok=True)


def leer_resultado(nombre):
    carpeta = os.path.join(BASE, nombre)
    archivos = glob.glob(os.path.join(carpeta, "part-*.csv"))

    if not archivos:
        raise FileNotFoundError(
            "No se encontraron resultados en " + carpeta
        )

    return pd.read_csv(archivos[0])


# Grafico 1: productos con mayores ingresos
productos = leer_resultado("top_productos")
productos = productos.sort_values(
    "Ingresos_Totales", ascending=True
)

plt.figure(figsize=(12, 7))
plt.barh(
    productos["Description"].astype(str),
    productos["Ingresos_Totales"]
)
plt.xlabel("Ingresos")
plt.title("Top 10 productos por ingresos")
plt.tight_layout()
plt.savefig(
    os.path.join(SALIDA, "top_productos.png"),
    dpi=150
)
plt.close()


# Grafico 2: ingresos por pais
paises = leer_resultado("paises")
paises = paises.sort_values(
    "Ingresos_Totales", ascending=True
)

plt.figure(figsize=(10, 6))
plt.barh(
    paises["Country"].astype(str),
    paises["Ingresos_Totales"]
)
plt.xlabel("Ingresos")
plt.title("Top 10 paises por ingresos")
plt.tight_layout()
plt.savefig(
    os.path.join(SALIDA, "ingresos_paises.png"),
    dpi=150
)
plt.close()


# Grafico 3: evolucion mensual de los ingresos
mensual = leer_resultado("ingresos_mensuales")
mensual["Periodo"] = (
    mensual["Anio"].astype(str)
    + "-"
    + mensual["Mes"].astype(int).astype(str).str.zfill(2)
)
mensual = mensual.sort_values(["Anio", "Mes"])

plt.figure(figsize=(12, 6))
plt.plot(
    mensual["Periodo"],
    mensual["Ingresos_Totales"],
    marker="o"
)
plt.xlabel("Ano y mes")
plt.ylabel("Ingresos")
plt.title("Evolucion mensual de los ingresos")
plt.xticks(rotation=60, ha="right")
plt.tight_layout()
plt.savefig(
    os.path.join(SALIDA, "ingresos_mensuales.png"),
    dpi=150
)
plt.close()

print("GRAFICOS CREADOS CORRECTAMENTE")
print("Carpeta de salida:", SALIDA)
---
title: "Formatos de descarga"
description: "Instrucciones de uso para cada formato de descarga de eleccionesdb"
---

Los datos de `eleccionesdb` están disponibles para descarga en tres formatos, pensados para diferentes perfiles de uso y herramientas de análisis.

<div class="callout callout-tip">
<strong>¿Prefieres acceso programático?</strong>
<p><a href="https://hmeleiro.github.io/eleccionesdb-api/" target="_blank" rel="noopener">EleccionesDB API</a> ofrece una API REST gratuita, abierta y sin autenticación para consultar todos estos datos directamente desde tu código.</p>
</div>

<div class="download-options">
  <div class="download-option">
    <span class="download-option-icon">🗂️</span>
    <h3>Parquet</h3>
    <p>Tablas normalizadas en formato columnar. Ideal para R, Python y DuckDB.</p>
    <a href="https://data.spainelectoralproject.com/eleccionesdb-etl/descargas/parquet.zip" class="btn btn-primary">⬇ Descargar Parquet</a>
  </div>
  <div class="download-option">
    <span class="download-option-icon">🗄️</span>
    <h3>SQLite</h3>
    <p>Base de datos relacional en un solo fichero. Ideal para consultas SQL sin servidor.</p>
    <a href="https://data.spainelectoralproject.com/eleccionesdb-etl/descargas/eleccionesdb.sqlite" class="btn btn-primary">⬇ Descargar SQLite</a>
  </div>
  <div class="download-option">
    <span class="download-option-icon">📄</span>
    <h3>CSV planos</h3>
    <p>Tablas pre-joineadas listas para usar en Excel, Google Sheets o cualquier herramienta.</p>
    <a href="https://data.spainelectoralproject.com/eleccionesdb-etl/descargas/csv.zip" class="btn btn-primary">⬇ Descargar CSV</a>
  </div>
</div>

## Parquet

**Ideal para**: análisis de datos con R, Python, DuckDB o cualquier herramienta compatible con Apache Arrow.

### Contenido

Tablas individuales normalizadas en formato columnar:

```
descargas/parquet/
├── dimensiones/
│   ├── elecciones.parquet
│   ├── partidos.parquet
│   ├── partidos_recode.parquet
│   ├── territorios.parquet
│   └── tipos_eleccion.parquet
└── hechos/
    ├── resumen_territorial.parquet
    └── votos_territoriales.parquet
```

### Ventajas

- Formato columnar altamente eficiente en espacio y velocidad de lectura.
- Conserva tipos de datos (enteros, fechas, texto).
- Ideal para cargar en data frames y hacer análisis exploratorio.
- Compatible con DuckDB para consultas SQL directamente sobre los ficheros.

### Cómo usarlo en R

```r
library(arrow)

# Leer una tabla
elecciones <- read_parquet("descargas/parquet/dimensiones/elecciones.parquet")
votos <- read_parquet("descargas/parquet/hechos/votos_territoriales.parquet")

# Unir dimensiones con hechos
library(dplyr)
votos_con_partido <- votos |>
  left_join(read_parquet("descargas/parquet/dimensiones/partidos.parquet"),
            by = c("partido_id" = "id"))
```

### Cómo usarlo en Python

```python
import pandas as pd

# Leer una tabla
elecciones = pd.read_parquet("descargas/parquet/dimensiones/elecciones.parquet")
votos = pd.read_parquet("descargas/parquet/hechos/votos_territoriales.parquet")

# Con PyArrow
import pyarrow.parquet as pq
table = pq.read_table("descargas/parquet/hechos/votos_territoriales.parquet")
df = table.to_pandas()
```

---

## SQLite

**Ideal para**: consultas SQL, exploración relacional, prototipos de aplicaciones.

### Contenido

Un único fichero `descargas/eleccionesdb.sqlite` con el esquema relacional completo:

- Todas las tablas de dimensiones y hechos.
- Claves primarias (PKs) y claves foráneas (FKs).
- Restricciones UNIQUE.
- Índices para consultas eficientes.

### Ventajas

- Base de datos relacional autocontenida en un solo fichero.
- No requiere servidor: funciona directamente en local.
- Permite consultas SQL complejas con JOINs, agregaciones, subqueries.
- Esquema con integridad referencial.

### Cómo usarlo en R

```r
library(DBI)
library(RSQLite)

con <- dbConnect(SQLite(), "descargas/eleccionesdb.sqlite")

# Listar tablas
dbListTables(con)

# Consulta SQL
votos_psoe <- dbGetQuery(con, "
  SELECT e.year, e.descripcion, SUM(v.votos) as total_votos
  FROM votos_territoriales v
  JOIN elecciones e ON v.eleccion_id = e.id
  JOIN partidos p ON v.partido_id = p.id
  JOIN partidos_recode pr ON p.partido_recode_id = pr.id
  WHERE pr.partido_recode = 'PSOE'
  GROUP BY e.id
  ORDER BY e.year
")

dbDisconnect(con)
```

### Cómo usarlo en Python

```python
import sqlite3
import pandas as pd

con = sqlite3.connect("descargas/eleccionesdb.sqlite")

# Listar tablas
tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", con)

# Consulta SQL
df = pd.read_sql("""
    SELECT e.year, e.descripcion, SUM(v.votos) as total_votos
    FROM votos_territoriales v
    JOIN elecciones e ON v.eleccion_id = e.id
    JOIN partidos p ON v.partido_id = p.id
    JOIN partidos_recode pr ON p.partido_recode_id = pr.id
    WHERE pr.partido_recode = 'PSOE'
    GROUP BY e.id
    ORDER BY e.year
""", con)

con.close()
```

---

## CSV planos (pre-joineados)

**Ideal para**: uso rápido, hojas de cálculo, importación directa sin necesidad de JOINs.

### Contenido

Dos ficheros CSV con las tablas de hechos ya unidas con todas sus dimensiones:

```
descargas/csv/
├── resumen_territorial.csv
└── votos_territoriales.csv
```

Cada fila incluye tanto los datos numéricos (votos, censo, participación) como los campos descriptivos de las dimensiones (nombre del territorio, siglas del partido, descripción de la elección, etc.).

### Ventajas

- Listos para usar: no requieren JOINs ni esquema relacional.
- Abribles directamente en Excel, Google Sheets o cualquier editor de texto.
- Formato universal compatible con cualquier herramienta.

### Cómo usarlo en R

```r
library(readr)

resumen <- read_csv("descargas/csv/resumen_territorial.csv")
votos <- read_csv("descargas/csv/votos_territoriales.csv")
```

### Cómo usarlo en Python

```python
import pandas as pd

resumen = pd.read_csv("descargas/csv/resumen_territorial.csv")
votos = pd.read_csv("descargas/csv/votos_territoriales.csv")
```

---

## Comparativa de formatos

| Característica | Parquet | SQLite | CSV planos |
|----------------|---------|--------|------------|
| **Estructura** | Tablas normalizadas individuales | Esquema relacional con FKs | Tablas pre-joineadas |
| **Requiere JOINs** | Sí | Sí (SQL) | No |
| **Tamaño en disco** | Muy compacto | Compacto | Grande |
| **Tipos de datos** | Preservados | Preservados | Todo como texto |
| **Consultas SQL** | Vía DuckDB | Nativo | No |
| **Herramientas** | R, Python, DuckDB, Spark | Cualquier cliente SQL | Excel, editores de texto, R, Python |
| **Integridad referencial** | No (ficheros sueltos) | Sí (FKs, PKs, UNIQUE) | No aplica |

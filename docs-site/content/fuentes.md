---
title: "Fuentes de datos"
description: "Origen y detalle de las fuentes de datos de eleccionesdb"
---

## Resumen de fuentes

| Elección | Fuente principal | Formato | API/Manual | Cobertura |
|----------|------------------|---------|------------|-----------|
| Congreso | Infoelectoral (Ministerio del Interior) | R package | API (paquete) | Todas las convocatorias |
| Municipales | Infoelectoral (Ministerio del Interior) | R package | API (paquete) | Todas las convocatorias |
| Andalucía | Junta de Andalucía (SIEL) | REST JSON | API | Todas |
| Aragón | Gobierno de Aragón (datos abiertos) | CSV/XLSX | Manual | 2011–2023 |
| Asturias | SADEI + GIPEYOP | .px + XLSX | Scraping + manual | Todas |
| Baleares | IBESTAT + GIPEYOP | .px + XLSX | Manual | 1983–2023 |
| Canarias | PARCAN + ISTAC + GIPEYOP | CSV + XLSX | API + manual | 1983–2023 |
| Cantabria | GIPEYOP | XLSX | Manual | 1983–2023 |
| Castilla-La Mancha | Junta de Castilla-La Mancha (datos abiertos) | CSV/XLSX | Descarga | 1983–2023 |
| Castilla y León | Junta de Castilla y León | CSV (`;`, Latin-1) | Manual | 1983–2022 |
| Cataluña | Generalitat de Catalunya | CSV | Manual | 1980–2023 |
| C. Valenciana | Generalitat Valenciana | CSV + XLSX | Manual | 1983–2023 |
| Extremadura | Junta de Extremadura | XLSX | Manual | 1983–2023 |
| Galicia | Xunta de Galicia | XLS/XLSX | Manual | 1981–2024 |
| C. de Madrid | Comunidad de Madrid | XLS/XLSX | Manual | 1983–2023 |
| Murcia | Región de Murcia | XLS | Manual | 1983–2023 |
| Navarra | Gobierno de Navarra | CSV + XLS | Manual | 1979–2023 |
| País Vasco | Gobierno Vasco | XLS/XLSX | Manual | 1980–2024 |
| La Rioja | Gobierno de La Rioja | XLS | Manual | 1983–2023 |

## Detalle por elección / comunidad

### Congreso (`00-congreso`)

- **Fuente**: Paquete R [`infoelectoral`](https://infoelectoral.spainelectoralproject.com/) (datos del Ministerio del Interior).
- **Método**: Funciones `provincias()`, `municipios()`, `mesas()` del paquete.
- **Granularidad**: Provincia, municipio y sección censal (mesa).
- **Ficheros brutos**: No hay CSV en `data-raw/`; el paquete descarga directamente desde la fuente oficial.
- **Scripts**: `sauron_formats.R` (orquestador) → `format_provincias.R`, `format_municipios.R`, `format_secciones.R`.
- **Notas**: Separa CER (residentes) de CERA (residentes ausentes).

---

### Andalucía (`01-andalucia`)

- **Fuente**: API del [Sistema de Información Electoral de Andalucía](https://resultadoseleccionesandalucia.es/) de la Junta de Andalucía.
- **URL base**: `https://ws040.juntadeandalucia.es/siel-api/v1`
- **Método**: REST API (`httr::GET`) con endpoints: `/opciones/fconvocatoria`, `/opciones/provincia`, `/opciones/municipio`, `/opciones/distrito`, `/opciones/seccion`.
- **Granularidad**: Provincia, municipio y sección censal.
- **Ficheros brutos**: `resumen_{provincias,municipios,secciones}.csv`, `escrutinio_{provincias,municipios,secciones}.csv`, `nomenclators/*.rds`.
- **Scripts**: `00_fetch-nomenclators.R` → `01-06_fetch-info/votos-*.R` (descarga paralela con `future_pmap_dfr`) → `07-format-data.R`.
- **Notas**: Descarga paralela con rate limiting (5 reintentos, 2s de pausa base). CERA en municipio=901. Agrega a nivel CCAA.

---

### Aragón (`02-aragon`)

- **Fuente**: [Portal de datos abiertos](https://opendata.aragon.es/) del Gobierno de Aragón.
- **Método**: Ficheros CSV/XLSX descargados manualmente.
- **Granularidad**: Municipal y provincial (histórico); mesa/sección en elecciones recientes.
- **Ficheros brutos**: `Resultados electorales agrupados a escala municipal (histórico).csv`, `Resultados electorales agrupados a escala provincial (histórico).xlsx`, carpetas por año (`2011/`, `2015/`, `2019/`, `2023/`).
- **Scripts**: `01_format-info.R` (resumen/participación), `02_format-votos.R` (votos por partido).
- **Notas**: CERA identificado como municipio='000'. Formatos de tabla variables por año. Cobertura: 2011–2023.

---

### Asturias (`03-asturias`)

- **Fuente**: Doble fuente — [SADEI](https://www.sadei.es/sadei/resultados-electorales/elecciones-autonomicas_249_1_ap.html) (pre-2015) + [GIPEYOP](https://gipeyop.uv.es/gipeyop/sea.html) (2015+).
- **Método**: SADEI: scraping web + lectura de ficheros `.px` (paquete `pxR`). GIPEYOP: lectura directa de xlsx.
- **Granularidad**: Mesa (2015+), municipal (pre-2015).
- **Ficheros brutos**: `SADEI/*.px`, `gipeyop/Asturias{2015,2019,2023}_mesas.xlsx`, `censo_ine/`, `correspondencia_municipio-circunscripcion.xlsx`.
- **Scripts**: `00_get-nomenclator-sadei.R`, `01_format-data.R`.
- **Notas**: Asturias tiene circunscripciones sub-provinciales (Occidente/Centro/Oriente, códigos 331/332/333). Transformaciones pivot complejas para el formato `.px`.

---

### Baleares (`04-baleares`)

- **Fuente**: [IBESTAT](https://ibestat.es/edatos/apps/statistical-visualizer/visualizer/collection.html?resourceType=collection&agencyId=IBESTAT&resourceId=000199A_000001) (Institut d'Estadística de les Illes Balears) + [GIPEYOP](https://gipeyop.uv.es/gipeyop/sea.html).
- **Método**: Lectura de ficheros `.px` (formato PX-WEB estadístico) con `pxR::read.px()`.
- **Granularidad**: Mesa/sección.
- **Ficheros brutos**: Carpetas por año (`1983/`–`2023/`) con ficheros `.px`, `22codislas.xlsx` (mapeo islas), `cera/`, `gipeyop/`.
- **Scripts**: `format.R`.
- **Notas**: Circunscripciones insulares (Mallorca, Menorca, Ibiza, Formentera). Códigos de 12 caracteres (pre-1995) vs 10 caracteres (1995+). Cobertura: 1983–2023.

---

### Canarias (`05-canarias`)

- **Fuente**: [PARCAN](https://www.parcan.es/elecciones/) (1999+) + [ISTAC](https://www.gobiernodecanarias.org/istac/estadisticas/sociedad/elecciones/Elecciones/C00010A.html) + [GIPEYOP](https://gipeyop.uv.es/gipeyop/sea.html) (1983–2015).
- **Método**: PARCAN: descarga CSV por municipio. ISTAC: lectura de xlsx. GIPEYOP: lectura de xlsx.
- **Granularidad**: Municipal (PARCAN), provincial/sección (GIPEYOP/ISTAC).
- **Ficheros brutos**: `parcan/` (CSV por municipio), `istac/` (datasets ISTAC xlsx), `gipeyop/Canarias{1983,1987}_provincias.xlsx`, `Canarias{1991-2015}_secciones.xlsx`.
- **Scripts**: `00_download-data-parcan.R`, `format.R`.
- **Notas**: 7 circunscripciones insulares (Fuerteventura, Gran Canaria, La Gomera, El Hierro, La Palma, Tenerife, Lanzarote) + circunscripción autonómica. Cobertura: 1983–2023.

---

### Castilla y León (`07-cyl`)

- **Fuente**: [Portal de datos abiertos](https://jcyl.opendatasoft.com/explore/dataset/resultados-electorales-1983-actualidad/information/) de la Junta de Castilla y León.
- **Método**: Ficheros CSV descargados manualmente.
- **Granularidad**: Mesa (código de mesa parseado a sección/distrito/municipio/provincia).
- **Ficheros brutos**: Un CSV por año (`1983.csv`–`2022.csv`), delimitados por `;`, codificación Latin-1.
- **Scripts**: `format.R`.
- **Notas**: Parsea la estructura del código de mesa para extraer provincia/municipio/distrito/sección. Agrega por niveles. Cobertura: 1983–2022.

---

### Cataluña (`09-catalunya`)

- **Fuente**: [Generalitat de Catalunya](https://eleccions.gencat.cat/es/resultats-i-eleccions-anteriors/resultats-electorals/#/).
- **Método**: Ficheros CSV descargados (datos abiertos electorales).
- **Granularidad**: Mesa.
- **Ficheros brutos**: Patrón de nombre `A{YYYY}{N}-{TIPO}-ME.csv` (ej: `A20231-Columnes-ME.csv`).
- **Scripts**: `01_format-data.R`.
- **Notas**: Nombres de columna en catalán. Locale con coma decimal y punto de agrupación. CERA en municipio='998'. Cobertura: 1980–2023.

---

### Comunidad Valenciana (`10-comunidad-valenciana`)

- **Fuente**: [Portal de Datos Abiertos](https://portaldadesobertes.gva.es/es) de la Generalitat Valenciana.
- **Método**: Ficheros CSV y xlsx descargados manualmente.
- **Granularidad**: Mesa/sección.
- **Ficheros brutos**: CSV por año (`1987.csv`–`2023.csv`) + `Comunitat Valenciana1983_municipios.xlsx` (1983 en formato especial).
- **Scripts**: `format.R`.
- **Notas**: Naming de columnas variable entre años. Codificación variable. 1983 es xlsx con resultados municipales precalculados. Cobertura: 1983–2023.

---

### Comunidad de Madrid (`13-comunidad-madrid`)

- **Fuente**: [Portal de Datos Abiertos](https://www.comunidad.madrid/gobierno/datos-abiertos) de la Comunidad de Madrid.
- **Método**: Ficheros Excel (.xls/.xlsx) descargados manualmente.
- **Granularidad**: Mesa.
- **Ficheros brutos**: `{año}_Mesas.xls` (1995–2021), `datos_electorales_elecciones_autonomicas_comunidad_de_madrid_2023.xlsx` (2023, formato diferente), `Madrid{1983,1987,1991}_circunscripcion.xlsx` (histórico).
- **Scripts**: `format.R`.
- **Notas**: 2003 tiene dos elecciones (mayo y octubre). Multi-header parsing en Excel. Circunscripción única (provincial). Cobertura: 1983–2023.

---

## Fuentes transversales

### Fechas de elecciones

- **Fichero**: `data-raw/fechas_elecciones.csv` — contiene `tipo_eleccion`, `fecha` (en texto español, ej: "9 de junio de 2024"), `ccaa`.
- **Fuente alternativa**: Scraping de la [Junta Electoral Central](https://www.juntaelectoralcentral.es) (`fechas-elecciones-scrap.R`).

### Códigos territoriales

| Fichero | Descripción |
|---------|-------------|
| `codigos_secciones.rds` | Códigos de secciones censales canónicos |
| `codigos_secciones_infoelectoral.rds` | Codificación alternativa Infoelectoral |
| `codigos_secciones_{01,03,07}.rds` | Secciones específicas por CCAA (Andalucía, Asturias, CyL) |
| `circunscripciones.csv` | Circunscripciones sub-provinciales (Asturias, Canarias, Baleares) |
| `correspondencia_municipio_circunscripcion.csv` | Mapeo municipio → circunscripción |
| `nombres_municipios.csv` | Nomenclátor de municipios (INE) |

### Representantes y escaños

| Fichero | Descripción |
|---------|-------------|
| `nrepresentantes_prov.xlsx` | Escaños asignados por provincia/circunscripción y elección |
| `nrepresentantes_muni.xlsx` | Escaños asignados por municipio (locales) |
| `representantes_prov.xlsx` | Representantes electos por provincia |
| `representantes_muni.xlsx` | Representantes electos por municipio |

## Agradecimientos

Cuando no se ha encontrado una fuente oficial directa se han empleado los datos históricos  del [**Spanish Electoral Archive (SEA)**](https://gipeyop.uv.es/gipeyop/sea.html), elaborado por el [**GIPEYOP** (Grupo de Investigación en Procesos Electorales y Opinión Pública)](https://gipeyop.uv.es/) de la Universitat de València. Agradecemos enormemente su labor de recopilación y puesta a disposición de estos datos electorales.

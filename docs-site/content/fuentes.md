---
title: "Fuentes de datos"
description: "Origen y detalle de las fuentes de datos de eleccionesdb"
---

## Cobertura electoral

Actualmente la base incluye:

- Elecciones al Congreso: todas las convocatorias.
- Elecciones municipales: todas las convocatorias.
- Elecciones autonómicas: las 17 comunidades autónomas.

<div class="callout callout-warning">
<p><strong>Datos provisionales:</strong> Para las convocatorias más recientes en las que todavía no se han publicado datos oficiales definitivos (Extremadura 2025, Aragón 2026, Castilla y León 2026), la base incluye datos del <strong>escrutinio provisional</strong> de la noche electoral facilitados por <strong>Minsait</strong>. Estos datos pueden diferir de los resultados definitivos. Véase la sección <a href="#minsait-escrutinio-provisional">Minsait (escrutinio provisional)</a>.</p>
</div>

### Cobertura y granularidad

La granularidad varía según elección y fuente original; en todos los casos se publican agregados territoriales consistentes (CCAA, provincia, circunscripción, municipio y sección censal cuando está disponible).

| Ámbito | Años cubiertos | Granularidad máxima | Notas |
|--------|----------------|---------------------|-------|
| Congreso | Todas las convocatorias desde 1977 | Sección censal | Datos vía paquete `infoelectoral` |
| Municipales | Todas las convocatorias | Municipal | Datos vía paquete `infoelectoral` |
| Andalucía | Todas las convocatorias | Sección censal | API del Sistema de Información Electoral de Andalucía |
| Aragón | 2011–2026 (2026: provisional) | Sección censal (recientes) / Municipal (histórico) | Datos abiertos Gobierno de Aragón (2011–2023); escrutinio provisional Minsait (2026) |
| Asturias | Todas las convocatorias | Mesa (2015+) / Municipal (pre-2015) | SADEI (histórico) + GIPEYOP (reciente) |
| Baleares | 1983–2023 | Sección censal | IBESTAT + GIPEYOP |
| Canarias | 1983–2023 | Municipal (PARCAN) / Sección (GIPEYOP) | Múltiples fuentes |
| Cantabria | 1983–2023 | Mesa | GIPEYOP |
| Castilla-La Mancha | 1983–2023 | Mesa (2019+) / Municipal (histórico) | Junta de Castilla-La Mancha (datos abiertos) |
| Castilla y León | 1983–2026 (2026: provisional) | Mesa | Datos abiertos Junta de CyL (1983–2022); escrutinio provisional Minsait (2026) |
| Cataluña | 1980–2023 | Mesa | Datos abiertos Generalitat |
| C. Valenciana | 1983–2023 | Sección censal | Datos abiertos GVA (1987-2023); GIPEYOP (1983) |
| Extremadura | 1983–2025 (2025: provisional) | Mesa (2015+) / Municipal (histórico) | Junta de Extremadura (datos abiertos) |
| Galicia | 1981–2024 | Mesa (reciente) / Municipal-provincial (histórico) | Xunta de Galicia |
| C. de Madrid | 1983–2023 | Mesa | Datos abiertos Comunidad de Madrid |
| Murcia | 1983–2023 | Mesa | Región de Murcia (datos abiertos) |
| Navarra | 1979–2023 | Mesa | Gobierno de Navarra |
| País Vasco | 1980–2024 | Mesa | Gobierno Vasco |
| La Rioja | 1983–2023 | Mesa | Gobierno de La Rioja |

## Resumen de fuentes

| Elección | Fuente principal | Formato | Descarga | Cobertura |
|----------|------------------|---------|------------|-----------|
| Congreso | Ministerio del Interior | Texto sin delimitar | [infoelectoral](https://infoelectoral.spainelectoralproject.com/) (R package) | Todas las convocatorias |
| Municipales | Ministerio del Interior | Texto sin delimitar | [infoelectoral](https://infoelectoral.spainelectoralproject.com/) (R package) | Todas las convocatorias |
| Andalucía | Junta de Andalucía (SIEL) | JSON | API | Todas las convocatorias |
| Aragón | Gobierno de Aragón (datos abiertos) + Minsait (provisional) | CSV/XLSX | Manual | 2011–2026 |
| Asturias | SADEI + GIPEYOP | .px + XLSX | Scraping + manual | Todas las convocatorias |
| Baleares | IBESTAT + GIPEYOP | .px + XLSX | Manual | 1983–2023 |
| Canarias | PARCAN + ISTAC + GIPEYOP | CSV + XLSX | API + manual | 1983–2023 |
| Cantabria | GIPEYOP | XLSX | Manual | 1983–2023 |
| Castilla-La Mancha | Junta de Castilla-La Mancha (datos abiertos) | CSV/XLSX | Descarga | 1983–2023 |
| Castilla y León | Junta de Castilla y León + Minsait (provisional) | CSV (`;`, Latin-1) | Manual | 1983–2026 |
| Cataluña | Generalitat de Catalunya | CSV | Manual | 1980–2023 |
| C. Valenciana | Generalitat Valenciana + GIPEYOP | CSV + XLSX | Manual | 1983–2023 |
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
- **Granularidad**: Provincia, municipio y sección censal.
- **Ficheros brutos**: No hay CSV en `data-raw/`; el paquete descarga directamente desde la fuente oficial.
- **Scripts**: `sauron_formats.R` (orquestador) → `format_provincias.R`, `format_municipios.R`, `format_secciones.R`.
- **Notas**: Separa CER (residentes) de CERA (residentes ausentes).

---

### Municipales (`00b-municipales`)

- **Fuente**: Paquete R [`infoelectoral`](https://infoelectoral.spainelectoralproject.com/) (datos del Ministerio del Interior).
- **Método**: Funciones `provincias()`, `municipios()` y `mesas()`; para algunas convocatorias históricas se integra solo a nivel municipal.
- **Granularidad**: Sección censal (cuando hay mesa/sección disponible) y municipal en los años históricos sin mesa.
- **Ficheros brutos**: No hay CSV en `data-raw/`; el paquete descarga directamente desde la fuente oficial.
- **Scripts**: `format.R`.
- **Notas**: Separa CER/CERA y homogeneiza estructura territorial para todas las convocatorias.

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
- **Granularidad**: Municipal y provincial (histórico); sección en elecciones recientes.
- **Ficheros brutos**: `Resultados electorales agrupados a escala municipal (histórico).csv`, `Resultados electorales agrupados a escala provincial (histórico).xlsx`, carpetas por año (`2011/`, `2015/`, `2019/`, `2023/`).
- **Scripts**: `01_format-info.R` (resumen/participación), `02_format-votos.R` (votos por partido).
- **Notas**: CERA identificado como municipio='000'. Formatos de tabla variables por año. Cobertura: 2011–2023. Los datos de la convocatoria de 2026 proceden del escrutinio provisional Minsait; véase la sección correspondiente.

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
- **Granularidad**: Sección.
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

### Cantabria (`06-cantabria`)

- **Fuente**: [GIPEYOP](https://gipeyop.uv.es/gipeyop/sea.html) (Spanish Electoral Archive).
- **Método**: Lectura de ficheros XLSX descargados manualmente.
- **Granularidad**: Mesa.
- **Ficheros brutos**: Serie histórica de ficheros `Cantabria{YYYY}_mesas.xlsx` (1983–2023).
- **Scripts**: `format.R`.
- **Notas**: Parsing dinámico de columnas de partido según convocatoria.

---

### Castilla y León (`07-cyl`)

- **Fuente**: [Portal de datos abiertos](https://jcyl.opendatasoft.com/explore/dataset/resultados-electorales-1983-actualidad/information/) de la Junta de Castilla y León.
- **Método**: Ficheros CSV descargados manualmente.
- **Granularidad**: Sección.
- **Ficheros brutos**: Un CSV por año (`1983.csv`–`2022.csv`), delimitados por `;`, codificación Latin-1.
- **Scripts**: `format.R`.
- **Notas**: Parsea la estructura del código de mesa para extraer provincia/municipio/distrito/sección. Agrega por niveles. Cobertura: 1983–2022. Los datos de la convocatoria de 2026 proceden del escrutinio provisional Minsait; véase la sección correspondiente.

---

### Castilla-La Mancha (`08-clm`)

- **Fuente**: Junta de Castilla-La Mancha (datos abiertos).
- **Método**: Integración mixta de CSV y XLSX, con lectura específica por periodo.
- **Granularidad**: Mesa (2019–2023) y municipal (histórico, 1983–2015).
- **Ficheros brutos**: CSV históricos por año y libros Excel recientes (`resultados2019.xlsx`, `resultados2023.xlsx`).
- **Scripts**: `format.R`.
- **Notas**: En parte del histórico faltan algunos campos de participación (blancos/nulos) y se conservan como `NA` cuando la fuente no los publica.

---

### Cataluña (`09-catalunya`)

- **Fuente**: [Generalitat de Catalunya](https://eleccions.gencat.cat/es/resultats-i-eleccions-anteriors/resultats-electorals/#/).
- **Método**: Ficheros CSV descargados (datos abiertos electorales).
- **Granularidad**: Sección.
- **Ficheros brutos**: Patrón de nombre `A{YYYY}{N}-{TIPO}-ME.csv` (ej: `A20231-Columnes-ME.csv`).
- **Scripts**: `01_format-data.R`.
- **Notas**: Nombres de columna en catalán. Locale con coma decimal y punto de agrupación. CERA en municipio='998'. Cobertura: 1980–2023.

---

### Comunidad Valenciana (`10-comunidad-valenciana`)

- **Fuente**: [Portal de Datos Abiertos](https://portaldadesobertes.gva.es/es) de la Generalitat Valenciana, y [GIPEYOP](https://gipeyop.uv.es/gipeyop/sea.html) (1983).
- **Método**: Ficheros CSV y xlsx descargados manualmente.
- **Granularidad**: Sección.
- **Ficheros brutos**: CSV por año (`1987.csv`–`2023.csv`) + `Comunitat Valenciana1983_municipios.xlsx` (1983 en formato especial).
- **Scripts**: `format.R`.
- **Notas**: Naming de columnas variable entre años. Codificación variable. 1983 es xlsx con resultados municipales precalculados. Cobertura: 1983–2023.

---

### Extremadura (`11-extremadura`)

- **Fuente**: Junta de Extremadura (datos abiertos).
- **Método**: Lectura de XLSX con dos formatos (municipal histórico y mesa en convocatorias recientes).
- **Granularidad**: Mesa (2015+), municipal (histórico).
- **Ficheros brutos**: Serie de `Extremadura_{YYYY}_municipios.xlsx` y `Extremadura_{YYYY}_mesas.xlsx`.
- **Scripts**: `format.R`.
- **Notas**: Cobertura 1983–2023. Los datos de la convocatoria de 2025 proceden del escrutinio provisional Minsait; véase la sección correspondiente.

---

### Galicia (`12-galicia`)

- **Fuente**: Xunta de Galicia (datos abiertos) + histórico provincial.
- **Método**: Lectura de XLS/XLSX con funciones auxiliares para homogeneizar nombres de columnas por convocatoria.
- **Granularidad**: Mesa (convocatorias recientes) y provincial/municipal en parte del histórico.
- **Ficheros brutos**: Carpetas por año (`2001/`, `2005/`, `2009/`, `2012/`, `2016/`, `2020/`, `2024/`) y ficheros históricos (1981–1997).
- **Scripts**: `00_functions.R`, `format.R`.
- **Notas**: Cambios de estructura entre años y codificaciones heterogéneas.

---

### Comunidad de Madrid (`13-comunidad-madrid`)

- **Fuente**: [Portal de Datos Abiertos](https://www.comunidad.madrid/gobierno/datos-abiertos) de la Comunidad de Madrid.
- **Método**: Ficheros Excel (.xls/.xlsx) descargados manualmente.
- **Granularidad**: Sección.
- **Ficheros brutos**: `{año}_Mesas.xls` (1995–2021), `datos_electorales_elecciones_autonomicas_comunidad_de_madrid_2023.xlsx` (2023, formato diferente), `Madrid{1983,1987,1991}_circunscripcion.xlsx` (histórico).
- **Scripts**: `format.R`.
- **Notas**: 2003 tiene dos elecciones (mayo y octubre). Multi-header parsing en Excel. Circunscripción única (provincial). Cobertura: 1983–2023.

---

### Murcia (`14-murcia`)

- **Fuente**: Región de Murcia (datos abiertos).
- **Método**: Lectura de XLS/XLSX con parseo de cabeceras específicas según periodo.
- **Granularidad**: Mesa.
- **Ficheros brutos**: Ficheros históricos por municipio (pre-2003), `Murcia2003_mesas.xlsx` y serie posterior por mesa.
- **Scripts**: `format.R`.
- **Notas**: Se aplican transformaciones distintas para formatos pre y post 2003.

---

### Navarra (`15-navarra`)

- **Fuente**: Gobierno de Navarra (datos abiertos).
- **Método**: Integración mixta CSV (1999+) y XLS histórico (1979–1995).
- **Granularidad**: Mesa.
- **Ficheros brutos**: `navarra_mesas_{YYYY}.csv` y `soc_elec_parla_nav_{YYYY}.xls`.
- **Scripts**: `format.R`.
- **Notas**: Homogeneización de múltiples variantes de nombres de columna entre periodos.

---

### País Vasco (`16-pais-vasco`)

- **Fuente**: Gobierno Vasco (datos abiertos).
- **Método**: Lectura de XLS/XLSX con normalización de formatos históricos.
- **Granularidad**: Mesa.
- **Ficheros brutos**: Serie `MesP{YY}_c.xls(x)` (1980–2024).
- **Scripts**: `format.R`.
- **Notas**: Estructura de ficheros consistente por convocatoria con ajustes menores de columnas.

---

### La Rioja (`17-la-rioja`)

- **Fuente**: Gobierno de La Rioja (datos abiertos).
- **Método**: Integración de tres ficheros XLS complementarios por convocatoria (resumen, detalle de votos y avances).
- **Granularidad**: Mesa.
- **Ficheros brutos**: `resumen_mesa.xls`, `detalle_votos_mesa.xls`, `avances_mesa.xls`.
- **Scripts**: `format.R`.
- **Notas**: Cobertura 1983–2023.

---

### Minsait — escrutinio provisional {#minsait-escrutinio-provisional}

<div class="callout callout-warning">
<p><strong>⚠ Datos provisionales.</strong> Esta fuente contiene los resultados del escrutinio provisional de la noche electoral. Estos datos <strong>pueden diferir de los resultados definitivos</strong> publicados por las administraciones autonómicas una vez concluido el escrutinio oficial completo. En cuanto estén disponibles los datos definitivos, esta fuente será sustituida.</p>
</div>

- **Fuente**: [Minsait](https://minsait.com/es) — empresa que gestiona el escrutinio provisional para varias comunidades autónomas.
- **Método**: Ficheros CSV del escrutinio provisional facilitados directamente. Cada fila corresponde a una mesa electoral y un partido.
- **Granularidad**: Mesa (fuente), publicado a nivel de sección, municipio, provincia y CCAA.
- **Ficheros brutos**: `data-raw/hechos/minsait/` — un CSV por CCAA (`02-aragon.csv`, `07-cyl.csv`, `11-extremadura.csv`).
- **Scripts**: `R/01-generate-data/hechos/minsait/format.R` (script unificado para todas las CCAA de esta fuente).
- **Elecciones cubiertas**:
  - Aragón — Elecciones Autonómicas 2026 (8 de febrero de 2026)
  - Castilla y León — Elecciones Autonómicas 2026 (15 de marzo de 2026)
  - Extremadura — Elecciones Autonómicas 2025 (21 de diciembre de 2025)
- **Estructura del CSV**: `n_envio`, `timestamp`, `codigo_ccaa`\*, `codigo_provincia`, `codigo_municipio`, `codigo_distrito`, `codigo_seccion`, `codigo_mesa`, `censo`, `total_votantes`, `abstencion`, `votos_blanco`, `votos_nulos`, `partido`, `votos`, `recode`.

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

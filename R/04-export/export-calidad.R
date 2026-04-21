# export-calidad.R
#
# Lee docs-site/data/diagnosticos/incidencias.csv y genera
# docs-site/content/calidad.md para el sitio Hugo del proyecto.
#
# La página incluye:
#   - Disclaimer / nota de responsabilidad
#   - Resumen agregado de incidencias conocidas
#   - Una sección por cada tipo de incidencia con tabla de casos
#   - Nota metodológica
#   - Instrucciones de mantenimiento (para el repositorio)
#
# ----- Uso directo -----
#   source("R/04-export/export-calidad.R")
#   export_calidad()
#
# ----- Uso en pipeline -----
#   Llamado desde R/pipeline_helpers.R -> run_export_calidad()

library(dplyr)
library(readr)

INCIDENCIAS_CSV      <- "docs-site/data/diagnosticos/incidencias.csv"
SECCIONES_4DIG_CSV   <- "docs-site/data/diagnosticos/secciones_4_digitos.csv"
OUTPUT_MD            <- "docs-site/content/calidad.md"

GITHUB_ISSUES   <- "https://github.com/hmeleiro/eleccionesdb-etl/issues"

# ---------------------------------------------------------------------------
# Helpers para generar markdown
# ---------------------------------------------------------------------------

md_table <- function(df, cols, col_labels = NULL) {
  if (is.null(col_labels)) col_labels <- cols
  df_sel <- df[, cols, drop = FALSE]
  names(df_sel) <- col_labels

  header <- paste("|", paste(col_labels, collapse = " | "), "|")
  sep    <- paste("|", paste(rep("---", length(col_labels)), collapse = " | "), "|")
  rows   <- apply(df_sel, 1, function(r) {
    paste("|", paste(ifelse(is.na(r), "—", r), collapse = " | "), "|")
  })
  paste(c(header, sep, rows), collapse = "\n")
}

badge_resoluble <- function(x) {
  ifelse(as.logical(x),
         '<span class="badge badge-resoluble">resoluble</span>',
         '<span class="badge badge-no-resoluble">no resoluble</span>')
}

badge_origen <- function(x) {
  ifelse(x == "automatico",
         '<span class="badge badge-automatico">automático</span>',
         '<span class="badge badge-manual">manual</span>')
}

# ---------------------------------------------------------------------------
# Bloque de disclaimer (texto fijo)
# ---------------------------------------------------------------------------

DISCLAIMER_TEXT <- '
**eleccionesdb** es un proyecto de integración de resultados electorales
procedentes de múltiples fuentes oficiales, organismos autonómicos y portales
de datos abiertos. A pesar del esfuerzo sistemático de validación, depuración
y estandarización, **la base de datos puede contener errores**.

Estos errores tienen dos orígenes posibles:

- **Errores en la fuente original.** Los datos publicados por organismos
  oficiales no están exentos de errores de transcripción, agregación o
  codificación. En algunos casos las propias fuentes presentan inconsistencias
  internas (por ejemplo, totales provinciales que no coinciden con la suma de
  municipios). Cuando esto ocurre, **el dato se mantiene tal como aparece en
  la fuente**, sin corrección, salvo que exista una alternativa documental
  fiable.

- **Errores introducidos en el procesamiento.** Las operaciones de
  normalización, unificación de codificaciones territoriales y asignación de
  identificadores pueden generar errores no detectados por las validaciones
  actuales.

Los datos se ofrecen **tal cual** (*as is*), sin garantía de exactitud o
exhaustividad. El proyecto **no asume responsabilidad** por análisis,
conclusiones o decisiones basadas exclusivamente en estos datos.

Si detectas un error o una inconsistencia, por favor abre un issue en el
repositorio:
'

# ---------------------------------------------------------------------------
# Generación de sección por tipo
# ---------------------------------------------------------------------------

render_tipo_section <- function(df_tipo) {
  tipo      <- df_tipo$tipo[1]
  label     <- df_tipo$tipo_label[1]
  desc      <- df_tipo$tipo_descripcion[1]
  n_total   <- sum(df_tipo$n_casos)
  n_elec    <- nrow(df_tipo)

  # Tabla de casos: columnas seleccionadas
  tabla_df <- df_tipo %>%
    mutate(
      Resoluble = badge_resoluble(resoluble),
      Origen    = badge_origen(origen),
      `N territorios` = format(n_casos, big.mark = ".")
    ) %>%
    select(
      Elección       = eleccion,
      Año            = anio,
      `N territorios`,
      `Nivel`        = nivel_territorial,
      `Ejemplo`      = peor_caso_territorio,
      Resoluble,
      Origen
    )

  tabla_md <- md_table(
    tabla_df,
    cols       = names(tabla_df),
    col_labels = names(tabla_df)
  )

  # Plegar con <details> si hay más de 8 filas
  tabla_html <- if (nrow(tabla_df) > 8) {
    sprintf(
      '<details>\n<summary>Ver %d elecciones afectadas</summary>\n\n%s\n\n</details>',
      n_elec, tabla_md
    )
  } else {
    tabla_md
  }

  sprintf(
    '### %s\n\n%s\n\n**%d elecciones afectadas · %s territorios con incidencia en total**\n\n%s\n',
    label,
    desc,
    n_elec,
    format(n_total, big.mark = "."),
    tabla_html
  )
}

# ---------------------------------------------------------------------------
# Sección especial: secciones censales con código de 4 dígitos
# ---------------------------------------------------------------------------

CCAA_NOMBRES <- c(
  "01" = "País Vasco",    "02" = "Cataluña",       "03" = "Galicia",
  "04" = "Andalucía",     "05" = "Asturias",        "06" = "Cantabria",
  "07" = "La Rioja",      "08" = "Murcia",           "09" = "Navarra",
  "10" = "Aragón",        "11" = "C. de Madrid",    "12" = "Castilla-La Mancha",
  "13" = "Canarias",      "14" = "Extremadura",     "15" = "Baleares",
  "16" = "Castilla y León", "17" = "C. Valenciana", "18" = "C. Valenciana (2)",
  "19" = "Cantabria (2)"
)

render_secciones_4_digitos <- function(csv_path = SECCIONES_4DIG_CSV) {
  if (!file.exists(csv_path)) {
    message(sprintf("[calidad] No se encontró %s; omitiendo sección de secciones 4 dígitos.", csv_path))
    return("")
  }

  df <- read_csv(csv_path, show_col_types = FALSE, col_types = cols(
    year          = col_integer(),
    tipo_eleccion = col_character(),
    codigo_ccaa   = col_character(),
    codigo_seccion = col_character(),
    n             = col_integer()
  ))

  # Normalizar codigo_ccaa a 2 dígitos para el lookup
  df <- df %>%
    mutate(
      codigo_ccaa_pad = sprintf("%02d", as.integer(codigo_ccaa)),
      ccaa_nombre     = coalesce(CCAA_NOMBRES[codigo_ccaa_pad], codigo_ccaa_pad),
      tipo_label      = case_when(
        tipo_eleccion == "G" ~ "Generales",
        tipo_eleccion == "A" ~ "Autonómicas",
        tipo_eleccion == "L" ~ "Locales",
        tipo_eleccion == "E" ~ "Europeas",
        TRUE                 ~ tipo_eleccion
      )
    ) %>%
    arrange(year, tipo_eleccion, codigo_ccaa_pad, codigo_seccion)

  n_secciones <- nrow(df)
  n_ccaa      <- n_distinct(df$codigo_ccaa_pad)
  n_anios     <- n_distinct(df$year)

  tabla_df <- df %>%
    select(
      Año            = year,
      `Tipo elección` = tipo_label,
      CCAA           = ccaa_nombre,
      `Cód. sección` = codigo_seccion,
      `N filas`      = n
    )

  tabla_md <- md_table(tabla_df, cols = names(tabla_df))

  tabla_html <- sprintf(
    '<details>\n<summary>Ver las %d secciones con código de 4 dígitos</summary>\n\n%s\n\n</details>',
    n_secciones, tabla_md
  )

  sprintf(
    '## Aviso: secciones censales con código de 4 dígitos\n\n
En España, el código de sección censal tiene habitualmente **3 dígitos** (p. ej. `001`, `042`).
Sin embargo, existen casos puntuales —concentrados en elecciones autonómicas antiguas y en
algunas convocatorias de los años 80 y 90— en los que el código de sección tiene **4 dígitos**
(p. ej. `1170`, `001A`, `001B`).\n\n
Para cumplir con la restricción de integridad de la base de datos, que requiere que todos los
códigos de sección tengan la misma longitud, **las secciones con código de 3 dígitos están
rellenas con un cero por la izquierda** (`001` → `0001`). Las secciones que ya tenían 4 dígitos
se almacenan tal como aparecen en la fuente original.\n\n
<div class="callout callout-warning">
<p><strong>Importante si unes estos datos con otras fuentes:</strong> Al cruzar <code>codigo_seccion</code> con datos de otras fuentes (p. ej. cartografía del INE, datos del Padrón, otras bases electorales), ten en cuenta que el primer dígito puede ser un cero de relleno. Para la mayoría de secciones <strong>deberás eliminar ese cero inicial</strong> antes de hacer el join. Verifica previamente si la elección y el territorio concreto aparecen en la lista de secciones con código original de 4 dígitos.</p>
</div>\n\n
En total se han identificado **%d secciones con código de 4 dígitos** en **%d CCAA** y **%d años**
distintos. La práctica totalidad corresponden a elecciones autonómicas.\n\n%s\n',
    n_secciones, n_ccaa, n_anios, tabla_html
  )
}

# ---------------------------------------------------------------------------
# Función principal
# ---------------------------------------------------------------------------

export_calidad <- function(
    csv_path         = INCIDENCIAS_CSV,
    secciones_path   = SECCIONES_4DIG_CSV,
    output_path      = OUTPUT_MD
) {
  if (!file.exists(csv_path)) {
    stop(sprintf(
      "No se encontró %s. Ejecuta primero generate_diagnosticos().",
      csv_path
    ), call. = FALSE)
  }

  df <- read_csv(csv_path, show_col_types = FALSE)
  message(sprintf("[calidad] %d incidencias leídas de %s", nrow(df), csv_path))

  # Resumen global
  n_total      <- nrow(df)
  n_tipos      <- n_distinct(df$tipo)
  n_elecciones <- n_distinct(df$eleccion_id)
  n_territorios_total <- sum(df$n_casos, na.rm = TRUE)
  fecha_gen    <- format(Sys.Date(), "%d de %B de %Y")

  # Secciones por tipo
  tipos_ordered <- df %>%
    group_by(tipo, tipo_label) %>%
    summarise(n = sum(n_casos), .groups = "drop") %>%
    arrange(desc(n)) %>%
    pull(tipo)

  secciones <- lapply(tipos_ordered, function(t) {
    render_tipo_section(filter(df, tipo == t))
  })

  # Sección de secciones con 4 dígitos
  seccion_4dig <- render_secciones_4_digitos(secciones_path)

  # ---------------------------------------------------------------------------
  # Ensamblar el documento
  # ---------------------------------------------------------------------------
  lineas <- c(
    '---',
    'title: "Calidad de los datos"',
    'description: "Disclaimer, limitaciones conocidas e incidencias de calidad detectadas en eleccionesdb"',
    '---',
    '',
    '<div class="callout callout-warning">',
    '<p><strong>Aviso:</strong> Esta página documenta limitaciones y errores conocidos en los datos de <strong>eleccionesdb</strong>. La presencia de una incidencia no invalida necesariamente el conjunto de datos de esa elección.</p>',
    '</div>',
    '',
    '## Nota sobre la calidad de los datos',
    '',
    DISCLAIMER_TEXT,
    '',
    '<div class="callout callout-note">',
    sprintf('<p><a href="%s" target="_blank" rel="noopener">Reportar un error en GitHub →</a></p>', GITHUB_ISSUES),
    '</div>',
    '',
    '---',
    '',
    '## Incidencias conocidas',
    '',
    sprintf(
      'A continuación se documentan **%d grupos de incidencias** detectados en **%d elecciones**,',
      n_total, n_elecciones
    ),
    sprintf(
      'con un total de **%s territorios afectados** en algún grado. Estas incidencias han sido',
      format(n_territorios_total, big.mark = ".")
    ),
    'identificadas de forma automática a partir de los datos finales, o documentadas manualmente.',
    '',
    '### Resumen',
    '',
    sprintf('| Indicador | Valor |'),
    sprintf('|---|---|'),
    sprintf('| Tipos de incidencia | %d |', n_tipos),
    sprintf('| Grupos documentados | %d |', n_total),
    sprintf('| Elecciones afectadas | %d |', n_elecciones),
    sprintf('| Territorios afectados (total) | %s |', format(n_territorios_total, big.mark = ".")),
    sprintf('| Última actualización | %s |', fecha_gen),
    '',
    '---',
    '',
    '## Detalle por tipo de incidencia',
    '',
    paste(secciones, collapse = "\n---\n\n"),
    '',
    '---',
    '',
    seccion_4dig,
    '',
    '---',
    '',
    '## Nota metodológica',
    '',
    'Las incidencias aquí documentadas no implican necesariamente que los datos de una',
    'elección sean inválidos o inutilizables. En muchos casos:',
    '',
    '- La incidencia afecta a un número reducido de territorios (a menudo secciones censales',
    '  con censos muy pequeños o con errores de publicación puntuales).',
    '- La magnitud del error es lo suficientemente pequeña como para no afectar a',
    '  conclusiones agregadas a nivel municipal, provincial o autonómico.',
    '- La propia fuente oficial contiene la inconsistencia y no existe una publicación',
    '  alternativa fiable que permita corregirla.',
    '',
    'El objetivo de esta página es precisamente mejorar la **transparencia del proyecto**:',
    'que cualquier usuario pueda conocer de antemano las limitaciones del dato antes de',
    'usarlo en un análisis.',
    '',
    '---',
    '',
    '## Mantenimiento',
    '',
    'Esta página se genera automáticamente a partir de dos fuentes:',
    '',
    '1. **Incidencias automáticas** — detectadas al ejecutar el pipeline sobre',
    '   `tablas-finales/hechos/info.rds`. Se regeneran con cada actualización de datos.',
    '',
    '2. **Incidencias manuales** — mantenidas en',
    '   `docs-site/data/diagnosticos/incidencias_manual.csv`. Este fichero se puede',
    '   editar directamente para añadir incidencias que no pueden detectarse',
    '   automáticamente (inconsistencias documentales, discrepancias entre publicaciones, etc.).',
    '',
    '### Estructura del CSV de incidencias manuales',
    '',
    '```',
    'tipo              # ID de máquina del tipo de incidencia',
    'tipo_label        # Etiqueta legible',
    'tipo_descripcion  # Descripción del tipo',
    'eleccion_id       # ID numérico de la elección',
    'eleccion          # Descripción de la elección',
    'anio              # Año',
    'tipo_eleccion     # G / A / L / E',
    'n_casos           # Nº de territorios afectados en esta elección',
    'nivel_territorial # Nivel de granularidad (municipio, sección, etc.)',
    'peor_caso_territorio  # Nombre del territorio más afectado',
    'peor_caso_valor       # Valor observado en ese territorio',
    'peor_caso_esperado    # Valor esperado',
    'fuente            # fuente_original / integracion',
    'resoluble         # TRUE / FALSE',
    'origen            # Siempre "manual" en este fichero',
    'comentario        # Texto libre',
    '```',
    '',
    '### Pasos para añadir una incidencia manual',
    '',
    '1. Editar `docs-site/data/diagnosticos/incidencias_manual.csv`.',
    '2. Añadir una fila con `origen = "manual"` y los campos correspondientes.',
    '3. Ejecutar `source("R/04-export/export-calidad.R")` (o el target `export_calidad` en el pipeline).',
    '4. Commitear `docs-site/data/diagnosticos/incidencias_manual.csv` y `docs-site/content/calidad.md`.',
    ''
  )

  writeLines(lineas, output_path, useBytes = FALSE)
  message(sprintf("[calidad] Página generada: %s", output_path))
  invisible(output_path)
}

if (sys.nframe() == 0) {
  export_calidad()
}

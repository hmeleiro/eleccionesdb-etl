---
title: "Registro de cambios"
description: "Historial de versiones de eleccionesdb"
---

<div class="callout callout-tip">
<strong>Versionado de datasets</strong>
<p>Los datasets de <code>eleccionesdb</code> se versionan con <a href="https://semver.org/lang/es/">versionado semántico</a>:</p>
<ul>
<li><strong>Mayor</strong> (X.0.0): cambios incompatibles en el esquema (renombrado de columnas, cambio de tipos, eliminación de tablas).</li>
<li><strong>Menor</strong> (0.X.0): nuevos datos o nuevas tablas sin romper compatibilidad.</li>
<li><strong>Parche</strong> (0.0.X): correcciones de datos, errores en valores, ajustes menores.</li>
</ul>
</div>

---

## v1.2.0 — 2026-04-28

### Added

- **Elecciones europeas:** se incorporan las convocatorias 1987–2024 a partir del Ministerio del Interior vía paquete `infoelectoral`.
  - Nuevo procesamiento `R/01-generate-data/hechos/00c-europeas/` con descarga en `fetch-data.R` y normalización en `format.R`.
  - Integración en el pipeline de `{targets}` mediante el target `hechos_europeas`.
  - Granularidad hasta sección censal cuando está disponible, con fallback municipal para convocatorias históricas sin mesas.

---

## v1.1.0 — 2026-04-22

### Added

- **Escrutinio provisional (Minsait):** se incorporan los datos de la noche electoral para tres convocatorias autonómicas recientes sin datos definitivos publicados: Extremadura 2025 (21 dic 2025), Aragón 2026 (8 feb 2026) y Castilla y León 2026 (15 mar 2026).
  - Nuevo script unificado `R/01-generate-data/hechos/minsait/format.R` que procesa los tres ficheros CSV conjuntamente.
  - Granularidad hasta nivel de sección censal (fuente: mesa electoral).
  - **Nota:** el voto exterior (CERA) no está incluido en el escrutinio provisional.
- Nuevo apartado *Datos provisionales* en la página de calidad de datos con advertencias explícitas sobre la naturaleza de esta fuente.
- Documentación de la fuente Minsait en la página de fuentes de datos.

---

## v1.0.0 — 2026-04-20

Primera versión pública de la base de datos.

### Nuevos datos

- Elecciones generales al Congreso: todas las convocatorias.
- Elecciones municipales: todas las convocatorias.
- Elecciones autonómicas: Andalucía, Aragón, Asturias, Baleares, Canarias, Castilla y León, Cataluña, Comunidad Valenciana, Comunidad de Madrid.

### Modelo de datos

- Esquema relacional con 5 dimensiones (`tipos_eleccion`, `elecciones`, `territorios`, `partidos_recode`, `partidos`) y 4 tablas de hechos (`resumen_territorial`, `votos_territoriales`, `resumen_cera`, `votos_cera`).
- Jerarquía territorial completa: CCAA → provincia → circunscripción → municipio → distrito → sección censal.
- Sistema de recodificación de partidos con dos niveles (`partidos_recode` + `partidos`).

### Formatos de descarga

- Parquet (tablas normalizadas).
- SQLite (esquema relacional con PKs, FKs, índices).
- CSV planos pre-joineados.

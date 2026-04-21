---
title: "Calidad de los datos"
description: "Disclaimer, limitaciones conocidas e incidencias de calidad detectadas en eleccionesdb"
---

<div class="callout callout-warning">
<p><strong>Aviso:</strong> Esta página documenta limitaciones y errores conocidos en los datos de <strong>eleccionesdb</strong>. La presencia de una incidencia no invalida necesariamente el conjunto de datos de esa elección.</p>
</div>

## Nota sobre la calidad de los datos


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


<div class="callout callout-note">
<p><a href="https://github.com/hmeleiro/eleccionesdb-etl/issues" target="_blank" rel="noopener">Reportar un error en GitHub →</a></p>
</div>

---

## Incidencias conocidas

A continuación se documentan **30 grupos de incidencias** detectados en **26 elecciones**,
con un total de **4.445 territorios afectados** en algún grado. Estas incidencias han sido
identificadas de forma automática a partir de los datos finales, o documentadas manualmente.

### Resumen

| Indicador | Valor |
|---|---|
| Tipos de incidencia | 5 |
| Grupos documentados | 30 |
| Elecciones afectadas | 26 |
| Territorios afectados (total) | 4.445 |
| Última actualización | 21 de abril de 2026 |

---

## Detalle por tipo de incidencia

### Votos > censo

La suma de votos válidos y abstenciones supera el censo INE. Puede indicar un error en el censo publicado, en los totales de participación, o en la fuente original.

**11 elecciones afectadas · 3.881 territorios con incidencia en total**

<details>
<summary>Ver 11 elecciones afectadas</summary>

| Elección | Año | N territorios | Nivel | Ejemplo | Resoluble | Origen |
| --- | --- | --- | --- | --- | --- | --- |
| Elecciones Autonómicas Valencia 1987 | 1987 |     1 | seccion | — | <span class="badge badge-no-resoluble">no resoluble</span> | <span class="badge badge-automatico">automático</span> |
| Elecciones Autonómicas Valencia 1995 | 1995 |     4 | municipio / seccion | — | <span class="badge badge-no-resoluble">no resoluble</span> | <span class="badge badge-automatico">automático</span> |
| Elecciones Autonómicas Canarias 1999 | 1999 |     3 | municipio / seccion | — | <span class="badge badge-no-resoluble">no resoluble</span> | <span class="badge badge-automatico">automático</span> |
| Elecciones Autonómicas Galicia 2001 | 2001 |     2 | municipio | Mesía | <span class="badge badge-no-resoluble">no resoluble</span> | <span class="badge badge-automatico">automático</span> |
| Elecciones Autonómicas Canarias 2003 | 2003 |    18 | circunscripcion / municipio / provincia / seccion | Tenerife | <span class="badge badge-no-resoluble">no resoluble</span> | <span class="badge badge-automatico">automático</span> |
| Elecciones Autonómicas Canarias 2007 | 2007 |    42 | ccaa / circunscripcion / municipio / provincia / seccion | Santa Cruz de Tenerife | <span class="badge badge-no-resoluble">no resoluble</span> | <span class="badge badge-automatico">automático</span> |
| Elecciones Autonómicas Canarias 2011 | 2011 |    12 | circunscripcion / municipio / seccion | — | <span class="badge badge-no-resoluble">no resoluble</span> | <span class="badge badge-automatico">automático</span> |
| Elecciones Autonómicas Galicia 2016 | 2016 |     9 | municipio / seccion | Verea | <span class="badge badge-no-resoluble">no resoluble</span> | <span class="badge badge-automatico">automático</span> |
| Elecciones Autonómicas Valencia 2019 | 2019 | 3.786 | ccaa / municipio / provincia / seccion | Comunidad Valenciana | <span class="badge badge-no-resoluble">no resoluble</span> | <span class="badge badge-automatico">automático</span> |
| Elecciones Autonómicas Canarias 2019 | 2019 |     2 | municipio / seccion | — | <span class="badge badge-no-resoluble">no resoluble</span> | <span class="badge badge-automatico">automático</span> |
| Elecciones Autonómicas Galicia 2024 | 2024 |     2 | municipio | Amoeiro | <span class="badge badge-no-resoluble">no resoluble</span> | <span class="badge badge-automatico">automático</span> |

</details>

---

### Votos nulos > votos válidos

El número de votos nulos supera al total de votos válidos, lo cual es aritméticamente posible, pero probablemente indica un error en la fuente original o en el proceso de integración.

**16 elecciones afectadas · 320 territorios con incidencia en total**

<details>
<summary>Ver 16 elecciones afectadas</summary>

| Elección | Año | N territorios | Nivel | Ejemplo | Resoluble | Origen |
| --- | --- | --- | --- | --- | --- | --- |
| Elecciones Generales 1977 | 1977 |   1 | municipio | Sant Esteve de la Sarga | <span class="badge badge-no-resoluble">no resoluble</span> | <span class="badge badge-automatico">automático</span> |
| Elecciones Locales 1979 | 1979 |  28 | municipio | Fayos, Los | <span class="badge badge-no-resoluble">no resoluble</span> | <span class="badge badge-automatico">automático</span> |
| Elecciones Autonómicas Catalunya 1980 | 1980 | 229 | municipio / provincia / seccion | Lleida | <span class="badge badge-no-resoluble">no resoluble</span> | <span class="badge badge-automatico">automático</span> |
| Elecciones Generales 1982 | 1982 |   2 | seccion | — | <span class="badge badge-no-resoluble">no resoluble</span> | <span class="badge badge-automatico">automático</span> |
| Elecciones Locales 1983 | 1983 |   2 | municipio | Población de Cerrato | <span class="badge badge-no-resoluble">no resoluble</span> | <span class="badge badge-automatico">automático</span> |
| Elecciones Autonómicas Castillayleon 1987 | 1987 |   4 | municipio / seccion | Zarza de Pumareda, La | <span class="badge badge-no-resoluble">no resoluble</span> | <span class="badge badge-automatico">automático</span> |
| Elecciones Autonómicas Valencia 1987 | 1987 |   1 | seccion | — | <span class="badge badge-no-resoluble">no resoluble</span> | <span class="badge badge-automatico">automático</span> |
| Elecciones Locales 1987 | 1987 |   3 | seccion | — | <span class="badge badge-no-resoluble">no resoluble</span> | <span class="badge badge-automatico">automático</span> |
| Elecciones Locales 1995 | 1995 |   1 | seccion | — | <span class="badge badge-no-resoluble">no resoluble</span> | <span class="badge badge-automatico">automático</span> |
| Elecciones Locales 2003 | 2003 |  18 | municipio / seccion | Aizarnazabal | <span class="badge badge-no-resoluble">no resoluble</span> | <span class="badge badge-automatico">automático</span> |
| Elecciones Generales 2004 | 2004 |   8 | municipio / seccion | Lizartza | <span class="badge badge-no-resoluble">no resoluble</span> | <span class="badge badge-automatico">automático</span> |
| Elecciones Autonómicas Castillayleon 2007 | 2007 |   2 | municipio / seccion | Barcones | <span class="badge badge-no-resoluble">no resoluble</span> | <span class="badge badge-automatico">automático</span> |
| Elecciones Locales 2007 | 2007 |   9 | municipio / seccion | — | <span class="badge badge-no-resoluble">no resoluble</span> | <span class="badge badge-automatico">automático</span> |
| Elecciones Autonómicas Paisvasco 2009 | 2009 |   8 | municipio / seccion | Lizartza | <span class="badge badge-no-resoluble">no resoluble</span> | <span class="badge badge-automatico">automático</span> |
| Elecciones Autonómicas Castillayleon 2019 | 2019 |   2 | municipio / seccion | Fuenteguinaldo | <span class="badge badge-no-resoluble">no resoluble</span> | <span class="badge badge-automatico">automático</span> |
| Elecciones Generales 2023 | 2023 |   2 | municipio / seccion | Jaramillo Quemado | <span class="badge badge-no-resoluble">no resoluble</span> | <span class="badge badge-automatico">automático</span> |

</details>

---

### Votos en blanco > votos válidos

El número de votos en blanco supera al total de votos válidos. Aritméticamente imposible; indica un error en el dato de origen o en la integración.

**1 elecciones afectadas · 229 territorios con incidencia en total**

| Elección | Año | N territorios | Nivel | Ejemplo | Resoluble | Origen |
| --- | --- | --- | --- | --- | --- | --- |
| Elecciones Autonómicas Catalunya 1980 | 1980 | 229 | municipio / provincia / seccion | Lleida | <span class="badge badge-no-resoluble">no resoluble</span> | <span class="badge badge-automatico">automático</span> |

---

### Inconsistencia en agregaciones territoriales

La suma de votos por municipio no coincide con el total provincial publicado por la misma fuente para esa elección.

**1 elecciones afectadas · 12 territorios con incidencia en total**

| Elección | Año | N territorios | Nivel | Ejemplo | Resoluble | Origen |
| --- | --- | --- | --- | --- | --- | --- |
| Elecciones Municipales 2019 | 2019 | 12 | municipio / provincia | Barcelona | <span class="badge badge-no-resoluble">no resoluble</span> | <span class="badge badge-manual">manual</span> |

---

### Discrepancia entre publicaciones oficiales

El total de votos publicado en la página del Ministerio difiere del dato publicado en el BOE para la misma elección y circunscripción.

**1 elecciones afectadas · 3 territorios con incidencia en total**

| Elección | Año | N territorios | Nivel | Ejemplo | Resoluble | Origen |
| --- | --- | --- | --- | --- | --- | --- |
| Elecciones Generales 1977 | 1977 | 3 | provincia | Huelva | <span class="badge badge-no-resoluble">no resoluble</span> | <span class="badge badge-manual">manual</span> |


---

## Aviso: secciones censales con código de 4 dígitos


En España, el código de sección censal tiene habitualmente **3 dígitos** (p. ej. `001`, `042`).
Sin embargo, existen casos puntuales —concentrados en elecciones autonómicas antiguas y en
algunas convocatorias de los años 80 y 90— en los que el código de sección tiene **4 dígitos**
(p. ej. `1170`, `001A`, `001B`).


Para cumplir con la restricción de integridad de la base de datos, que requiere que todos los
códigos de sección tengan la misma longitud, **las secciones con código de 3 dígitos están
rellenas con un cero por la izquierda** (`001` → `0001`). Las secciones que ya tenían 4 dígitos
se almacenan tal como aparecen en la fuente original.


<div class="callout callout-warning">
<p><strong>Importante si unes estos datos con otras fuentes:</strong> Al cruzar <code>codigo_seccion</code> con datos de otras fuentes (p. ej. cartografía del INE, datos del Padrón, otras bases electorales), ten en cuenta que el primer dígito puede ser un cero de relleno. Para la mayoría de secciones <strong>deberás eliminar ese cero inicial</strong> antes de hacer el join. Verifica previamente si la elección y el territorio concreto aparecen en la lista de secciones con código original de 4 dígitos.</p>
</div>


En total se han identificado **209 secciones con código de 4 dígitos** en **8 CCAA** y **10 años**
distintos. La práctica totalidad corresponden a elecciones autonómicas.

<details>
<summary>Ver las 209 secciones con código de 4 dígitos</summary>

| Año | Tipo elección | CCAA | Cód. sección | N filas |
| --- | --- | --- | --- | --- |
| 1982 | Generales | Canarias | 1170 |  1 |
| 1982 | Generales | Extremadura | 2800 |  1 |
| 1982 | Generales | Castilla y León | 3300 |  1 |
| 1991 | Autonómicas | La Rioja | 001A | 47 |
| 1991 | Autonómicas | La Rioja | 001B | 47 |
| 1991 | Autonómicas | La Rioja | 001C |  7 |
| 1991 | Autonómicas | La Rioja | 001D |  2 |
| 1991 | Autonómicas | La Rioja | 002A |  3 |
| 1991 | Autonómicas | La Rioja | 002B |  3 |
| 1991 | Autonómicas | La Rioja | 002C |  2 |
| 1991 | Autonómicas | La Rioja | 002D |  1 |
| 1991 | Autonómicas | La Rioja | 003A |  4 |
| 1991 | Autonómicas | La Rioja | 003B |  4 |
| 1991 | Autonómicas | La Rioja | 003C |  1 |
| 1991 | Autonómicas | La Rioja | 004A |  1 |
| 1991 | Autonómicas | La Rioja | 004B |  1 |
| 1991 | Locales | Cantabria | 001A |  9 |
| 1991 | Locales | Cantabria | 001B |  9 |
| 1991 | Locales | Cantabria | 002A |  2 |
| 1991 | Locales | Cantabria | 002B |  2 |
| 1991 | Locales | La Rioja | 001A | 47 |
| 1991 | Locales | La Rioja | 001B | 47 |
| 1991 | Locales | La Rioja | 001C |  7 |
| 1991 | Locales | La Rioja | 001D |  2 |
| 1991 | Locales | La Rioja | 002A |  3 |
| 1991 | Locales | La Rioja | 002B |  3 |
| 1991 | Locales | La Rioja | 002C |  2 |
| 1991 | Locales | La Rioja | 002D |  1 |
| 1991 | Locales | La Rioja | 003A |  4 |
| 1991 | Locales | La Rioja | 003B |  4 |
| 1991 | Locales | La Rioja | 003C |  1 |
| 1991 | Locales | La Rioja | 004A |  1 |
| 1991 | Locales | La Rioja | 004B |  1 |
| 1991 | Locales | Canarias | 013A |  1 |
| 1991 | Locales | Canarias | 013B |  1 |
| 1991 | Locales | Canarias | 018A |  1 |
| 1991 | Locales | Canarias | 018B |  1 |
| 1991 | Locales | Canarias | 049A |  1 |
| 1991 | Locales | Canarias | 049B |  1 |
| 1991 | Locales | Canarias | 093A |  1 |
| 1991 | Locales | Canarias | 093B |  1 |
| 1991 | Locales | Baleares | 001A | 18 |
| 1991 | Locales | Baleares | 001B | 18 |
| 1991 | Locales | Baleares | 001C |  4 |
| 1991 | Locales | Baleares | 001D |  1 |
| 1991 | Locales | Baleares | 003A |  1 |
| 1991 | Locales | Baleares | 003B |  1 |
| 1991 | Locales | Castilla y León | 014A |  1 |
| 1991 | Locales | Castilla y León | 014B |  1 |
| 1991 | Locales | Castilla y León | 015A |  1 |
| 1991 | Locales | Castilla y León | 015B |  1 |
| 1993 | Generales | Cataluña | 005A |  1 |
| 1993 | Generales | Cataluña | 005B |  1 |
| 1993 | Generales | Cataluña | 005C |  1 |
| 1993 | Generales | Cataluña | 011A |  1 |
| 1993 | Generales | Cataluña | 011B |  1 |
| 1993 | Generales | Cataluña | 011C |  1 |
| 1993 | Generales | Cataluña | 041A |  1 |
| 1993 | Generales | Cataluña | 041B |  1 |
| 1993 | Generales | Cataluña | 041C |  1 |
| 1993 | Generales | Cataluña | 044A |  1 |
| 1993 | Generales | Cataluña | 044B |  1 |
| 1993 | Generales | Canarias | 013A |  1 |
| 1993 | Generales | Canarias | 013B |  1 |
| 1993 | Generales | Canarias | 018A |  1 |
| 1993 | Generales | Canarias | 018B |  1 |
| 1993 | Generales | Canarias | 049A |  1 |
| 1993 | Generales | Canarias | 049B |  1 |
| 1993 | Generales | Canarias | 093A |  1 |
| 1993 | Generales | Canarias | 093B |  1 |
| 1995 | Autonómicas | La Rioja | 001A | 50 |
| 1995 | Autonómicas | La Rioja | 001B | 50 |
| 1995 | Autonómicas | La Rioja | 001C | 14 |
| 1995 | Autonómicas | La Rioja | 001D |  3 |
| 1995 | Autonómicas | La Rioja | 002A |  2 |
| 1995 | Autonómicas | La Rioja | 002B |  2 |
| 1995 | Autonómicas | La Rioja | 002C |  2 |
| 1995 | Autonómicas | La Rioja | 002D |  1 |
| 1995 | Autonómicas | La Rioja | 003A |  5 |
| 1995 | Autonómicas | La Rioja | 003B |  5 |
| 1995 | Autonómicas | La Rioja | 003C |  2 |
| 1995 | Autonómicas | La Rioja | 003D |  1 |
| 1995 | Autonómicas | La Rioja | 004A |  1 |
| 1995 | Autonómicas | La Rioja | 004B |  1 |
| 1995 | Locales | Cataluña | 005A |  1 |
| 1995 | Locales | Cataluña | 005B |  1 |
| 1995 | Locales | Cataluña | 005C |  1 |
| 1995 | Locales | Cataluña | 011A |  1 |
| 1995 | Locales | Cataluña | 011B |  1 |
| 1995 | Locales | Cataluña | 011C |  1 |
| 1995 | Locales | Cataluña | 041A |  1 |
| 1995 | Locales | Cataluña | 041B |  1 |
| 1995 | Locales | Cataluña | 041C |  1 |
| 1995 | Locales | Cataluña | 044A |  1 |
| 1995 | Locales | Cataluña | 044B |  1 |
| 1995 | Locales | Cantabria | 001A | 10 |
| 1995 | Locales | Cantabria | 001B | 10 |
| 1995 | Locales | Cantabria | 001C |  3 |
| 1995 | Locales | Cantabria | 001D |  1 |
| 1995 | Locales | Cantabria | 002A |  2 |
| 1995 | Locales | Cantabria | 002B |  2 |
| 1995 | Locales | Cantabria | 002C |  1 |
| 1995 | Locales | La Rioja | 001A | 50 |
| 1995 | Locales | La Rioja | 001B | 50 |
| 1995 | Locales | La Rioja | 001C | 14 |
| 1995 | Locales | La Rioja | 001D |  3 |
| 1995 | Locales | La Rioja | 002A |  2 |
| 1995 | Locales | La Rioja | 002B |  2 |
| 1995 | Locales | La Rioja | 002C |  2 |
| 1995 | Locales | La Rioja | 002D |  1 |
| 1995 | Locales | La Rioja | 003A |  5 |
| 1995 | Locales | La Rioja | 003B |  5 |
| 1995 | Locales | La Rioja | 003C |  2 |
| 1995 | Locales | La Rioja | 003D |  1 |
| 1995 | Locales | La Rioja | 004A |  1 |
| 1995 | Locales | La Rioja | 004B |  1 |
| 1995 | Locales | Canarias | 001A |  4 |
| 1995 | Locales | Canarias | 001B |  4 |
| 1995 | Locales | Canarias | 001C |  1 |
| 1995 | Locales | Canarias | 002A |  1 |
| 1995 | Locales | Canarias | 002B |  1 |
| 1995 | Locales | Canarias | 003A |  1 |
| 1995 | Locales | Canarias | 003B |  1 |
| 1995 | Locales | Castilla y León | 014A |  1 |
| 1995 | Locales | Castilla y León | 014B |  1 |
| 1995 | Locales | Castilla y León | 015A |  1 |
| 1995 | Locales | Castilla y León | 015B |  1 |
| 1996 | Generales | Cataluña | 005A |  1 |
| 1996 | Generales | Cataluña | 005B |  1 |
| 1996 | Generales | Cataluña | 005C |  1 |
| 1996 | Generales | Cataluña | 011A |  1 |
| 1996 | Generales | Cataluña | 011B |  1 |
| 1996 | Generales | Cataluña | 011C |  1 |
| 1996 | Generales | Cataluña | 041A |  1 |
| 1996 | Generales | Cataluña | 041B |  1 |
| 1996 | Generales | Cataluña | 041C |  1 |
| 1996 | Generales | Cataluña | 044A |  1 |
| 1996 | Generales | Cataluña | 044B |  1 |
| 1999 | Locales | Cataluña | 011A |  1 |
| 1999 | Locales | Cataluña | 011B |  1 |
| 1999 | Locales | Cataluña | 011C |  1 |
| 1999 | Locales | Cataluña | 041A |  1 |
| 1999 | Locales | Cataluña | 041B |  1 |
| 1999 | Locales | Cataluña | 044A |  1 |
| 1999 | Locales | Cataluña | 044B |  1 |
| 2000 | Generales | Cataluña | 011A |  1 |
| 2000 | Generales | Cataluña | 011B |  1 |
| 2000 | Generales | Cataluña | 011C |  1 |
| 2000 | Generales | Cataluña | 041A |  1 |
| 2000 | Generales | Cataluña | 041B |  1 |
| 2000 | Generales | Cataluña | 044A |  1 |
| 2000 | Generales | Cataluña | 044B |  1 |
| 2003 | Locales | Cataluña | 011A |  1 |
| 2003 | Locales | Cataluña | 011B |  1 |
| 2003 | Locales | Cataluña | 011C |  1 |
| 2003 | Locales | Cataluña | 041A |  1 |
| 2003 | Locales | Cataluña | 041B |  1 |
| 2003 | Locales | Cataluña | 044A |  1 |
| 2003 | Locales | Cataluña | 044B |  1 |
| 2004 | Generales | Cataluña | 011A |  1 |
| 2004 | Generales | Cataluña | 011B |  1 |
| 2004 | Generales | Cataluña | 011C |  1 |
| 2004 | Generales | Cataluña | 041A |  1 |
| 2004 | Generales | Cataluña | 041B |  1 |
| 2004 | Generales | Cataluña | 044A |  1 |
| 2004 | Generales | Cataluña | 044B |  1 |
| 2019 | Autonómicas | Galicia | 001A |  4 |
| 2019 | Autonómicas | Galicia | 001B |  4 |
| 2019 | Autonómicas | Galicia | 001U |  3 |
| 2019 | Autonómicas | Galicia | 002A |  1 |
| 2019 | Autonómicas | Galicia | 002B |  1 |
| 2019 | Autonómicas | Galicia | 002U |  3 |
| 2019 | Autonómicas | Galicia | 003A |  4 |
| 2019 | Autonómicas | Galicia | 003B |  4 |
| 2019 | Autonómicas | Galicia | 003U |  3 |
| 2019 | Autonómicas | Galicia | 004A |  2 |
| 2019 | Autonómicas | Galicia | 004B |  2 |
| 2019 | Autonómicas | Galicia | 004U |  2 |
| 2019 | Autonómicas | Galicia | 005A |  4 |
| 2019 | Autonómicas | Galicia | 005B |  4 |
| 2019 | Autonómicas | Galicia | 005U |  1 |
| 2019 | Autonómicas | Galicia | 006A |  2 |
| 2019 | Autonómicas | Galicia | 006B |  2 |
| 2019 | Autonómicas | Galicia | 006U |  2 |
| 2019 | Autonómicas | Galicia | 007A |  1 |
| 2019 | Autonómicas | Galicia | 007B |  1 |
| 2019 | Autonómicas | Galicia | 007U |  2 |
| 2019 | Autonómicas | Galicia | 008A |  2 |
| 2019 | Autonómicas | Galicia | 008B |  2 |
| 2019 | Autonómicas | Galicia | 008U |  1 |
| 2019 | Autonómicas | Galicia | 009A |  2 |
| 2019 | Autonómicas | Galicia | 009B |  2 |
| 2019 | Autonómicas | Galicia | 009C |  1 |
| 2019 | Autonómicas | Galicia | 009U |  1 |
| 2019 | Autonómicas | Galicia | 010A |  1 |
| 2019 | Autonómicas | Galicia | 010B |  1 |
| 2019 | Autonómicas | Galicia | 010U |  2 |
| 2019 | Autonómicas | Galicia | 011A |  1 |
| 2019 | Autonómicas | Galicia | 011B |  1 |
| 2019 | Autonómicas | Galicia | 011U |  2 |
| 2019 | Autonómicas | Galicia | 012U |  1 |
| 2019 | Autonómicas | Galicia | 013A |  1 |
| 2019 | Autonómicas | Galicia | 013B |  1 |
| 2019 | Autonómicas | Galicia | 013U |  1 |
| 2019 | Autonómicas | Galicia | 014A |  1 |
| 2019 | Autonómicas | Galicia | 014B |  1 |
| 2019 | Autonómicas | Galicia | 015A |  2 |
| 2019 | Autonómicas | Galicia | 015B |  2 |
| 2019 | Autonómicas | Galicia | 016U |  1 |

</details>


---

## Nota metodológica

Las incidencias aquí documentadas no implican necesariamente que los datos de una
elección sean inválidos o inutilizables. En muchos casos:

- La incidencia afecta a un número reducido de territorios (a menudo secciones censales
  con censos muy pequeños o con errores de publicación puntuales).
- La magnitud del error es lo suficientemente pequeña como para no afectar a
  conclusiones agregadas a nivel municipal, provincial o autonómico.
- La propia fuente oficial contiene la inconsistencia y no existe una publicación
  alternativa fiable que permita corregirla.

El objetivo de esta página es precisamente mejorar la **transparencia del proyecto**:
que cualquier usuario pueda conocer de antemano las limitaciones del dato antes de
usarlo en un análisis.

---

## Mantenimiento

Esta página se genera automáticamente a partir de dos fuentes:

1. **Incidencias automáticas** — detectadas al ejecutar el pipeline sobre
   `tablas-finales/hechos/info.rds`. Se regeneran con cada actualización de datos.

2. **Incidencias manuales** — mantenidas en
   `docs-site/data/diagnosticos/incidencias_manual.csv`. Este fichero se puede
   editar directamente para añadir incidencias que no pueden detectarse
   automáticamente (inconsistencias documentales, discrepancias entre publicaciones, etc.).

### Estructura del CSV de incidencias manuales

```
tipo              # ID de máquina del tipo de incidencia
tipo_label        # Etiqueta legible
tipo_descripcion  # Descripción del tipo
eleccion_id       # ID numérico de la elección
eleccion          # Descripción de la elección
anio              # Año
tipo_eleccion     # G / A / L / E
n_casos           # Nº de territorios afectados en esta elección
nivel_territorial # Nivel de granularidad (municipio, sección, etc.)
peor_caso_territorio  # Nombre del territorio más afectado
peor_caso_valor       # Valor observado en ese territorio
peor_caso_esperado    # Valor esperado
fuente            # fuente_original / integracion
resoluble         # TRUE / FALSE
origen            # Siempre "manual" en este fichero
comentario        # Texto libre
```

### Pasos para añadir una incidencia manual

1. Editar `docs-site/data/diagnosticos/incidencias_manual.csv`.
2. Añadir una fila con `origen = "manual"` y los campos correspondientes.
3. Ejecutar `source("R/04-export/export-calidad.R")` (o el target `export_calidad` en el pipeline).
4. Commitear `docs-site/data/diagnosticos/incidencias_manual.csv` y `docs-site/content/calidad.md`.


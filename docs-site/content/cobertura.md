---
title: "Cobertura electoral"
description: "Elecciones cubiertas y granularidad disponible en eleccionesdb"
---

## Matriz de cobertura

La siguiente tabla muestra las elecciones cubiertas en la base de datos, organizadas por ámbito y año.

| Ámbito | Años cubiertos |
|--------|---------------|
| Congreso (nacional) | Todas las convocatorias desde 1977 |
| Andalucía | Todas las convocatorias |
| Aragón | 2011–2023 |
| Asturias | Todas las convocatorias |
| Baleares | 1983–2023 |
| Canarias | 1983–2023 |
| Castilla y León | 1983–2022 |
| Cataluña | 1980–2023 |
| C. Valenciana | 1983–2023 |
| C. de Madrid | 1983–2023 |

## Granularidad disponible

La granularidad de los datos varía según el tipo de elección y la fuente de datos:

| Ámbito | Granularidad máxima | Notas |
|--------|---------------------|-------|
| Congreso | Sección censal | Todas las convocatorias, datos vía paquete `infoelectoral` |
| Andalucía | Sección censal | API del Sistema de Información Electoral de Andalucía |
| Aragón | Sección censal (recientes) / Municipal (histórico) | Datos abiertos Gobierno de Aragón. 2011–2023 |
| Asturias | Mesa (2015+) / Municipal (pre-2015) | SADEI (histórico) + GIPEYOP (reciente) |
| Baleares | Sección censal | IBESTAT + GIPEYOP. 1983–2023 |
| Canarias | Municipal (PARCAN) / Sección (GIPEYOP) | Múltiples fuentes. 1983–2023 |
| Castilla y León | Mesa | Datos abiertos Junta de CyL. 1983–2022 |
| Cataluña | Mesa | Datos abiertos Generalitat. 1980–2023 |
| C. Valenciana | Sección censal | Datos abiertos GVA. 1983–2023 |
| C. de Madrid | Mesa | Datos abiertos Comunidad de Madrid. 1983–2023 |

<div class="callout callout-note">
<strong>Nota</strong>
<p>La base de datos almacena resultados agregados a múltiples niveles territoriales (CCAA, provincia, circunscripción, municipio, distrito, sección censal) según la disponibilidad de la fuente original. Consulta la sección de <a href="../fuentes/">fuentes de datos</a> para más detalles sobre cada comunidad.</p>
</div>

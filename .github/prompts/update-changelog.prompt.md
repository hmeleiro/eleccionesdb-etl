---
name: update-changelog
description: Actualiza el CHANGELOG.md a partir de los cambios del repositorio
agent: agent
tools: ['search/codebase']
argument-hint: Describe el rango de cambios, por ejemplo: "usa el diff actual" o "desde el último tag"
---

Tu tarea es actualizar `CHANGELOG.md`.

Instrucciones:
1. Revisa los archivos modificados y detecta cambios relevantes para el changelog.
2. Usa solo evidencia real del repositorio.
3. Clasifica los cambios en: Added, Changed, Fixed, Removed, Security.
4. Si ya existe una sección `[Unreleased]`, añade ahí los cambios.
5. Si no existe, créala al principio siguiendo el formato actual del archivo.
6. Mantén el estilo ya usado en `CHANGELOG.md`.
7. No incluyas cambios triviales de formato, refactors sin impacto o comentarios internos, salvo que afecten a mantenimiento de forma relevante.
8. Al final, muestra un breve resumen de lo añadido antes de aplicar cambios.

Contexto opcional del usuario:
{{input}}
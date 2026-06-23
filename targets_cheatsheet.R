source("run.R")

targets::tar_manifest() |> print(n = 40)

run_hechos()

# run_writedb()   # todo hasta escritura a BD
run_export_calidad() # todo hasta export de csv de calidad de datos
run_export()    # todo hasta exportación

# Ejecutar solo fases específicas
run_dims()      # solo dimensiones
run_hechos()    # dimensiones + hechos regionales
run_bind()      # dimensiones + hechos + bind

run_representantes_coverage()

# Visualizar el grafo de dependencias
show_pipeline()

targets::tar_meta(fields = warnings, complete_only = TRUE)

tar_invalidate(names = c("bind_hechos"))
# tar_invalidate(names = everything())
# tar_destroy()


targets::tar_meta(fields =  c(warnings), complete_only = TRUE)


source("run.R")

targets::tar_manifest() |> print(n = 30)

# run_writedb()   # todo hasta escritura a BD
run_export_calidad() # todo hasta export de csv de calidad de datos
run_export()    # todo hasta exportación

# Ejecutar solo fases específicas
run_dims()      # solo dimensiones
run_hechos()    # dimensiones + hechos regionales
run_bind()      # dimensiones + hechos + bind

# Visualizar el grafo de dependencias
show_pipeline()

targets::tar_meta(fields = warnings, complete_only = TRUE)

tar_invalidate(names = "hechos_andalucia")
# tar_invalidate(names = everything())
# tar_destroy()


targets::tar_meta(fields =  c(warnings), complete_only = TRUE)


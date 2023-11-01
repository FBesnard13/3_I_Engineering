library(future)
library(furrr)
library(parallel)


# Liste des chemins des scripts à exécuter en parallèle
script_paths <- c(
  "C:/Users/fbesnard/OneDrive - Laitram/R/3_E_Assembly/A1_Capacity_Wehl_perEmp_perDate_Flag.R",
  "C:/Users/fbesnard/OneDrive - Laitram/R/3_E_Assembly/A2_Capacity_Wehl_Refresh_Input.R")

# Exécuter les scripts en parallèle
futures <- future_map(script_paths, source, .options = furrr_options(seed = T))


# Run this to run this Sequential ====
lapply(script_paths, source)




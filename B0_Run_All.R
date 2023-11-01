library(future)
library(furrr)
library(parallel)


# Liste des chemins des scripts à exécuter en parallèle
script_paths <- c(
  "C:/Users/fbesnard/OneDrive - Laitram/R/3_E_Assembly/B2_Ass_Fcst_LMH.R",
  "C:/Users/fbesnard/OneDrive - Laitram/R/3_E_Assembly/B3_Ass_Fcst_ATO.R",
  "C:/Users/fbesnard/OneDrive - Laitram/R/3_E_Assembly/B4_Ass_Fcst_P2P.R",
  "C:/Users/fbesnard/OneDrive - Laitram/R/3_E_Assembly/B10_Ass_Fcst_Consolidation.R")

# Exécuter les scripts en parallèle
futures <- future_map(script_paths, source, .options = furrr_options(seed = T))


# Run this to run this Sequential ====
lapply(script_paths, source)




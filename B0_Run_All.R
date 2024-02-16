library(future)
library(furrr)
library(parallel)


# Liste des chemins des scripts à exécuter en parallèle
script_paths <- c(
  "C:/Users/fbesnard/OneDrive - Laitram/R/3_I_Engineering/B5_OE_Fcst_LMH_CTO.R",
  "C:/Users/fbesnard/OneDrive - Laitram/R/3_I_Engineering/B10_OE_Fcst_Consolidation.R",
  "C:/Users/fbesnard/OneDrive - Laitram/R/3_I_Engineering/B11_OE_Fcst_Hist_Save.R")

# Run this to run this Sequential ====
lapply(script_paths, source)




# Initialize the project ====

source("C:/Users/fbesnard/OneDrive - Laitram/Ressources/R/R_Programs/Load_Libraries.R")


# Import data ====

Ind_BOM_Complete <- read_parquet(r"(\\prod\root\v_drive\team\Intralox Dashboard\Intralox EMEA Equipment Operations App\Francois\Data\0_Cleaned\β_Indented_BOM\Ind_BOM_Complete.parquet)") |> type_convert()

Jobs_Programs <- read_parquet(r"(\\prod\root\v_drive\team\Intralox Dashboard\Intralox EMEA Equipment Operations App\Francois\Data\0_Cleaned\E_Jobs\Jobs_Programs.parquet)") |> type_convert()

Jobs_Clean <- read_parquet(r"(\\prod\root\v_drive\team\Intralox Dashboard\Intralox EMEA Equipment Operations App\Francois\Data\0_Cleaned\E_Jobs\Jobs_Clean.parquet)") |> type_convert()


# Consolidated ====

Ind_BOM_RnR <- Ind_BOM_Complete |> 
  
  filter(ATO_Module_Name == 'RnR')


Jobs_RnR <- Jobs_Clean |> 
  
  filter((Item_DPP_ID %in% Ind_BOM_RnR$Item_DPP_ID) | Item_DPP_ID %in% Ind_BOM_RnR$ATO_Module_DPP_ID) |> 
  filter(Job_Status == 'Closed')





# Clean & Save CSV ====

LMH_Fcst <- fclean_csv(LMH_Fcst)
write_parquet(LMH_Fcst, r"(\\prod\root\v_drive\team\Intralox Dashboard\Intralox EMEA Equipment Operations App\Francois\Data\1_Complex Cleaning\E_Assembly\Fcst\LMH\LMH_Fcst.parquet)")


# Print ====

print("B2_Ass_Fcst_LMH")






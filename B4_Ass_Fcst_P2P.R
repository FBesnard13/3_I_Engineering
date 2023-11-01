# Initialize the project ====

source("C:/Users/fbesnard/OneDrive - Laitram/Ressources/R/R_Programs/Load_Libraries.R")


# Set Up Parameters ====

source("C:/Users/fbesnard/OneDrive - Laitram/R/3_E_Assembly/B1_Ass_Fcst_Parameters.R")


# Import data ====

P2P_Opps_Cons_Sum <- read_parquet(r"(\\prod\root\v_drive\team\Intralox Dashboard\Intralox EMEA Equipment Operations App\Francois\Data\0_Cleaned\μ_Intralox_Opportunities\P2P\P2P_Opps_Cons_Sum.parquet)") |> type_convert()

Dates_Working_Filtered <- read_parquet(r"(\\prod\root\v_drive\team\Intralox Dashboard\Intralox EMEA Equipment Operations App\Francois\Data\0_Cleaned\Q_Dates\Dates_Working_Filtered.parquet)") |> type_convert() |> 
  mutate(Year = year(Date)) |> 
  mutate(Month = as.character(month(Date, label = F))) |> 
  mutate(Month = case_when(
    as.double(Month) > 9 ~ Month,
    .default = paste0('0', Month)))


# Consolidated ====

# Calculate Fcst_Hrs_Real & Fcst_Hrs_Opt
P2P_Fcst_Cons_Sum <- P2P_Opps_Cons_Sum |> 
  mutate(Fcst_Hrs_Real = Opp_Amount_Real * Cons_Coef * 0.001) |> 
  mutate(Fcst_Hrs_Opt = Opp_Amount_Opt * Cons_Coef * 0.001)
  

# Fill in data per day
for (i in 1:nrow(P2P_Fcst_Cons_Sum)) {
  
  tamp <- Dates_Working_Filtered |> 
    filter(Year == as.double(P2P_Fcst_Cons_Sum[i, 'Opp_Year'])) |> 
    filter(Month == as.character(P2P_Fcst_Cons_Sum[i, 'Opp_Month'])) |> 
    mutate(Fcst_Hrs_Real = P2P_Fcst_Cons_Sum[i, 'Fcst_Hrs_Real'] / n()) |> 
    mutate(Fcst_Hrs_Opt = P2P_Fcst_Cons_Sum[i, 'Fcst_Hrs_Opt'] / n())

  if(i == 1) {
    P2P_Fcst_Cons <- tamp
  } else {
    P2P_Fcst_Cons <- P2P_Fcst_Cons |>  rbind(tamp)
  }
}

P2P_Fcst_Cons <- P2P_Fcst_Cons |> 
  rename('Fcst_Hrs_Real' = 4) |> 
  rename('Fcst_Hrs_Opt' = 5) |> 
  select(Date, Fcst_Hrs_Real, Fcst_Hrs_Opt)


# Overall ====

P2P_Fcst <- P2P_Fcst_Cons |>
  group_by(Date) |> 
  summarise(Fcst_Hrs_Real = sum(Fcst_Hrs_Real),
            Fcst_Hrs_Opt = sum(Fcst_Hrs_Opt)) |> 
  mutate(Fcst_Hrs_Real = round(Fcst_Hrs_Real, 2)) |> 
  mutate(Fcst_Hrs_Opt = round(Fcst_Hrs_Opt, 2))


# Clean & Save CSV ====

P2P_Fcst <- fclean_csv(P2P_Fcst)
write_parquet(P2P_Fcst, r"(\\prod\root\v_drive\team\Intralox Dashboard\Intralox EMEA Equipment Operations App\Francois\Data\1_Complex Cleaning\E_Assembly\Fcst\P2P\P2P_Fcst.parquet)")


# Print ====

print("B4_Ass_Fcst_P2P")








# Initialize the project ====

source("C:/Users/fbesnard/OneDrive - Laitram/Ressources/R/R_Programs/Load_Libraries.R")


# Set Up Parameters ====

source("C:/Users/fbesnard/OneDrive - Laitram/R/3_E_Assembly/B1_Ass_Fcst_Parameters.R")


# Import data ====

LMH_Opps_Cons_Sum <- read_parquet(r"(\\prod\root\v_drive\team\Intralox Dashboard\Intralox EMEA Equipment Operations App\Francois\Data\0_Cleaned\μ_Intralox_Opportunities\LMH\LMH_Opps_Cons_Sum.parquet)") |> type_convert()

LMH_Opps_Spec <- read_parquet(r"(\\prod\root\v_drive\team\Intralox Dashboard\Intralox EMEA Equipment Operations App\Francois\Data\0_Cleaned\μ_Intralox_Opportunities\LMH\LMH_Opps_Spec.parquet)") |> type_convert()

TCS_Ass_Fcst <- read_parquet(r"(\\prod\root\v_drive\team\Intralox Dashboard\Intralox EMEA Equipment Operations App\Francois\Data\0_Cleaned\ι_TCS\TCS_Ass_Fcst.parquet)") |> type_convert()

Dates_Working_Filtered <- read_parquet(r"(\\prod\root\v_drive\team\Intralox Dashboard\Intralox EMEA Equipment Operations App\Francois\Data\0_Cleaned\Q_Dates\Dates_Working_Filtered.parquet)") |> type_convert() |> 
  mutate(Year = year(Date)) |> 
  mutate(Month = as.character(month(Date, label = F))) |> 
  mutate(Month = case_when(
    as.double(Month) > 9 ~ Month,
    .default = paste0('0', Month)))


# Consolidated ====

# Calculate Fcst_Hrs_Real & Fcst_Hrs_Opt
LMH_Fcst_Cons_Sum <- LMH_Opps_Cons_Sum |> 
  mutate(Fcst_Hrs_Real = Opp_Amount_Real * Cons_Coef * 0.001) |> 
  mutate(Fcst_Hrs_Opt = Opp_Amount_Opt * Cons_Coef * 0.001)
  

# Fill in data per day
for (i in 1:nrow(LMH_Fcst_Cons_Sum)) {
  
  tamp <- Dates_Working_Filtered |> 
    filter(Year == as.double(LMH_Fcst_Cons_Sum[i, 'Opp_Year'])) |> 
    filter(Month == as.character(LMH_Fcst_Cons_Sum[i, 'Opp_Month'])) |> 
    mutate(Fcst_Hrs_Real = LMH_Fcst_Cons_Sum[i, 'Fcst_Hrs_Real'] / n()) |> 
    mutate(Fcst_Hrs_Opt = LMH_Fcst_Cons_Sum[i, 'Fcst_Hrs_Opt'] / n())

  if(i == 1) {
    LMH_Fcst_Cons <- tamp
  } else {
    LMH_Fcst_Cons <- LMH_Fcst_Cons |>  rbind(tamp)
  }
}

LMH_Fcst_Cons <- LMH_Fcst_Cons |> 
  select(Date, Fcst_Hrs_Real, Fcst_Hrs_Opt)


# Specific Projects ====

# Calculate Fcst_Hrs_Real & Fcst_Hrs_Opt
LMH_Fcst_Spec_Calc <- LMH_Opps_Spec |> 
  merge(TCS_Ass_Fcst, by.x = 'Opp_TCS_ID', by.y = 'GD_Quotation_number') |> 
  mutate(Fcst_Hrs_Opt = case_when(
    GD_Belt_serie == 'S400' ~ 
      (Spec_S400_SP * Com_Sale_price_US_dollar * 0.001) + 
      (Spec_S400_BH * TP_Assembly_Total_Hrs) + 
      (Spec_S400_A * CS_Belt_width_in * CS_Conveyor_length_mm *  0.0000254),
    GD_Belt_serie == 'S4500' ~ 
      (Spec_S4500_SP * TP_Assembly_Total_Hrs),
    GD_Belt_serie == 'S7000' ~ 
      (Spec_S7000_SP * Com_Sale_price_US_dollar * 0.001) + 
      (Spec_S7000_LP * Com_List_price_US_dollar * 0.001),
    .default = 0)) |> 
  mutate(Fcst_Hrs_Real = Fcst_Hrs_Opt * Opp_Prob) |> 
  select(Opp_Ship_Date, Fcst_Hrs_Real, Fcst_Hrs_Opt) |> 
  filter(Opp_Ship_Date >= min(LMH_Fcst_Cons$Date)) |> 
  filter(Opp_Ship_Date <= max(LMH_Fcst_Cons$Date))
  
# Fill in data per day
for (i in 1:nrow(LMH_Fcst_Spec_Calc)) {
  
  tamp <- Dates_Working_Filtered |> 
    select(Date) |> 
    filter(Date < (LMH_Fcst_Spec_Calc[i, 'Opp_Ship_Date'] - 14)) |>
    arrange(desc(Date)) %>% 
    slice(1:10) |> 
    mutate(Fcst_Hrs_Real = LMH_Fcst_Spec_Calc[i, 'Fcst_Hrs_Real'] / 10) |> 
    mutate(Fcst_Hrs_Opt = LMH_Fcst_Spec_Calc[i, 'Fcst_Hrs_Opt'] / 10)
  
  if(i == 1) {
    LMH_Fcst_Spec <- tamp
  } else {
    LMH_Fcst_Spec <- LMH_Fcst_Spec |>  rbind(tamp)
  }
}


# Overall ====

LMH_Fcst <- LMH_Fcst_Cons |> 
  rbind(LMH_Fcst_Spec) |> 
  group_by(Date) |> 
  summarise(Fcst_Hrs_Real = sum(Fcst_Hrs_Real),
            Fcst_Hrs_Opt = sum(Fcst_Hrs_Opt))


# Clean & Save CSV ====

LMH_Fcst <- fclean_csv(LMH_Fcst)
write_parquet(LMH_Fcst, r"(\\prod\root\v_drive\team\Intralox Dashboard\Intralox EMEA Equipment Operations App\Francois\Data\1_Complex Cleaning\E_Assembly\Fcst\LMH\LMH_Fcst.parquet)")


# Print ====

print("B2_Ass_Fcst_LMH")






# Initialize the project ====

source("C:/Users/fbesnard/Documents/.Rprofile")


# Set Up Parameters ====

source("C:/Users/fbesnard/OneDrive - Laitram/R/3_I_Engineering/B1_OE_Fcst_Parameters.R")


# Import data ====

LMH_CTO_Opps_Cons_Sum <- read_parquet(r"(\\prod\root\v_drive\team\Intralox Dashboard\Intralox EMEA Equipment Operations App\Francois\Data\0_Cleaned\μ_Intralox_Opportunities\LMH\LMH_CTO_Opps_Cons_Sum.parquet)") |> 
  type_convert() %>% suppressMessages()

LMH_Opps_Spec <- read_parquet(r"(\\prod\root\v_drive\team\Intralox Dashboard\Intralox EMEA Equipment Operations App\Francois\Data\0_Cleaned\μ_Intralox_Opportunities\LMH\LMH_Opps_Spec.parquet)") |> 
  type_convert() %>% suppressMessages()

TCS_Ass_Fcst <- read_parquet(r"(\\prod\root\v_drive\team\Intralox Dashboard\Intralox EMEA Equipment Operations App\Francois\Data\0_Cleaned\ι_TCS\TCS_Ass_Fcst.parquet)") |> 
  type_convert() %>% suppressMessages()

Dates_Working_Filtered <- read_parquet(r"(\\prod\root\v_drive\team\Intralox Dashboard\Intralox EMEA Equipment Operations App\Francois\Data\0_Cleaned\Q_Dates\Dates_Working_Filtered.parquet)") |> 
  type_convert() %>% suppressMessages() %>% 
  mutate(Year = year(Date)) |> 
  mutate(Month = as.character(month(Date, label = F))) |> 
  mutate(Month = case_when(
    as.double(Month) > 9 ~ Month,
    .default = paste0('0', Month)))


# Consolidated ====

# Calculate Fcst_Hrs_Real & Fcst_Hrs_Opt
LMH_CTO_Fcst_Cons_Sum <- LMH_CTO_Opps_Cons_Sum |> 
  mutate(Fcst_Hrs_Real = Opp_Amount_Real * Cons_Coef * 0.001) |> 
  mutate(Fcst_Hrs_Opt = Opp_Amount_Opt * Cons_Coef * 0.001)
  

# Fill in data per day
for (i in 1:nrow(LMH_CTO_Fcst_Cons_Sum)) {
  
  tamp <- Dates_Working_Filtered |> 
    filter(Year == as.double(LMH_CTO_Fcst_Cons_Sum[i, 'Opp_Year'])) |> 
    filter(Month == as.character(LMH_CTO_Fcst_Cons_Sum[i, 'Opp_Month'])) |> 
    mutate(Fcst_Hrs_Real = LMH_CTO_Fcst_Cons_Sum[i, 'Fcst_Hrs_Real'] / n()) |> 
    mutate(Fcst_Hrs_Opt = LMH_CTO_Fcst_Cons_Sum[i, 'Fcst_Hrs_Opt'] / n())

  if(i == 1) {
    LMH_CTO_Fcst_Cons <- tamp
  } else {
    LMH_CTO_Fcst_Cons <- LMH_CTO_Fcst_Cons |>  rbind(tamp)
  }
}

LMH_CTO_Fcst_Cons <- LMH_CTO_Fcst_Cons |> 
  select(Date, Fcst_Hrs_Real, Fcst_Hrs_Opt)


# Specific Projects ====

# Calculate Fcst_Hrs_Real & Fcst_Hrs_Opt
LMH_CTO_Fcst_Spec_Calc <- LMH_Opps_Spec |> 
  merge(TCS_Ass_Fcst, by.x = 'Opp_TCS_ID', by.y = 'GD_Quotation_number') |> 
  filter(str_detect(GD_Product_name_Internal, 'CTO')) |> 
  mutate(Fcst_Hrs_Opt = EMEA_OE_Budjeted_Hrs) |> 
  mutate(Fcst_Hrs_Real = Fcst_Hrs_Opt * Opp_Prob) |> 
  select(Opp_Ship_Date, Fcst_Hrs_Real, Fcst_Hrs_Opt) |> 
  filter(Opp_Ship_Date >= min(LMH_CTO_Fcst_Cons$Date)) |> 
  filter(Opp_Ship_Date <= max(LMH_CTO_Fcst_Cons$Date))
  
# Fill in data per day
for (i in 1:nrow(LMH_CTO_Fcst_Spec_Calc)) {
  
  tamp <- Dates_Working_Filtered |> 
    select(Date) |> 
    filter(Date < (LMH_CTO_Fcst_Spec_Calc[i, 'Opp_Ship_Date'] - 14)) |>
    arrange(desc(Date)) %>% 
    slice(1:10) |> 
    mutate(Fcst_Hrs_Real = LMH_CTO_Fcst_Spec_Calc[i, 'Fcst_Hrs_Real'] / 10) |> 
    mutate(Fcst_Hrs_Opt = LMH_CTO_Fcst_Spec_Calc[i, 'Fcst_Hrs_Opt'] / 10)
  
  if(i == 1) {
    LMH_CTO_Fcst_Spec <- tamp
  } else {
    LMH_CTO_Fcst_Spec <- LMH_CTO_Fcst_Spec |>  rbind(tamp)
  }
}


# Overall ====

LMH_CTO_Fcst <- LMH_CTO_Fcst_Cons |> 
  rbind(LMH_CTO_Fcst_Spec) |> 
  group_by(Date) |> 
  summarise(Fcst_Hrs_Real = sum(Fcst_Hrs_Real),
            Fcst_Hrs_Opt = sum(Fcst_Hrs_Opt))


# Clean & Save CSV ====

LMH_CTO_Fcst <- f_clean_csv(LMH_CTO_Fcst)
write_parquet(LMH_CTO_Fcst, r"(\\prod\root\v_drive\team\Intralox Dashboard\Intralox EMEA Equipment Operations App\Francois\Data\1_Complex Cleaning\I_Engineering\OE\LMH_CTO\I_B_LMH_CTO_Fcst.parquet)")


# Print ====

print("3_I_Engineering - B_OE_Forecast - B5_OE_Fcst_LMH_CTO")






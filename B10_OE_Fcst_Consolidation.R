# Initialize the project ====

source("C:/Users/fbesnard/OneDrive - Laitram/R/9_Set_Up/Load_Libraries.R")


# Import data ====

Dates_Working_Filtered <- read_parquet(r"(\\prod\root\v_drive\team\Intralox Dashboard\Intralox EMEA Equipment Operations App\Francois\Data\0_Cleaned\Q_Dates\Dates_Working_Filtered.parquet)") |> 
  type_convert() %>% suppressMessages()

LMH_CTO_Fcst <- read_parquet(r"(\\prod\root\v_drive\team\Intralox Dashboard\Intralox EMEA Equipment Operations App\Francois\Data\1_Complex Cleaning\I_Engineering\OE\LMH_CTO\I_B_LMH_CTO_Fcst.parquet)") |> 
  type_convert() %>% suppressMessages()


# CTO_Fcst ====

CTO_Fcst_tamp <- LMH_CTO_Fcst |> 
  group_by(Date) |> 
  summarise(Fcst_Hrs_Real = sum(Fcst_Hrs_Real),
            Fcst_Hrs_Opt = sum(Fcst_Hrs_Opt)) |> 
  mutate(Fcst_Hrs_Real = round(Fcst_Hrs_Real, 0)) |> 
  mutate(Fcst_Hrs_Opt = round(Fcst_Hrs_Opt, 0)) |> 
  filter(Date >= (Sys.Date() + 90)) |> 
  
  filter(!(Date %in% c('2024-12-30', '2024-12-31'))) # To avoid problems of data hierarchy (week 1 year 2025 but in 2024)


CTO_Fcst <- Dates_Working_Filtered |> 
  
  filter(Date >= min(CTO_Fcst_tamp$Date)) |> 
  filter(Date <= max(CTO_Fcst_tamp$Date)) |> 
  left_join(CTO_Fcst_tamp, by = 'Date') |> 
  
  mutate(Fcst_Hrs_Real = case_when(
    is.na(Fcst_Hrs_Real) ~ 0,
    .default = Fcst_Hrs_Real)) |> 
  mutate(Fcst_Hrs_Opt = case_when(
    is.na(Fcst_Hrs_Opt) ~ 0,
    .default = Fcst_Hrs_Opt)) |> 
  
  filter(!(Date %in% c('2024-12-30', '2024-12-31'))) # To avoid problems of data hierarchy (week 1 year 2025 but in 2024)


# Clean & Save CSV ====

CTO_Fcst <- f_clean_csv(CTO_Fcst)
write_parquet(CTO_Fcst, r"(\\prod\root\v_drive\team\Intralox Dashboard\Intralox EMEA Equipment Operations App\Francois\Data\1_Complex Cleaning\I_Engineering\OE\LMH_CTO\I_B_CTO_Fcst.parquet)")


# Print ====

print("3_I_Engineering - B_OE_Forecast - B10_OE_Fcst_Consolidation")






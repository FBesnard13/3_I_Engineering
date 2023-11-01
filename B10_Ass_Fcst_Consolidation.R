# Initialize the project ====

source("C:/Users/fbesnard/OneDrive - Laitram/Ressources/R/R_Programs/Load_Libraries.R")


# Import data ====

Dates_Working_Filtered <- read_parquet(r"(\\prod\root\v_drive\team\Intralox Dashboard\Intralox EMEA Equipment Operations App\Francois\Data\0_Cleaned\Q_Dates\Dates_Working_Filtered.parquet)") |> type_convert()

ATO_Fcst <- read_parquet(r"(\\prod\root\v_drive\team\Intralox Dashboard\Intralox EMEA Equipment Operations App\Francois\Data\1_Complex Cleaning\E_Assembly\Fcst\ATO\ATO_Fcst.parquet)") |> type_convert()
LMH_Fcst <- read_parquet(r"(\\prod\root\v_drive\team\Intralox Dashboard\Intralox EMEA Equipment Operations App\Francois\Data\1_Complex Cleaning\E_Assembly\Fcst\LMH\LMH_Fcst.parquet)") |> type_convert()
P2P_Fcst <- read_parquet(r"(\\prod\root\v_drive\team\Intralox Dashboard\Intralox EMEA Equipment Operations App\Francois\Data\1_Complex Cleaning\E_Assembly\Fcst\P2P\P2P_Fcst.parquet)") |> type_convert()


# All_Fcst ====

All_Fcst_tamp <- ATO_Fcst |> 
  rbind(P2P_Fcst) |> 
  rbind(LMH_Fcst) |> 
  group_by(Date) |> 
  summarise(Fcst_Hrs_Real = sum(Fcst_Hrs_Real),
            Fcst_Hrs_Opt = sum(Fcst_Hrs_Opt)) |> 
  mutate(Fcst_Hrs_Real = round(Fcst_Hrs_Real, 0)) |> 
  mutate(Fcst_Hrs_Opt = round(Fcst_Hrs_Opt, 0)) |> 
  filter(Date >= (Sys.Date() + 90)) 
  

All_Fcst <- Dates_Working_Filtered |> 
  
  filter(Date >= min(All_Fcst_tamp$Date)) |> 
  filter(Date <= max(All_Fcst_tamp$Date)) |> 
  left_join(All_Fcst_tamp, by = 'Date') |> 
  
  mutate(Fcst_Hrs_Real = case_when(
    is.na(Fcst_Hrs_Real) ~ 0,
    .default = Fcst_Hrs_Real)) |> 
  mutate(Fcst_Hrs_Opt = case_when(
    is.na(Fcst_Hrs_Opt) ~ 0,
    .default = Fcst_Hrs_Opt)) |> 
  
  filter(!(Date %in% c('2024-12-30', '2024-12-31'))) # To avoid problems of data hierarchy (week 1 year 2025 but in 2024)


# ATO_Fcst ====

ATO_Fcst_tamp <- ATO_Fcst |> 
  mutate(Fcst_Hrs_Real = round(Fcst_Hrs_Real, 0)) |> 
  mutate(Fcst_Hrs_Opt = round(Fcst_Hrs_Opt, 0)) |> 
  filter(Date >= (Sys.Date() + 90)) |> 
  
  filter(!(Date %in% c('2024-12-30', '2024-12-31'))) # To avoid problems of data hierarchy (week 1 year 2025 but in 2024)


ATO_Fcst <- Dates_Working_Filtered |> 
  
  filter(Date >= min(ATO_Fcst_tamp$Date)) |> 
  filter(Date <= max(ATO_Fcst_tamp$Date)) |> 
  left_join(ATO_Fcst_tamp, by = 'Date') |> 
  
  mutate(Fcst_Hrs_Real = case_when(
    is.na(Fcst_Hrs_Real) ~ 0,
    .default = Fcst_Hrs_Real)) |> 
  mutate(Fcst_Hrs_Opt = case_when(
    is.na(Fcst_Hrs_Opt) ~ 0,
    .default = Fcst_Hrs_Opt)) |> 
  
  filter(!(Date %in% c('2024-12-30', '2024-12-31'))) # To avoid problems of data hierarchy (week 1 year 2025 but in 2024)


# ETO_Fcst ====

ETO_Fcst_tamp <- LMH_Fcst |> 
  rbind(P2P_Fcst) |> 
  group_by(Date) |> 
  summarise(Fcst_Hrs_Real = sum(Fcst_Hrs_Real),
            Fcst_Hrs_Opt = sum(Fcst_Hrs_Opt)) |> 
  mutate(Fcst_Hrs_Real = round(Fcst_Hrs_Real, 0)) |> 
  mutate(Fcst_Hrs_Opt = round(Fcst_Hrs_Opt, 0)) |> 
  filter(Date >= (Sys.Date() + 90)) |> 
  
  filter(!(Date %in% c('2024-12-30', '2024-12-31'))) # To avoid problems of data hierarchy (week 1 year 2025 but in 2024)


ETO_Fcst <- Dates_Working_Filtered |> 
  
  filter(Date >= min(ETO_Fcst_tamp$Date)) |> 
  filter(Date <= max(ETO_Fcst_tamp$Date)) |> 
  left_join(ETO_Fcst_tamp, by = 'Date') |> 
  
  mutate(Fcst_Hrs_Real = case_when(
    is.na(Fcst_Hrs_Real) ~ 0,
    .default = Fcst_Hrs_Real)) |> 
  mutate(Fcst_Hrs_Opt = case_when(
    is.na(Fcst_Hrs_Opt) ~ 0,
    .default = Fcst_Hrs_Opt)) |> 
  
  filter(!(Date %in% c('2024-12-30', '2024-12-31'))) # To avoid problems of data hierarchy (week 1 year 2025 but in 2024)


# Clean & Save CSV ====

ATO_Fcst <- fclean_csv(ATO_Fcst)
write_parquet(ATO_Fcst, r"(\\prod\root\v_drive\team\Intralox Dashboard\Intralox EMEA Equipment Operations App\Francois\Data\1_Complex Cleaning\E_Assembly\Fcst\ATO_Fcst.parquet)")

ETO_Fcst <- fclean_csv(ETO_Fcst)
write_parquet(ETO_Fcst, r"(\\prod\root\v_drive\team\Intralox Dashboard\Intralox EMEA Equipment Operations App\Francois\Data\1_Complex Cleaning\E_Assembly\Fcst\ETO_Fcst.parquet)")

All_Fcst <- fclean_csv(All_Fcst)
write_parquet(All_Fcst, r"(\\prod\root\v_drive\team\Intralox Dashboard\Intralox EMEA Equipment Operations App\Francois\Data\1_Complex Cleaning\E_Assembly\Fcst\All_Fcst.parquet)")


# Print ====

print("B10_Ass_Fcst_Consolidation")






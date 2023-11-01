# Initialize the project ====

source("C:/Users/fbesnard/OneDrive - Laitram/Ressources/R/R_Programs/Load_Libraries.R")


# Import data ====

ATO_Opps_Francois <- read_parquet("//prod/root/v_drive/team/Intralox Dashboard/Intralox EMEA Equipment Operations App/Francois/Data/0_Cleaned/μ_Intralox_Opportunities/ATO/ATO_Opps_Francois.parquet") |> type_convert()

ATO_TCS <- read_csv(r"(\\prod\root\S_Drive\euam\All_Shared\SHARE\ARB team Europe & Asia-Pac\OPERATIONS\Data Input - Francois\Opportunities - Francois\ATO\ATO_TCS Features.csv)") |> type_convert()

Dates_Working_Filtered <- read_parquet(r"(\\prod\root\v_drive\team\Intralox Dashboard\Intralox EMEA Equipment Operations App\Francois\Data\0_Cleaned\Q_Dates\Dates_Working_Filtered.parquet)") |> type_convert()


# ATO_Opps_Calc ====

ATO_Opps_Calc <- ATO_Opps_Francois |> 
  
  merge(ATO_TCS) |> 
  mutate(Fcst_Hrs_Real = Real_Qty * TP_Assembly_Total_Hrs) |> 
  mutate(Fcst_Hrs_Opt = Opt_Qty * TP_Assembly_Total_Hrs) |> 
  select(Program_Label, Scenario, First_Ship_Date, Last_Ship_Date, Fcst_Hrs_Real, Fcst_Hrs_Opt)


for (i in 1:nrow(ATO_Opps_Calc)) {

  #Clean First & Last ship dates not a wokring day
  while(!(ATO_Opps_Calc[i, 'First_Ship_Date'] %in% Dates_Working_Filtered$Date)){
    ATO_Opps_Calc[i, 'First_Ship_Date'] <- ATO_Opps_Calc[i, 'First_Ship_Date']-1
  }
  while(!(ATO_Opps_Calc[i, 'Last_Ship_Date'] %in% Dates_Working_Filtered$Date)){
    ATO_Opps_Calc[i, 'Last_Ship_Date'] <- ATO_Opps_Calc[i, 'Last_Ship_Date']-1
  }
  
  # Fill in data per day
  tamp <- Dates_Working_Filtered |> 
    filter(Date >= ATO_Opps_Calc[i, 'First_Ship_Date']) |> 
    filter(Date <= ATO_Opps_Calc[i, 'Last_Ship_Date']) |> 
    mutate(Fcst_Hrs_Real = ATO_Opps_Calc[i, 'Fcst_Hrs_Real'] / n()) |> 
    mutate(Fcst_Hrs_Opt = ATO_Opps_Calc[i, 'Fcst_Hrs_Opt'] / n()) |> 
    mutate(Program_Label = ATO_Opps_Calc[i, 'Program_Label']) |> 
    mutate(Scenario = ATO_Opps_Calc[i, 'Scenario'])
  
  if(i == 1) {
    ATO_Fcst_tamp <- tamp
  } else {
    ATO_Fcst_tamp <- ATO_Fcst_tamp |>  rbind(tamp)
  }
  
}


# Summarise the data
ATO_Fcst <- ATO_Fcst_tamp |> 
  filter(str_sub(Scenario, 1, 1) == '1') |> 
  group_by(Date) |> 
  summarise(Fcst_Hrs_Real = sum(Fcst_Hrs_Real),
            Fcst_Hrs_Opt = sum(Fcst_Hrs_Opt))


# ATO_Fcst_Detail ====

ATO_Fcst_Detail <- ATO_Fcst_tamp |> 
  group_by(Date, Program_Label, Scenario) |> 
  summarise(Fcst_Hrs_Real = sum(Fcst_Hrs_Real)) |> 
  mutate(Fcst_Hrs_Real = round(Fcst_Hrs_Real, 1))

#Add the Total per week per scenario
tamp_total <- ATO_Fcst_Detail |> 
  group_by(Date, Scenario) |> 
  summarise(Fcst_Hrs_Real = sum(Fcst_Hrs_Real)) |> 
  mutate(Program_Label = 'Total')

ATO_Fcst_Detail <- ATO_Fcst_Detail |> 
  rbind(tamp_total)



# Clean & Save CSV ====

ATO_Fcst <- fclean_csv(ATO_Fcst)
write_parquet(ATO_Fcst, r"(\\prod\root\v_drive\team\Intralox Dashboard\Intralox EMEA Equipment Operations App\Francois\Data\1_Complex Cleaning\E_Assembly\Fcst\ATO\ATO_Fcst.parquet)")

ATO_Fcst_Detail <- fclean_csv(ATO_Fcst_Detail)
write_parquet(ATO_Fcst_Detail, r"(\\prod\root\v_drive\team\Intralox Dashboard\Intralox EMEA Equipment Operations App\Francois\Data\1_Complex Cleaning\E_Assembly\Fcst\ATO_Fcst_Detail.parquet)")


# Print ====

print("B3_Ass_Fcst_ATO")






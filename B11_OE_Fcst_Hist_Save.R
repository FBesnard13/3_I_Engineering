# Initialize the project ====

source("C:/Users/fbesnard/Documents/.Rprofile")


# Import data ====

Dates_Working_Filtered <- read_parquet(r"(\\prod\root\v_drive\team\Intralox Dashboard\Intralox EMEA Equipment Operations App\Francois\Data\0_Cleaned\Q_Dates\Dates_Working_Filtered.parquet)") |> 
  type_convert() %>% suppressMessages()

CTO_Fcst <- read_parquet(r"(\\prod\root\v_drive\team\Intralox Dashboard\Intralox EMEA Equipment Operations App\Francois\Data\1_Complex Cleaning\I_Engineering\OE\LMH_CTO\I_B_CTO_Fcst.parquet)") |> 
  type_convert() %>% suppressMessages()

Link_DateWithWeek <- data.frame(read_parquet(r"(\\prod\root\v_drive\team\Intralox Dashboard\Intralox EMEA Equipment Operations App\Francois\Data\0_Cleaned\Q_Dates\Dates_Hierarchy_v2.parquet)"))


# Reformat data ====

Link_DateWithWeek <- Link_DateWithWeek %>% 
  rename('Year_Week' = 'Year_Week_ISO') %>% 
  mutate(Date = as.Date(Date)) %>% 
  mutate(Year_Week = paste0('20', Year_Week))


# Extract the Year_Week of today ====

TodayDate <- Sys.Date()
Year_Week <- Link_DateWithWeek[Link_DateWithWeek$Date==TodayDate,"Year_Week"]


# Look if the Year_Week folder exist in history, and if not, create it ====

folder_dir <- "C:/Users/fbesnard/OneDrive - Laitram/SIOP/Forecast OE/Forecast OE History/"
folder_dir <- paste(folder_dir, Year_Week, sep="")

if(file.exists(folder_dir) == FALSE){
  dir.create(path = folder_dir)
}

write_parquet(CTO_Fcst, paste(folder_dir, "/CTO_Fcst.parquet", sep=""))


# Print ====

print("3_I_Engineering - B_OE_Forecast - B11_OE_Fcst_Hist_Save")






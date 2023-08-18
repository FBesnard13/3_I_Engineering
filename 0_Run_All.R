print("")
print("")
print("")
print("#################################################################################################")
print("#################################################################################################")
print("#################################################################################################")
print(paste("Run of the", Sys.time(), sep = " "))
print("#################################################################################################")
print("#################################################################################################")
print("#################################################################################################")


source("C:/Users/fbesnard/OneDrive - Laitram/R/3_E_Assembly/A_Capacity_Wehl_perEmp_perDate_Flag.R")
print("A_Capacity_Wehl_perEmp_perDate_Flag.R")

# source("C:/Users/fbesnard/OneDrive - Laitram/R/3_E_Assembly/C_ATO_Track_Hours.R")
# print("C_ATO_Track_Hours.R")

source("C:/Users/fbesnard/OneDrive - Laitram/R/3_E_Assembly/G0_Run_All.R")
print("G0_Run_All.R")


# Register the run in the excel check file
library(tidyverse)
RStudio_Runs <- read_csv(r"(\\prod\root\S_Drive\euam\All_Shared\SHARE\ARB team Europe & Asia-Pac\OPERATIONS\Data Input - Francois\PBI Management - Francois\RStudio_Runs.csv)")
RStudio_Runs[RStudio_Runs$Program == 'RStudio_3_E_Assembly', 'Ran_Flag'] <- 1
RStudio_Runs[RStudio_Runs$Program == 'RStudio_3_E_Assembly', 'Refreshed_Date'] <- Sys.Date()
RStudio_Runs$Refreshed_Time <- as.character(RStudio_Runs$Refreshed_Time)
RStudio_Runs[RStudio_Runs$Program == 'RStudio_3_E_Assembly', 'Refreshed_Time'] <- str_sub(Sys.time(), 12, 16)
write_excel_csv(RStudio_Runs, r"(\\prod\root\S_Drive\euam\All_Shared\SHARE\ARB team Europe & Asia-Pac\OPERATIONS\Data Input - Francois\PBI Management - Francois\RStudio_Runs.csv)")

rm(list = ls(all.names = TRUE))

# Initialize the project ==========================================================================

source("C:/Users/fbesnard/OneDrive - Laitram/Ressources/R/R_Programs/Load_Libraries.R")


# Import data ==========================================================================

Capacity_Wehl_perEmp_perDate <-  read_csv("//prod/root/v_drive/team/Intralox Dashboard/Intralox EMEA Equipment Operations App/Francois/Data/0_Cleaned/R_Capacity_Wehl/Capacity_Wehl_perEmp_perDate.csv", name_repair = "minimal", show_col_types = F)
Capacity_Wehl_Clean <-  read_csv("//prod/root/v_drive/team/Intralox Dashboard/Intralox EMEA Equipment Operations App/Francois/Data/0_Cleaned/R_Capacity_Wehl/Capacity_Wehl_Clean.csv", name_repair = "minimal", show_col_types = F)

Dates_Ess_NL <-  read_csv("//prod/root/v_drive/team/Intralox Dashboard/Intralox EMEA Equipment Operations App/Francois/Data/0_Cleaned/Q_Dates/Dates_Ess_NL.csv", name_repair = "minimal", show_col_types = F)


# Build Capacity_Wehl_perEmp_perDate_Flag  ==========================================================================

# Extract next 3 weeks ----
TodayDay <- as.Date(Sys.time())
YearWeeksToKeep <- list(as.character(Dates_Ess_NL[Dates_Ess_NL$Date == as.Date(Sys.time()), 'Year_Week_ISO']), 
                        as.character(Dates_Ess_NL[Dates_Ess_NL$Date == as.Date(Sys.time()) + 7, 'Year_Week_ISO']), 
                        as.character(Dates_Ess_NL[Dates_Ess_NL$Date == as.Date(Sys.time()) + 14, 'Year_Week_ISO']))

# Extract Working days for next 3 weeks ----
Dates_Ess_NL <- Dates_Ess_NL %>% 
  filter(Year_Week_ISO %in% YearWeeksToKeep) %>% 
  filter(Day_of_Week_ISO %in% c(1:5))

# Preparing Capacity_Wehl_perEmp_perDate_Flag ----
Capacity_Wehl_perEmp_perDate_Flag <- tibble(Dates_Ess_NL$Date) %>% 
  rename(Date = 1)
Capacity_Wehl_perEmp_perDate_Flag[ ,Capacity_Wehl_Clean$Emp_Name] <- NA

# Filling in Capacity_Wehl_perEmp_perDate_Flag ----
for (i in 1:nrow(Capacity_Wehl_perEmp_perDate_Flag)) {
  
  for (j in 2:ncol(Capacity_Wehl_perEmp_perDate_Flag)) {
    
    emp = colnames(Capacity_Wehl_perEmp_perDate_Flag)[j]
    date = as.Date(as.integer(Capacity_Wehl_perEmp_perDate_Flag[i, "Date"]), origin = "1970-01-01")
    
    #check if it's a national holiday
    if (date %in% Capacity_Wehl_perEmp_perDate$Date) {
    
      hr_plan = as.numeric(Capacity_Wehl_Clean[Capacity_Wehl_Clean$Emp_Name == emp, "Emp_HrPerDay_Eff"])
      hr_real = as.numeric(Capacity_Wehl_perEmp_perDate[as.Date(Capacity_Wehl_perEmp_perDate$Date) == date, emp])
      
      if (hr_real == 0) {
        Capacity_Wehl_perEmp_perDate_Flag[i, j] <- 'Day off'
        
      } else if (hr_plan == hr_real) {
        Capacity_Wehl_perEmp_perDate_Flag[i, j] <- 'Working'
        
      } else {
        Capacity_Wehl_perEmp_perDate_Flag[i, j] <- 'Part Day'
        
      }
            
    }        
          
  }
  
}
Capacity_Wehl_perEmp_perDate_Flag[is.na(Capacity_Wehl_perEmp_perDate_Flag)] <- 'Day off'

Capacity_Wehl_perEmp_perDate_Flag <- data.frame(t(Capacity_Wehl_perEmp_perDate_Flag))
names(Capacity_Wehl_perEmp_perDate_Flag) <- c('Mon_1', 'Tue_1', 'Wed_1', 'Thu_1', 'Fri_1', 'Mon_2', 'Tue_2', 'Wed_2', 'Thu_2', 'Fri_2', 'Mon_3', 'Tue_3', 'Wed_3', 'Thu_3', 'Fri_3')
Capacity_Wehl_perEmp_perDate_Flag <- Capacity_Wehl_perEmp_perDate_Flag[2:nrow(Capacity_Wehl_perEmp_perDate_Flag), ]

Capacity_Wehl_perEmp_perDate_Flag <- as.tibble(Capacity_Wehl_perEmp_perDate_Flag %>% 
  mutate(EmpName = row.names(Capacity_Wehl_perEmp_perDate_Flag)))

# Split it in final tables
ATO <- Capacity_Wehl_perEmp_perDate_Flag %>% 
  filter(EmpName %in% unlist(Capacity_Wehl_Clean[Capacity_Wehl_Clean$Plan_Ass_Line=='ATO', 'Emp_Name']))
ETO <- Capacity_Wehl_perEmp_perDate_Flag %>% 
  filter(EmpName %in% unlist(Capacity_Wehl_Clean[Capacity_Wehl_Clean$Plan_Ass_Line=='ETO', 'Emp_Name']))
TD <- Capacity_Wehl_perEmp_perDate_Flag %>% 
  filter(EmpName %in% unlist(Capacity_Wehl_Clean[Capacity_Wehl_Clean$Plan_Ass_Line=='TD', 'Emp_Name']))

W0_ATO <- ATO[, c(16, 1:5)]
W1_ATO <- ATO[, c(16, 6:10)]
W2_ATO <- ATO[, c(16, 11:15)]

W0_ETO <- ETO[, c(16, 1:5)]
W1_ETO <- ETO[, c(16, 6:10)]
W2_ETO <- ETO[, c(16, 11:15)]

W0_TD <- TD[, c(16, 1:5)]
W1_TD <- TD[, c(16, 6:10)]
W2_TD <- TD[, c(16, 11:15)]


# Write CSV  ==========================================================================

write_excel_csv(W0_ATO, "//prod/root/v_drive/team/Intralox Dashboard/Intralox EMEA Equipment Operations App/Francois/Data/1_Complex Cleaning/E_Assembly/W0_ATO.csv")
write_excel_csv(W1_ATO, "//prod/root/v_drive/team/Intralox Dashboard/Intralox EMEA Equipment Operations App/Francois/Data/1_Complex Cleaning/E_Assembly/W1_ATO.csv")
write_excel_csv(W2_ATO, "//prod/root/v_drive/team/Intralox Dashboard/Intralox EMEA Equipment Operations App/Francois/Data/1_Complex Cleaning/E_Assembly/W2_ATO.csv")

write_excel_csv(W0_ETO, "//prod/root/v_drive/team/Intralox Dashboard/Intralox EMEA Equipment Operations App/Francois/Data/1_Complex Cleaning/E_Assembly/W0_ETO.csv")
write_excel_csv(W1_ETO, "//prod/root/v_drive/team/Intralox Dashboard/Intralox EMEA Equipment Operations App/Francois/Data/1_Complex Cleaning/E_Assembly/W1_ETO.csv")
write_excel_csv(W2_ETO, "//prod/root/v_drive/team/Intralox Dashboard/Intralox EMEA Equipment Operations App/Francois/Data/1_Complex Cleaning/E_Assembly/W2_ETO.csv")

write_excel_csv(W0_TD, "//prod/root/v_drive/team/Intralox Dashboard/Intralox EMEA Equipment Operations App/Francois/Data/1_Complex Cleaning/E_Assembly/W0_TD.csv")
write_excel_csv(W1_TD, "//prod/root/v_drive/team/Intralox Dashboard/Intralox EMEA Equipment Operations App/Francois/Data/1_Complex Cleaning/E_Assembly/W1_TD.csv")
write_excel_csv(W2_TD, "//prod/root/v_drive/team/Intralox Dashboard/Intralox EMEA Equipment Operations App/Francois/Data/1_Complex Cleaning/E_Assembly/W2_TD.csv")











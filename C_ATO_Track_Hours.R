# Initialize the project ==========================================================================

source("C:/Users/fbesnard/OneDrive - Laitram/Ressources/R/R_Programs/Load_Libraries.R")


# Import data ==========================================================================

Resources <- data.frame(read_csv("//prod/root/v_drive/team/Intralox Dashboard/Intralox EMEA Equipment Operations App/Francois/Noetix_V2/Resources_EMEA.csv", name_repair = "minimal", show_col_types = F))
Jobs <- data.frame(read_csv("//prod/root/v_drive/team/Intralox Dashboard/Intralox EMEA Equipment Operations App/Francois/Noetix_V2/Jobs_EMEA.csv", name_repair = "minimal", show_col_types = F))


# Select data: Keep only Amazon Programs ==========================================================================

Jobs <- Jobs[!is.na(Jobs$Order_Line_Seiban_ID) & (grepl("ADTA", Jobs$Order_Line_Seiban_ID) | grepl("FS", Jobs$Order_Line_Seiban_ID)) & Jobs$Job_Status == "Closed" & !grepl("SPARE", Jobs$Order_Line_Seiban_ID), c('Job_ID', 'Order_Line_Seiban_ID', "Job_Status", "Job_Date_Closed", "Job_Qty_Completed", "Job_Item_Assembly_Description")]


# Put all resources in min ==========================================================================

Resources[Resources$Resource_Unit_Measure == "MIN", "Resource_Unit_Actual"] <- Resources[Resources$Resource_Unit_Measure == "MIN", "Resource_Unit_Actual"]/60
Resources[Resources$Resource_Unit_Measure == "MIN", "Resource_Unit_Budgeted"] <- Resources[Resources$Resource_Unit_Measure == "MIN", "Resource_Unit_Budgeted"]/60
Resources <- select(Resources, -Resource_Unit_Measure)


# Merge resources & Jobs ==========================================================================

Resources <- merge(Resources, Jobs, by.x = "Job_ID", by.y = "Job_ID", all.y = T)
Resources <- Resources[Resources$Resource_Unit_Actual > 0.1, c('Job_ID', "Resource_Department", "Resource_Unit_Actual", "Resource_Unit_Budgeted", "Order_Line_Seiban_ID", "Job_Date_Closed", "Job_Qty_Completed", "Job_Item_Assembly_Description")]


# Format as date & Create Year_Month ==========================================================================

Resources$Job_Date_Closed <- gsub( " .*$", "", Resources$Job_Date_Closed)
Resources$Job_Date_Closed <- as.Date(Resources$Job_Date_Closed, "%m/%d/%Y")

Resources$YearMonth <- month(Resources$Job_Date_Closed)
Resources[Resources$YearMonth < 10, "YearMonth"] <- paste("0", Resources[Resources$YearMonth < 10, "YearMonth"], sep="")
Resources$YearMonth <- paste(substr(year(Resources$Job_Date_Closed), 3, 4), Resources$YearMonth, sep = "_")


# Create columns Program & Module ==========================================================================

Resources$Program <- NA
Resources[grepl("ADTA", Resources$Order_Line_Seiban_ID), "Program"] <- "ADTA-2023"
Resources[grepl("FS", Resources$Order_Line_Seiban_ID), "Program"] <- "ADTA-2022"

Resources$Module <- NA

# 4FT Divert
Resources[Resources$Job_Item_Assembly_Description == "BED SECTION ASSEMBLY DIVERT, 4FT, ADTA, METRIC, NO CONTROLS", "Module"] <- "4FT Divert"
Resources[Resources$Job_Item_Assembly_Description == "S7000 ADTA MODULE - DIVERT 4FT METRIC - NO CONTROLS", "Module"] <- "4FT Divert"

# 8FT Divert
Resources[Resources$Job_Item_Assembly_Description == "S7000 ADTA MODULE - DIVERT 8FT METRIC - NO CONTROLS", "Module"] <- "8FT Divert"
Resources[Resources$Job_Item_Assembly_Description == "BED SECTION ASSY, THROUGHPUT 8 FOOT, METRIC, NO", "Module"] <- "8FT Divert"
Resources[Resources$Job_Item_Assembly_Description == "MODULE MAIN ASSY, THROUGHPUT 8 FOOT, METRIC, NO CNTRLS", "Module"] <- "8FT Divert"

# Drive
Resources[Resources$Job_Item_Assembly_Description == "MODULE FRAME ASSEMBLY, RH DRIVE 7.5KW, METRIC, NO CONTROLS", "Module"] <- "Drive"
Resources[Resources$Job_Item_Assembly_Description == "MODULE FRM S7000 ADTA MODULE, DRIVE 7.5KW METRIC, NO, RH DRIVE MODULE 10 HP, METRIC, NO CONTROLS", "Module"] <- "Drive"

# Empty RnR
Resources[Resources$Job_Item_Assembly_Description == "MODULE CRYWY ASSY, WRSTRP SUPT BASED ON V4 RNR COMPONENTS W/ HARDWARE", "Module"] <- "Empty RnR Pre-Assembly"
Resources[Resources$Job_Item_Assembly_Description == "MODULE RNR ASSEMBLY, WEARSTRIP SUPPORT BASED ON V4 RNR COMPONENTS", "Module"] <- "Empty RnR Pre-Assembly"

# Idle
Resources[Resources$Job_Item_Assembly_Description == "BED SECTION ASSEMBLY IDLE, METRIC, NO CONTROLS", "Module"] <- "Idle"
Resources[Resources$Job_Item_Assembly_Description == "S7000 ADTA MODULE - IDLE METRIC - NO CONTROLS, IDLE MODULE, METRIC, NO CONTROLS", "Module"] <- "Idle"

# Legs Divert
Resources[Resources$Job_Item_Assembly_Description == "LEG MAIN ASSEMBLY, INTERMEDIATE SECTIONS, 1400MM TOB MAX HEIGHT", "Module"] <- "Legs Divert"
Resources[Resources$Job_Item_Assembly_Description == "MODULE CONVEYOR SUPPORT ASSEMBLY, LEGS, INTERMEDIATE SECTIONS", "Module"] <- "Legs Divert"

# Legs Drive
Resources[Resources$Job_Item_Assembly_Description == "MODULE CONVEYOR SUPPORT ASSEMBLY, LEGS, DRIVE END", "Module"] <- "Legs Drive"
Resources[Resources$Job_Item_Assembly_Description == "LEG MAIN ASSEMBLY, DRIVE END, 1400MM TOB MAX HEIGHT", "Module"] <- "Legs Drive"

# RNR Pre-Assembly
Resources[Resources$Job_Item_Assembly_Description == "MODULE RNR ASSEMBLY", "Module"] <- "RNR Pre-Assembly"
Resources[Resources$Job_Item_Assembly_Description == "MODULE RNR ASSEMBLY, WITH HARDWARE", "Module"] <- "RNR Pre-Assembly"


# Create summarized table ==========================================================================

test <- Resources[, c("Resource_Unit_Actual", "Job_Qty_Completed", "YearMonth", "Program", "Module")]
test$groupedVar <- paste(test$YearMonth, test$Program, test$Module, sep = "_")

Resources_Sumarized <- test[is.na(test$groupedVar), ]

for (i in 1:nrow(test)){
  
  if(test[i, "groupedVar"] %in% Resources_Sumarized[, "groupedVar"]){
    
    Resources_Sumarized[Resources_Sumarized$groupedVar == test[i, "groupedVar"], "Resource_Unit_Actual"] <- 
      Resources_Sumarized[Resources_Sumarized$groupedVar == test[i, "groupedVar"], "Resource_Unit_Actual"] +
      test[i, "Resource_Unit_Actual"]
    Resources_Sumarized[Resources_Sumarized$groupedVar == test[i, "groupedVar"], "Job_Qty_Completed"] <- 
      Resources_Sumarized[Resources_Sumarized$groupedVar == test[i, "groupedVar"], "Job_Qty_Completed"] +
      test[i, "Job_Qty_Completed"]
    
  } else{
    
    Resources_Sumarized <- rbind(Resources_Sumarized, test[i, ])
    
  }
    
}

Resources_Sumarized <- Resources_Sumarized[, c("Resource_Unit_Actual", "Job_Qty_Completed", "YearMonth", "Program", "Module")]


# Add Average Nr of Hrs by module ==========================================================================

Resources_Sumarized$AvrgNrHrsbyModule <- Resources_Sumarized$Resource_Unit_Actual / Resources_Sumarized$Job_Qty_Completed


# CSV Clean ==========================================================================

Resources <- Resources %>%  mutate_all(as.character)
Resources[is.na(Resources)] <- ""

Resources_Sumarized <- Resources_Sumarized %>%  mutate_all(as.character)
Resources_Sumarized[is.na(Resources_Sumarized)] <- ""


# Extract csv files ==========================================================================

#In Oracle App
write_csv(Resources_Sumarized, "//prod/root/v_drive/team/Intralox Dashboard/Intralox EMEA Equipment Operations App/Francois/Data/1_Complex Cleaning/E_Assembly/AmazonProgram_Resources_Sumarized.csv")


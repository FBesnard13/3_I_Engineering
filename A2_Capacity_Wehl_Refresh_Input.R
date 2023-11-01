# Initialize the project ====

source("C:/Users/fbesnard/OneDrive - Laitram/Ressources/R/R_Programs/Load_Libraries.R")


# Import data ====

People_Bart_Emp <-  read_parquet(r"(\\prod\root\v_drive\team\Intralox Dashboard\Intralox EMEA Equipment Operations App\Francois\Data\0_Cleaned\A_People\People_Bart_Emp.parquet)", name_repair = "minimal", show_col_types = F)

wb <- loadWorkbook(r"(\\prod\root\S_Drive\euam\All_Shared\SHARE\ARB team Europe & Asia-Pac\OPERATIONS\Data Input - Francois\Assembly - EMEA\Capacity_Wehl.xlsx)")


# Build People_Bart_Emp_List ====

People_Bart_Emp_List <- People_Bart_Emp |> 
  
  select(Emp_Name) |>
  rbind('Temp') |> 
  arrange(Emp_Name)
  

# OverWrite the People_List Sheet ====

writeData(wb, sheet = "People_List", People_Bart_Emp_List)

# Write CSV  ====

saveWorkbook(wb, r"(\\prod\root\S_Drive\euam\All_Shared\SHARE\ARB team Europe & Asia-Pac\OPERATIONS\Data Input - Francois\Assembly - EMEA\Capacity_Wehl.xlsx)", overwrite = TRUE)


# Print ====

print("3_E_Assembly_A2")  


















library(readr)

# Get a list of file names in the directory
data_files <- list.files("data_uploads/")
data_files

# To import each csv file into the database table
for (file in data_files) {
  this_filepath <- paste0("data_uploads/", file)
  this_file_contents <- readr::read_csv(this_filepath)
  
  number_of_rows <- nrow(this_file_contents)
  number_of_columns <- ncol(this_file_contents)
  
  #To print the number of columns and rows of each dataset
  print(paste0("The file: ",file,
               " has: ",
               format(number_of_rows,big.mark = ","),
               " rows and ",
               number_of_columns," columns"))
  
  table_name <- gsub(".csv","",file)
}

# Check if the first column of each file is a primary
for (variable in data_files) {
  this_filepath <- paste0("data_uploads/", variable)
  this_file_contents <- readr::read_csv(this_filepath)
  number_of_rows <- nrow(this_file_contents)
  
  print(paste0("Checking for: ", variable))
  
  print(paste0(" is ", nrow(unique(this_file_contents[, 1]))==number_of_rows))
}

#simplify the tables by removing the suffix "-Table 1.csv"
suffix <- "-Table 1.csv"
data_files <- gsub("-Table 1.csv","", data_files)
data_files

#Check duplicates (its not that necessary)

#Check email format


#import to database

#referential intergrity checks

#data quality checks

read.csv
summary("orders")

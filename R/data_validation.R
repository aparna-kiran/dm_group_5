
# All the codes are basically working, but some of them have some small issues

library(readr)

# Get a list of file names in the directory
data_files <- list.files("data_uploads/")
data_files

#simplify the tables by removing the suffix "-Table 1.csv"
#suffix <- "-Table 1.csv"
#data_files <- gsub("-Table 1.csv","", data_files)
#data_files
#no need for this, its creating errors


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


#Check duplicates
for (variable in data_files) {
  # Construct the file path
  this_filepath <- paste0("data_uploads/", variable)
  # Read the CSV file
  this_file_contents <- readr::read_csv(this_filepath)
  # Check for duplicate rows for just the first column
  duplicates <- duplicated(this_file_contents[[1]])
  # Count the number of duplicates
  number_of_duplicates <- sum(duplicates)
  # Print the result
  print(paste0("Checking for duplicates in '", variable, "': ", number_of_duplicates, " duplicates found"))
}
#This code is working, but for transaction, its still showing duplicates even I only did for the first column


# Check missing values
# Loop over the files and print the number of missing values for each one
for (file_path in data_files) {
  # Calculate the number of missing values for each column
  missing_values <- sapply(data_files, function(x) sum(is.na(x)))
  # Print the results for the current file
  cat("Missing values in", basename(file_path), ":\n")
  print(missing_values)
  cat("\n") 
}
# It has problems as its printing the entire set of results for all files every time it is called




#Check email format, both of the codes have some erros
#First code
for (variable in data_files) {
  this_filepath <- paste0("data_uploads/", variable)
  this_file_contents <- readr::read_csv(this_filepath)
  # Define a regex pattern for the email format
  email_pattern <- "^[a-zA-Z]+\\.[a-zA-Z]+@email\\.com$"
  # Check if the email format is correct in the first column
  email_format_correct <- grepl(email_pattern, this_file_contents[[1]], perl = TRUE)
  # Print out the results
  print(paste0("Checking email format for: '", variable, "'"))
  if (all(email_format_correct)) {
    print("All emails are in the correct format.")
  } else {
    print("Some emails are not in the correct format.")
  }
}

#Second Codes
for (variable in data_files) {
    this_filepath <- paste0("data_uploads/", variable)
    this_file_contents <- readr::read_csv(this_filepath)  
  # Determine which dataset we are checking and set the appropriate pattern
  if (grepl("customers", variable)) {
    # Customer email pattern
    email_column <- "email" 
    email_pattern <- "^[a-zA-Z]+\\.[a-zA-Z]+@email\\.com$"
  } else if (grepl("suppliers", file_path)) {
    # Supplier email pattern
    email_column <- "supplier_email"  
    email_pattern <- "^.+@(email\\.com|email\\.net)$"
  } else {
    next  # Skip the file if it's neither customer nor supplier
  }
  
  # Check if the email format is correct in the respective column
  email_format_correct <- grepl(email_pattern, this_file_contents[[email_column]], perl = TRUE)
  
  # Print out the results
  print(paste0("Checking email format for: '", basename(file_path), "'"))
  if (all(email_format_correct)) {
    print("All emails are in the correct format.")
  } else {
    incorrect_emails <- this_file_contents[!email_format_correct, email_column]
    print(paste("The following emails are not in the correct format:", toString(incorrect_emails)))
  }
}




#Check phone number is in format of +44 7XXX-XXX-XXX
# "customers' dataset contains the phone numbers
customer_data_path <- "data_uploads/customers-Table 1.csv"
customer_data <- read_csv(customer_data_path)

# Define the phone number format
phone_pattern <- "^\\+44 7\\d{3}-\\d{3}-\\d{3}$"

# Check if the phone format is correct in the specified column
# Replace 'phone_column_name' with the actual name of the column containing phone numbers
phone_format_correct <- grepl(phone_pattern, customer_data$phone)

# Printing the results
cat("Checking phone format for customers dataset:\n")
if (all(phone_format_correct)) {
  cat("All phone numbers are in the correct format.\n")
} else {
  incorrect_phones <- customer_data$phone_column_name[!phone_format_correct]
  cat("The following phone numbers are not in the correct format:\n")
  print(incorrect_phones)
}




#check date is in DD/MM/YYYY format, this code don't run really fine
library(readr)

# Assuming data_files is a vector of CSV filenames
for (variable in data_files) {
  this_filepath <- paste0("data_uploads/", variable)
  this_file_contents <- read_csv(this_filepath)
  
  # Check for the presence of a column named 'date'
  if ('date' %in% names(this_file_contents)) {
    # Define a regex pattern for the date format
    date_pattern <- "^(0[1-9]|[12][0-9]|3[01])/(0[1-9]|1[012])/[0-9]{4}$"
    
    # Check if the date format is correct in the date column
    date_format_correct <- grepl(date_pattern, this_file_contents[["date"]], perl = TRUE)
    
    # Print out the results
    print(paste0("Checking date format for '", variable, "' in 'date' column:"))
    if (all(date_format_correct)) {
      print("All dates are in the correct format.")
    } else {
      print("Some dates are not in the correct format.")
    }
  }
}






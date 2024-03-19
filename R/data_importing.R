library(readr)
library(RSQLite)

#Data Validation
count <- 0
data_files <- list.files("data_uploads/")
#Check if the first column of each file is a primary
for (variable in data_files) {
  this_filepath <- paste0("data_uploads/", variable)
  this_file_contents <- readr::read_csv(this_filepath, col_types = cols())
  number_of_rows <- nrow(this_file_contents)
  print(paste0("Checking for: ", variable))
  print(paste0(" is ", nrow(unique(this_file_contents[, 1]))==number_of_rows))
  
  if(nrow(unique(this_file_contents[, 1]))==number_of_rows){
    count <- 1
  }
  #Check duplicates of the first column
  duplicates <- duplicated(this_file_contents[[1]])
  number_of_duplicates <- sum(duplicates)
  print(paste0("Checking for duplicates in '", variable, "': ", number_of_duplicates, " duplicates found"))
  
  if(number_of_duplicates==0 && count == 1){
    count <- 2
  }
  # Calculate the number of missing values for each column
  missing_values <- colSums(is.na(this_file_contents))
  
  # Print the results for the current file
  print(paste("Missing values in", basename(this_filepath), ":"))
  print(missing_values)
  
  if(sum(missing_values == 0) && count == 2){
    count <- 3
  }
}

#Check email is in format of xxx@xxx.com
# "customers' dataset contains the phone numbers
customer_data_path <- "data_uploads/customers.csv"
customer_data <- read_csv(customer_data_path)

email_pattern <- "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"

# Assuming 'email_column_name' is the actual name of the column containing email addresses
email_column_name <- "email"

# Check if the email format is correct in the specified column
email_format_correct <- grepl(email_pattern, customer_data[[email_column_name]])

# Printing the results
cat("Checking email format for customers dataset:\n")
if (all(email_format_correct) && count == 3) {
  cat("All email addresses are in the correct format.\n")
  count <- 4
} else {
  incorrect_emails <- customer_data[[email_column_name]][!email_format_correct]
  cat("The following email addresses are not in the correct format:\n")
  print(incorrect_emails)
}


#Check phone number is in format of +44 7XXX-XXX-XXX
# "customers' dataset contains the phone numbers
customer_data_path <- "data_uploads/customers.csv"
customer_data <- read_csv(customer_data_path)

# Define the phone number format
phone_pattern <- "^\\+44 7\\d{3}-\\d{3}-\\d{3}$"

# Check if the phone format is correct in the specified column
# Replace 'phone_column_name' with the actual name of the column containing phone numbers
phone_format_correct <- grepl(phone_pattern, customer_data$phone)

# Printing the results
cat("Checking phone format for customers dataset:\n")
if (all(phone_format_correct) && count == 4) {
  cat("All phone numbers are in the correct format.\n")
  count <- 5
} else {
  incorrect_phones <- customer_data$phone_column_name[!phone_format_correct]
  cat("The following phone numbers are not in the correct format:\n")
  print(incorrect_phones)
}

if (count == 5) {
  cat("The dataset passed all five data validation tests")
}

## Data Importing

db_connection <- RSQLite::dbConnect(RSQLite::SQLite(),"ecommerce.db")

RSQLite::dbExecute(db_connection, "DROP TABLE IF EXISTS customers")
RSQLite::dbExecute(db_connection, "DROP TABLE IF EXISTS order_details")
RSQLite::dbExecute(db_connection, "DROP TABLE IF EXISTS transactions")
RSQLite::dbExecute(db_connection, "DROP TABLE IF EXISTS products")
RSQLite::dbExecute(db_connection, "DROP TABLE IF EXISTS product_categories")
RSQLite::dbExecute(db_connection, "DROP TABLE IF EXISTS suppliers")
RSQLite::dbExecute(db_connection, "DROP TABLE IF EXISTS promotion")
RSQLite::dbExecute(db_connection, "DROP TABLE IF EXISTS orders")


#Writing the csv file contents to the database and
#creating the table with the table_name
RSQLite::dbWriteTable(db_connection,"customers","data_uploads/customers.csv",append=TRUE)
RSQLite::dbWriteTable(db_connection,"order_details","data_uploads/order_details.csv",append=TRUE)
RSQLite::dbWriteTable(db_connection,"transactions","data_uploads/transactions.csv",append=TRUE)
RSQLite::dbWriteTable(db_connection,"products","data_uploads/products.csv",append=TRUE)
RSQLite::dbWriteTable(db_connection,"product_categories","data_uploads/product_categories.csv",append=TRUE)
RSQLite::dbWriteTable(db_connection,"suppliers","data_uploads/suppliers.csv",append=TRUE)
RSQLite::dbWriteTable(db_connection,"promotions","data_uploads/promotions.csv",append=TRUE)
RSQLite::dbWriteTable(db_connection,"orders","data_uploads/orders.csv",append=TRUE)

RSQLite::dbDisconnect(db_connection)


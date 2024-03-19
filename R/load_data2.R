library(readr)
library(RSQLite)

data_files <- list.files("data_uploads/")
db_connection <- RSQLite::dbConnect(RSQLite::SQLite(),"try_ecommerce.db")

dbExecute(db_connection, "DROP TABLE IF EXISTS customers")
customers <- "
CREATE TABLE IF NOT EXISTS 'customers' (
  'customer_id' INT PRIMARY KEY, 
  'first_name' VARCHAR(50) NOT NULL, 
  'last_name' VARCHAR(50) NOT NULL,
  'email' VARCHAR(100) NOT NULL, 
  'password' VARCHAR(100) NOT NULL, 
  'phone_number' VARCHAR(20) NOT NULL, 
);
"

dbExecute(db_connection, "DROP TABLE IF EXISTS order_details")
order_details <- "
CREATE TABLE IF NOT EXISTS 'order_details' (
  'order_detail_id' INT PRIMARY KEY,
  'order_id' INT NOT NULL,
  'product_id' INT NOT NULL, 
  'order_quantity' INT, 
  'product_price' DECIMAL(10, 2) NOT NULL,
  'shipping_address' VARCHAR(250) NOT NULL,
  'city' VARCHAR(50) NOT NULL,
  'postcode' VARCHAR(10) NOT NULL,
  FOREIGN KEY ('product_id')
  REFERENCES products ('product_id'),
  FOREIGN KEY ('order_id')
  REFERENCES orders ('order_id')
);
"

dbExecute(db_connection, "DROP TABLE IF EXISTS transactions")
transactions <- "
CREATE TABLE IF NOT EXISTS 'transactions' (
  'transaction_id' INT PRIMARY KEY,
  'customer_id' INT NOT NULL,
  'order_id' INT NOT NULL,
  'receipt_invoice' VARCHAR(20) NOT NULL,
  'transaction_method' TEXT NOT NULL,
  'transaction_date' DATE NOT NULL,
  FOREIGN KEY ('customer_id') 
  REFERENCES customers('customer_id'),
  FOREIGN KEY ('order_id') 
  REFERENCES orders('order_id')
);
"

dbExecute(db_connection, "DROP TABLE IF EXISTS products")
products <- "
CREATE TABLE IF NOT EXISTS 'products' (
  'product_id' INT PRIMARY KEY,
  'category_id' INT NOT NULL,
  'supplier_id' INT NOT NULL,
  'product_name' VARCHAR(100) NOT NULL,
  'product_sku' VARCHAR(20),
  'product_description' VARCHAR(500),
  'price' DECIMAL(1000,2) NOT NULL,
  'product_quantity' INT,
  FOREIGN KEY ('category_id')
  REFERENCES product_categories ('category_id'),
  FOREIGN KEY ('supplier_id')
  REFERENCES suppliers ('supplier_id')
);
"

dbExecute(db_connection, "DROP TABLE IF EXISTS product_categories")
product_categories <- "
CREATE TABLE IF NOT EXISTS 'product_categories' (
  'category_id' INT PRIMARY KEY,
  'category_name' VARCHAR(50) NOT NULL,
);
"

dbExecute(db_connection, "DROP TABLE IF EXISTS suppliers")
suppliers <- "
CREATE TABLE IF NOT EXISTS 'suppliers' (
  'supplier_id' INT PRIMARY KEY,
  'supplier_name' VARCHAR(100) NOT NULL,
  'supplier_email' VARCHAR(100) NOT NULL
);
"

dbExecute(db_connection, "DROP TABLE IF EXISTS promotion")
promotion <- "
CREATE TABLE IF NOT EXISTS 'promotion'(
  'promotion_id' INT PRIMARY KEY,
  'discount_start_date' DATE NOT NULL,
  'discount_end_date' DATE NOT NULL,
  'discount_code' VARCHAR(20),
  'discount_rate' VARCHAR(3)
);
"

dbExecute(db_connection, "DROP TABLE IF EXISTS orders")
orders <- "
CREATE TABLE IF NOT EXISTS 'orders'(
  'order_id' INT PRIMARY KEY,
  'customer_id' INT NOT NULL,
  'estimated_delivery_date' DATE NOT NULL,
  'order_date' DATE NOT NULL,
  'order_status' VARCHAR(100) NOT NULL,
  'total_price' INT NOT NULL,
  FOREIGN KEY('customer_id')
  REFERENCES customers('customer_id'),
  FOREIGN KEY('shipping_address_id')
  REFERENCES addresses('address_id')
);
"
  
#Writing the csv file contents to the database and
#creating the table with the table_name
  RSQLite::dbWriteTable(db_connection,"customers","data_uploads/customers.csv",append=TRUE)
  RSQLite::dbWriteTable(db_connection,"order_details","data_uploads/order_details.csv",append=TRUE)
  RSQLite::dbWriteTable(db_connection,"transactions","data_uploads/transactions.csv",append=TRUE)
  RSQLite::dbWriteTable(db_connection,"products","data_uploads/products.csv",append=TRUE)
  RSQLite::dbWriteTable(db_connection,"product_categories","data_uploads/product_categories.csv",append=TRUE)
  RSQLite::dbWriteTable(db_connection,"suppliers","data_uploads/supplier.csv",append=TRUE)
  RSQLite::dbWriteTable(db_connection,"promotion","data_uploads/promotion.csv",append=TRUE)
  RSQLite::dbWriteTable(db_connection,"orders","data_uploads/orders.csv",append=TRUE)

  
#Data Validation
  
#Check if the first column of each file is a primary
for (variable in data_files) {
  this_filepath <- paste0("data_uploads/", variable)
  this_file_contents <- readr::read_csv(this_filepath)
  number_of_rows <- nrow(this_file_contents)
  print(paste0("Checking for: ", variable))
  print(paste0(" is ", nrow(unique(this_file_contents[, 1]))==number_of_rows))
}


#Check duplicates of the first column
for (variable in data_files) {
  this_filepath <- paste0("data_uploads/", variable)
  this_file_contents <- readr::read_csv(this_filepath)
  duplicates <- duplicated(this_file_contents[[1]])
  number_of_duplicates <- sum(duplicates)
  print(paste0("Checking for duplicates in '", variable, "': ", number_of_duplicates, " duplicates found"))
}

  
#Check missing values
  for (file_path in data_files) {
    this_filepath <- paste0("data_uploads/", file_path)
    # Read the CSV file
    data <- readr::read_csv(this_filepath)
    
    # Read the data from the file
    
    print(data)
    # Calculate the number of missing values for each column
    missing_values <- colSums(is.na(data))
    
    # Print the results for the current file
    print(paste("Missing values in", basename(file_path), ":"))
    print(missing_values)
    print("")
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
if (all(email_format_correct)) {
  cat("All email addresses are in the correct format.\n")
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
if (all(phone_format_correct)) {
  cat("All phone numbers are in the correct format.\n")
} else {
  incorrect_phones <- customer_data$phone_column_name[!phone_format_correct]
  cat("The following phone numbers are not in the correct format:\n")
  print(incorrect_phones)
}


#check date is in DD/MM/YYYY format, this code don't run really fine
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

for (variable in data_files) {
  this_filepath <- paste0("data_uploads/", variable)
  this_file_contents <- read_csv(this_filepath)
  
  # Check for columns containing the word 'date'
  date_columns <- grep("\\bdate\\b", names(this_file_contents), value = TRUE)
  
  if (length(date_columns) > 0) {
    # Define a regex pattern for the date format
    date_pattern <- "^([0-2][0-9]|3[01])/(0[1-9]|1[0-2])/\\d{4}$"
    
    for (date_column in date_columns) {
      # Check if the date format is correct in the date column
      date_format_correct <- grepl(date_pattern, this_file_contents[[date_column]], perl = TRUE)
      
      # Print out the results
      print(paste0("Checking date format for '", variable, "' in '", date_column, "' column:"))
      if (all(date_format_correct)) {
        print("All dates are in the correct format (DD/MM/YYYY).")
      } else {
        print("Some dates are not in the correct format (DD/MM/YYYY).")
      }
    }
  }
}

RSQLite::dbDisconnect(db_connection)
library(readr)

# Get a list of file names in the directory
directory <- "data_uploads/"
<<<<<<< HEAD

data_files <- list.files(directory, pattern = "\\.csv$")
data_files

db_connection <- RSQLite::dbConnect(RSQLite::SQLite(),"try_ecommerce.db")

table_names <- dbListTables(db_connection)
table_names

for (tablename in table_names){
  print(tablename)
  
  # Filter files containing "customers" in their name
  table_files <- grep(tablename, data_files, ignore.case = TRUE, value = TRUE)
  print(table_files)
  
  if(length(table_files) > 1){
    if(tablename == "customers"){
      for (i in seq_along(table_files)){
        existing_dataset <- dbReadTable(db_connection, tablename)
        filepath <- paste0(directory,table_files[i])
        dataset <- read_csv(filepath, col_types = cols())
      
        id_length <- max(nchar(existing_dataset$customer_id))
        existing_dataset$customer_id <- sprintf(paste0("%0", id_length, "d"), as.numeric(existing_dataset$customer_id))
        
        filtered_dataset <- dataset[!dataset$customer_id %in% existing_dataset$customer_id, ]
        RSQLite::dbWriteTable(db_connection,tablename, filtered_dataset, append=TRUE)
        if(nrow(filtered_dataset) != 0){
          print("Customers are appending")
        }
      }
    }
    if(tablename == "products"){
      for (i in seq_along(table_files)){
        existing_dataset <- dbReadTable(db_connection, tablename)
        filepath <- paste0(directory,table_files[i])
        dataset <- read_csv(filepath, col_types = cols())
        
        id_length <- max(nchar(existing_dataset$product_id))
        existing_dataset$product_id <- sprintf(paste0("%0", id_length, "d"), as.numeric(existing_dataset$product_id))
        
        filtered_dataset <- dataset[!dataset$product_id %in% existing_dataset$product_id, ]
        
        RSQLite::dbWriteTable(db_connection,tablename, filtered_dataset, append=TRUE)
        if(nrow(filtered_dataset) != 0){
          print("Products are appending")
        }
      }
    }
    if(tablename == "order_details"){
      for (i in seq_along(table_files)){
        existing_dataset <- dbReadTable(db_connection, tablename)
        filepath <- paste0(directory,table_files[i])
        dataset <- read_csv(filepath, col_types = cols())
        
        id_length <- max(nchar(existing_dataset$order_detail_id))
        existing_dataset$order_detail_id <- sprintf(paste0("%0", id_length, "d"), as.numeric(existing_dataset$order_detail_id))
        
        filtered_dataset <- dataset[!dataset$order_detail_id %in% existing_dataset$order_detail_id, ]
        RSQLite::dbWriteTable(db_connection,tablename, new_dataset, append=TRUE)
        if(nrow(filtered_dataset) != 0){
          print("Order Details are appending")
        }
      }
    }
    if(tablename == "orders"){
      for (i in seq_along(table_files)){
        existing_dataset <- dbReadTable(db_connection, tablename)
        filepath <- paste0(directory,table_files[i])
        dataset <- read_csv(filepath, col_types = cols())
        
        id_length <- max(nchar(existing_dataset$order_id))
        existing_dataset$order_id <- sprintf(paste0("%0", id_length, "d"), as.numeric(existing_dataset$order_id))
        
        filtered_dataset <- dataset[!dataset$order_id %in% existing_dataset$order_id, ]
        RSQLite::dbWriteTable(db_connection,tablename, new_dataset, append=TRUE)
        if(nrow(filtered_dataset) != 0){
          print("Orders are appending")
        }
      }
    }
    if(tablename == "promotions"){
      for (i in seq_along(table_files)){
        existing_dataset <- dbReadTable(db_connection, tablename)
        filepath <- paste0(directory,table_files[i])
        dataset <- read_csv(filepath, col_types = cols())
        
        id_length <- max(nchar(existing_dataset$promotion_id))
        existing_dataset$promotion_id <- sprintf(paste0("%0", id_length, "d"), as.numeric(existing_dataset$promotion_id))
        
        filtered_dataset <- dataset[!dataset$promotion_id %in% existing_dataset$promotion_id, ]
        RSQLite::dbWriteTable(db_connection,tablename, new_dataset, append=TRUE)
        if(nrow(filtered_dataset) != 0){
          print("Promotions are appending")
        }
      }
    }
    if(tablename == "suppliers"){
      for (i in seq_along(table_files)){
        existing_dataset <- dbReadTable(db_connection, tablename)
        filepath <- paste0(directory,table_files[i])
        dataset <- read_csv(filepath, col_types = cols())
        
        id_length <- max(nchar(existing_dataset$supplier_id))
        existing_dataset$supplier_id <- sprintf(paste0("%0", id_length, "d"), as.numeric(existing_dataset$supplier_id))
        
        filtered_dataset <- dataset[!dataset$supplier_id %in% existing_dataset$supplier_id, ]
        RSQLite::dbWriteTable(db_connection,tablename, new_dataset, append=TRUE)
        if(nrow(filtered_dataset) != 0){
          print("Suppliers are appending")
        }
      }
    }
    if(tablename == "transactions"){
      for (i in seq_along(table_files)){
        existing_dataset <- dbReadTable(db_connection, tablename)
        filepath <- paste0(directory,table_files[i])
        dataset <- read_csv(filepath, col_types = cols())
        
        id_length <- max(nchar(existing_dataset$transaction_id))
        existing_dataset$transaction_id <- sprintf(paste0("%0", id_length, "d"), as.numeric(existing_dataset$transaction_id))
        
        filtered_dataset <- dataset[!dataset$transaction_id %in% existing_dataset$transaction_id, ]
        RSQLite::dbWriteTable(db_connection,tablename, new_dataset, append=TRUE)
        if(nrow(filtered_dataset) != 0){
          print("Transactions are appending")
        }
      }
    }
    if(tablename == "product_categories"){
      for (i in seq_along(table_files)){
        existing_dataset <- dbReadTable(db_connection, tablename)
        filepath <- paste0(directory,table_files[i])
        dataset <- read_csv(filepath, col_types = cols())
        
        id_length <- max(nchar(existing_dataset$category_id))
        existing_dataset$category_id <- sprintf(paste0("%0", id_length, "d"), as.numeric(existing_dataset$category_id))
        
        filtered_dataset <- dataset[!dataset$category_id %in% existing_dataset$category_id, ]
        RSQLite::dbWriteTable(db_connection,tablename, new_dataset, append=TRUE)
        if(nrow(filtered_dataset) != 0){
          print("Product Categories are appending")
        }
      }
=======

data_files <- list.files(directory, pattern = "\\.csv$")
data_files

db_connection <- RSQLite::dbConnect(RSQLite::SQLite(),"try_ecommerce.db")

table_names <- dbListTables(db_connection)
table_names

for (tablename in table_names){
  print(tablename)
  
<<<<<<< HEAD
  # Filter files containing "customers" in their names
  table_files <- grep(tablename, data_files, ignore.case = TRUE, value = TRUE)
  print(table_files)
=======
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




<<<<<<< HEAD
=======
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


>>>>>>> 03ab08e84603798bd9d25e29c39111061f097604

#Date1
#check date is in DD/MM/YYYY format, this code don't run really fine
for (variable in data_files) {
  this_filepath <- paste0("data_uploads/", variable)
  this_file_contents <- read_csv(this_filepath)
>>>>>>> ef06af1a12a4f5353f1c80b882c6ce5ab56d80f0
  
  if(tablename == "customers"){
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
    for (filename in table_files){
      filepath <- paste(directory, filename)
      
      RSQLite::dbWriteTable(db_connection,tablename,"data_uploads/customers.csv",append=TRUE)
      
    }
    
  }
  
  if (length(table_files) > 1){
    for (filename in table_files){
      basename <- basename(filename)

      # extracting the filenames with the same table name
      table_filename <- grep(tablename, filename, ignore.case = TRUE, value = TRUE)
      
      print(table_filename)
>>>>>>> bb110dfaf145491062491f088a5575f7983457dc
    }
  }

}


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



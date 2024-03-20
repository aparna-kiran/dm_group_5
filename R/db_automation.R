library(readr)
library(RSQLite)
# Get a list of file names in the directory
directory <- "data_uploads/"

data_files <- list.files(directory, pattern = "\\.csv$")
data_files

db_connection <- RSQLite::dbConnect(RSQLite::SQLite(),"ecommerce.db")

table_names <- RSQLite::dbListTables(db_connection) 
table_names

for (tablename in table_names){
  print(tablename)
  
  # Filter files containing "customers" in their name
  table_files <- grep(tablename, data_files, ignore.case = TRUE, value = TRUE)
  print(table_files)
  
  #checking if there is more than one dataset for each table name
  if(length(table_files) > 1){
    if(tablename == "customers"){
      for (i in seq_along(table_files)){
        #importing customers database table
        existing_dataset <- dbReadTable(db_connection, tablename)
        #defining the file path of new dataset
        filepath <- paste0(directory,table_files[i])
        dataset <- read_csv(filepath, col_types = cols())
        
        #making all primary keys of max length
        id_length <- max(nchar(existing_dataset$customer_id))
        #adding leading zeroes
        existing_dataset$customer_id <- sprintf(paste0("%0", id_length, "d"), as.numeric(existing_dataset$customer_id))
        
        #filtering new fields from the new dataset that is not existing in the table
        filtered_dataset <- dataset[!dataset$customer_id %in% existing_dataset$customer_id, ]
        #if there are new fields then append them to the sql table
        RSQLite::dbWriteTable(db_connection,tablename, filtered_dataset, append=TRUE)
        if(nrow(filtered_dataset) != 0){
          print("Customers are appending")
        }
      }
    }
    #Doing the above workflow for products
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
    #Doing the above workflow for order details
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
      #Doing the above workflow for orders
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
      #Doing the above workflow for promotions
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
      #Doing the above workflow for suppliers
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
      #Doing the above workflow for transactions
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
      #Doing the above workflow for product categories
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
    }
  }
}

RSQLite::dbDisconnect(db_connection)

library(readr)

# Get a list of file names in the directory
directory <- "data_uploads/"

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
    }
  }
}




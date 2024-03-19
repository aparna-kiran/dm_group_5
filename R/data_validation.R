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
  
  # Filter files containing "customers" in their names
  table_files <- grep(tablename, data_files, ignore.case = TRUE, value = TRUE)
  print(table_files)
  
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



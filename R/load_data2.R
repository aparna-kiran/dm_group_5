library(readr)
library(RSQLite)

data_files <- list.files("data_uploads/Dataset/")

suffix <- "-Table 1"

# Rename files
for (file in data_files) {
  # Create a new filename
  new_filename <- paste0("data_uploads/Dataset/", gsub(suffix, "", file))
  file <- paste0("data_uploads/Dataset/", file)
  # Rename the file
  file.rename(from = file, to = new_filename)
}

data_files <- list.files("data_uploads/")

db_connection <- RSQLite::dbConnect(RSQLite::SQLite(),"try_ecommerce.db")

customers <- "
CREATE TABLE 'customers' (
  'customer_id' INT PRIMARY KEY, 
  'first_name' VARCHAR(50) NOT NULL, 
  'last_name' VARCHAR(50) NOT NULL,
  'email' VARCHAR(100) NOT NULL, 
  'password' VARCHAR(100) NOT NULL, 
  'phone_number' VARCHAR(20) NOT NULL, 
);
"

order_details <- "
CREATE TABLE 'order_details' (
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

transactions <- "
CREATE TABLE 'transactions' (
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

products <- "
CREATE TABLE 'products' (
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

product_categories <- "
CREATE TABLE 'product_categories' (
  'category_id' INT PRIMARY KEY,
  'category_name' VARCHAR(50) NOT NULL,
);
"

suppliers <- "CREATE TABLE 'suppliers' (
  'supplier_id' INT PRIMARY KEY,
  'supplier_name' VARCHAR(100) NOT NULL,
  'supplier_email' VARCHAR(100) NOT NULL
);
"

promotion <- "
CREATE TABLE 'promotion'(
  'promotion_id' INT PRIMARY KEY,
  'discount_start_date' DATE NOT NULL,
  'discount_end_date' DATE NOT NULL,
  'discount_code' VARCHAR(20),
  'discount_rate' VARCHAR(3)
);
"

orders <- "
CREATE TABLE 'orders'(
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
  RSQLite::dbWriteTable(db_connection,"customers","customers.csv",append=TRUE)
  RSQLite::dbWriteTable(db_connection,"order_details","order_details.csv",append=TRUE)
  RSQLite::dbWriteTable(db_connection,"transactions","transactions.csv",append=TRUE)
  RSQLite::dbWriteTable(db_connection,"products","products.csv",append=TRUE)
  RSQLite::dbWriteTable(db_connection,"product_categories","product_categories.csv",append=TRUE)
  RSQLite::dbWriteTable(db_connection,"suppliers","supplier.csv",append=TRUE)
  RSQLite::dbWriteTable(db_connection,"promotion","promotion.csv",append=TRUE)
  RSQLite::dbWriteTable(db_connection,"orders","orders.csv",append=TRUE)

RSQLite::dbDisconnect(db_connection)
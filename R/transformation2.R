library(RSQLite)

library(dplyr)
library(ggplot2)
library(tidyr)
library(lubridate)
library(readxl)
library(scales)


# Reading required database
customers <- read.csv("customers.csv")
order_details <- read.csv("order_details.csv")
orders <- read.csv("orders.csv")
product_categories <- read.csv("product_categories.csv")
products <- read.csv("products.csv")
promotion <- read.csv("promotion.csv")
suppliers <- read.csv("supplier.csv")
transactions <- read.csv("transactions.csv")

#Standardise date format
orders$order_date <- as.Date(orders$order_date, format= "%d/%m/%Y")
orders$estimated_delivery_date <- as.Date(orders$estimated_delivery_date, format = "%d/%m/%Y")

# Total Revenue per Year

# Exclude "cancelled" orders and calculate total revenue per year
yearly_revenue <- orders %>%
  filter(order_status != "cancelled") %>%  
  inner_join(order_details, by = "order_id") %>%
  mutate(year = year(order_date)) %>%
  group_by(year) %>%
  summarize(total_revenue = sum(product_price * order_quantity), .groups = 'drop')
print(yearly_revenue)

# Adjusting the total_revenue to be in thousands for visualization
yearly_revenue$total_revenue <- yearly_revenue$total_revenue / 1000

# Plotting line chart for the yearly revenue with y-axis values in thousands

yearly_revenue_plot <- ggplot(yearly_revenue, aes(x = as.factor(year), y = total_revenue, group = 1)) +
  geom_line(color = "blue") +
  geom_point(color = "red", size = 3) +
  labs(title = "Total Revenue per Year (in Thousands)", x = "Year", y = "Total Revenue (Thousands)") +
  theme_minimal() +
  scale_y_continuous(labels = label_number(suffix = "K"), 
                     limits = c(0, max(yearly_revenue$total_revenue, na.rm = TRUE) + 10), 
                     breaks = seq(0, max(yearly_revenue$total_revenue, na.rm = TRUE) + 10, by = 20))


ggsave(plot = yearly_revenue_plot, filename = "figures/yearly_revenue_plot.pdf", width = 10, height = 6, dpi = 300)

# Total Revenue per Quarter Across All Years

# Calculate total revenue per quarter across all years

quarterly_revenue <- orders %>%
  filter(order_status != "cancelled") %>%  # Exclude cancelled orders
  inner_join(order_details, by = "order_id") %>%
  mutate(quarter = paste("Q", quarter(order_date), sep="")) %>%
  group_by(quarter) %>%
  summarize(total_revenue = sum(product_price * order_quantity), .groups = 'drop') %>%
  mutate(quarter = factor(quarter, levels = c("Q1", "Q2", "Q3", "Q4")))
print(quarterly_revenue)


quarterly_revenue_all_years_plot <- ggplot(quarterly_revenue, aes(x = quarter, y = total_revenue, group = 1)) +
  geom_line() +
  geom_point() +
  labs(title = "Total Revenue per Quarter Across All Years", x = "Quarter", y = "Total Revenue") +
  theme_minimal() +
  scale_y_continuous(limits = c(0, NA))  # Start y-axis at 0


ggsave(plot = quarterly_revenue_all_years_plot, filename = "figures/quarterly_revenue_all_years_plot.pdf", width = 10, height = 6, dpi = 300)


# Total Revenue per Quarter 

# Calculate the quarterly revenue for each year
detailed_quarterly_revenue <- orders %>%
  filter(order_status != "cancelled") %>%  # Exclude cancelled orders
  inner_join(order_details, by = "order_id") %>%
  mutate(year_quarter = paste(year(order_date), "Q", quarter(order_date), sep="")) %>%
  group_by(year_quarter) %>%
  summarize(total_revenue = sum(product_price * order_quantity), .groups = 'drop') %>%
  arrange(year_quarter)
print(detailed_quarterly_revenue)


detailed_quarterly_revenue <- detailed_quarterly_revenue %>%
  mutate(year = substr(year_quarter, 1, 4),
         quarter = paste("Q", substr(year_quarter, 6, 7), sep=""))  # Add "Q" prefix to quarter


# Plot the line chart for quarterly revenue for each year
quarterly_revenue_plot <- ggplot(detailed_quarterly_revenue, aes(x = quarter, y = total_revenue, group = year, color = year)) +
  geom_line() +  
  geom_point() +
  labs(title = "Total Revenue by Quarter (2020-2023)", x = "Quarter", y = "Total Revenue") +
  scale_color_manual(values = c("2020" = "blue", "2021" = "red", "2022" = "green", "2023" = "purple")) +  
  theme_minimal()

ggsave(plot = quarterly_revenue_plot, filename = "figures/quarterly_revenue_plot.pdf", width = 10, height = 6, dpi = 300)


# Total Revenue By City

# Filter out cancelled orders
non_cancelled_orders <- orders %>%
  filter(order_status != "cancelled")

# Join non_cancelled_orders with order_details
total_revenue_by_city <- non_cancelled_orders %>%
  inner_join(order_details, by = "order_id") %>%
  group_by(city) %>%
  summarize(total_revenue = sum(product_price * order_quantity), .groups = 'drop')
print(total_revenue_by_city)

# Plot the bar chart for total revenue by city
revenue_by_city_plot <- ggplot(total_revenue_by_city, aes(x = reorder(city, total_revenue), y = total_revenue, fill = city)) +
  geom_col() +
  labs(title = "Total Revenue by City between 2020-2023", x = "City", y = "Total Revenue") +
  theme_minimal() +
  coord_flip() +
  guides(fill = "none")  # Removes the legend

ggsave(plot = revenue_by_city_plot, filename = "figures/revenue_by_city_plot.pdf", width = 10, height = 6, dpi = 300)

#Transaction Methods Based on Customers

# Excluding the cancelled orders and calculate the numbers for each transaction method used
non_cancelled_orders <- orders %>%
  filter(order_status != "cancelled")
transactions_summary <- non_cancelled_orders %>%
  inner_join(transactions, by = "order_id") %>%
  group_by(transaction_method) %>%
  summarize(number_of_transactions = n(), .groups = 'drop')

# Find the maximum number of transactions
max_transactions <- max(transactions_summary$number_of_transactions)

# Bar chart for usage of transaction methods
transaction_methods_plot <- ggplot(transactions_summary, aes(x = reorder(transaction_method, number_of_transactions), y = number_of_transactions, fill = transaction_method)) +
  geom_col(width = 0.5) +  
  geom_text(aes(label = number_of_transactions), vjust = -0.3, color = "black", size = 3.5) + 
  labs(title = "Transaction Methods Based on Customers", 
       x = "Transaction Method",
       y = "Number of Transactions",
       fill = "Transaction Method") +
  theme_minimal() +
  scale_y_continuous(breaks = seq(0, max_transactions, by = 150))

ggsave(plot = transaction_methods_plot, filename = "figures/transaction_methods_plot.pdf", width = 10, height = 6, dpi = 300)


#Total Orders per Year

# Exclude "cancelled" orders and calculate the toal orders per year
yearly_orders_quantity <- orders %>%
  filter(order_status != "cancelled") %>%  
  inner_join(order_details, by = "order_id") %>%
  mutate(year = year(order_date)) %>%
  group_by(year) %>%
  summarize(total_quantity = sum(order_quantity), .groups = 'drop')  # Summarize by total quantity
print(yearly_orders_quantity)

# Plotting the total orders per year
yearly_orders_plot <- ggplot(yearly_orders_quantity, aes(x = as.factor(year), y = total_quantity, group = 1)) +
  geom_line(color = "blue") +
  geom_point(color = "red", size = 3) +
  labs(title = "Total Orders per Year", x = "Year", y = "Total Quantity Sold") +
  theme_minimal() +
  scale_y_continuous(limits = c(0, NA), breaks = seq(0, 500, by = 100))

ggsave(plot = yearly_orders_plot, filename = "figures/yearly_orders_plot.pdf", width = 10, height = 6, dpi = 300)

#Total Orders per Season

# Calculate total orders per season for 4 different years, excluding cancelled orders

detailed_seasonal_orders_quantity <- orders %>%
  filter(order_status != "cancelled") %>%
  inner_join(order_details, by = "order_id") %>%
  mutate(year = year(order_date),
         month = month(order_date),
         season = case_when(
           month %in% c(12, 1, 2) ~ "Winter",
           month %in% c(3, 4, 5) ~ "Spring",
           month %in% c(6, 7, 8) ~ "Summer",
           month %in% c(9, 10, 11) ~ "Fall"
         )) %>%
  mutate(season = factor(season, levels = c("Fall", "Winter", "Spring", "Summer"))) %>%
  group_by(year, season) %>%
  summarize(total_quantity = sum(order_quantity), .groups = 'drop')  # Summarize by total quantity
print(detailed_seasonal_orders_quantity)

# Plotting the  seasonal orders for each year
seasonal_orders_plot <- ggplot(detailed_seasonal_orders_quantity, aes(x = season, y = total_quantity, color = as.factor(year), group = year)) +
  geom_line() + 
  geom_point() +
  scale_color_manual(values = c("2020" = "blue", "2021" = "red", "2022" = "green", "2023" = "purple"),
                     name = "Year") + 
  labs(title = "Total Orders by Season (2020-2023)", 
       x = "Season", 
       y = "Total Quantity Sold",
       color = "Year") +
  theme_minimal()

ggsave(plot = seasonal_orders_plot, filename = "figures/seasonal_orders_plot.pdf", width = 10, height = 6, dpi = 300)


# Top 5 Best-Selling Products

# Exclude "cancelled" orders
non_cancelled_orders <- orders %>%
  filter(order_status != "cancelled")

# Calculate total quantity sold for each product and find the top 5 best-selling products
top_selling_products <- non_cancelled_orders %>%
  inner_join(order_details, by = "order_id") %>%
  group_by(product_id) %>%
  summarise(total_quantity_sold = sum(order_quantity, na.rm = TRUE)) %>%
  ungroup() %>%
  arrange(desc(total_quantity_sold)) %>%
  slice_max(order_by = total_quantity_sold, n = 5) %>%
  distinct(product_id, .keep_all = TRUE)

# Join with the products data frame to get the product names
top_selling_products_with_names <- top_selling_products %>%
  left_join(products, by = "product_id") %>%
  select(product_name, total_quantity_sold) %>%
  arrange(desc(total_quantity_sold)) %>%
  slice_head(n = 10)
print(top_selling_products_with_names)

# Generate the bar chart r for the top 5 best-selling products

best_selling_products_plot <- ggplot(top_selling_products_with_names, aes(x = reorder(product_name, total_quantity_sold), y = total_quantity_sold, fill = product_name)) +
  geom_bar(stat = "identity") +
  coord_flip() + # Flip the axes to make it a horizontal bar chart
  geom_text(aes(label = scales::comma(total_quantity_sold)), position = position_dodge(width = 0.9), hjust = -0.2, size = 3) +
  labs(title = "Top 5 Best-Selling Products", x = "Product Name", y = "Total Quantity Sold") +
  scale_y_continuous(labels = scales::comma) + # Format the Y axis labels with commas
  scale_fill_brewer(palette = "Set3") +
  theme_minimal() +
  theme(axis.text.y = element_text(angle = 0), legend.position = "none")

ggsave(plot = best_selling_products_plot, filename = "figures/best_selling_products_plot.pdf", width = 10, height = 6, dpi = 300)


#Top 5 Selling Products per Season Across All Years

# Ensure order_date is Date type and calculate season
orders <- orders %>%
  mutate(
    order_date = as.Date(order_date, format = "%Y-%m-%d"), # Convert order_date to Date type
    season = case_when(
      month(order_date) %in% c(12, 1, 2) ~ "Winter",
      month(order_date) %in% c(3, 4, 5) ~ "Spring",
      month(order_date) %in% c(6, 7, 8) ~ "Summer",
      month(order_date) %in% c(9, 10, 11) ~ "Fall"
    )
  )

# Filter out cancelled orders from the orders dataframe
non_cancelled_orders <- orders %>%
  filter(order_status != "cancelled")

# Join non_cancelled_orders with order_details and then with products to get product names
sales_data <- non_cancelled_orders %>%
  inner_join(order_details, by = "order_id") %>%
  inner_join(products, by = "product_id")

# Group by season and product_name to summarize total quantity sold across all years
seasonal_sales <- sales_data %>%
  group_by(season, product_name) %>%
  summarise(total_quantity_sold = sum(order_quantity, na.rm = TRUE), .groups = "drop")

# For each season, find the top 10 products with the highest total quantity sold
top_5_seasonal_sales <- seasonal_sales %>%
  arrange(season, desc(total_quantity_sold)) %>%
  group_by(season) %>%
  slice_max(order_by = total_quantity_sold, n = 5, with_ties = FALSE) %>%
  ungroup()

# Print the top 10 best-selling products for each season across all years
print(top_5_seasonal_sales)


#Least Selling Product

# Filter out cancelled orders
non_cancelled_orders <- orders %>%
  filter(order_status != "cancelled")

# Calculate total quantity sold for each product
product_sales <- non_cancelled_orders %>%
  inner_join(order_details, by = "order_id") %>%
  group_by(product_id) %>%
  summarise(total_quantity_sold = sum(order_quantity, na.rm = TRUE)) %>%
  ungroup()

# join with the 'products' data frame to get the product names
product_sales_with_names <- product_sales %>%
  left_join(products, by = "product_id") %>%
  select(product_name, product_id, total_quantity_sold)

# Arrange the data by total quantity sold to find the least selling products
least_selling_products <- product_sales_with_names %>%
  arrange(total_quantity_sold) %>%
  slice_head(n = 3)

# Print the least selling products excluding cancelled orders
print(least_selling_products)


# Best Selling Product Category

# Filter out cancelled orders 
non_cancelled_orders <- orders %>%
  filter(order_status != "cancelled")

# Join non_cancelled_orders with order_details, then join with products
sales_data <- non_cancelled_orders %>%
  inner_join(order_details, by = "order_id") %>%
  inner_join(products, by = "product_id")

# Join sales data with product categories to get category names
sales_data_with_categories <- sales_data %>%
  inner_join(product_categories, by = "category_id")

# Aggregate total sales per category, considering the quantity sold and the product price
category_sales_totals <- sales_data_with_categories %>%
  group_by(category_name) %>%
  summarise(total_sales = sum(order_quantity, na.rm = TRUE)) %>%
  ungroup()

# Sort to find the best-selling categories
best_selling_category <- category_sales_totals %>%
  arrange(desc(total_sales)) %>%
  slice_head(n = 5)

# Print the top 5 best-selling product categories
print(best_selling_category)


# Plot the bar chart
best_selling_categories_plot <- ggplot(best_selling_category, aes(x = total_sales, y = reorder(category_name, total_sales), fill = category_name)) +
  geom_bar(stat = "identity") +
  geom_text(aes(label = total_sales, y = reorder(category_name, total_sales)), 
            position = position_dodge(width = 0.9), 
            hjust = -0.1, 
            size = 3.5, 
            color = "black") +
  labs(title = "Best Selling Product Category",
       x = "Total Sales", 
       y = "Product Category") +
  theme_minimal() +
  theme(legend.position = "none")

ggsave(plot = best_selling_categories_plot, filename = "figures/best_selling_categories_plot.pdf", width = 10, height = 6, dpi = 300)


# Best Selling Product Category per Season

# Ensure order_date is Date type and define seasons
orders <- orders %>%
  mutate(
    order_date = as.Date(order_date, format = "%Y-%m-%d"),
    season = case_when(
      month(order_date) %in% c(12, 1, 2) ~ "Winter",
      month(order_date) %in% c(3, 4, 5) ~ "Spring",
      month(order_date) %in% c(6, 7, 8) ~ "Summer",
      month(order_date) %in% c(9, 10, 11) ~ "Fall"
    )
  )

# Exclude "cancelled" orders
non_cancelled_orders <- orders %>%
  filter(order_status != "cancelled")

#Join the order details with non_cancelled_orders and products to get category_id for each product sold
sales_data <- order_details %>%
  inner_join(non_cancelled_orders, by = "order_id") %>%
  inner_join(products, by = "product_id")

# Join the sales data with product categories to get category names
category_sales_data <- sales_data %>%
  inner_join(product_categories, by = "category_id")

# Aggregate the total sales per category per season
category_season_sales_totals <- category_sales_data %>%
  group_by(season, category_name) %>%
  summarise(total_sales = sum(order_quantity, na.rm = TRUE)) %>%
  ungroup()

# Find the top 5 categories with the highest total sales for each season
best_selling_categories_per_season <- category_season_sales_totals %>%
  arrange(desc(total_sales)) %>%
  group_by(season) %>%
  slice_max(order_by = total_sales, n = 5) %>%
  ungroup()

# Arrange the result in order
best_selling_categories_per_season <- best_selling_categories_per_season %>%
  arrange(season, desc(total_sales))
print(best_selling_categories_per_season)

#Total Sales per Supplier per Year

# Ensure order_date is Date type
orders <- orders %>%
  mutate(
    order_date = as.Date(order_date, format = "%Y-%m-%d"),
    year = year(order_date)
  )

# Exclude "cancelled" orders
non_cancelled_orders <- orders %>%
  filter(order_status != "cancelled")


# Calculate the total sales per supplier per year
supplier_sales <- order_details %>%
  inner_join(non_cancelled_orders, by = "order_id") %>%
  inner_join(products, by = "product_id") %>%
  group_by(year, supplier_id) %>%
  summarise(total_sales = sum(order_quantity, na.rm = TRUE)) %>%
  ungroup()


supplier_sales <- supplier_sales %>%
  inner_join(suppliers, by = "supplier_id") %>%
  select(year, supplier_name, total_sales) %>%
  arrange(year, desc(total_sales))

# For each year, get the best-selling suppliers
best_selling_suppliers_per_year <- supplier_sales %>%
  group_by(year) %>%
  slice_max(order_by = total_sales, n = 1) %>%
  ungroup()

print(best_selling_suppliers_per_year)

#Total Sales per Supplier per Season

# Ensure order_date is Date type and define seasons
orders <- orders %>%
  mutate(
    order_date = as.Date(order_date, format = "%d/%m/%Y"), # Adjust format as necessary
    season = case_when(
      month(order_date) %in% c(12, 1, 2) ~ "Winter",
      month(order_date) %in% c(3, 4, 5) ~ "Spring",
      month(order_date) %in% c(6, 7, 8) ~ "Summer",
      month(order_date) %in% c(9, 10, 11) ~ "Fall"
    )
  )

# Exclude "cancelled" orders
non_cancelled_orders <- orders %>%
  filter(order_status != "cancelled")


# Calculate the total sales per supplier per season
seasonal_supplier_sales <- order_details %>%
  inner_join(non_cancelled_orders, by = "order_id") %>%
  inner_join(products, by = "product_id") %>%
  group_by(season, supplier_id) %>%
  summarise(total_sales = sum(product_quantity, na.rm = TRUE), .groups = "drop") %>%
  ungroup()

# Join with the suppliers dataframe to get the supplier names
seasonal_supplier_sales <- seasonal_supplier_sales %>%
  inner_join(suppliers, by = "supplier_id") %>%
  select(season, supplier_name, total_sales) %>%
  arrange(season, desc(total_sales))

# For each season, find the best-selling suppliers
best_selling_suppliers_per_season <- seasonal_supplier_sales %>%
  group_by(season) %>%
  slice_max(order_by = total_sales, n = 1) %>%
  ungroup()

print(best_selling_suppliers_per_season)




library(readr)

# Get a list of file names in the directory
dataset <- list.files("project/data_uploads/Dataset/")
dataset

#the tables by removing the suffix "-Table 1.csv"
suffix <- "-Table 1.csv"
dataset <- gsub("-Table 1.csv","",datasets)
dataset

#Check duplicates (its not that necessary)

#import to database

#referential intergrity checks

#data quality checks

read.csv
summary("orders")

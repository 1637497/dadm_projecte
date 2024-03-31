library(tidyverse)
library(mongolite)

# Connecta amb la base de dades MongoDB
mongo_client <- mongo(db = "co2_emissions", collection = "co2_emissions",  url = "mongodb://localhost")

# Read the CSV file
data <- read.csv("/Users/marioamadorhurtado/Desktop/CARRERA/3r/2ns/DADM/PROJECTE/owid-co2-data.csv")

# Select the important columns
data <- data %>% 
  select(country, year, population, gdp, cement_co2, co2, co2_per_capita,
         consumption_co2, energy_per_capita, flaring_co2, trade_co2,
         share_of_temperature_change_from_ghg)

# Remove rows with NA values
clean_data <- na.omit(data)

# Convert the data to a list of JSON documents
json_data <- jsonlite::toJSON(clean_data, dataframe = "rows", na = "null", auto_unbox = TRUE)
json_list <- jsonlite::fromJSON(json_data)

# Insert the documents into the MongoDB collection
mongo_client$insert(json_list)

# Query the collection for documents with country "Afghanistan"
mongo_client$find(query = '{"country": "Albania"}')

# Tancar la connexió amb MongoDB
mongo_client$disconnect()

library(tidyverse)
library(mongolite)

# --------------------------------------------------------------------------------------------------------------------------------------------

# Connecta amb la base de dades MongoDB
mongo_client <- mongo(db = "co2_emissions", collection = "co2_emissions",  url = "mongodb://localhost")

# Read the CSV file
data <- read.csv("/Users/marioamadorhurtado/Desktop/CARRERA/3r/2ns/DADM/PROJECTE/owid-co2-data.csv")

# Select the important columns
data <- data %>% 
  select(country, year, population, gdp, cement_co2, co2, co2_per_capita,
         consumption_co2, energy_per_capita, flaring_co2, trade_co2,
         share_of_temperature_change_from_ghg)

# --------------------------------------------------------------------------------------------------------------------------------------------

# Fusionar taules EXPORTS
data_exports <- read.csv("/Users/marioamadorhurtado/Desktop/CARRERA/3r/2ns/DADM/PROJECTE/WORLDWIDE/exports/API_NE.EXP.GNFS.CD_DS2_en_csv_v2_3025.csv", sep = ",", header = FALSE)
data_exports <- tail(data_exports, -2)

colnames(data_exports) <- c("country", "code", "indicator_name", "indicator_code", 1960:2021)  # Renombrar las columnas
exports <- data_exports[, c("country", 1960:2021)]

exports_long <- pivot_longer(exports, cols = -country, names_to = "year", values_to = "exports")
exports_long$year <- as.numeric(exports_long$year)

data <- merge(data, exports_long, by = c("country", "year"), all.x = TRUE)
data <- data[, c(1:3, 5:ncol(data), 4)]

# --------------------------------------------------------------------------------------------------------------------------------------------

# Fusionar taules GINI
data_gini <- read.csv("/Users/marioamadorhurtado/Desktop/CARRERA/3r/2ns/DADM/PROJECTE/WORLDWIDE/GINI/API_SI.POV.GINI_DS2_en_csv_v2_16.csv", sep = ",", header = FALSE)
data_gini <- tail(data_gini, -2)
head(data_gini)

colnames(data_gini) <- c("country", "code", "indicator_name", "indicator_code", 1960:2021)  # Renombrar las columnas
gini <- data_gini[, c("country", 1960:2021)]

gini_long <- pivot_longer(gini, cols = -country, names_to = "year", values_to = "gini")
gini_long$year <- as.numeric(gini_long$year)

data <- merge(data, gini_long, by = c("country", "year"), all.x = TRUE)
data <- data[, c(1:3, 5:ncol(data), 4)]

# --------------------------------------------------------------------------------------------------------------------------------------------

# Fusionar taules LIFE EXPECTANCY
data_lf <- read.csv("/Users/marioamadorhurtado/Desktop/CARRERA/3r/2ns/DADM/PROJECTE/WORLDWIDE/life_expectancy/API_SP.DYN.LE00.IN_DS2_en_csv_v2_107.csv", sep = ",", header = FALSE)
data_lf <- tail(data_lf, -2)
head(data_lf)

colnames(data_lf) <- c("country", "code", "indicator_name", "indicator_code", 1960:2021)  # Renombrar las columnas
lf <- data_lf[, c("country", 1960:2021)]

lf_long <- pivot_longer(lf, cols = -country, names_to = "year", values_to = "life_expectancy")
lf_long$year <- as.numeric(lf_long$year)

data <- merge(data, lf_long, by = c("country", "year"), all.x = TRUE)
data <- data[, c(1:3, 5:ncol(data), 4)]

data[is.na(data)] <- 0

# --------------------------------------------------------------------------------------------------------------------------------------------

# Convert the data to a list of JSON documents
json_data <- jsonlite::toJSON(data, dataframe = "rows", na = "null", auto_unbox = TRUE)
json_list <- jsonlite::fromJSON(json_data)

# Insert the documents into the MongoDB collection
mongo_client$insert(json_list)

# Query the collection for documents with country "Afghanistan"
mongo_client$find(query = '{"year": 1850}')

# Tancar la connexió amb MongoDB
mongo_client$disconnect()




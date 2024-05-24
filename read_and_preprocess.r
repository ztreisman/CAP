library(lubridate)
library(ggplot2)
library(dplyr)
library(tidyr)
library(scattermore)
library(plotly)

# Define the parent directory containing directories for each month
parent_dir <- "data/SensorData/collections"

# List all directories in the parent directory
month_dirs <- list.dirs(path = parent_dir, full.names = TRUE, recursive = FALSE)

# Function to read and preprocess each CSV file
read_and_preprocess <- function(file) {
  # Extract location from the file name (3rd to 5th characters)
  location <- substr(basename(file), 3, 5)
  
  # Read the first few lines to determine the structure
  initial_lines <- readLines(file, n = 3)  # Read the first three lines
  
  # Check if the first line contains a serial number
  skip_first_lines <- grepl("Serial", initial_lines[1]) && initial_lines[2] == ""
  #if(skip_first_lines) cat("skipping first lines \n")
  
  # Adjust the first line to read as data
  first_line <- ifelse(skip_first_lines, 3, 1)
  
  # Read the CSV file, skipping the appropriate number of lines
  skip_lines <- first_line - 1
  
  # Check if the first column contains unwanted metadata
  skip_first_column <- grepl("^#", initial_lines[first_line])
  #if(skip_first_column) cat("skipping first column\n")
  
  if (grepl("RH", initial_lines[first_line]) && grepl("Dew", initial_lines[first_line])) {
    # File contains temperature, relative humidity, and dewpoint
    # Define column specifications for files with rh and dewpoint
    col_classes <- c("character", "numeric", "numeric", "numeric", rep("NULL", 10))  # Adjust 10 based on the maximum expected number of extra columns
    col_names <- c("datetime", "temp", "rh", "dewpoint", rep("NULL", 10))  # Adjust 10 based on the maximum expected number of extra columns
  } else {
    # File contains only temperature
    # Define column specifications for files without rh and dewpoint
    col_classes <- c("character", "numeric", rep("NULL", 10))  # Adjust 10 based on the maximum expected number of extra columns
    col_names <- c("datetime", "temp", rep("NULL", 10))  # Adjust 10 based on the maximum expected number of extra columns
  }
  
  # Adjust column specifications to skip the first column if needed
  if (skip_first_column) {
    col_classes <- c("NULL", col_classes)
    col_names <- c("NULL", col_names)
  }
  
  
  # Attempt to read the CSV file with both unicode and latin 3 encodings
  data <- NULL
  for (encoding in c("latin1", "UTF-8", "")) {
    tryCatch({
      # Read the CSV file as characters
      data <- read.csv(file, colClasses = col_classes, col.names = col_names, skip = skip_lines, encoding = encoding)
      # If reading succeeds, break out of the loop
      break
    }, error = function(e) {
      # If an error occurs, try the next encoding
      cat("Error reading file with encoding", encoding, ": ", file, "\n")
    })
    
    # If data is not NULL, reading succeeded, so break out of the loop
    if (!is.null(data)) {
      break
    }
  }
  
  # Check if data is still NULL after trying both encodings
  if (is.null(data)) {
    cat("Failed to read file:", file, "\n")
    cat("Failed to read file:", file, "\n", file = "errors.txt", append = TRUE)
    return(NULL)
  }
  
 
  
  # Dynamically determine datetime format
  datetime_format <- ifelse(all(grepl("^\\d{4}[-/]\\d{2}[-/]\\d{2}", data$datetime)), "ymd", "mdy")
  
  # Parse datetime column based on the determined format
  if (datetime_format == "ymd") {
    if (any(grepl("-", data$datetime))) {
      data$datetime <- as.POSIXct(data$datetime, format = "%Y-%m-%d %H:%M", tz = "America/Denver", quiet = TRUE)
    } else if (any(grepl("/", data$datetime))) {
      data$datetime <- as.POSIXct(data$datetime, format = "%Y/%m/%d %H:%M", tz = "America/Denver", quiet = TRUE)
    } else {
      # Handle other cases or raise an error if neither '-' nor '/' is found
      stop("Unknown datetime format")
    }
  } else {
    if (any(grepl("-", data$datetime))) {
      data$datetime <- as.POSIXct(data$datetime, format = "%m-%d-%Y %H:%M", tz = "America/Denver", quiet = TRUE)
    } else if (any(grepl("/", data$datetime))) {
      data$datetime <- gsub("([0-9]{2}/[0-9]{2}/[0-9]{4})([0-9]{2}:[0-9]{2}:[0-9]{2})", "\\1 \\2", data$datetime)
      data$datetime <- as.POSIXct(data$datetime, format = "%m/%d/%Y %H:%M", tz = "America/Denver", quiet = TRUE)
    } else {
      # Handle other cases or raise an error if neither '-' nor '/' is found
      stop("Unknown datetime format")
    }
  }
  
  # Check for and handle NA values in the datetime field
  if (any(is.na(data$datetime))) {
    cat("Error parsing datetime in file:", file, "\n")
    cat("Original datetimes:\n")
    print(head(data$datetime))
    cat("Attempting to parse again with alternate formats...\n")
    
    # Try alternate datetime parsing formats
    data$datetime <- case_when(
      grepl("-", data$datetime) ~ parse_date_time(data$datetime, orders = c("ymd HM", "ymd HMS")),
      grepl("/", data$datetime) ~ parse_date_time(data$datetime, orders = c("mdy HM", "mdy HMS")),
      TRUE ~ NA_POSIXct_
    )
    
    if (any(is.na(data$datetime))) {
      cat("Error still present after alternate parsing attempts.\n")
      cat("Problematic datetimes:\n")
      print(head(data$datetime[is.na(data$datetime)]))
    }
  }
  
  data$location <- location  # Add location as a new column
  
  # Exclude rows without temperature data
  data <- data %>% filter(!is.na(temp))
  
  return(data)
}

# Function to read and preprocess data from a directory
read_and_preprocess_directory <- function(directory) {
  # List all CSV files in the directory
  file_list <- list.files(path = directory, pattern = "*.csv", full.names = TRUE)
  
  # Initialize a list to store data frames
  data_frames <- list()
  
  # Loop through each CSV file in the directory
  for (file in file_list) {
    tryCatch({
      cat("Reading file:", file, "\n")  # Print the name of the file being read
      # Read and preprocess the file
      data <- read_and_preprocess(file)
      # Add the data frame to the list
      data_frames[[file]] <- data
    }, error = function(e) {
      cat("Error reading file:", file, "\n")  # Print the error message and file name
      cat("Error message:", conditionMessage(e), "\n")
    })
  }
  
  # Combine all data frames into a single data frame
  all_data <- bind_rows(data_frames)
  
  return(all_data)
}


# Read and preprocess data from each directory
all_data <- month_dirs %>%
  lapply(read_and_preprocess_directory) %>%
  bind_rows()

# Remove duplicate rows in the combined data
all_data <- all_data %>% distinct()



# Inspect the combined data
View(all_data)

# Plot all locations
ggplot(all_data, aes(datetime, temp)) +
  geom_scattermore() +
  facet_wrap(~ location) +
  labs(title = "Temperature Over Time by Location", x = "Datetime", y = "Temperature")


# Plot temperature over time, colored by location
plot_locations <- c("WIL", "OBS", "ALM")
plot_data <- all_data %>%
  filter(location %in% plot_locations)

location_plot <- ggplot(plot_data, aes(datetime, temp, color = location)) +
  geom_scattermore() +
  scale_color_brewer(palette = "Dark2") +
  labs(title = "Temperature Over Time by Location", x = "Datetime", y = "Temperature")

location_plot

# Convert ggplot to plotly for interactivity
plotly_plot <- ggplotly(location_plot)

# Display the interactive plot
plotly_plot  

# Exclude rows where both rh and dewpoint are NA
rich_data <- all_data %>% filter(!(is.na(rh) & is.na(dewpoint)))

# Plot relative humidity vs dewpoint with temperature as color, faceted by location
ggplot(rich_data, aes(rh, dewpoint, color = temp)) +
  geom_scattermore() +
  scale_color_continuous(type = "viridis") +
  facet_wrap(~ location) +
  labs(title = "Relative Humidity vs Dewpoint by Location", x = "Relative Humidity", y = "Dewpoint")

# Filter data for a pair of locations
comps <- c("MIF", "MIA")
subset_data <- all_data %>%
  filter(location %in% comps) %>%
  select(datetime, temp, location) %>%
  mutate(datetime = floor_date(datetime, unit="hour"))

# Pivot the data to have temperature values for each location in separate columns
pivot_data <- subset_data %>%
  pivot_wider(names_from = location, values_from = temp, values_fn = mean)

# Calculate the temperature difference between the two locations
pivot_data$temp_diff <- numeric(nrow(pivot_data))
pivot_data[,"temp_diff"] <- pivot_data[,comps[1]] - pivot_data[,comps[2]]

# Plot time series of temperature differences between locations 
ggplot(pivot_data, aes(x = datetime, y = temp_diff)) +
  geom_line() +
  labs(title = paste0("Temperature Difference between ", comps[1], " and ", comps[2]), 
       x = "Datetime", y = paste0("Temperature Difference ", comps[1], " - ", comps[2]))


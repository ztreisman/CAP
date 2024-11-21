library(lubridate)
library(hms)
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
  
  # Create lagged temp values
  
  data <- data %>% arrange(datetime)
  
  data <- data %>%
    mutate(
      
      temp_lag1 = lag(temp, 1),
      temp_lag2 = lag(temp, 2),
      temp_lag3 = lag(temp, 3),
      temp_lag4 = lag(temp, 4),
      temp_lag5 = lag(temp, 5),
      temp_lag6 = lag(temp, 6),
      temp_lag7 = lag(temp, 7),
      temp_lag8 = lag(temp, 8),
      temp_lag9 = lag(temp, 9),
      temp_lag10 = lag(temp, 10),
      temp_lag11 = lag(temp, 11),
      temp_lag12 = lag(temp, 12),
    
    )
  
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

## define CAP events

# select relevant sites

comps <- c("MIA", "MIB", "CA")
subset_data <- all_data %>%
  filter(location %in% comps) %>%
  select(datetime, temp, temp_lag1, temp_lag2, 
         temp_lag3, temp_lag4, temp_lag5, 
         temp_lag6, temp_lag7, temp_lag8, 
         temp_lag9, temp_lag10, temp_lag11,
         temp_lag12, location) %>%
  mutate(datetime = floor_date(datetime, unit = "hour") + minutes(3 * (minute(datetime) %/% 3)))

# Pivot the data to have temperature values for each location in separate columns
pivot_data <- subset_data %>%
  pivot_wider(id_cols = datetime, names_from = location, 
              values_from = c(temp, temp_lag1, temp_lag2, 
                              temp_lag3, temp_lag4, temp_lag5, 
                              temp_lag6, temp_lag7, temp_lag8, 
                              temp_lag9, temp_lag10, temp_lag11,
                              temp_lag12), 
              values_fn = mean)

# Define CAP events

pivot_data$CAPEvent2 <- (pivot_data$temp_MIB - pivot_data$temp_MIA > 2) & (pivot_data$temp_CAS - pivot_data$temp_MIA > 2) &
  (pivot_data$temp_lag1_MIB - pivot_data$temp_lag1_MIA > 2) & (pivot_data$temp_lag1_CAS - pivot_data$temp_lag1_MIA > 2) &
  (pivot_data$temp_lag2_MIB - pivot_data$temp_lag2_MIA > 2) & (pivot_data$temp_lag2_CAS - pivot_data$temp_lag2_MIA > 2) &
  (pivot_data$temp_lag3_MIB - pivot_data$temp_lag3_MIA > 2) & (pivot_data$temp_lag3_CAS - pivot_data$temp_lag3_MIA > 2) &
  (pivot_data$temp_lag4_MIB - pivot_data$temp_lag4_MIA > 2) & (pivot_data$temp_lag4_CAS - pivot_data$temp_lag4_MIA > 2) &
  (pivot_data$temp_lag5_MIB - pivot_data$temp_lag5_MIA > 2) & (pivot_data$temp_lag5_CAS - pivot_data$temp_lag5_MIA > 2) &
  (pivot_data$temp_lag6_MIB - pivot_data$temp_lag6_MIA > 2) & (pivot_data$temp_lag6_CAS - pivot_data$temp_lag6_MIA > 2) &
  (pivot_data$temp_lag7_MIB - pivot_data$temp_lag7_MIA > 2) & (pivot_data$temp_lag7_CAS - pivot_data$temp_lag7_MIA > 2) &
  (pivot_data$temp_lag8_MIB - pivot_data$temp_lag8_MIA > 2) & (pivot_data$temp_lag8_CAS - pivot_data$temp_lag8_MIA > 2) &
  (pivot_data$temp_lag9_MIB - pivot_data$temp_lag9_MIA > 2) & (pivot_data$temp_lag9_CAS - pivot_data$temp_lag9_MIA > 2) &
  (pivot_data$temp_lag10_MIB - pivot_data$temp_lag10_MIA > 2) & (pivot_data$temp_lag10_CAS - pivot_data$temp_lag10_MIA > 2)

pivot_data$CAPEvent5 <- (pivot_data$temp_MIB - pivot_data$temp_MIA > 5) & (pivot_data$temp_CAS - pivot_data$temp_MIA > 5) &
  (pivot_data$temp_lag1_MIB - pivot_data$temp_lag1_MIA > 5) & (pivot_data$temp_lag1_CAS - pivot_data$temp_lag1_MIA > 5) &
  (pivot_data$temp_lag2_MIB - pivot_data$temp_lag2_MIA > 5) & (pivot_data$temp_lag2_CAS - pivot_data$temp_lag2_MIA > 5) &
  (pivot_data$temp_lag3_MIB - pivot_data$temp_lag3_MIA > 5) & (pivot_data$temp_lag3_CAS - pivot_data$temp_lag3_MIA > 5) &
  (pivot_data$temp_lag4_MIB - pivot_data$temp_lag4_MIA > 5) & (pivot_data$temp_lag4_CAS - pivot_data$temp_lag4_MIA > 5) &
  (pivot_data$temp_lag5_MIB - pivot_data$temp_lag5_MIA > 5) & (pivot_data$temp_lag5_CAS - pivot_data$temp_lag5_MIA > 5) &
  (pivot_data$temp_lag6_MIB - pivot_data$temp_lag6_MIA > 5) & (pivot_data$temp_lag6_CAS - pivot_data$temp_lag6_MIA > 5) &
  (pivot_data$temp_lag7_MIB - pivot_data$temp_lag7_MIA > 5) & (pivot_data$temp_lag7_CAS - pivot_data$temp_lag7_MIA > 5) &
  (pivot_data$temp_lag8_MIB - pivot_data$temp_lag8_MIA > 5) & (pivot_data$temp_lag8_CAS - pivot_data$temp_lag8_MIA > 5) &
  (pivot_data$temp_lag9_MIB - pivot_data$temp_lag9_MIA > 5) & (pivot_data$temp_lag9_CAS - pivot_data$temp_lag9_MIA > 5) &
  (pivot_data$temp_lag10_MIB - pivot_data$temp_lag10_MIA > 5) & (pivot_data$temp_lag10_CAS - pivot_data$temp_lag10_MIA > 5)

# Make lists of all the times when CAP is happening

CAPTimes2 <- unique(pivot_data[which(pivot_data$CAPEvent2 == TRUE), ]$datetime)
CAPTimes5 <- unique(pivot_data[which(pivot_data$CAPEvent5 == TRUE), ]$datetime)

# What time of year does CAP happen?

hist(yday(CAPTimes2))
hist(yday(CAPTimes5))

# What time of day?

plot(yday(CAPTimes2), as_hms(CAPTimes2))
plot(yday(CAPTimes5), as_hms(CAPTimes5))

# Add CAPEvents back in to all_data

all_data$CAPEvent2 <- all_data$datetime %in% CAPTimes2
all_data$CAPEvent5 <- all_data$datetime %in% CAPTimes5

# Plot all locations
ggplot(all_data, aes(x=datetime, temp)) +
  geom_scattermore() +
  facet_wrap(~ location) +
  labs(title = "Temperature Over Time by Location", x = "Datetime", y = "Temperature")


# Plot temperature over time, colored by location
plot_locations <- c("MIA", "CAS", "MIB")
date_range <- mdy_hms(c("1/05/2023 00:00:00", "2/25/2023 00:00:00" ))
plot_data <- all_data %>%
  filter(location %in% plot_locations) %>%
  filter(datetime>=date_range[1] & datetime<=date_range[2])

location_plot <- ggplot(plot_data, aes(datetime, temp, color = location)) +
  geom_line() +
  scale_color_brewer(palette = "Dark2") +
  labs(title = "Temperature Over Time by Location", x = "Datetime", y = "Temperature")

location_plot
location_plot+facet_grid(~location)



## Convert ggplot to plotly for interactivity THIS DOESN'T WORK
#plotly_plot <- ggplotly(location_plot)

## Display the interactive plot
#plotly_plot  

## Exclude rows where both rh and dewpoint are NA
#rich_data <- all_data %>% filter(!(is.na(rh) & is.na(dewpoint)))

## Plot relative humidity vs dewpoint with temperature as color, faceted by location
#ggplot(rich_data, aes(rh, dewpoint, color = temp)) +
#  geom_scattermore() +
#  scale_color_continuous(type = "viridis") +
#  facet_wrap(~ location) +
#  labs(title = "Relative Humidity vs Dewpoint by Location", x = "Relative Humidity", y = "Dewpoint")

# pick a couple of locations and a datetime range
comps <- c("MIA", "MIB")
date_range <- mdy_hms(c("1/05/2023 00:00:00", "2/25/2023 00:00:00" ))

subset_data <- all_data %>%
  filter(location %in% comps) %>%
  filter(datetime>=date_range[1] & datetime<=date_range[2])%>%
  select(datetime, temp, location) %>%
  mutate(datetime = floor_date(datetime, unit="hour"))

# Pivot the data to have temperature values for each location in separate columns
pivot_subset <- subset_data %>%
  pivot_wider(names_from = location, values_from = temp, values_fn = mean)


# Calculate the temperature difference between the two locations
pivot_subset$temp_diff <- numeric(nrow(pivot_subset))
pivot_subset[,"temp_diff"] <- pivot_subset[,comps[1]] - pivot_subset[,comps[2]]

# Count the number of cold pools and total days in the daterange
length(unique(floor_date(pivot_subset[pivot_subset$CAPevent == TRUE,]$datetime, 
                      unit="days")))
length(unique(floor_date(pivot_subset$datetime, unit="days")))

# Plot time series of temperature differences between locations 
ggplot(pivot_subset, aes(x = datetime, y = temp_diff)) +
  geom_line() +
  labs(title = paste0("Temperature Difference between ", comps[1], " and ", comps[2]), 
       x = "Datetime", y = paste0("Temperature Difference ", comps[1], " - ", comps[2]))

print(pivot_subset[which(pivot_subset$temp_diff < (-5)), c("datetime", "temp_diff")], n=60)

pivot_subset$CAPEvent2 <- pivot_subset$datetime %in% CAPTimes2
pivot_subset$CAPEvent5 <- pivot_subset$datetime %in% CAPTimes5

# Zoom out to hourly observations 
hourly_data <- all_data %>%
  mutate(datetime = floor_date(datetime, unit="hour")) %>%
  group_by(datetime, location) %>%
  summarise(temp = mean(temp),
            temp_lag0.5 = mean(temp_lag10),
            rh = mean(rh),
            dewpoint = mean(dewpoint),
            CAPEvent2 = mean(CAPEvent2) > 0.5,
            CAPEvent5 = mean(CAPEvent5) > 0.5)


ggplot(hourly_data, aes(x = rh, y = temp, size = diff_temp, alpha = diff_temp, color = CAPevent, shape = CAPevent)) +
  geom_point() +
  facet_wrap(~ location) +
  scale_size_continuous(range = c(-16, 22)) +  # Adjust size range for better visibility
  scale_alpha_continuous(range = c(0.1, 0.6)) +  # Adjust alpha transparency for better distinction
  scale_color_manual(values = c("red", "blue")) +  # Custom colors for CAPevent categories
  labs(
    title = "Relative Humidity vs Temperature by Location",
    x = "Relative Humidity (%)",
    y = "Temperature (°C)"
  ) +
  theme_minimal() +  # Clean theme for better readability
  theme(legend.position = "right")  # Position the legend on the right

# I don't see any patterns, maybe R does

# Logistic regression model

log_reg1 <- glm(CAPEvent2~location*(temp+temp_lag0.5+rh), family=poisson(), data=hourly_data)
summary(log_reg1)

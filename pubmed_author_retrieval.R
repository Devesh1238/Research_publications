# Title: R Script for Retrieving Disambiguated Author Publications from PubMed (Final)
# Description: This script reads a list of authors and their PMIDs,
#              cleans the author names (handling single and hyphenated last names),
#              queries the PubMed API with proper rate limiting and retry logic,
#              and saves the results to an output file.
# Author: Gemini & Coding Partner
# Date: 2025-09-02

# --- 1. Installation of Required Packages ---
# If you don't have these packages installed, uncomment and run the following lines:
# install.packages("httr")
# install.packages("jsonlite")
# install.packages("dplyr")
# install.packages("readr")
# install.packages("readxl")
# install.packages("stringr")

# --- 2. Load Libraries ---
library(httr)
library(jsonlite)
library(dplyr)
library(readr)
library(readxl)
library(stringr)

# --- 3. Configuration ---
# IMPORTANT: Please update this path to point to your actual input file.
input_file_path <- "C:/Users/deves/Downloads/clinical/stanford/Jeff choi/NPPES_Analysis/01_data_raw/test file for authors api.xlsx"

# Define the path for the output file where results will be saved.
output_file_path <- "author_publications_results_final.csv"

# --- 4. Helper Functions ---

# Updated function to clean author names, now handling hyphens.
clean_author_name <- function(raw_name) {
  # Remove tags like [AU] and trim whitespace
  name <- str_remove_all(raw_name, "\\[AU\\]")
  name <- str_trim(name)

  # Split the name into parts by space
  parts <- str_split(name, "\\s+")[[1]]

  # Ensure we have at least a name and an initial part
  if (length(parts) < 2) {
    return(NA)
  }

  # The first part is assumed to be the last name (e.g., "SANTIAGO-DIEPPA" or "ZIMMER")
  last_name_part <- parts[1]

  ## FIX: Remove all non-alphabetic characters (like hyphens) from the last name part.
  cleaned_last_name <- str_remove_all(last_name_part, "[^a-zA-Z]")

  # The second part contains the initials (e.g., "DR" or "E")
  initials_part <- parts[2]
  # We only need the very first initial for the query.
  first_initial <- str_sub(initials_part, 1, 1)

  # Combine the cleaned last name and the first initial
  return(paste(cleaned_last_name, first_initial))
}


# Function to query the PubMed Computed Authors API (with retry logic)
get_author_publications <- function(pmid, author_name) {
  base_url <- "https://www.ncbi.nlm.nih.gov/research/litsense-api/api/author/"
  query_string <- paste(pmid, author_name)
  request_url <- modify_url(base_url, query = list(query = query_string))

  message("Querying API for: ", query_string)

  response <- NULL
  max_retries <- 3
  retry_delay_secs <- 5

  for (attempt in 1:max_retries) {
    response <- tryCatch({
      GET(request_url, add_headers(`User-Agent` = "R-Script/1.0"), timeout(15))
    }, error = function(e) {
      message("An error occurred during the API request: ", e$message)
      return(NULL)
    })

    if (!is.null(response)) {
      if (status_code(response) != 429) {
        break
      }
      message("Rate limit hit. Waiting for ", retry_delay_secs, " seconds before retrying...")
      Sys.sleep(retry_delay_secs)
      retry_delay_secs <- retry_delay_secs * 2
    } else {
      Sys.sleep(retry_delay_secs)
    }
  }

  if (is.null(response) || http_status(response)$category != "Success") {
    warning("Failed to retrieve data for query: ", query_string, ". Status: ", http_status(response)$reason)
    return(NA)
  }

  content <- content(response, "text", encoding = "UTF-8")
  parsed_json <- fromJSON(content, flatten = TRUE)

  retrieved_pmids <- NULL
  if (!is.null(parsed_json$results) && nrow(parsed_json$results) > 0 && "pmids" %in% names(parsed_json$results)) {
    retrieved_pmids <- parsed_json$results$pmids[[1]]
  }

  if (is.null(retrieved_pmids) || length(retrieved_pmids) == 0) {
    return("")
  }

  return(paste(retrieved_pmids, collapse = ", "))
}


# --- 5. Script Execution ---
tryCatch({
  author_data <- read_excel(input_file_path)

  required_cols <- c("pmid", "last name and initial")
  if (!all(required_cols %in% names(author_data))) {
    stop("Input file is missing required columns: 'pmid', 'last name and initial'")
  }

  author_data$cleaned_author_name <- NA_character_
  author_data$retrieved_pmids <- NA_character_

  for (i in 1:nrow(author_data)) {
    current_pmid <- author_data$pmid[i]
    raw_author_name <- author_data$`last name and initial`[i]

    if (is.na(current_pmid) || is.na(raw_author_name) || raw_author_name == "") {
      message("Skipping row ", i, " due to missing PMID or author name.")
      next
    }

    cleaned_name <- clean_author_name(raw_author_name)
    author_data$cleaned_author_name[i] <- cleaned_name

    if(is.na(cleaned_name)) {
      message("Skipping row ", i, " because author name '", raw_author_name, "' could not be cleaned.")
      next
    }

    author_data$retrieved_pmids[i] <- get_author_publications(current_pmid, cleaned_name)

    # Respectful 1.1-second delay between API calls
    Sys.sleep(1.1)
  }

  write_csv(author_data, output_file_path)

  message("\nProcessing complete. Results saved to: ", output_file_path)

}, error = function(e) {
  message("An error occurred: ", e$message)
})

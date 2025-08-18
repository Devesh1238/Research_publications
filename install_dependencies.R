# install_dependencies.R
# This script will install all the required packages for the PubMed query project.

# Create a user library directory if it doesn't exist
user_lib <- file.path(Sys.getenv("HOME"), "R", "library")
dir.create(user_lib, recursive = TRUE, showWarnings = FALSE)

# List of packages to install
packages <- c(
  "data.table",
  "stringr",
  "httr",
  "jsonlite",
  "digest",
  "stringdist",
  "stringi"
)

# Loop through the packages and install them if they are not already installed
for (pkg in packages) {
  if (!require(pkg, character.only = TRUE, quietly = TRUE)) {
    cat(paste("Installing package:", pkg, "\n"))
    tryCatch({
      install.packages(pkg, dependencies = TRUE, lib = user_lib)
      cat(paste("Successfully installed:", pkg, "\n"))
    }, error = function(e) {
      cat(paste("Failed to install", pkg, ":", e$message, "\n"))
    })
  } else {
    cat(paste("Package", pkg, "is already installed.\n"))
  }
}

cat("\nAll required packages are installed and ready to use.\n")

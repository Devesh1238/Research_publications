# install_dependencies.R
# This script will install all the required packages for the PubMed query project.

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
  if (!require(pkg, character.only = TRUE)) {
    cat(paste("Installing package:", pkg, "\n"))
    install.packages(pkg, dependencies = TRUE)
  }
}

cat("\nAll required packages are installed and ready to use.\n")

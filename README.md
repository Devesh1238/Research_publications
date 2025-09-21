# PubMed Surgeon Publication Query Engine

This project provides a sophisticated R-based pipeline for querying the PubMed database to identify publications by a given list of surgeons. It is designed to maximize both precision and recall by using a multi-tiered query strategy, handling complex name variations, and leveraging affiliation data.

## Features

- **Automated Dependency Management**: A script is provided to install all required R packages.
- **Resilient HTTP Requests**: Built-in retry logic, caching, and support for NCBI API keys to handle network issues and API rate limits.
- **Advanced Author Disambiguation**: A multi-tier decision tree algorithm that uses author names (including variants), affiliation details (city, state, organization), and publication dates to accurately match publications to surgeons.
- **Comprehensive Logging**: Detailed logs provide insight into the query process, including ESearch terms, hit counts, and diagnostics for each surgeon.
- **Caching**: Caches API responses to speed up subsequent runs and reduce redundant queries.
- **Resume Functionality**: The main script can resume from where it left off, which is useful for large datasets.

## Setup

1.  **Install R**: Ensure you have a recent version of R installed on your system.
2.  **Install Dependencies**: Open an R session and run the following command to install the required packages:

    ```R
    source('install_dependencies.R')
    ```

3.  **(Optional) Set NCBI API Key**: For better performance and higher rate limits, it is recommended to obtain an NCBI API key and set it as an environment variable named `NCBI_API_KEY`.

    ```bash
    export NCBI_API_KEY="your_api_key_here"
    ```

## Usage

The main script, `03_run_pubmed_smart_query_v2_http_EXPERT.R`, reads a CSV file of surgeon data, queries PubMed for their publications, and writes the results to a new CSV file.

1.  **Prepare Input Data**: Create a CSV file named `outputs/surgeons_pilot_for_query.csv`. This file should contain the surgeon data, including columns like `last_name_norm`, `first_name_token`, `city_token`, `state_token`, and `org_token`.
2.  **Run the Script**: Open an R session and execute the main script:

    ```R
    source('03_run_pubmed_smart_query_v2_http_EXPERT.R')
    run_phase2_http_expert()
    ```

3.  **Check the Output**: The script will generate the following files in the `outputs/` directory:
    - `surgeons_with_pubcounts_EXPERT.csv`: The main output file, containing the original surgeon data enriched with publication counts and PMID lists.
    - `phase2_expert_run_log.txt`: A detailed log of the entire execution process.
    - `eutils_cache/`: A directory containing cached API responses.

## How It Works

The script implements a sophisticated, multi-tier decision tree to find publications for each surgeon:

-   **Tier 1: FAU + Affiliation**: The most specific query, using the full author name (`[FAU]`) combined with affiliation details.
-   **Tier 2: FAU Only**: If Tier 1 fails, it queries using only the full author name.
-   **Tier 3: AU + Affiliation**: A broader search using the author's last name and first initial (`[AU]`) with affiliation.
-   **Tier 4: AU Only**: The broadest search, used for standard-risk surnames, querying by author name without affiliation.

Each potential publication is verified using a scoring system that considers the similarity of the author's full name and affiliation details, ensuring high accuracy.

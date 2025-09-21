# =====================================================================
# SCRIPT: 03_run_pubmed_smart_query_v2_http_EXPERT.R (EXPERT SOLUTION)
# PURPOSE: Expert-level solution for precision-recall optimization
# FIXES: Vector safety, FAU variants, graduated thresholds, smart diagnostics
# AUTHOR: Expert-level refactor addressing all identified issues
# =====================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(stringr)
  library(httr)
  library(jsonlite)
  library(digest)
  library(stringdist)
})

`%||%` <- function(a,b) if (is.null(a) || length(a)==0) b else a

# ---------- CONFIG ----------
TOOL  <- "nppes_pubmed_pipeline"
EMAIL <- "deveshrk@stanford.edu"

IN_CSV     <- "outputs/surgeons_pilot_for_query.csv"
OUT_CSV    <- "outputs/surgeons_with_pubcounts_EXPERT.csv"
CACHE_DIR  <- "outputs/eutils_cache"
LOG_FILE   <- "outputs/phase2_expert_run_log.txt"

# EXPERT TUNED PARAMETERS
API_KEY <- Sys.getenv("NCBI_API_KEY")
API_DELAY_SEC       <- if (nchar(API_KEY)>0) 0.13 else 0.35
RETRY_MAX           <- 3
RETRY_BASE          <- 0.7
MAX_PMIDS_TO_FETCH  <- 300  # EXPERT: Increased from 200 for better coverage
EFETCH_CHUNK        <- 150
CHK_EVERY           <- 50   # More frequent checkpoints
RESUME              <- TRUE

LOOKBACK_YEARS      <- 25
DATE_CEILING_YEAR   <- as.integer(format(Sys.Date(), "%Y"))
SCORE_THRESHOLD     <- 0.70  # EXPERT: Relaxed from 0.75 for better recall
ROW_TIME_BUDGET_SEC <- 180   # EXPERT: Increased from 120 for complex cases

BASE_URL <- "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
ESEARCH  <- paste0(BASE_URL, "esearch.fcgi")
EFETCH   <- paste0(BASE_URL, "efetch.fcgi")

if (!dir.exists("outputs")) dir.create("outputs", recursive = TRUE)
if (!dir.exists(CACHE_DIR)) dir.create(CACHE_DIR, recursive = TRUE)

# ---------- EXPERT LOGGING & DIAGNOSTICS ----------

#' Log a Message to Console and File
#'
#' Appends a timestamped message to the console and to a log file.
#'
#' @param ... A series of character strings or objects that will be coerced to
#'   character strings and concatenated to form the log message.
#' @return Invisible NULL. The function is called for its side effect of logging.
log_line <- function(...) {
  msg <- paste0(format(Sys.time(), "%F %T"), " | ", paste(..., collapse=" "))
  cat(msg, "\n")
  try(write(msg, file = LOG_FILE, append = TRUE), silent = TRUE)
}

#' Log a Diagnostic Message
#'
#' A specialized logging function for recording diagnostic information related
#' to a specific NPI and processing stage.
#'
#' @param npi The NPI (National Provider Identifier) of the surgeon.
#' @param stage A character string describing the stage of the process (e.g., "RAW_HITS").
#' @param details A character string providing specific details for the log entry.
#' @return Invisible NULL. The function is called for its side effect of logging.
log_diagnostic <- function(npi, stage, details) {
  log_line("DIAGNOSTIC [", npi, "] ", stage, ": ", details)
}

#' Sanitize a String for Use as a Filename
#'
#' Replaces characters that are not alphanumeric, dot, underscore, or hyphen
#' with an underscore.
#'
#' @param x A character string to sanitize.
#' @return A sanitized character string.
.safe_name <- function(x) gsub("[^A-Za-z0-9._-]+", "_", x)

#' Create a Cache Key
#'
#' Generates a unique, safe key for caching based on a prefix and a list of
#' R objects. The objects are hashed using SHA-1.
#'
#' @param prefix A character string to prepend to the hash.
#' @param ... Additional R objects to be included in the hash calculation.
#' @return A character string representing the cache key.
cache_key  <- function(prefix, ...) .safe_name(paste(prefix, digest(list(...), algo="sha1"), sep = "__"))

#' Get the Full Path for a Cache Key
#'
#' Constructs the full file path for a given cache key.
#'
#' @param key A character string representing the cache key.
#' @return A character string representing the full file path to the cache file.
cache_path <- function(key) file.path(CACHE_DIR, paste0(key, ".rds"))

#' Retrieve an Object from the Cache
#'
#' Reads and returns an R object from a cache file if it exists.
#'
#' @param key The cache key of the object to retrieve.
#' @return The deserialized R object, or NULL if the cache file does not exist.
cache_get  <- function(key) { p <- cache_path(key); if (file.exists(p)) readRDS(p) else NULL }

#' Store an Object in the Cache
#'
#' Serializes and saves an R object to a file in the cache directory.
#'
#' @param key The cache key under which to store the object.
#' @param val The R object to be cached.
#' @return Invisible NULL. The function is called for its side effect of saving a file.
cache_put  <- function(key, val) saveRDS(val, cache_path(key))

# ---------- HTTP wrappers with enhanced resilience ----------

#' Perform an HTTP GET Request and Parse JSON Response
#'
#' Sends an HTTP GET request, with built-in caching, retry logic, and error
#' handling. It parses the response body as JSON.
#'
#' @param url The base URL for the request.
#' @param query A named list of query parameters to be appended to the URL.
#' @return The parsed JSON response as an R object (typically a list).
#' @throws An error if the request fails after multiple retries.
http_get_json <- function(url, query) {
  attempt <- 0
  query$tool  <- TOOL
  query$email <- EMAIL
  if (nchar(API_KEY) > 0) query$api_key <- API_KEY
  key <- cache_key("GET_JSON", url, query)
  if (!is.null(res <- cache_get(key))) return(res)
  repeat {
    attempt <- attempt + 1
    Sys.sleep(API_DELAY_SEC)
    resp <- try(httr::GET(url, query = query, timeout(60)), silent = TRUE)
    if (!inherits(resp, "try-error") && !httr::http_error(resp)) {
      out <- jsonlite::fromJSON(httr::content(resp, as = "text", encoding = "UTF-8"), simplifyVector = TRUE)
      cache_put(key, out)
      return(out)
    }
    if (attempt >= RETRY_MAX) {
      stop(paste("http_get_json failed:", if (inherits(resp, "try-error")) as.character(resp) else paste("HTTP", status_code(resp))))
    }
    Sys.sleep(RETRY_BASE * (2^(attempt-1)))
  }
}

#' Perform an HTTP GET Request and Return Text Response
#'
#' Sends an HTTP GET request with built-in caching, retry logic, and error
#' handling. It returns the response body as a raw text string.
#'
#' @param url The base URL for the request.
#' @param query A named list of query parameters to be appended to the URL.
#' @return The response body as a single character string.
#' @throws An error if the request fails after multiple retries.
http_get_text <- function(url, query) {
  attempt <- 0
  query$tool  <- TOOL
  query$email <- EMAIL
  if (nchar(API_KEY) > 0) query$api_key <- API_KEY
  key <- cache_key("GET_TEXT", url, query)
  if (!is.null(res <- cache_get(key))) return(res)
  repeat {
    attempt <- attempt + 1
    Sys.sleep(API_DELAY_SEC)
    resp <- try(httr::GET(url, query = query, timeout(60)), silent = TRUE)
    if (!inherits(resp, "try-error") && !httr::http_error(resp)) {
      txt <- httr::content(resp, as = "text", encoding = "UTF-8")
      cache_put(key, txt)
      return(txt)
    }
    if (attempt >= RETRY_MAX) {
      stop(paste("http_get_text failed:", if (inherits(resp, "try-error")) as.character(resp) else paste("HTTP", status_code(resp))))
    }
    Sys.sleep(RETRY_BASE * (2^(attempt-1)))
  }
}

# ---------- ESearch / EFetch ----------

#' Search PubMed using ESearch
#'
#' Performs a search on the PubMed database using the NCBI ESearch utility.
#'
#' @param term The search term (string) to be used in the query.
#' @param retmax The maximum number of PMIDs to return.
#' @return A list containing two elements: `ids` (a character vector of
#'   PubMed IDs) and `count` (the total number of hits found).
esearch_pubmed <- function(term, retmax = MAX_PMIDS_TO_FETCH) {
  out <- http_get_json(ESEARCH, list(db = "pubmed", term = term, retmode = "json", retmax = retmax))
  idlist <- out$esearchresult$idlist %||% character(0)
  count  <- as.integer(out$esearchresult$count %||% 0)
  list(ids = idlist, count = count)
}

#' Fetch MEDLINE Records by PubMed IDs
#'
#' Retrieves full MEDLINE records for a given vector of PubMed IDs using the
#' NCBI EFetch utility. It fetches records in chunks and includes a retry
#' mechanism for robustness.
#'
#' @param id_vec A character vector of PubMed IDs.
#' @return A single character string containing the MEDLINE records, separated
#'   by double newlines. Returns an empty string if `id_vec` is empty.
efetch_medline_by_ids <- function(id_vec) {
  if (length(id_vec) == 0) return("")
  ids <- unique(id_vec)
  out_txt <- character(0)
  for (start in seq(1, length(ids), by = EFETCH_CHUNK)) {
    chunk <- ids[start:min(start + EFETCH_CHUNK - 1, length(ids))]
    t0 <- proc.time()[["elapsed"]]
    
    # EXPERT: Retry logic for efetch 500s
    med_attempt <- 1
    txt <- ""
    while (med_attempt <= 2 && !nzchar(txt)) {
      if (med_attempt > 1) {
        log_line("EFETCH retry attempt", med_attempt, "for", length(chunk), "ids")
        Sys.sleep(0.5)
      }
      txt <- try(http_get_text(EFETCH, list(db = "pubmed", id = paste(chunk, collapse=","), rettype = "medline", retmode = "text")), silent = TRUE)
      if (inherits(txt, "try-error")) txt <- ""
      med_attempt <- med_attempt + 1
    }
    
    t1 <- proc.time()[["elapsed"]]
    log_line("EFETCH seconds:", round(t1 - t0, 2), "for", length(chunk), "ids")
    out_txt <- c(out_txt, txt)
  }
  paste(out_txt, collapse = "\n\n")
}

# ---------- MEDLINE parsing ----------

#' Parse MEDLINE Text into a List of Records
#'
#' Splits a raw MEDLINE text block into individual records and then parses
#' each record into a structured list containing key fields.
#'
#' @param medline_txt A single character string containing one or more MEDLINE
#'   records, separated by double newlines.
#' @return A list of lists, where each inner list represents a publication and
#'   contains the fields `PMID`, `AU` (Authors), `FAU` (Full Authors), and
#'   `AD` (Affiliation).
parse_medline_records <- function(medline_txt) {
  if (is.null(medline_txt) || medline_txt == "") return(list())
  recs <- strsplit(medline_txt, "\n\n", fixed = TRUE)[[1]]
  lapply(recs, function(rec) {
    lines <- strsplit(rec, "\n", fixed = TRUE)[[1]]
    if (length(lines) == 0) return(NULL)
    tag <- substr(lines, 1, 4)
    val <- sub("^....\\s*-\\s*", "", lines)
    list(
      PMID = val[tag == "PMID"],
      AU   = val[tag == "AU  "],
      FAU  = val[tag == "FAU "],
      AD   = val[tag == "AD  "]
    )
  })
}

# ---------- EXPERT HELPER FUNCTIONS ----------

#' Normalize a String
#'
#' Converts a string to uppercase, replaces commas and periods with spaces,
#' collapses multiple spaces, and trims whitespace from the ends. Handles NA
#' inputs by converting them to empty strings.
#'
#' @param x A character string to normalize.
#' @return A normalized character string.
normalize_str <- function(x) {
  x <- ifelse(is.na(x), "", x)
  x <- toupper(x)
  x <- gsub("[,\\.]", " ", x)
  x <- gsub("\\s+", " ", x)
  trimws(x)
}

#' Extract First Name from a FAU (Full Author) String
#'
#' Given a full author name string (typically in "LAST, First M" format) and
#' the expected last name, this function extracts the first name token.
#' It is designed to be safe for vector and NA inputs.
#'
#' @param fau_line A single string from a FAU field.
#' @param last_norm The normalized last name to match against.
#' @return The extracted first name as a character string, or `NA_character_`
#'   if the pattern is not matched or inputs are invalid.
.fau_first_from <- function(fau_line, last_norm) {
  if (is.null(fau_line) || length(fau_line) == 0) return(NA_character_)
  x <- toupper(fau_line[1])  # EXPERT: Force single element
  if (!nzchar(x)) return(NA_character_)
  lastU <- toupper(last_norm %||% "")
  if (!nzchar(lastU)) return(NA_character_)

  # EXPERT: Expect "LAST, First Middle" - if pattern not found, return NA
  pat <- paste0("^", lastU, "\\s*,\\s*")
  if (!grepl(pat, x, perl = TRUE)) return(NA_character_)

  x <- sub(pat, "", x, perl = TRUE)          # drop "LAST, "
  x <- sub("\\s+[A-Z]$", "", x, perl = TRUE) # drop one trailing MI
  x <- sub("\\s+[A-Z]$", "", x, perl = TRUE) # drop second trailing MI if present
  toks <- strsplit(x, "\\s+", perl = TRUE)[[1]]
  if (length(toks) == 0) return(NA_character_)
  toks[1]
}

#' Score an Affiliation String
#'
#' Calculates a score from 0 to 1 based on the presence of city, state, or
#' organization tokens within a vector of affiliation strings.
#'
#' @param ad_vec A character vector of affiliation strings for a single publication.
#' @param city_token The city to search for.
#' @param state_token The state to search for.
#' @param org_token The organization to search for.
#' @return A numeric score: 1.0 if both city and state match, 0.5 if one of
#'   them matches or the organization matches, and 0.0 otherwise.
.affil_score <- function(ad_vec, city_token, state_token, org_token) {
  if (length(ad_vec) == 0) return(0)
  ad_u <- toupper(ad_vec)
  ct <- toupper(city_token); st <- toupper(state_token); ot <- toupper(org_token)
  has_city  <- nzchar(ct) && any(grepl(ct,  ad_u, fixed = TRUE))
  has_state <- nzchar(st) && any(grepl(st, ad_u, fixed = TRUE))
  if (has_city && has_state) return(1.0)
  if (has_city || has_state) return(0.5)
  if (nzchar(ot) && any(grepl(ot, ad_u, fixed = TRUE))) return(0.5)
  0.0
}

#' Build Modal First Name from FAU Records
#'
#' Analyzes a list of full author names (FAU) from multiple publications to
#' determine the most common (modal) first name associated with a given last
#' name, filtering by affiliation strength.
#'
#' @param all_fau A character vector of all FAU strings from the search results.
#' @param all_ad A list of character vectors, where each element corresponds to
#'   the affiliation strings for a publication.
#' @param last_norm The normalized last name of the target author.
#' @param city_token The target city.
#' @param state_token The target state.
#' @param org_token The target organization.
#' @return The most frequent first name as a character string, or `NA_character_`
#'   if no suitable first name can be determined.
build_modal_fau_first <- function(all_fau, all_ad, last_norm, city_token, state_token, org_token) {
  if (length(all_fau) == 0) return(NA_character_)
  K <- min(60L, length(all_fau))
  all_fau <- all_fau[seq_len(K)]
  all_ad  <- if (length(all_ad) >= K) all_ad[seq_len(K)] else all_ad
  keep <- logical(length(all_fau))
  for (i in seq_along(all_fau)) {
    ad_i <- if (length(all_ad) >= i) all_ad[[i]] else all_ad
    keep[i] <- .affil_score(ad_i, city_token, state_token, org_token) >= 0.5 &&
      startsWith(toupper(all_fau[i]), paste0(toupper(last_norm), ","))
  }
  f <- lapply(which(keep), function(i) .fau_first_from(all_fau[i], last_norm))
  f <- toupper(unlist(f))
  f <- f[nzchar(f) & !is.na(f)]
  if (length(f) == 0) return(NA_character_)
  tab <- sort(table(f), decreasing = TRUE)
  names(tab)[1]
}

#' Calculate Verification Score for a Publication Record
#'
#' Computes a composite score for a single publication to determine how likely
#' it is to belong to the target author. The score is a weighted average of
#' the name similarity score and the affiliation score.
#'
#' @param fau_vec A character vector of full author names from the publication.
#' @param ad_vec A character vector of affiliation strings from the publication.
#' @param last_norm The normalized last name of the target author.
#' @param target_first The normalized first name of the target author.
#' @param modal_first The modal first name derived from all search results.
#' @param city_token The target city.
#' @param state_token The target state.
#' @param org_token The target organization.
#' @return A list containing the `total` score, the `fau_score`, and the
#'   `aff_score`.
verify_record_score <- function(fau_vec, ad_vec,
                                last_norm, target_first, modal_first,
                                city_token, state_token, org_token) {
  # --- FAU name score (0..1) with vector safety ---
  fau_score <- 0
  if (length(fau_vec) > 0 && nzchar(last_norm %||% "")) {
    same_last <- startsWith(toupper(fau_vec), paste0(toupper(last_norm), ","))
    fau_same_last <- fau_vec[same_last]

    if (length(fau_same_last) > 0) {
      fnames <- vapply(fau_same_last, .fau_first_from, character(1), last_norm = last_norm)
      fnames <- toupper(fnames[!is.na(fnames) & fnames != ""])
      if (length(fnames) > 0) {
        tgt <- toupper(target_first %||% "")
        mod <- toupper(modal_first  %||% "")

        # EXPERT: first-initial constraint to avoid "Jing" vs "Jennifer"
        initials <- substr(fnames, 1, 1)

        sim_tgt <- 0
        if (nzchar(tgt)) {
          mask_tgt <- initials == substr(tgt, 1, 1)
          if (any(mask_tgt)) {
            sim_tgt <- max(stringdist::stringsim(tgt, fnames[mask_tgt], method = "jw", p = 0.1))
          }
        }

        sim_mod <- 0
        if (nzchar(mod)) {
          mask_mod <- initials == substr(mod, 1, 1)
          if (any(mask_mod)) {
            sim_mod <- max(stringdist::stringsim(mod, fnames[mask_mod], method = "jw", p = 0.1))
          }
        }

        fau_score <- max(sim_tgt, sim_mod, 0)
      }
    }
  }

  # --- Affiliation score ---
  aff <- .affil_score(ad_vec, city_token, state_token, org_token)

  # EXPERT: Adjusted weighting for better balance
  total <- 0.55 * fau_score + 0.40 * aff + 0.05 * 0
  list(total = total, fau_score = fau_score, aff_score = aff)
}

# ==== [EXPERT PATCH B] Optimized FAU Gate with graduated thresholds ====
.VERY_COMMON_LAST <- c("WANG","KIM","LEE","SMITH","PATEL","KHAN","WU","BROWN","GARCIA","RODRIGUEZ",
                       "JOHNSON","WILLIAMS","JONES","MILLER","DAVIS","WILSON","MOORE","TAYLOR","ANDERSON","THOMAS")

#' Calculate Jaro-Winkler String Similarity
#'
#' A wrapper around `stringdist::stringsim` to compute the Jaro-Winkler
#' similarity between two strings. Returns 0 if either string is NA or empty.
#'
#' @param a The first character string.
#' @param b The second character string.
#' @return The Jaro-Winkler similarity score, a numeric value between 0 and 1.
.jw <- function(a, b) {
  if (is.na(a) || is.na(b) || !nzchar(a) || !nzchar(b)) return(0)
  stringdist::stringsim(a, b, method = "jw", p = 0.1)
}

#' Find the Best FAU Match for a Target Name
#'
#' Searches through a vector of FAU strings to find the best match for a given
#' last and first name. It constrains the search to entries with the same
#' first initial.
#'
#' @param fau_vec A character vector of FAU strings.
#' @param last_norm The normalized last name of the target author.
#' @param target_first The normalized first name of the target author.
#' @return A list containing `first` (the best matching first name found),
#'   `sim` (the similarity score of the best match), and `matched` (a boolean
#'   indicating if a plausible match was found).
.best_fau_match <- function(fau_vec, last_norm, target_first) {
  if (length(fau_vec) == 0) return(list(first=NA_character_, sim=0, matched=FALSE))
  lastU  <- toupper(last_norm %||% "")
  tfirst <- toupper(target_first %||% "")
  if (!nzchar(lastU) || !nzchar(tfirst)) return(list(first=NA_character_, sim=0, matched=FALSE))
  cand <- fau_vec[startsWith(toupper(fau_vec), paste0(lastU, ","))]
  if (length(cand) == 0) return(list(first=NA_character_, sim=0, matched=FALSE))

  first_tokens <- vapply(cand, .fau_first_from, character(1), last_norm=last_norm)
  first_tokens <- toupper(first_tokens)
  first_tokens <- first_tokens[nzchar(first_tokens) & !is.na(first_tokens)]

  if (length(first_tokens) == 0) return(list(first=NA_character_, sim=0, matched=FALSE))

  # EXPERT: Vector-safe same initial check
  same_init <- sapply(first_tokens, function(x) {
    if (is.na(x) || !nzchar(x)) return(FALSE)
    substr(x, 1, 1) == substr(tfirst, 1, 1)
  })
  first_tokens <- first_tokens[same_init & !is.na(same_init)]
  if (length(first_tokens) == 0) return(list(first=NA_character_, sim=0, matched=FALSE))

  sims <- vapply(first_tokens, function(f) .jw(tfirst, f), numeric(1))
  i <- which.max(sims)
  list(first = first_tokens[i], sim = sims[i], matched = TRUE)
}

#' Determine if a Publication Passes the "FAU Gate"
#'
#' This function acts as a pre-filter. It checks if any author in the FAU list
#' of a publication is a plausible match for the target author. It uses
#' graduated Jaro-Winkler similarity thresholds that depend on the query source,
#' surname risk, and affiliation context.
#'
#' @param fau_vec A character vector of FAU strings from a publication.
#' @param last_norm The normalized last name of the target author.
#' @param first_tok The first name token of the target author.
#' @param source The source of the query ("AU" or "FAU").
#' @param risk_last The risk category of the surname ("STD" or "HIGH").
#' @param tier_context The context of the query tier (e.g., "AU_WITH_AFFIL").
#' @return A boolean value: `TRUE` if the publication passes the gate, `FALSE` otherwise.
.fau_gate_pass <- function(fau_vec, last_norm, first_tok, source, risk_last, tier_context = "AU") {
  lastU  <- toupper(last_norm %||% "")
  firstU <- toupper(first_tok %||% "")
  if (!nzchar(lastU) || !nzchar(firstU)) return(FALSE)

  # EXPERT: For FAU-sourced queries - relaxed threshold
  if (identical(source, "FAU")) {
    bf <- .best_fau_match(fau_vec, last_norm, first_tok)
    return(bf$matched && bf$sim >= 0.82)  # More forgiving for FAU
  }

  # EXPERT: AU-sourced with context-aware thresholds
  bf <- .best_fau_match(fau_vec, last_norm, first_tok)
  if (!bf$matched) return(FALSE)

  very_common <- lastU %in% .VERY_COMMON_LAST || identical(risk_last, "HIGH")
  
  # EXPERT: Graduated thresholds based on context
  if (identical(tier_context, "AU_WITH_STRONG_AFFIL")) {
    # Strong affiliation context - most lenient
    thr <- if (very_common) 0.88 else 0.85
  } else if (identical(tier_context, "AU_WITH_AFFIL")) {
    # Regular affiliation context - moderate
    thr <- if (very_common) 0.90 else 0.87
  } else {
    # No affiliation context - strict
    thr <- if (very_common) 0.92 else 0.90
  }
  
  bf$sim >= thr
}

# ---------- EXPERT AUTHOR MATCHING ----------

#' Build a Set of Allowed Author Name Variants
#'
#' Generates a set of normalized author name variants based on the last name,
#' first name token, and middle name token. This set is used for quick matching
#' against author lists.
#'
#' @param last_norm The normalized last name.
#' @param first_tok The first name token.
#' @param middle_tok The middle name token.
#' @return A character vector of unique, normalized author name variants.
build_allowed_author_set <- function(last_norm, first_tok, middle_tok) {
  last_norm  <- normalize_str(ifelse(is.na(last_norm),  "", last_norm))
  first_tok  <- normalize_str(ifelse(is.na(first_tok),  "", first_tok))
  middle_tok <- normalize_str(ifelse(is.na(middle_tok), "", middle_tok))
  fi <- if (nzchar(first_tok)) substr(first_tok, 1, 1) else ""
  mi <- if (nzchar(middle_tok)) substr(middle_tok, 1, 1) else ""
  cand <- c(
    paste(last_norm, first_tok),
    paste(last_norm, fi),
    paste(last_norm, fi, mi),
    paste(last_norm, paste0(fi, mi))
  )
  allowed <- unique(normalize_str(cand))
  allowed[nzchar(allowed)]
}

#' Check if an Author Match is OK
#'
#' Determines if any author in the AU or FAU lists of a publication matches
#' the allowed set of name variants for the target author.
#'
#' @param au_vec Character vector of AU (author) strings.
#' @param fau_vec Character vector of FAU (full author) strings.
#' @param allowed_norm A character vector of allowed normalized name variants,
#'   generated by `build_allowed_author_set`.
#' @param last_norm Optional normalized last name for a starts-with check.
#' @param first_tok Optional first name token for a starts-with check.
#' @return `TRUE` if a match is found, `FALSE` otherwise.
author_match_ok <- function(au_vec, fau_vec, allowed_norm, last_norm = NULL, first_tok = NULL) {
  au_norm  <- normalize_str(au_vec)
  fau_norm <- normalize_str(fau_vec)
  if (any(au_norm %in% allowed_norm) || any(fau_norm %in% allowed_norm)) return(TRUE)
  if (!is.null(last_norm) && !is.null(first_tok) && nzchar(last_norm) && nzchar(first_tok)) {
    base <- normalize_str(paste(last_norm, first_tok))
    if (any(startsWith(fau_norm, paste0(base, " ")))) return(TRUE)
  }
  FALSE
}

#' Check for First and Last Author Hits
#'
#' Checks if the target author appears as the first or last author in a
#' publication's author list.
#'
#' @param au_vec A character vector of AU (author) strings.
#' @param allowed_norm A character vector of allowed normalized name variants.
#' @return A named logical vector with elements `first` and `last`, indicating
#'   if the target author was found in those positions.
first_last_hits <- function(au_vec, allowed_norm) {
  au_norm <- normalize_str(au_vec)
  au_norm <- au_norm[au_norm != ""]
  if (length(au_norm) == 0) return(c(first = FALSE, last = FALSE))
  c(first = au_norm[1] %in% allowed_norm, last = tail(au_norm, 1) %in% allowed_norm)
}

# ---------- AFFILIATION HELPERS ----------
GENERIC_ORG_WORDS <- c("MEDICAL","GROUP","CENTER","CLINIC","GENERAL","HOSPITAL",
                       "HEALTH","PRACTICE","ASSOCIATES","PARTNERS","DEPT","DEPARTMENT",
                       "LLC","INC","PC","PLLC")

#' Check if an Organization Name is Generic
#'
#' Determines if an organization name consists only of common, non-specific
#' terms (e.g., "MEDICAL GROUP").
#'
#' @param org_chr The organization name as a character string.
#' @return `TRUE` if the name is considered generic, `FALSE` otherwise.
is_generic_org <- function(org_chr) {
  if (is.null(org_chr) || is.na(org_chr) || org_chr == "") return(TRUE)
  toks <- strsplit(org_chr, "\\s+")[[1]]
  all(toks %in% GENERIC_ORG_WORDS) && length(toks) <= 2
}

#' Rebuild an Affiliation Block for a PubMed Query
#'
#' Constructs a PubMed query string for affiliation based on city, state, and
#' organization tokens. Generic organization names are excluded.
#'
#' @param city_token The city token.
#' @param state_token The state token.
#' @param org_token The organization token.
#' @return A character string formatted for use in a PubMed query, e.g.,
#'   `("STANFORD"[Affiliation] OR "CA"[Affiliation])`. Returns an empty string
#'   if no valid tokens are provided.
rebuild_affil_block <- function(city_token, state_token, org_token) {
  parts <- character(0)
  if (nzchar(city_token))  parts <- c(parts, sprintf('"%s"[Affiliation]', city_token))
  if (nzchar(state_token)) parts <- c(parts, sprintf('"%s"[Affiliation]', state_token))
  if (nzchar(org_token) && !is_generic_org(org_token)) parts <- c(parts, sprintf('"%s"[Affiliation]', org_token))
  if (length(parts) == 0) return("")
  paste0("(", paste(parts, collapse = " OR "), ")")
}

#' Create a Vector of Affiliation Tokens
#'
#' Consolidates city, state, and non-generic organization tokens into a single
#' uppercase character vector for matching.
#'
#' @param city_token The city token.
#' @param state_token The state token.
#' @param org_token The organization token.
#' @return A character vector of unique, non-empty, uppercase affiliation tokens.
affil_tokens_from <- function(city_token, state_token, org_token) {
  toks <- unique(toupper(c(city_token, state_token,
                           if (nzchar(org_token) && !is_generic_org(org_token)) org_token else "")))
  toks[nzchar(toks)]
}

#' Check if an Affiliation Match is OK
#'
#' Determines if any of the provided affiliation strings from a publication
#' contain any of the target affiliation tokens.
#'
#' @param ad_vec A character vector of affiliation strings from a publication.
#' @param affil_tokens_upper A character vector of uppercase affiliation tokens
#'   to search for.
#' @return `TRUE` if a match is found, `FALSE` otherwise.
affil_match_ok <- function(ad_vec, affil_tokens_upper) {
  if (length(affil_tokens_upper) == 0) return(FALSE)
  if (length(ad_vec) == 0) return(FALSE)
  ad_u <- toupper(ad_vec)
  any(sapply(ad_u, function(ad) any(sapply(affil_tokens_upper, function(tok) grepl(tok, ad, fixed = TRUE)))))
}

# ---------- DATE HELPERS ----------

#' Parse Year from an Enumeration Date String
#'
#' Extracts the four-digit year from a date string that could be in one of
#' several common formats.
#'
#' @param enum_chr A character string representing a date.
#' @return An integer representing the year, or `NA_integer_` if parsing fails.
.parse_enum_year <- function(enum_chr) {
  if (is.null(enum_chr) || is.na(enum_chr) || !nzchar(enum_chr)) return(NA_integer_)
  for (fmt in c("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d")) {
    d <- try(as.Date(enum_chr, format = fmt), silent = TRUE)
    if (!inherits(d, "try-error") && !is.na(d)) return(as.integer(format(d, "%Y")))
  }
  suppressWarnings({
    d2 <- as.Date(enum_chr)
    if (!is.na(d2)) return(as.integer(format(d2, "%Y")))
  })
  NA_integer_
}

#' Build a Date Filter for a PubMed Query
#'
#' Creates a date range filter for a PubMed query. The range starts a specified
#' number of years before the provider's enumeration date (or the current date
#' if unavailable) and ends at the current date.
#'
#' @param enum_chr The enumeration date string for the provider.
#' @return A character string formatted as a date filter for a PubMed query.
.build_date_filter <- function(enum_chr) {
  ey <- .parse_enum_year(enum_chr)
  start_y <- if (is.na(ey)) DATE_CEILING_YEAR - LOOKBACK_YEARS else max(1900L, ey - LOOKBACK_YEARS)
  end_y   <- DATE_CEILING_YEAR
  sprintf('AND ("%d/01/01"[Date - Publication] : "%d/12/31"[Date - Publication])', start_y, end_y)
}

#' Run the Multi-Tier Query Decision Tree for a Single Author
#'
#' This is the core logic function for a single author (row). It executes a
#' series of queries with decreasing specificity (tiers) to find and verify
#' publications. It starts with highly specific queries (e.g., full name plus
#' affiliation) and falls back to broader queries if no results are found.
#'
#' @param row A data.table row (or list) containing the information for a single
#'   author, including name tokens, affiliation tokens, and risk category.
#' @return A list containing the results of the query, including the tier that
#'   yielded a match, the query term used, publication counts, and lists of
#'   PMIDs for verified, first-author, last-author, and ambiguous publications.
run_decision_tree <- function(row) {
  last_norm  <- toupper(ifelse(is.na(row$last_name_norm), "", row$last_name_norm))
  first_tok  <- toupper(ifelse(is.na(row$first_name_token), "", row$first_name_token))
  middle_tok <- toupper(ifelse(is.na(row$middle_name_token), "", row$middle_name_token))
  risk       <- row$risk_surname
  
  city  <- toupper(ifelse(is.na(row$city_token),  "", row$city_token))
  state <- toupper(ifelse(is.na(row$state_token), "", row$state_token))
  org   <- toupper(ifelse(is.na(row$org_token),   "", row$org_token))
  
  date_filter  <- .build_date_filter(row$EnumerationDate)
  affil_blk    <- rebuild_affil_block(city, state, org)
  has_affil    <- nchar(affil_blk) > 0
  affil_tokens <- affil_tokens_from(city, state, org)
  
  allowed_set  <- build_allowed_author_set(last_norm, first_tok, middle_tok)
  
  strict_fm <- row$name_strict_fm
  relaxed_f <- row$name_relaxed_f
  
  # This is a nested helper function, not exported.
  do_query <- function(name_term, use_affil, source = "AU", tier_context = "AU") {
    row_start <- proc.time()[["elapsed"]]
    term <- if (use_affil && has_affil) paste(name_term, "AND", affil_blk) else name_term
    term <- gsub('"+','"', term, perl=TRUE)
    term <- paste(term, date_filter)
    log_line("ESearch term:", shQuote(term))
    
    s <- esearch_pubmed(term, retmax = MAX_PMIDS_TO_FETCH)
    raw_n  <- s$count
    idlist <- s$ids
    if (length(idlist) == 0) {
      log_diagnostic(row$NPI, "NO_HITS", paste("Term:", substr(term, 1, 100)))
      return(list(term_used=term, raw_n=raw_n,
                  verified_total=0L, verified_first=0L, verified_last=0L,
                  pmids_verified=character(0), pmids_firstauthor=character(0),
                  pmids_lastauthor=character(0), pmids_ambiguous=character(0)))
    }
    
    log_diagnostic(row$NPI, "RAW_HITS", paste("Found", raw_n, "raw hits"))
    
    t0 <- proc.time()[["elapsed"]]
    med <- efetch_medline_by_ids(head(idlist, MAX_PMIDS_TO_FETCH))
    t1 <- proc.time()[["elapsed"]]
    log_line("EFETCH seconds:", round(t1 - t0, 2), "for", length(idlist), "ids")
    
    t2 <- proc.time()[["elapsed"]]
    recs <- parse_medline_records(med)
    t3 <- proc.time()[["elapsed"]]
    log_line("Parse seconds:", round(t3 - t2, 2), "records:", length(recs))
    
    all_fau <- unlist(lapply(recs, function(r) r$FAU %||% character(0)))
    all_ad  <- lapply(recs, function(r) r$AD %||% character(0))
    modal_first <- build_modal_fau_first(all_fau, all_ad, last_norm, city, state, org)
    
    v_total <- v_first <- v_last <- 0L
    pmids_v <- pmids_fa <- pmids_la <- character(0)
    pmids_fail <- character(0)
    gate_fails <- score_fails <- 0L
    
    for (rec in recs) {
      if ((proc.time()[["elapsed"]] - row_start) > ROW_TIME_BUDGET_SEC) {
        log_line("Watchdog: aborting verification early for term due to time budget.")
        break
      }
      if (is.null(rec)) next
      pmid <- ifelse(length(rec$PMID)>0, rec$PMID[1], NA_character_)
      au   <- rec$AU %||% character(0)
      fau  <- rec$FAU %||% character(0)
      ad   <- rec$AD %||% character(0)
      
      # EXPERT: FAU Gate with tier context
      if (!.fau_gate_pass(fau, last_norm, first_tok, source, risk, tier_context)) {
        if (!is.na(pmid)) pmids_fail <- c(pmids_fail, pmid)
        gate_fails <- gate_fails + 1L
        next
      }
      
      sc <- verify_record_score(fau, ad, last_norm, first_tok, modal_first, city, state, org)
      if (!is.na(pmid) && sc$total < SCORE_THRESHOLD) {
        pmids_fail <- c(pmids_fail, pmid)
        score_fails <- score_fails + 1L
        next
      }
      
      if (!is.na(pmid) && sc$total >= SCORE_THRESHOLD) {
        v_total <- v_total + 1L
        pmids_v <- c(pmids_v, pmid)
        fl <- first_last_hits(au, allowed_set)
        if (fl["first"]) { v_first <- v_first + 1L; pmids_fa <- c(pmids_fa, pmid) }
        if (fl["last"])  { v_last  <- v_last  + 1L; pmids_la <- c(pmids_la, pmid) }
      }
      
      pmids_fail <- unique(na.omit(pmids_fail))
      if (length(pmids_fail) > 200) pmids_fail <- pmids_fail[1:200]
    }
    
    # EXPERT: Enhanced diagnostics
    log_diagnostic(row$NPI, "FILTERING", paste("Gate fails:", gate_fails, "Score fails:", score_fails, "Verified:", v_total))
    
    list(term_used=term, raw_n=raw_n,
         verified_total=v_total, verified_first=v_first, verified_last=v_last,
         pmids_verified=unique(na.omit(pmids_v)),
         pmids_firstauthor=unique(na.omit(pmids_fa)),
         pmids_lastauthor=unique(na.omit(pmids_la)),
         pmids_ambiguous=unique(na.omit(pmids_fail)))
  }
  
  # ==== [EXPERT PATCH C] FAU variants with comprehensive coverage ====
  ln <- last_norm; fn <- first_tok
  mi <- if (nzchar(middle_tok)) substr(middle_tok, 1, 1) else ""
  
  fau_variants <- c(
    sprintf('"%s %s"[FAU]', ln, fn),
    sprintf('"%s, %s"[FAU]', ln, fn)
  )
  if (nzchar(mi)) {
    fau_variants <- c(fau_variants,
                     sprintf('"%s %s %s"[FAU]', ln, fn, mi),
                     sprintf('"%s, %s %s"[FAU]', ln, fn, mi))
  }
  fau_variants <- unique(fau_variants)
  
  # EXPERT: Tier 1 - FAU + Affiliation (try all variants)
  for (fau_full in fau_variants) {
    res <- try(do_query(fau_full, use_affil = TRUE, source = "FAU", tier_context = "FAU"), silent = TRUE)
    if (!inherits(res, "try-error") && res$verified_total > 0) {
      log_diagnostic(row$NPI, "TIER1_SUCCESS", paste("FAU variant worked:", fau_full))
      return(c(list(match_tier = 1), res))
    }
  }
  
  # EXPERT: Tier 2 - FAU only (try all variants)
  for (fau_full in fau_variants) {
    res <- try(do_query(fau_full, use_affil = FALSE, source = "FAU", tier_context = "FAU"), silent = TRUE)
    if (!inherits(res, "try-error") && res$verified_total > 0) {
      log_diagnostic(row$NPI, "TIER2_SUCCESS", paste("FAU variant worked:", fau_full))
      return(c(list(match_tier = 2), res))
    }
  }
  
  # EXPERT: Tier 3 - AU + Affiliation with LENIENT gate
  res <- try(do_query(strict_fm, use_affil = TRUE, source = "AU", tier_context = "AU_WITH_AFFIL"), silent = TRUE)
  if (!inherits(res, "try-error") && res$verified_total > 0) {
    log_diagnostic(row$NPI, "TIER3_SUCCESS", "AU + affiliation worked")
    return(c(list(match_tier = 3), res))
  }
  
  res <- try(do_query(relaxed_f, use_affil = TRUE, source = "AU", tier_context = "AU_WITH_AFFIL"), silent = TRUE)
  if (!inherits(res, "try-error") && res$verified_total > 0) {
    log_diagnostic(row$NPI, "TIER3_SUCCESS", "AU relaxed + affiliation worked")
    return(c(list(match_tier = 3), res))
  }
  
  # EXPERT: Tier 3.5 - Strong affiliation recovery
  if (has_affil && nchar(affil_blk) > 20) {
    permissive_au <- sprintf('%s %s[AU]', last_norm, substr(first_tok, 1, 1))
    res <- try(do_query(permissive_au, use_affil = TRUE, source = "AU", tier_context = "AU_WITH_STRONG_AFFIL"), silent = TRUE)
    if (!inherits(res, "try-error") && res$verified_total > 0 && res$verified_total <= 25) {
      log_diagnostic(row$NPI, "TIER3.5_SUCCESS", "Strong affiliation recovery worked")
      return(c(list(match_tier = 3), res))
    }
  }
  
  # EXPERT: Tier 4 - AU without affiliation (strict gate, small caps)
  if (identical(risk, "STD")) {
    res <- try(do_query(strict_fm, use_affil = FALSE, source = "AU", tier_context = "AU_NO_AFFIL"), silent = TRUE)
    if (!inherits(res, "try-error")) {
      if (res$verified_total > 0 && res$verified_total <= 8) {  # Slightly increased cap
        log_diagnostic(row$NPI, "TIER4_SUCCESS", "AU no affiliation worked")
        return(c(list(match_tier = 4), res))
      } else if (res$raw_n > 0) {
        log_diagnostic(row$NPI, "TIER4_FAIL", paste("Too many hits:", res$raw_n, "verified:", res$verified_total))
        return(c(list(match_tier = 4, ambiguous_reason = "TooManyHits_NoAffil"), res))
      }
    }
  }
  
  # EXPERT: Enhanced fallback diagnostics
  log_diagnostic(row$NPI, "ALL_TIERS_FAILED", "No matches found across all tiers")
  
  list(
    match_tier = NA_integer_, term_used = NA_character_, raw_n = 0L,
    verified_total = 0L, verified_first = 0L, verified_last = 0L,
    pmids_verified = character(0), pmids_firstauthor = character(0),
    pmids_lastauthor = character(0),
    ambiguous_reason = "Ambiguous_NoAffil"
  )
}

#' Run the Main PubMed Query and Verification Pipeline
#'
#' This is the main execution function for the script. It reads an input CSV of
#' authors, iterates through each one, and calls `run_decision_tree` to find
#' and verify their publications. It handles resuming from a previous run, saves
#' checkpoints, and writes the final, enriched data to an output CSV. It also
#' logs summary statistics upon completion.
#'
#' @param in_csv The file path for the input CSV file. This file should contain
#'   the authors to be queried.
#' @param out_csv The file path for the output CSV file where the results will
#'   be written.
#' @return Invisibly returns the final `data.table` with all the results.
run_phase2_http_expert <- function(in_csv = IN_CSV, out_csv = OUT_CSV) {
  dt <- fread(in_csv)
  
  init_cols <- c("match_tier","query_used","raw_n_total","verified_n_total",
                 "verified_n_first","verified_n_last","ambiguous_reason",
                 "pmids_verified","pmids_firstauthor","pmids_lastauthor",
                 "pmids_ambiguous")
  for (col in init_cols) if (!col %in% names(dt)) dt[, (col) := NA_character_]
  
  num_cols <- c("match_tier","raw_n_total","verified_n_total","verified_n_first","verified_n_last")
  dt[, (intersect(num_cols, names(dt))) := lapply(.SD, as.integer), .SDcols = intersect(num_cols, names(dt))]
  
  row_idx <- if (RESUME) which(is.na(dt$verified_n_total)) else seq_len(nrow(dt))
  if (length(row_idx) == 0) { log_line("Nothing to do."); fwrite(dt, out_csv); return(invisible(dt)) }
  
  log_line("Starting EXPERT Phase-2 on ", length(row_idx), " rows (of ", nrow(dt), ").")
  
  for (k in seq_along(row_idx)) {
    i <- row_idx[k]
    row <- dt[i]
    log_line("EXPERT Row ", k, "/", length(row_idx), " (NPI=", row$NPI, ")")
    
    ans <- try({
      if (is.na(row$last_name_norm) || !nzchar(row$last_name_norm)) {
        stop("Missing last_name_norm")
      }
      if (is.na(row$first_name_token) || !nzchar(row$first_name_token)) {
        stop("Missing first_name_token") 
      }
      run_decision_tree(row)
    }, silent = TRUE)
    
    if (inherits(ans, "try-error")) {
      error_msg <- as.character(ans)
      log_line("ERROR for NPI", row$NPI, ":", substr(error_msg, 1, 150))
      dt[i, `:=`(
        match_tier        = NA_integer_,
        query_used        = NA_character_,
        raw_n_total       = 0L,
        verified_n_total  = 0L,
        verified_n_first  = 0L,
        verified_n_last   = 0L,
        ambiguous_reason  = paste0("Error_Query: ", substr(error_msg, 1, 200)),
        pmids_verified    = "",
        pmids_firstauthor = "",
        pmids_lastauthor  = "",
        pmids_ambiguous   = ""
      )]
    } else {
      pmids_v   <- paste(unique(ans$pmids_verified),    collapse = ";")
      pmids_fa  <- paste(unique(ans$pmids_firstauthor), collapse = ";")
      pmids_la  <- paste(unique(ans$pmids_lastauthor),  collapse = ";")
      pmids_amb <- paste(unique(ans$pmids_ambiguous),   collapse = ";")
      
      dt[i, `:=`(
        match_tier        = as.integer(ans$match_tier),
        query_used        = ans$term_used,
        raw_n_total       = as.integer(ans$raw_n),
        verified_n_total  = as.integer(ans$verified_total),
        verified_n_first  = as.integer(ans$verified_first),
        verified_n_last   = as.integer(ans$verified_last),
        ambiguous_reason  = ifelse(is.null(ans$ambiguous_reason), NA_character_, ans$ambiguous_reason),
        pmids_verified    = pmids_v,
        pmids_firstauthor = pmids_fa,
        pmids_lastauthor  = pmids_la,
        pmids_ambiguous   = pmids_amb
      )]
    }
    
    if (k %% CHK_EVERY == 0) {
      chk_path <- sub("\\.csv$", paste0("_expert_checkpoint_", row_idx[k], ".csv"), out_csv)
      fwrite(dt, chk_path)
      log_line("EXPERT Checkpoint written: ", chk_path)
    }
  }
  
  fwrite(dt, out_csv)
  log_line("EXPERT DONE. Output written to: ", out_csv)
  
  # EXPERT: Summary statistics
  summary_stats <- dt[, .(
    total_rows = .N,
    with_verified = sum(verified_n_total > 0, na.rm = TRUE),
    tier1_matches = sum(match_tier == 1, na.rm = TRUE),
    tier2_matches = sum(match_tier == 2, na.rm = TRUE),
    tier3_matches = sum(match_tier == 3, na.rm = TRUE),
    tier4_matches = sum(match_tier == 4, na.rm = TRUE),
    errors = sum(grepl("Error_", ambiguous_reason), na.rm = TRUE),
    zero_raw = sum(raw_n_total == 0, na.rm = TRUE),
    zero_verified = sum(verified_n_total == 0, na.rm = TRUE)
  )]
  
  log_line("EXPERT SUMMARY:")
  log_line("Total rows:", summary_stats$total_rows)
  log_line("With verified pubs:", summary_stats$with_verified, "(", round(100*summary_stats$with_verified/summary_stats$total_rows, 1), "%)")
  log_line("Tier distribution - T1:", summary_stats$tier1_matches, "T2:", summary_stats$tier2_matches, "T3:", summary_stats$tier3_matches, "T4:", summary_stats$tier4_matches)
  log_line("Errors:", summary_stats$errors, "Zero raw:", summary_stats$zero_raw, "Zero verified:", summary_stats$zero_verified)
  
  invisible(dt)
}

cat(
  "EXPERT HTTP runner loaded. Enhanced with:\n",
  "- Vector/NA safety fixes\n",
  "- FAU variant matching (comma, middle initial)\n", 
  "- Graduated FAU Gate thresholds\n",
  "- Smart diagnostics and logging\n",
  "- Optimized parameters for recall\n",
  "To execute: run_phase2_http_expert()\n",
  sep = ""
)
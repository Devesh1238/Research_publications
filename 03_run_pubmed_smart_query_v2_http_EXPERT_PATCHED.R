# =====================================================================
# SCRIPT: 03_run_pubmed_smart_query_v2_http.R (PATCHED)
# PURPOSE: Integrates FAU-first patch for improved precision and recall.
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

IN_CSV      <- "C:/Users/deves/Downloads/clinical/stanford/Jeff choi/NPPES_Analysis/outputs/surgeons_pilot_100_for_query.csv"
OUT_CSV     <- "C:/Users/deves/Downloads/clinical/stanford/Jeff choi/NPPES_Analysis/outputs/surgeons_pilot_100_for_query_PUBCOUNT.csv"
CACHE_DIR   <- "outputs/eutils_cache"
LOG_FILE    <- "outputs/phase2_expert_run_log.txt"

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
SCORE_THRESHOLD     <- 0.70 # EXPERT: Relaxed from 0.75 for better recall
ROW_TIME_BUDGET_SEC <- 180   # EXPERT: Increased from 120 for complex cases

BASE_URL <- "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
ESEARCH  <- paste0(BASE_URL, "esearch.fcgi")
EFETCH   <- paste0(BASE_URL, "efetch.fcgi")

if (!dir.exists("outputs")) dir.create("outputs", recursive = TRUE)
if (!dir.exists(CACHE_DIR)) dir.create(CACHE_DIR, recursive = TRUE)

# ---------- EXPERT LOGGING & DIAGNOSTICS ----------
log_line <- function(...) {
  msg <- paste0(format(Sys.time(), "%F %T"), " | ", paste(..., collapse=" "))
  cat(msg, "\n")
  try(write(msg, file = LOG_FILE, append = TRUE), silent = TRUE)
}

# EXPERT: Enhanced diagnostics
log_diagnostic <- function(npi, stage, details) {
  log_line("DIAGNOSTIC [", npi, "] ", stage, ": ", details)
}

.safe_name <- function(x) gsub("[^A-Za-z0-9._-]+", "_", x)
cache_key  <- function(prefix, ...) .safe_name(paste(prefix, digest(list(...), algo="sha1"), sep = "__"))
cache_path <- function(key) file.path(CACHE_DIR, paste0(key, ".rds"))
cache_get  <- function(key) { p <- cache_path(key); if (file.exists(p)) readRDS(p) else NULL }
cache_put  <- function(key, val) saveRDS(val, cache_path(key))

# ---------- HTTP wrappers with enhanced resilience ----------
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
esearch_pubmed <- function(term, retmax = MAX_PMIDS_TO_FETCH) {
  out <- http_get_json(ESEARCH, list(db = "pubmed", term = term, retmode = "json", retmax = retmax))
  idlist <- out$esearchresult$idlist %||% character(0)
  count  <- as.integer(out$esearchresult$count %||% 0)
  list(ids = idlist, count = count)
}

# EXPERT: Enhanced EFetch with retry for 500s
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
# CHANGE S0 — NA-safe helpers (add near other small utils)
safe_nzchar <- function(x) {
  is.character(x) && length(x) == 1L && !is.na(x) && nzchar(x)
}
safe_any <- function(x) {
  isTRUE(any(x, na.rm = TRUE))
}
# CHANGE A0 — add near top-level helpers
.VERY_COMMON_LAST <- c(
  "SMITH","JOHNSON","WILLIAMS","BROWN","JONES","GARCIA","MILLER","DAVIS",
  "RODRIGUEZ","MARTINEZ","HERNANDEZ","LOPEZ","GONZALEZ","WILSON","ANDERSON",
  "TAYLOR","THOMAS","MOORE","JACKSON","MARTIN","LEE","PEREZ","THOMPSON",
  "WHITE","HARRIS","SANCHEZ","CLARK","RAMIREZ","LEWIS","ROBINSON","WALKER",
  "YOUNG","ALLEN","KING","WRIGHT","SCOTT","TORRES","NGUYEN","HILL","FLORES",
  "GREEN","ADAMS","NELSON","BAKER","HALL","RIVERA","CAMPBELL","MITCHELL",
  "CARTER","ROBERTS","KIM","PARK","CHEN","LI","ZHANG","WANG","KAPLAN",
  "SINGH","PATEL"
)

# CHANGE C0 — normalizers used by .affil_score
try({ library(stringi) }, silent = TRUE)
.STATE_MAP <- list(
  "DC"="DISTRICT OF COLUMBIA","NY"="NEW YORK","CA"="CALIFORNIA","TX"="TEXAS",
  "FL"="FLORIDA","PA"="PENNSYLVANIA","IL"="ILLINOIS","OH"="OHIO","GA"="GEORGIA",
  "NC"="NORTH CAROLINA","MI"="MICHIGAN","NJ"="NEW JERSEY","VA"="VIRGINIA",
  "WA"="WASHINGTON","AZ"="ARIZONA","MA"="MASSACHUSETTS","TN"="TENNESSEE",
  "IN"="INDIANA","MO"="MISSOURI","MD"="MARYLAND","WI"="WISCONSIN","CO"="COLORADO",
  "MN"="MINNESOTA","SC"="SOUTH CAROLINA","AL"="ALABAMA","LA"="LOUISIANA",
  "KY"="KENTUCKY","OR"="OREGON","OK"="OKLAHOMA","CT"="CONNECTICUT",
  "UT"="UTAH","IA"="IOWA","NV"="NEVADA","AR"="ARKANSAS","MS"="MISSISSIPPI",
  "KS"="KANSAS","NM"="NEW MEXICO","NE"="NEBRASKA","WV"="WEST VIRGINIA",
  "ID"="IDAHO","HI"="HAWAII","ME"="MAINE","NH"="NEW HAMPSHIRE",
  "MT"="MONTANA","RI"="RHODE ISLAND","DE"="DELAWARE","SD"="SOUTH DAKOTA",
  "ND"="NORTH DAKOTA","AK"="ALASKA","VT"="VERMONT","WY"="WYOMING"
)
.normalize_token <- function(x) {
  x <- toupper(x %||% "")
  if (requireNamespace("stringi", quietly = TRUE)) x <- stringi::stri_trans_general(x, "Latin-ASCII")
  gsub("[^A-Z0-9]+", " ", x)
}
# CHANGE C0.1 — return "" not NA; callers use safe_nzchar()
.token_regex <- function(tok) {
  t <- .normalize_token(tok); t <- trimws(t)
  if (!nzchar(t)) return("")                       # <-- was NA_character_
  if (nchar(t) == 2L && t %in% names(.STATE_MAP)) {
    full <- .STATE_MAP[[t]]
    return(paste0("\\b(", t, "|", gsub(" ", "\\\\s+", full), ")\\b"))
  }
  paste0("\\b", gsub(" ", "\\\\s+", t), "\\b")
}

# CHANGE C2 — add this helper; used by run_decision_tree()
.normalize_name_for_query <- function(x) {
  x <- toupper(x %||% "");
  if (requireNamespace("stringi", quietly = TRUE)) x <- stringi::stri_trans_general(x, "Latin-ASCII")
  x <- gsub("[’'`]+", " ", x); x <- gsub("[-]+", " ", x); trimws(gsub("\\s+", " ", x))
}
# CHANGE V1a — canonicalize last names for comparison (hyphen/space insensitive)
.canon_last <- function(x) {
  x <- toupper(x %||% "")
  x <- gsub("[’'`]+", "", x)
  x <- gsub("[-\\s]+", "", x)   # remove hyphens and spaces
  x
}

# CHANGE V1b — generate particle-aware last-name variants for FAU queries
.last_variants_from <- function(last_norm) {
  base <- .normalize_name_for_query(last_norm)
  if (!nzchar(base)) return(character(0))
  toks <- strsplit(base, "\\s+", perl = TRUE)[[1]]
  v <- character(0)
  
  # always include: spaced, hyphenated, collapsed
  spaced   <- paste(toks, collapse = " ")
  hyphened <- paste(toks, collapse = "-")
  collapsed<- gsub("\\s+", "", spaced)
  
  v <- c(v, spaced, hyphened, collapsed)
  
  # handle common particles (prefix stuck or separated)
  particles <- c("AL","EL","DE","DEL","DI","DA","LA","LE","VAN","VON","MC","MAC")
  # Case A: multi-token last (e.g., EL NAJJAR) -> add collapsed + hyphen (already), also keep spaced (already)
  # Case B: single-token but starts with particle stuck to root (e.g., VANOLST, ELNAJJAR, MCDONALD)
  if (length(toks) == 1L) {
    one <- toks[1]
    for (p in particles) {
      if (startsWith(one, p) && nchar(one) > nchar(p) + 1) {
        root <- substr(one, nchar(p) + 1, nchar(one))
        # insert space and hyphen between particle and root
        v <- c(v, paste(p, root), paste(p, root, sep = "-"))
      }
    }
  }
  
  unique(v[nzchar(v)])
}

# CHANGE V1c — REPLACE your current build_fau_variants with this
build_fau_variants <- function(last_norm, first_tok, middle_tok) {
  ln <- .normalize_name_for_query(last_norm)
  fn <- .normalize_name_for_query(first_tok)
  mi <- .normalize_name_for_query(middle_tok)
  mi1 <- if (nzchar(mi)) substr(mi,1,1) else ""
  
  lasts <- .last_variants_from(ln)
  if (!length(lasts)) lasts <- ln
  
  out <- character(0)
  for (L in unique(lasts)) {
    out <- c(out,
             sprintf('"%s %s"[FAU]', L, fn),
             sprintf('"%s, %s"[FAU]', L, fn)
    )
    if (nzchar(mi1)) {
      out <- c(out,
               sprintf('"%s %s %s"[FAU]', L, fn, mi1),
               sprintf('"%s, %s %s"[FAU]', L, fn, mi1)
      )
    }
  }
  unique(out)
}

normalize_str <- function(x) {
  x <- ifelse(is.na(x), "", x)
  x <- toupper(x)
  x <- gsub("[,\\.]", " ", x)
  x <- gsub("\\s+", " ", x)
  trimws(x)
}
# CHANGE A1 — REPLACE .fau_first_from to compare canonical last names
.fau_first_from <- function(fau_line, last_norm) {
  if (is.null(fau_line) || length(fau_line) == 0) return(NA_character_)
  x <- toupper(fau_line[1])
  if (!nzchar(x)) return(NA_character_)
  parts <- strsplit(x, ",", fixed = TRUE)[[1]]
  if (!length(parts)) return(NA_character_)
  last_fau <- trimws(parts[1])
  if (.canon_last(last_fau) != .canon_last(last_norm %||% "")) return(NA_character_)
  right <- if (length(parts) >= 2) trimws(parts[2]) else ""
  # drop trailing single-letter initials (once or twice)
  right <- sub("\\s+[A-Z]$", "", right, perl = TRUE)
  right <- sub("\\s+[A-Z]$", "", right, perl = TRUE)
  toks <- strsplit(right, "\\s+", perl = TRUE)[[1]]
  if (!length(toks)) return(NA_character_)
  toks[1]
}
# CHANGE A1.5 — REPLACE .best_fau_match to use canonical last filter + NA safety
.best_fau_match <- function(fau_vec, last_norm, first_tok) {
  lastU  <- toupper(last_norm %||% "")
  firstU <- toupper(first_tok %||% "")
  if (!length(fau_vec) || !nzchar(lastU) || !nzchar(firstU)) {
    return(list(matched = FALSE, sim = 0))
  }
  # keep FAUs whose canonical last equals target canonical last
  keep <- vapply(strsplit(toupper(fau_vec), ",", fixed = TRUE),
                 function(p) .canon_last(trimws(p[1])) == .canon_last(lastU),
                 logical(1))
  cand <- fau_vec[keep]
  if (!length(cand)) return(list(matched = FALSE, sim = 0))
  
  fn <- vapply(cand, .fau_first_from, character(1), last_norm = lastU)
  fn <- toupper(fn[!is.na(fn) & fn != ""])
  if (!length(fn)) return(list(matched = FALSE, sim = 0))
  
  # same initial gate, then JW similarity
  same_init <- substr(fn, 1, 1) == substr(firstU, 1, 1)
  if (!safe_any(same_init)) return(list(matched = FALSE, sim = 0))
  
  sim <- max(stringdist::stringsim(firstU, fn[same_init], method = "jw", p = 0.1), na.rm = TRUE)
  if (!is.finite(sim)) sim <- 0
  list(matched = TRUE, sim = sim)
}
# CHANGE B — REPLACE .fau_gate_pass with affiliation-aware thresholds for AU
.fau_gate_pass <- function(fau_vec, last_norm, first_tok,
                           source = c("AU","FAU"),
                           risk_last = "STD",
                           ad_vec = NULL, city_token = NULL, state_token = NULL, org_token = NULL) {
  source <- match.arg(source)
  lastU  <- toupper(last_norm %||% "")
  firstU <- toupper(first_tok %||% "")
  if (!nzchar(lastU) || !nzchar(firstU)) return(FALSE)
  
  bf <- .best_fau_match(fau_vec, lastU, firstU)
  if (!bf$matched) return(FALSE)
  
  # base thresholds (tightened to avoid false positives)
  if (identical(source, "FAU")) return(bf$sim >= 0.96)
  very_common <- (toupper(lastU) %in% .VERY_COMMON_LAST) || identical(risk_last, "HIGH")
  base_thr <- if (very_common) 0.97 else 0.95

  # affiliation-aware easing (only for AU)
  aff <- 0
  if (length(ad_vec)) aff <- .affil_score(ad_vec, toupper(city_token %||% ""), toupper(state_token %||% ""), toupper(org_token %||% ""))

  if (aff >= 1.0) {
    return(bf$sim >= (base_thr - 0.02))  # strong match: -0.02
  } else if (aff >= 0.5) {
    return(bf$sim >= (base_thr - 0.01))  # partial signal: -0.01
  } else {
    return(bf$sim >= base_thr)
  }
}

# CHANGE C1-fix — NA-safe affiliation scoring
.affil_score <- function(ad_vec, city_token, state_token, org_token) {
  if (length(ad_vec) == 0) return(0)
  ads <- toupper(ad_vec)
  if (requireNamespace("stringi", quietly = TRUE)) {
    ads <- stringi::stri_trans_general(ads, "Latin-ASCII")
  }
  re_city  <- .token_regex(city_token)
  re_state <- .token_regex(state_token)
  re_org   <- if (nzchar(org_token) && !is_generic_org(org_token)) .token_regex(org_token) else ""
  
  has_city  <- safe_nzchar(re_city)  && safe_any(grepl(re_city,  ads, perl = TRUE))
  has_state <- safe_nzchar(re_state) && safe_any(grepl(re_state, ads, perl = TRUE))
  has_org   <- safe_nzchar(re_org)   && safe_any(grepl(re_org,   ads, perl = TRUE))
  
  if (has_city && has_state) return(1.0)
  if (has_city || has_state || has_org) return(0.5)
  0.0
}

# EXPERT: Modal first name builder
build_modal_fau_first <- function(all_fau, all_ad, last_norm, city_token, state_token, org_token) {
  if (length(all_fau) == 0) return(NA_character_)
  K <- min(60L, length(all_fau))
  all_fau <- all_fau[seq_len(K)]
  all_ad  <- if (length(all_ad) >= K) all_ad[seq_len(K)] else all_ad
  keep <- logical(length(all_fau))
  for (i in seq_along(all_fau)) {
    ad_i <- if (length(all_ad) >= i) all_ad[[i]] else all_ad
    keep[i] <- isTRUE(.affil_score(ad_i, city_token, state_token, org_token) >= 0.5) &&
      startsWith(toupper(all_fau[i]), paste0(toupper(last_norm), ","))
  }
  f <- lapply(which(keep), function(i) .fau_first_from(all_fau[i], last_norm))
  f <- toupper(unlist(f))
  f <- f[nzchar(f) & !is.na(f)]
  if (length(f) == 0) return(NA_character_)
  tab <- sort(table(f), decreasing = TRUE)
  names(tab)[1]
}
# CHANGE A2-fix — NA-safe similarity masks
verify_record_score <- function(fau_vec, ad_vec,
                                last_norm, target_first, modal_first,
                                city_token, state_token, org_token,
                                mode = c("default","fau_only")) {
  mode <- match.arg(mode)

  # ---- compute FAU-only similarity (NA-safe) ----
  fau_score <- 0
  if (length(fau_vec) > 0 && nzchar(last_norm %||% "")) {
    same_last <- startsWith(toupper(fau_vec), paste0(toupper(last_norm), ","))
    fau_same_last <- fau_vec[which(same_last)]
    if (length(fau_same_last) > 0) {
      fnames <- vapply(fau_same_last, .fau_first_from, character(1), last_norm = last_norm)
      fnames <- toupper(fnames[!is.na(fnames) & fnames != ""])
      if (length(fnames) > 0) {
        tgt <- toupper(target_first %||% "")
        mod <- toupper(modal_first  %||% "")
        initials <- substr(fnames, 1, 1)

        sim_tgt <- 0
        if (nzchar(tgt)) {
          mask_tgt <- initials == substr(tgt, 1, 1)
          if (safe_any(mask_tgt)) {
            sim_tgt <- max(stringdist::stringsim(tgt, fnames[mask_tgt], method = "jw", p = 0.1), na.rm = TRUE)
            if (!is.finite(sim_tgt)) sim_tgt <- 0
          }
        }

        sim_mod <- 0
        if (nzchar(mod)) {
          mask_mod <- initials == substr(mod, 1, 1)
          if (safe_any(mask_mod)) {
            sim_mod <- max(stringdist::stringsim(mod, fnames[mask_mod], method = "jw", p = 0.1), na.rm = TRUE)
            if (!is.finite(sim_mod)) sim_mod <- 0
          }
        }

        fau_score <- max(sim_tgt, sim_mod, 0, na.rm = TRUE)
        if (!is.finite(fau_score)) fau_score <- 0
      }
    }
  }

  # ---- affiliation evidence ----
  aff <- .affil_score(ad_vec, city_token, state_token, org_token)

  # ---- dynamic fusion + threshold ----
  if (mode == "fau_only") {
    total <- fau_score
    thr   <- 0.78
  } else {
    if (aff >= 0.5) {
      total <- 0.65 * fau_score + 0.35 * aff
      thr   <- 0.72
    } else {
      total <- fau_score
      thr   <- 0.80
    }
  }

  list(total = total, fau_score = fau_score, aff_score = aff, threshold = thr)
}
# ---------- EXPERT AUTHOR MATCHING ----------
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

is_generic_org <- function(org_chr) {
  if (is.null(org_chr) || is.na(org_chr) || org_chr == "") return(TRUE)
  toks <- strsplit(org_chr, "\\s+")[[1]]
  all(toks %in% GENERIC_ORG_WORDS) && length(toks) <= 2
}

rebuild_affil_block <- function(city_token, state_token, org_token) {
  parts <- character(0)
  if (nzchar(city_token))  parts <- c(parts, sprintf('"%s"[Affiliation]', city_token))
  if (nzchar(state_token)) parts <- c(parts, sprintf('"%s"[Affiliation]', state_token))
  if (nzchar(org_token) && !is_generic_org(org_token)) parts <- c(parts, sprintf('"%s"[Affiliation]', org_token))
  if (length(parts) == 0) return("")
  paste0("(", paste(parts, collapse = " OR "), ")")
}

affil_tokens_from <- function(city_token, state_token, org_token) {
  toks <- unique(toupper(c(city_token, state_token,
                           if (nzchar(org_token) && !is_generic_org(org_token)) org_token else "")))
  toks[nzchar(toks)]
}

affil_match_ok <- function(ad_vec, affil_tokens_upper) {
  if (length(affil_tokens_upper) == 0) return(FALSE)
  if (length(ad_vec) == 0) return(FALSE)
  ad_u <- toupper(ad_vec)
  any(sapply(ad_u, function(ad) any(sapply(affil_tokens_upper, function(tok) grepl(tok, ad, fixed = TRUE)))))
}

# ---------- DATE HELPERS ----------
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

.build_date_filter <- function(enum_chr) {
  ey <- .parse_enum_year(enum_chr)
  start_y <- if (is.na(ey)) DATE_CEILING_YEAR - LOOKBACK_YEARS else max(1900L, ey - LOOKBACK_YEARS)
  end_y   <- DATE_CEILING_YEAR
  sprintf('AND ("%d/01/01"[Date - Publication] : "%d/12/31"[Date - Publication])', start_y, end_y)
}
# --------- F.1: Helpers (add once, below your ESearch/EFetch helpers) ---------

# Safe-null infix, in case not defined elsewhere
`%||%` <- function(x, y) if (is.null(x) || length(x) == 0) y else x

# Basic logger passthrough (reuse yours if you already have one)
log_line <- function(...) { cat(paste0("[pubmed] ", paste(..., collapse=" "), "\n")) }

# Map of USPS -> full state names (minimal to fix OR/AND collisions; extend as needed)
.STATE_MAP <- c(
  "AL"="Alabama","AK"="Alaska","AZ"="Arizona","AR"="Arkansas","CA"="California",
  "CO"="Colorado","CT"="Connecticut","DE"="Delaware","FL"="Florida","GA"="Georgia",
  "HI"="Hawaii","ID"="Idaho","IL"="Illinois","IN"="Indiana","IA"="Iowa","KS"="Kansas",
  "KY"="Kentucky","LA"="Louisiana","ME"="Maine","MD"="Maryland","MA"="Massachusetts",
  "MI"="Michigan","MN"="Minnesota","MS"="Mississippi","MO"="Missouri","MT"="Montana",
  "NE"="Nebraska","NV"="Nevada","NH"="New Hampshire","NJ"="New Jersey","NM"="New Mexico",
  "NY"="New York","NC"="North Carolina","ND"="North Dakota","OH"="Ohio","OK"="Oklahoma",
  "OR"="Oregon","PA"="Pennsylvania","RI"="Rhode Island","SC"="South Carolina",
  "SD"="South Dakota","TN"="Tennessee","TX"="Texas","UT"="Utah","VT"="Vermont",
  "VA"="Virginia","WA"="Washington","WV"="West Virginia","WI"="Wisconsin","WY"="Wyoming",
  "DC"="District of Columbia"
)

.is_reserved_boolean <- function(x) {
  t <- toupper(x %||% "")
  t %in% c("OR","AND","NOT")
}

.state_full_or_self <- function(st) {
  stU <- toupper(st %||% "")
  if (nchar(stU) == 2L && stU %in% names(.STATE_MAP)) return(.STATE_MAP[[stU]])
  st
}

# Treat obviously generic “org” tokens as non-useful in Affiliation literals (tune as needed)
is_generic_org <- function(org_raw) {
  if (is.null(org_raw) || !nzchar(org_raw)) return(TRUE)
  o <- tolower(org_raw)
  any(grepl("\\b(hospital|clinic|medical center|health|healthcare|university|college|dept|department)\\b", o))
}

# Build a sanitized affiliation filter (city OR full-state OR org), stripping boolean collisions
build_affil_block_sanitized <- function(city_token, state_token, org_token) {
  city <- toupper(city_token %||% "")
  state_full <- toupper(.state_full_or_self(state_token) %||% "")
  org_raw <- toupper(org_token %||% "")
  org <- if (!is_generic_org(org_raw) && !.is_reserved_boolean(org_raw)) org_raw else ""
  
  parts <- character(0)
  if (nzchar(city)       && !.is_reserved_boolean(city))       parts <- c(parts, sprintf('"%s"[Affiliation]', city))
  if (nzchar(state_full) && !.is_reserved_boolean(state_full)) parts <- c(parts, sprintf('"%s"[Affiliation]', state_full))
  if (nzchar(org)        && !.is_reserved_boolean(org))        parts <- c(parts, sprintf('"%s"[Affiliation]', org))
  if (!length(parts)) return("")
  paste0("(", paste(parts, collapse = " OR "), ")")
}

build_affil_variants <- function(city_token, state_token, org_token) {
  state_full <- .state_full_or_self(state_token)
  unique(c(
    build_affil_block_sanitized(city_token, state_full, org_token), # city OR state_full OR org
    build_affil_block_sanitized(city_token, state_full, ""),        # city OR state_full
    build_affil_block_sanitized("",          state_full, ""),        # state_full only
    build_affil_block_sanitized(city_token, "",         ""),         # city only
    ""                                                               # no affiliation
  ))
}

# Your existing constants used here (adjust names if yours differ)
DATE_CEILING_YEAR <- getOption("pubmed.date_ceiling_year", 2025L)
LOOKBACK_YEARS    <- getOption("pubmed.lookback_years",    35L)
MAX_PMIDS_TO_FETCH<- getOption("pubmed.max_pmids",         300L)

# If you already have this; otherwise define a simple “exact window” builder
.build_date_filter <- function(enum_chr = NA_character_) {
  # Keep your original behavior if you already had one; this simple version matches your CSV window
  sprintf('AND ("%d/01/01"[Date - Publication] : "%d/12/31"[Date - Publication])',
          DATE_CEILING_YEAR - LOOKBACK_YEARS, DATE_CEILING_YEAR)
}

.build_date_filter_from_year <- function(start_year, end_year = DATE_CEILING_YEAR) {
  start_year <- max(1900L, as.integer(start_year %||% (DATE_CEILING_YEAR - LOOKBACK_YEARS)))
  end_year   <- max(start_year, as.integer(end_year))
  sprintf('AND ("%d/01/01"[Date - Publication] : "%d/12/31"[Date - Publication])', start_year, end_year)
}

date_backoff_ladder <- function(enum_chr) {
  c(
    .build_date_filter(enum_chr),
    .build_date_filter_from_year(DATE_CEILING_YEAR - max(LOOKBACK_YEARS + 10L, 35L)),
    .build_date_filter_from_year(DATE_CEILING_YEAR - 50L),
    .build_date_filter_from_year(1950L),
    .build_date_filter_from_year(1900L)
  )
}

# ESearch backoff: try affil variants + date widening; final attempt flips AU<->FAU for recall
esearch_pubmed_backoff <- function(name_term,
                                   use_affil,
                                   city_token, state_token, org_token,
                                   date_filter, source_field,
                                   retmax = MAX_PMIDS_TO_FETCH) {
  
  affil_opts <- if (isTRUE(use_affil)) build_affil_variants(city_token, state_token, org_token) else c("")
  date_opts  <- unique(c(date_filter, date_backoff_ladder(NA_character_)))
  
  # attempt order: (affil variants) x (date ladder) with original field, then final field flip last
  attempt_specs <- list()
  for (aff in affil_opts) {
    for (df in date_opts) {
      attempt_specs[[length(attempt_specs)+1L]] <- list(aff = aff, df = df, fld = source_field)
    }
  }
  flip_field <- if (identical(source_field, "FAU")) "AU" else "FAU"
  attempt_specs[[length(attempt_specs)+1L]] <- list(aff = "", df = tail(date_opts, 1L), fld = flip_field)
  
  for (att in attempt_specs) {
    term_try <- paste(
      sub("\\[(AU|FAU)\\]$", paste0("[", att$fld, "]"), name_term, perl = TRUE),
      if (nzchar(att$aff)) paste("AND", att$aff) else "",
      att$df
    )
    term_try <- gsub('"+','"', term_try, perl=TRUE)  # normalize any doubled quotes
    log_line("ESearch(backoff) →", term_try)
    s <- esearch_pubmed(term_try, retmax = retmax)
    if ((s$count %||% 0L) > 0L) {
      return(list(ids = s$ids, count = s$count, term_used = term_try, field_used = att$fld, used_affil = nzchar(att$aff)))
    }
  }
  list(ids = character(0), count = 0L, term_used = paste(name_term, date_filter), field_used = source_field, used_affil = FALSE)
}
# ==== [EXPERT DECISION TREE] Optimized multi-tier matching ====
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

  # Aggregators to expose the bottleneck explicitly
  affil_raw_max <- 0L;  affil_ver_max <- 0L
  noaff_raw_max <- 0L;  noaff_ver_max <- 0L

  strict_fm <- row$name_strict_fm
  relaxed_f <- row$name_relaxed_f
  
  # CHANGE D — do_query with source flag + FAU-only scoring for FAU/no-affil
  do_query <- function(name_term, use_affil, source = c("AU","FAU")) {
    source <- match.arg(source)
    term <- if (use_affil && has_affil) paste(name_term, "AND", affil_blk) else name_term
    term <- gsub('"+','"', term, perl=TRUE)
    # append per-row date filter already computed above as `date_filter`
    term <- paste(term, date_filter)
    log_line("ESearch term:", term)  # NOTE: log raw term (no shQuote), fixes double-quote output
    
    s_back <- esearch_pubmed_backoff(
      name_term   = name_term,
      use_affil   = (use_affil && has_affil),
      city_token  = city, state_token = state, org_token = org,
      date_filter = date_filter, source_field = source,
      retmax      = MAX_PMIDS_TO_FETCH
    )
    term        <- s_back$term_used
    raw_n       <- s_back$count
    idlist      <- s_back$ids
    field_used  <- s_back$field_used
    used_affil  <- isTRUE(s_back$used_affil)
    if (!length(idlist)) {
      return(list(term_used=term, raw_n=raw_n,
                  verified_total=0L, verified_first=0L, verified_last=0L,
                  pmids_verified=character(0), pmids_firstauthor=character(0),
                  pmids_lastauthor=character(0), pmids_ambiguous=character(0),
                  used_affil = used_affil))
    }
    
    # EFETCH with lightweight retry (fix occasional HTTP 500)
    t0 <- proc.time()[["elapsed"]]
    med <- try(efetch_medline_by_ids(head(idlist, MAX_PMIDS_TO_FETCH)), silent = TRUE)
    if (inherits(med, "try-error") || !nzchar(med)) {
      Sys.sleep(0.5)
      med <- efetch_medline_by_ids(head(idlist, MAX_PMIDS_TO_FETCH))
    }
    t1 <- proc.time()[["elapsed"]]
    log_line("EFETCH seconds:", round(t1 - t0, 2), "for", length(idlist),
             "ids (capped to", MAX_PMIDS_TO_FETCH, ")")
    
    recs <- parse_medline_records(med)
    
    # Build per-query modal first name, then choose scoring mode/threshold
    all_fau <- unlist(lapply(recs, function(r) r$FAU %||% character(0)))
    all_ad  <- lapply(recs, function(r) r$AD  %||% character(0))
    modal_first <- build_modal_fau_first(all_fau, all_ad, last_norm, city, state, org)
    
    local_mode <- if (identical(source, "FAU") && !use_affil) "fau_only" else "default"
    
    v_total <- v_first <- v_last <- 0L
    pmids_v <- pmids_fa <- pmids_la <- character(0)
    pmids_fail <- character(0)
    
    row_start <- proc.time()[["elapsed"]]
    for (rec in recs) {
      if ((proc.time()[["elapsed"]] - row_start) > 180) {  # watchdog: 3 min
        log_line("Watchdog: aborting verification early for term due to time budget.")
        break
      }
      if (is.null(rec)) next
      pmid <- ifelse(length(rec$PMID)>0, rec$PMID[1], NA_character_)
      au   <- rec$AU  %||% character(0)
      fau  <- rec$FAU %||% character(0)
      ad   <- rec$AD  %||% character(0)
      # inside for (rec in recs) { ... }
      if (identical(source, "AU")) {
        if (!.fau_gate_pass(fau, last_norm, first_tok,
                            source = "AU", risk_last = risk,
                            ad_vec = ad, city_token = city, state_token = state, org_token = org)) {
          if (!is.na(pmid)) pmids_fail <- c(pmids_fail, pmid)
          next
        }
      }
      sc <- verify_record_score(fau, ad, last_norm, first_tok, modal_first,
                                city, state, org, mode = local_mode)

      if (!is.na(pmid) && sc$total < sc$threshold) {
        pmids_fail <- c(pmids_fail, pmid)
        next
      }
      
      if (!is.na(pmid)) {
        v_total <- v_total + 1L
        pmids_v <- c(pmids_v, pmid)
        fl <- first_last_hits(au, allowed_set)
        if (fl["first"]) { v_first <- v_first + 1L; pmids_fa <- c(pmids_fa, pmid) }
        if (fl["last"])  { v_last  <- v_last  + 1L; pmids_la <- c(pmids_la, pmid) }
      }
    }
    
    pmids_fail <- unique(na.omit(pmids_fail)); if (length(pmids_fail) > 200) pmids_fail <- pmids_fail[1:200]
    
    list(term_used=term, raw_n=raw_n,
         verified_total=v_total, verified_first=v_first, verified_last=v_last,
         pmids_verified=unique(na.omit(pmids_v)),
         pmids_firstauthor=unique(na.omit(pmids_fa)),
         pmids_lastauthor =unique(na.omit(pmids_la)),
         pmids_ambiguous  =pmids_fail,
         used_affil = used_affil)
  }
  
  # CHANGE E — inside run_decision_tree(), replace the tier logic with:
  # --- PATCH F: helper for consistent ambiguity classification ---
  classify_ambiguity <- function(raw_n, verified_n, has_affil_tokens) {
    raw_n      <- raw_n %||% 0L
    verified_n <- verified_n %||% 0L
    if (raw_n == 0L) return("ZeroHits_NoAffil")
    if (raw_n > 0L && verified_n == 0L) {
      if (isTRUE(has_affil_tokens)) return("ZeroHits_AffilOnly")
      return("ZeroHits_NoAffil")
    }
    return(NA_character_)
  }
  # CHANGE E — inside run_decision_tree(), replace the tier logic with:
  
  # We’ll collect reasons as we go; final fallback will pick the best label
  reasons <- character(0)
  
  # 1) FAU-first, with affiliation
  fau_variants <- build_fau_variants(last_norm, first_tok, row$middle_name_token)
  for (fau_full in fau_variants) {
    res <- do_query(fau_full, use_affil = TRUE, source = "FAU")
    if (!is.null(res$raw_n)) {
      if (isTRUE(res$used_affil)) {
        affil_raw_max <- max(affil_raw_max, res$raw_n %||% 0L)
        affil_ver_max <- max(affil_ver_max, res$verified_total %||% 0L)
      } else {
        noaff_raw_max <- max(noaff_raw_max, res$raw_n %||% 0L)
        noaff_ver_max <- max(noaff_ver_max, res$verified_total %||% 0L)
      }
    }
    if (res$verified_total > 0) {
      return(c(list(match_tier = 1, ambiguous_reason = NA_character_,
                    raw_n_with_affil = affil_raw_max, raw_n_without_affil = noaff_raw_max,
                    verified_with_affil_total = affil_ver_max,
                    verified_without_affil_total = noaff_ver_max),
               res))
    } else {
      reasons <- c(reasons, classify_ambiguity(res$raw_n, res$verified_total, has_affil))
    }
  }
  
  # 2) FAU-only (no affiliation) — uses FAU-only scoring mode (already handled inside do_query)
  for (fau_full in fau_variants) {
    res <- do_query(fau_full, use_affil = FALSE, source = "FAU")
    if (!is.null(res$raw_n)) {
      if (isTRUE(res$used_affil)) {
        affil_raw_max <- max(affil_raw_max, res$raw_n %||% 0L)
        affil_ver_max <- max(affil_ver_max, res$verified_total %||% 0L)
      } else {
        noaff_raw_max <- max(noaff_raw_max, res$raw_n %||% 0L)
        noaff_ver_max <- max(noaff_ver_max, res$verified_total %||% 0L)
      }
    }
    if (res$verified_total > 0) {
      return(c(list(match_tier = 2, ambiguous_reason = NA_character_,
                    raw_n_with_affil = affil_raw_max, raw_n_without_affil = noaff_raw_max,
                    verified_with_affil_total = affil_ver_max,
                    verified_without_affil_total = noaff_ver_max),
               res))
    } else {
      reasons <- c(reasons, classify_ambiguity(res$raw_n, res$verified_total, FALSE))
    }
  }
  
  # 3) AU + affiliation (strict FM), FAU-gated per-record
  res <- do_query(strict_fm, use_affil = TRUE, source = "AU")
  if (!is.null(res$raw_n)) {
    if (isTRUE(res$used_affil)) {
      affil_raw_max <- max(affil_raw_max, res$raw_n %||% 0L)
      affil_ver_max <- max(affil_ver_max, res$verified_total %||% 0L)
    } else {
      noaff_raw_max <- max(noaff_raw_max, res$raw_n %||% 0L)
      noaff_ver_max <- max(noaff_ver_max, res$verified_total %||% 0L)
    }
  }
  if (res$verified_total > 0) {
    return(c(list(match_tier = 3, ambiguous_reason = NA_character_,
                  raw_n_with_affil = affil_raw_max, raw_n_without_affil = noaff_raw_max,
                  verified_with_affil_total = affil_ver_max,
                  verified_without_affil_total = noaff_ver_max),
             res))
  } else {
    reasons <- c(reasons, classify_ambiguity(res$raw_n, res$verified_total, has_affil))
  }
  
  # 3b) AU + affiliation (relaxed F), FAU-gated per-record
  res <- do_query(relaxed_f, use_affil = TRUE, source = "AU")
  if (!is.null(res$raw_n)) {
    if (isTRUE(res$used_affil)) {
      affil_raw_max <- max(affil_raw_max, res$raw_n %||% 0L)
      affil_ver_max <- max(affil_ver_max, res$verified_total %||% 0L)
    } else {
      noaff_raw_max <- max(noaff_raw_max, res$raw_n %||% 0L)
      noaff_ver_max <- max(noaff_ver_max, res$verified_total %||% 0L)
    }
  }
  if (res$verified_total > 0) {
    return(c(list(match_tier = 3, ambiguous_reason = NA_character_,
                  raw_n_with_affil = affil_raw_max, raw_n_without_affil = noaff_raw_max,
                  verified_with_affil_total = affil_ver_max,
                  verified_without_affil_total = noaff_ver_max),
             res))
  } else {
    reasons <- c(reasons, classify_ambiguity(res$raw_n, res$verified_total, has_affil))
  }
  
  # 4) AU strict (no affiliation) for STD risk only — tight cap
  if (identical(risk, "STD")) {
    res <- do_query(strict_fm, use_affil = FALSE, source = "AU")
    if (!is.null(res$raw_n)) {
      if (isTRUE(res$used_affil)) {
        affil_raw_max <- max(affil_raw_max, res$raw_n %||% 0L)
        affil_ver_max <- max(affil_ver_max, res$verified_total %||% 0L)
      } else {
        noaff_raw_max <- max(noaff_raw_max, res$raw_n %||% 0L)
        noaff_ver_max <- max(noaff_ver_max, res$verified_total %||% 0L)
      }
    }
    if (res$verified_total > 0 && res$verified_total <= 5) {
      return(c(list(match_tier = 4, ambiguous_reason = NA_character_,
                    raw_n_with_affil = affil_raw_max, raw_n_without_affil = noaff_raw_max,
                    verified_with_affil_total = affil_ver_max,
                    verified_without_affil_total = noaff_ver_max),
               res))
    } else {
      reason <- if (res$verified_total == 0) {
        classify_ambiguity(res$raw_n, res$verified_total, FALSE)
      } else {
        "TooManyHits_NoAffil"
      }
      return(c(list(match_tier = 4, ambiguous_reason = reason,
                    raw_n_with_affil = affil_raw_max, raw_n_without_affil = noaff_raw_max,
                    verified_with_affil_total = affil_ver_max,
                    verified_without_affil_total = noaff_ver_max),
               res))
    }
  }

  final_reason <- if (affil_raw_max > 0L && affil_ver_max == 0L) {
    "ZeroHits_AffilOnly"
  } else if (noaff_raw_max == 0L && affil_raw_max == 0L) {
    "ZeroHits_NoAffil"
  } else {
    "Ambiguous_NoAffil"
  }

  list(match_tier = NA_integer_, term_used = NA_character_, raw_n = 0L,
       verified_total = 0L, verified_first = 0L, verified_last = 0L,
       pmids_verified = character(0), pmids_firstauthor = character(0), pmids_lastauthor = character(0),
       ambiguous_reason = final_reason,
       raw_n_with_affil = affil_raw_max, raw_n_without_affil = noaff_raw_max,
       verified_with_affil_total = affil_ver_max,
       verified_without_affil_total = noaff_ver_max)
}
# ---------- EXPERT MAIN EXECUTION ----------
run_phase2_http_expert <- function(in_csv = IN_CSV, out_csv = OUT_CSV) {
  dt <- fread(in_csv)
  
  init_cols <- c("match_tier","query_used","raw_n_total","verified_n_total",
                 "verified_n_first","verified_n_last","ambiguous_reason",
                 "pmids_verified","pmids_firstauthor","pmids_lastauthor",
                 "pmids_ambiguous",
                 "raw_n_with_affil","raw_n_without_affil",
                 "verified_with_affil_total","verified_without_affil_total")
  for (col in init_cols) if (!col %in% names(dt)) dt[, (col) := NA_character_]

  num_cols <- c("match_tier","raw_n_total","verified_n_total","verified_n_first","verified_n_last",
                "raw_n_with_affil","raw_n_without_affil",
                "verified_with_affil_total","verified_without_affil_total")
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
        pmids_ambiguous   = "",
        raw_n_with_affil             = as.integer(NA),
        raw_n_without_affil          = as.integer(NA),
        verified_with_affil_total    = as.integer(NA),
        verified_without_affil_total = as.integer(NA)
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
        pmids_ambiguous   = pmids_amb,
        raw_n_with_affil             = as.integer(ans$raw_n_with_affil %||% NA),
        raw_n_without_affil          = as.integer(ans$raw_n_without_affil %||% NA),
        verified_with_affil_total    = as.integer(ans$verified_with_affil_total %||% NA),
        verified_without_affil_total = as.integer(ans$verified_without_affil_total %||% NA)
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

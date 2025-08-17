# =====================================================================
# SCRIPT: 03_run_pubmed_smart_query_v2_http.R  (cleaned & debugged)
# PURPOSE: Smart Query v2 using direct HTTP (httr/jsonlite) E-utilities
# INPUT : outputs/surgeons_prepared_for_query.csv  (or pilot CSV)
# OUTPUT: outputs/surgeons_with_pubcounts.csv (+ periodic checkpoints)
# NOTE  : Expects your API key in Sys.getenv("NCBI_API_KEY")
# AUTHOR: pass-through refactor with minimal structural changes
# ---------------------------------------------------------------------
#  CHANGE GUIDE
#   - CHANGE 1 : Fix API key env var and add config for lookback window
#   - CHANGE 2 : Safer logging cat() usage and consistent newline
#   - CHANGE 3 : Chunked EFetch to avoid stalls; timing logs
#   - CHANGE 4 : Robust MEDLINE parsing (split + regex); tag handling
#   - CHANGE 5 : Name/affiliation scoring helpers; modal FAU first name
#   - CHANGE 6 : Date window per-row based on EnumerationDate
#   - CHANGE 7 : Watchdog timer per do_query; better ambiguity capture
#   - CHANGE 8 : run_phase2_http() accepts in/out args (backwards compat)
# =====================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(stringr)
  library(httr)
  library(jsonlite)
  library(digest)
  library(stringdist)   # CHANGE 5: for name similarity scoring
})

`%||%` <- function(a,b) if (is.null(a) || length(a)==0) b else a

# ---------- CONFIG ----------
TOOL  <- "nppes_pubmed_pipeline"
EMAIL <- "deveshrk@stanford.edu"   # contact per NCBI guidance

IN_CSV     <- "outputs/surgeons_pilot_for_query.csv"
OUT_CSV    <- "outputs/surgeons_with_pubcounts.csv"
CACHE_DIR  <- "outputs/eutils_cache"
LOG_FILE   <- "outputs/phase2_run_log.txt"

# CHANGE 1: Correct env var + date-window knobs
API_KEY <- Sys.getenv("NCBI_API_KEY")   # <— FIXED
API_DELAY_SEC       <- if (nchar(API_KEY)>0) 0.13 else 0.35  # ~7–3 req/s
RETRY_MAX           <- 3
RETRY_BASE          <- 0.7
MAX_PMIDS_TO_FETCH  <- 200
EFETCH_CHUNK        <- 150  # CHANGE 3: fetch ids in chunks
CHK_EVERY           <- 100
RESUME              <- TRUE

LOOKBACK_YEARS      <- 25   # CHANGE 6: per-surgeon window length
DATE_CEILING_YEAR   <- as.integer(format(Sys.Date(), "%Y"))
SCORE_THRESHOLD     <- 0.75 # CHANGE 5: verification score cutoff
ROW_TIME_BUDGET_SEC <- 120  # CHANGE 7: watchdog per row

BASE_URL <- "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
ESEARCH  <- paste0(BASE_URL, "esearch.fcgi")
EFETCH   <- paste0(BASE_URL, "efetch.fcgi")

if (!dir.exists("outputs")) dir.create("outputs", recursive = TRUE)
if (!dir.exists(CACHE_DIR)) dir.create(CACHE_DIR, recursive = TRUE)

# ---------- logging & caching ----------
log_line <- function(...) {
  # CHANGE 2: always one newline, no stray spaces
  msg <- paste0(format(Sys.time(), "%F %T"), " | ", paste(..., collapse=" "))
  cat(msg, "\n")
  try(write(msg, file = LOG_FILE, append = TRUE), silent = TRUE)
}

.safe_name <- function(x) gsub("[^A-Za-z0-9._-]+", "_", x)
cache_key  <- function(prefix, ...) .safe_name(paste(prefix, digest(list(...), algo="sha1"), sep = "__"))
cache_path <- function(key) file.path(CACHE_DIR, paste0(key, ".rds"))
cache_get  <- function(key) { p <- cache_path(key); if (file.exists(p)) readRDS(p) else NULL }
cache_put  <- function(key, val) saveRDS(val, cache_path(key))

# ---------- HTTP wrappers ----------
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

# CHANGE 3: chunked efetch with timing + caching
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

# ---------- ESearch / EFetch via HTTP ----------
esearch_pubmed <- function(term, retmax = MAX_PMIDS_TO_FETCH) {
  out <- http_get_json(ESEARCH, list(db = "pubmed", term = term, retmode = "json", retmax = retmax))
  idlist <- out$esearchresult$idlist %||% character(0)
  count  <- as.integer(out$esearchresult$count %||% 0)
  list(ids = idlist, count = count)
}

# CHANGE 3: EFETCH in manageable chunks, concatenate MEDLINE
efetch_medline_by_ids <- function(id_vec) {
  if (length(id_vec) == 0) return("")
  ids <- unique(id_vec)
  out_txt <- character(0)
  for (start in seq(1, length(ids), by = EFETCH_CHUNK)) {
    chunk <- ids[start:min(start + EFETCH_CHUNK - 1, length(ids))]
    t0 <- proc.time()[["elapsed"]]
    txt <- http_get_text(EFETCH, list(db = "pubmed", id = paste(chunk, collapse=","), rettype = "medline", retmode = "text"))
    t1 <- proc.time()[["elapsed"]]
    log_line("EFETCH seconds:", round(t1 - t0, 2), "for", length(chunk), "ids (capped to", MAX_PMIDS_TO_FETCH, ")")
    out_txt <- c(out_txt, txt)
  }
  paste(out_txt, collapse = "\n\n")
}

# ---------- MEDLINE parsing ----------
# CHANGE 4: consistent split + regex hyphen handling
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

# ---------- matching & scoring helpers ----------
normalize_str <- function(x) {
  x <- ifelse(is.na(x), "", x)
  x <- toupper(x)
  x <- gsub("[,\\.]", " ", x)
  x <- gsub("\\s+", " ", x)
  trimws(x)
}

# CHANGE 5: extract first name token from FAU line with given LAST
.fau_first_from <- function(fau_line, last_norm) {
  if (is.na(fau_line) || !nzchar(fau_line)) return(NA_character_)
  x <- toupper(fau_line)
  pat <- paste0("^", toupper(last_norm), "[, ]+")
  x <- sub(pat, "", x)
  x <- sub("\\s+[A-Z]$", "", x); x <- sub("\\s+[A-Z]$", "", x)
  strsplit(x, "\\s+")[[1]][1] %||% NA_character_
}

# CHANGE 5: affiliation score with city/state strong match, org partial
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

# CHANGE 5: pick modal first name among candidates that share LAST and some affiliation
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

# CHANGE 5: verification score combining FAU similarity + affiliation
verify_record_score <- function(fau_vec, ad_vec,
                                last_norm, target_first, modal_first,
                                city_token, state_token, org_token) {
  fau_same_last <- fau_vec[startsWith(toupper(fau_vec), paste0(toupper(last_norm), ","))]
  fau_score <- 0
  if (length(fau_same_last) > 0) {
    fnames <- vapply(fau_same_last, .fau_first_from, character(1), last_norm = last_norm)
    fnames <- fnames[nzchar(fnames)]
    if (length(fnames) > 0) {
      tgt <- toupper(target_first %||% "")
      mod <- toupper(modal_first %||% "")
      cand <- unique(fnames)
      init_ok <- function(a, b) nzchar(a) && nzchar(b) && substr(a,1,1) == substr(b,1,1)
      sims <- numeric(0)
      if (nzchar(tgt) && any(init_ok(tgt, cand)))
        sims <- c(sims, max(stringsim(tgt, cand[init_ok(tgt, cand)], method="jw", p=0.1)))
      if (nzchar(mod) && any(init_ok(mod, cand)))
        sims <- c(sims, max(stringsim(mod, cand[init_ok(mod, cand)], method="jw", p=0.1)))
      fau_score <- max(sims, 0, na.rm = TRUE)
    }
  }
  aff <- .affil_score(ad_vec, city_token, state_token, org_token)
  total <- 0.60 * fau_score + 0.35 * aff + 0.05 * 0
  list(total = total, fau_score = fau_score, aff_score = aff)
}

# ==== [PATCH 1] FAU Gate helpers (ADD THIS BLOCK) ===============================

# Set of very common surnames that need slightly stricter JW threshold on AU results
.VERY_COMMON_LAST <- c("WANG","KIM","LEE","SMITH","PATEL","KHAN","WU","BROWN","GARCIA","RODRIGUEZ")

# Cheap Jaro–Winkler wrapper (returns 0 if inputs are empty)
.jw <- function(a, b) {
  if (!nzchar(a) || !nzchar(b)) return(0)
  stringdist::stringsim(a, b, method = "jw", p = 0.1)
}

# From a vector of FAU lines, pick the BEST candidate for our target:
#  - same last name
#  - same first initial
# Returns a list(first=FIRSTNAME, sim=similarity, matched=logical)
.best_fau_match <- function(fau_vec, last_norm, target_first) {
  if (length(fau_vec) == 0) return(list(first=NA_character_, sim=0, matched=FALSE))
  lastU  <- toupper(last_norm %||% "")
  tfirst <- toupper(target_first %||% "")
  if (!nzchar(lastU) || !nzchar(tfirst)) return(list(first=NA_character_, sim=0, matched=FALSE))
  cand <- fau_vec[startsWith(toupper(fau_vec), paste0(lastU, ","))]
  if (length(cand) == 0) return(list(first=NA_character_, sim=0, matched=FALSE))
  
  # extract first token after "LAST, "
  first_tokens <- vapply(cand, .fau_first_from, character(1), last_norm=last_norm)
  first_tokens <- toupper(first_tokens)
  first_tokens <- first_tokens[nzchar(first_tokens)]
  
  if (length(first_tokens) == 0) return(list(first=NA_character_, sim=0, matched=FALSE))
  
  # keep only same first-initial
  same_init <- substr(first_tokens, 1, 1) == substr(tfirst, 1, 1)
  first_tokens <- first_tokens[same_init]
  if (length(first_tokens) == 0) return(list(first=NA_character_, sim=0, matched=FALSE))
  
  sims <- vapply(first_tokens, function(f) .jw(tfirst, f), numeric(1))
  i <- which.max(sims)
  list(first = first_tokens[i], sim = sims[i], matched = TRUE)
}

# Decide pass/fail for AU-derived records using the FAU Gate
# For FAU-derived queries, we can be lenient or even skip this gate.
.fau_gate_pass <- function(fau_vec, last_norm, first_tok, source, risk_last) {
  # If this record came from a FAU query, it's already precise:
  if (identical(source, "FAU")) {
    # Soft check: ensure there is at least one FAU with same last & same initial
    bf <- .best_fau_match(fau_vec, last_norm, first_tok)
    return(bf$matched && bf$sim >= 0.85)  # FAU threshold: 0.85
  }
  
  # AU source: require a stricter match
  bf <- .best_fau_match(fau_vec, last_norm, first_tok)
  if (!bf$matched) return(FALSE)
  
  lastU <- toupper(last_norm %||% "")
  # Stricter JW for very common last names (or if user tagged risk as HIGH)
  thr <- if (lastU %in% .VERY_COMMON_LAST || identical(risk_last, "HIGH")) 0.95 else 0.93
  bf$sim >= thr
}
# ==== [END PATCH 1] =============================================================

# Build a compact set of allowed patterns (covers AU with/without space & FAU)
build_allowed_author_set <- function(last_norm, first_tok, middle_tok) {
  last_norm  <- normalize_str(ifelse(is.na(last_norm),  "", last_norm))
  first_tok  <- normalize_str(ifelse(is.na(first_tok),  "", first_tok))
  middle_tok <- normalize_str(ifelse(is.na(middle_tok), "", middle_tok))
  fi <- if (nzchar(first_tok)) substr(first_tok, 1, 1) else ""
  mi <- if (nzchar(middle_tok)) substr(middle_tok, 1, 1) else ""
  cand <- c(
    paste(last_norm, first_tok),      # FAU: LAST FIRST
    paste(last_norm, fi),             # AU : LAST F
    paste(last_norm, fi, mi),         # AU : LAST F M
    paste(last_norm, paste0(fi, mi))  # AU : LAST FM
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

# Affiliation helpers
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

# ---------- date helpers (CHANGE 6) ----------
.parse_enum_year <- function(enum_chr) {
  if (is.null(enum_chr) || is.na(enum_chr) || !nzchar(enum_chr)) return(NA_integer_)
  # try multiple formats: m/d/Y, m/d/YY, ISO
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

# ---------- decision tree ----------
run_decision_tree <- function(row) {
  last_norm  <- toupper(ifelse(is.na(row$last_name_norm), "", row$last_name_norm))
  first_tok  <- toupper(ifelse(is.na(row$first_name_token), "", row$first_name_token))
  middle_tok <- toupper(ifelse(is.na(row$middle_name_token), "", row$middle_name_token))
  risk       <- row$risk_surname
  
  city  <- toupper(ifelse(is.na(row$city_token),  "", row$city_token))
  state <- toupper(ifelse(is.na(row$state_token), "", row$state_token))
  org   <- toupper(ifelse(is.na(row$org_token),   "", row$org_token))
  
  date_filter  <- .build_date_filter(row$EnumerationDate)  # CHANGE 6
  affil_blk    <- rebuild_affil_block(city, state, org)
  has_affil    <- nchar(affil_blk) > 0
  affil_tokens <- affil_tokens_from(city, state, org)
  
  allowed_set  <- build_allowed_author_set(last_norm, first_tok, middle_tok)
  
  strict_fm <- row$name_strict_fm
  relaxed_f <- row$name_relaxed_f
  full_fau  <- row$name_full_fau
  
  do_query <- function(name_term, use_affil, source = "AU") {  # source is "FAU" or "AU"
    row_start <- proc.time()[["elapsed"]]            # CHANGE 7
    term <- if (use_affil && has_affil) paste(name_term, "AND", affil_blk) else name_term
    term <- gsub('"+','"', term, perl=TRUE)
    term <- paste(term, date_filter)
    log_line("ESearch term:", shQuote(term))
    
    s <- esearch_pubmed(term, retmax = MAX_PMIDS_TO_FETCH)
    raw_n  <- s$count
    idlist <- s$ids
    if (length(idlist) == 0) {
      return(list(term_used=term, raw_n=raw_n,
                  verified_total=0L, verified_first=0L, verified_last=0L,
                  pmids_verified=character(0),
                  pmids_firstauthor=character(0),
                  pmids_lastauthor=character(0),
                  pmids_ambiguous=character(0)))
    }
    
    t0 <- proc.time()[["elapsed"]]
    med  <- efetch_medline_by_ids(head(idlist, MAX_PMIDS_TO_FETCH))
    t1 <- proc.time()[["elapsed"]]
    log_line("EFETCH seconds:", round(t1 - t0, 2), "for", length(idlist), "ids (capped to", MAX_PMIDS_TO_FETCH, ")")
    
    t2 <- proc.time()[["elapsed"]]
    recs <- parse_medline_records(med)
    t3 <- proc.time()[["elapsed"]]
    log_line("Parse seconds:", round(t3 - t2, 2), "records:", length(recs))
    
    # modal first name to stabilize matching
    all_fau <- unlist(lapply(recs, function(r) r$FAU %||% character(0)))
    all_ad  <- lapply(recs, function(r) r$AD %||% character(0))
    modal_first <- build_modal_fau_first(all_fau, all_ad, last_norm, city, state, org)
    
    v_total <- v_first <- v_last <- 0L
    pmids_v <- pmids_fa <- pmids_la <- character(0)
    pmids_fail <- character(0)
    
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
      
      # ---- [PATCH 2] FAU Gate (ADD THIS BLOCK) --------------------------------
      # Reject AU-derived candidates unless a FAU in this record looks like our surgeon
      if (!.fau_gate_pass(fau, last_norm, first_tok, source, risk)) {
        if (!is.na(pmid)) pmids_fail <- c(pmids_fail, pmid)
        next
      }
      # ---- [END PATCH 2] -------------------------------------------------------
      
      sc <- verify_record_score(fau, ad, last_norm, first_tok, modal_first, city, state, org)
      if (!is.na(pmid) && sc$total < SCORE_THRESHOLD) pmids_fail <- c(pmids_fail, pmid)
      
      if (!is.na(pmid) && sc$total >= SCORE_THRESHOLD) {
        v_total <- v_total + 1L
        pmids_v <- c(pmids_v, pmid)
        fl <- first_last_hits(au, allowed_set)
        if (fl["first"]) { v_first <- v_first + 1L; pmids_fa <- c(pmids_fa, pmid) }
        if (fl["last"])  { v_last  <- v_last  + 1L; pmids_la <- c(pmids_la, pmid) }
      }
      
      # keep pmids_fail bounded
      pmids_fail <- unique(na.omit(pmids_fail))
      if (length(pmids_fail) > 200) pmids_fail <- pmids_fail[1:200]
    }
    
    list(term_used=term, raw_n=raw_n,
         verified_total=v_total, verified_first=v_first, verified_last=v_last,
         pmids_verified=unique(na.omit(pmids_v)),
         pmids_firstauthor=unique(na.omit(pmids_fa)),
         pmids_lastauthor =unique(na.omit(pmids_la)),
         pmids_ambiguous  =unique(na.omit(pmids_fail)))
  }
  
  # ---- [PATCH 3] Re-ordered tiers: FAU-first ----------------------------------
  
  # Build FAU "LAST FIRST" term from tokens you already have
  # NOTE: If you already have row$name_full_fau as something like LAST FIRST[FAU],
  # you can reuse it; just ensure it's quoted properly.
  fau_full <- sprintf('"%s %s"[FAU]', last_norm, first_tok)
  
  # Tier 1: FAU + Affiliation + Date window
  res <- do_query(fau_full, use_affil = TRUE,  source = "FAU")
  if (res$verified_total > 0) return(c(list(match_tier = 1), res))
  
  # Tier 2: FAU only + Date window (no affiliation in query)
  res <- do_query(fau_full, use_affil = FALSE, source = "FAU")
  if (res$verified_total > 0) return(c(list(match_tier = 2), res))
  
  # Tier 3: AU (your existing strict/relaxed terms), but gated by FAU Gate
  # You already build allowed_set etc. Keep that logic; just pass source="AU".
  res <- do_query(strict_fm, use_affil = TRUE, source = "AU")
  if (res$verified_total > 0) return(c(list(match_tier = 3), res))
  
  res <- do_query(relaxed_f, use_affil = TRUE, source = "AU")
  if (res$verified_total > 0) return(c(list(match_tier = 3), res))
  
  # Tier 4: Only for STD risk — AU without affiliation but with FAU Gate + small cap
  if (identical(risk, "STD")) {
    res <- do_query(strict_fm, use_affil = FALSE, source = "AU")
    if (res$verified_total > 0 && res$verified_total <= 5) {
      return(c(list(match_tier = 4), res))
    } else {
      return(c(list(match_tier = 4, ambiguous_reason = "TooManyHits_NoAffil"), res))
    }
  }
  
  # Fallback: No match
  list(
    match_tier = NA_integer_, term_used = NA_character_, raw_n = 0L,
    verified_total = 0L, verified_first = 0L, verified_last = 0L,
    pmids_verified = character(0), pmids_firstauthor = character(0),
    pmids_lastauthor = character(0),
    ambiguous_reason = "Ambiguous_NoAffil"
  )
  # ---- [END PATCH 3] -----------------------------------------------------------
}

# ---------- MAIN EXECUTION (wrapped; does not auto-run) ----------
# CHANGE 8: allow overriding in/out paths while keeping globals as default
run_phase2_http <- function(in_csv = IN_CSV, out_csv = OUT_CSV) {
  dt <- fread(in_csv)
  
  # init/preserve result columns
  init_cols <- c("match_tier","query_used","raw_n_total","verified_n_total",
                 "verified_n_first","verified_n_last","ambiguous_reason",
                 "pmids_verified","pmids_firstauthor","pmids_lastauthor",
                 "pmids_ambiguous")  # CHANGE 7: new column
  for (col in init_cols) if (!col %in% names(dt)) dt[, (col) := NA_character_]
  
  num_cols <- c("match_tier","raw_n_total","verified_n_total","verified_n_first","verified_n_last")
  dt[, (intersect(num_cols, names(dt))) := lapply(.SD, as.integer), .SDcols = intersect(num_cols, names(dt))]
  
  row_idx <- if (RESUME) which(is.na(dt$verified_n_total)) else seq_len(nrow(dt))
  if (length(row_idx) == 0) { log_line("Nothing to do."); fwrite(dt, out_csv); return(invisible(dt)) }
  
  log_line("Starting Phase-2 (HTTP backend) on ", length(row_idx), " rows (of ", nrow(dt), ").")
  
  # sanity preview of first term
  r0 <- dt[row_idx[1]]
  affil_blk0 <- rebuild_affil_block(toupper(r0$city_token %||% ""), toupper(r0$state_token %||% ""), toupper(r0$org_token %||% ""))
  cat("Sanity test term:\n", paste(r0$name_strict_fm, if (nchar(affil_blk0)>0) paste("AND", affil_blk0) else ""), "\n\n")
  
  for (k in seq_along(row_idx)) {
    i <- row_idx[k]
    row <- dt[i]
    log_line("Row ", k, "/", length(row_idx), " (NPI=", row$NPI, ")")
    
    ans <- try(run_decision_tree(row), silent = TRUE)
    if (inherits(ans, "try-error")) {
      dt[i, `:=`(
        match_tier        = NA_integer_,
        query_used        = NA_character_,
        raw_n_total       = 0L,
        verified_n_total  = 0L,
        verified_n_first  = 0L,
        verified_n_last   = 0L,
        ambiguous_reason  = paste0("Error_Query: ", substr(as.character(ans), 1, 200)),
        pmids_verified    = "",
        pmids_firstauthor = "",
        pmids_lastauthor  = "",
        pmids_ambiguous   = ""      # CHANGE 7
      )]
    } else {
      pmids_v   <- paste(unique(ans$pmids_verified),    collapse = ";")
      pmids_fa  <- paste(unique(ans$pmids_firstauthor), collapse = ";")
      pmids_la  <- paste(unique(ans$pmids_lastauthor),  collapse = ";")
      pmids_amb <- paste(unique(ans$pmids_ambiguous),   collapse = ";")  # CHANGE 7
      
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
        pmids_ambiguous   = pmids_amb   # CHANGE 7
      )]
    }
    
    if (k %% CHK_EVERY == 0) {
      chk_path <- sub("\\.csv$", paste0("_checkpoint_", row_idx[k], ".csv"), out_csv)
      fwrite(dt, chk_path)
      log_line("Checkpoint written: ", chk_path)
    }
  }
  
  fwrite(dt, out_csv)
  log_line("DONE. Output written to: ", out_csv)
  invisible(dt)
}

cat(
  "HTTP runner sourced. Helpers & run_phase2_http() are now available.\n",
  "To execute the pipeline: set IN_CSV as desired, then call run_phase2_http().\n",
  sep = ""
)
# =====================================================================
# ZEROHITS_NOAFFIL_FIX.R
# PURPOSE: Fix the ZeroHits_NoAffil problem by implementing enhanced affiliation analysis
# AUTHOR: Expert-level solution for missing affiliation data
# =====================================================================

# NEW: Enhanced affiliation analysis function
enhanced_affiliation_analysis <- function(city, state, org) {
  # Normalize inputs
  city <- toupper(trimws(city %||% ""))
  state <- toupper(trimws(state %||% ""))
  org <- toupper(trimws(org %||% ""))
  
  # Analyze affiliation quality
  has_city <- nzchar(city) && !is_generic_location(city)
  has_state <- nzchar(state) && !is_generic_location(state)
  has_org <- nzchar(org) && !is_generic_org(org)
  
  # Calculate affiliation strength score (0-100)
  affil_score <- 0
  if (has_city) affil_score <- affil_score + 30
  if (has_state) affil_score <- affil_score + 40
  if (has_org) affil_score <- affil_score + 30
  
  # Determine query strategy
  if (affil_score >= 70) {
    strategy <- "strong_affiliation"
    use_affil_primary <- TRUE
    use_affil_fallback <- TRUE
  } else if (affil_score >= 40) {
    strategy <- "moderate_affiliation"
    use_affil_primary <- TRUE
    use_affil_fallback <- FALSE
  } else if (affil_score >= 20) {
    strategy <- "weak_affiliation"
    use_affil_primary <- FALSE
    use_affil_fallback <- TRUE
  } else {
    strategy <- "no_affiliation"
    use_affil_primary <- FALSE
    use_affil_fallback <- FALSE
  }
  
  # Build enhanced affiliation variants
  affil_variants <- build_enhanced_affiliation_variants(city, state, org, strategy)
  
  list(
    strategy = strategy,
    score = affil_score,
    has_city = has_city,
    has_state = has_state,
    has_org = has_org,
    use_affil_primary = use_affil_primary,
    use_affil_fallback = use_affil_fallback,
    variants = affil_variants,
    raw_tokens = list(city = city, state = state, org = org)
  )
}

# NEW: Enhanced affiliation variant builder
build_enhanced_affiliation_variants <- function(city, state, org, strategy) {
  variants <- list()
  
  # Strong affiliation: use all available tokens
  if (strategy == "strong_affiliation") {
    parts <- character(0)
    if (nzchar(city)) parts <- c(parts, sprintf('"%s"[Affiliation]', city))
    if (nzchar(state)) parts <- c(parts, sprintf('"%s"[Affiliation]', state))
    if (nzchar(org)) parts <- c(parts, sprintf('"%s"[Affiliation]', org))
    if (length(parts) > 0) {
      variants$strong <- paste0("(", paste(parts, collapse = " OR "), ")")
    }
    
    # Also try city+state combination
    if (nzchar(city) && nzchar(state)) {
      variants$city_state <- sprintf('("%s"[Affiliation] AND "%s"[Affiliation])', city, state)
    }
  }
  
  # Moderate affiliation: try combinations
  if (strategy %in% c("moderate_affiliation", "strong_affiliation")) {
    if (nzchar(city)) variants$city_only <- sprintf('"%s"[Affiliation]', city)
    if (nzchar(state)) variants$state_only <- sprintf('"%s"[Affiliation]', state)
    if (nzchar(org)) variants$org_only <- sprintf('"%s"[Affiliation]', org)
  }
  
  # Weak affiliation: try state only or org only
  if (strategy %in% c("weak_affiliation", "moderate_affiliation", "strong_affiliation")) {
    if (nzchar(state)) variants$state_fallback <- sprintf('"%s"[Affiliation]', state)
    if (nzchar(org)) variants$org_fallback <- sprintf('"%s"[Affiliation]', org)
  }
  
  # Always include empty variant for no-affiliation queries
  variants$none <- ""
  
  # Remove duplicates and empty variants
  variants <- variants[sapply(variants, nzchar)]
  unique(variants)
}

# NEW: Generic location detector
is_generic_location <- function(location) {
  if (is.null(location) || !nzchar(location)) return(TRUE)
  loc <- tolower(location)
  generic_patterns <- c(
    "\\b(unknown|n\\/a|na|none|blank|empty|test|sample)\\b",
    "\\b(city|town|village|county|district|area|region)\\b",
    "^[0-9]+$",  # Just numbers
    "^[a-z]$",   # Single letter
    "^[a-z]{1,2}$"  # Very short
  )
  any(sapply(generic_patterns, function(pattern) grepl(pattern, loc)))
}

# ENHANCED: Improved generic org detector
is_generic_org <- function(org_raw) {
  if (is.null(org_raw) || !nzchar(org_raw)) return(TRUE)
  o <- tolower(org_raw)
  generic_patterns <- c(
    "\\b(hospital|clinic|medical center|health|healthcare|university|college|dept|department|inc|llc|corp|corporation|company|co)\\b",
    "\\b(private|practice|group|associates|partners|physicians|doctors|surgeons)\\b",
    "^[0-9]+$",  # Just numbers
    "^[a-z]$",   # Single letter
    "^[a-z]{1,2}$"  # Very short
  )
  any(sapply(generic_patterns, function(pattern) grepl(pattern, o)))
}

# NEW: Enhanced ambiguity classification
enhanced_classify_ambiguity <- function(raw_n, verified_n, affiliation_analysis) {
  raw_n <- raw_n %||% 0L
  verified_n <- verified_n %||% 0L
  
  if (raw_n == 0L) {
    if (affiliation_analysis$score >= 70) {
      return("ZeroHits_StrongAffil")
    } else if (affiliation_analysis$score >= 40) {
      return("ZeroHits_ModerateAffil")
    } else if (affiliation_analysis$score >= 20) {
      return("ZeroHits_WeakAffil")
    } else {
      return("ZeroHits_NoAffil")
    }
  }
  
  if (raw_n > 0L && verified_n == 0L) {
    if (affiliation_analysis$score >= 70) {
      return("ZeroVerified_StrongAffil")
    } else if (affiliation_analysis$score >= 40) {
      return("ZeroVerified_ModerateAffil")
    } else if (affiliation_analysis$score >= 20) {
      return("ZeroVerified_WeakAffil")
    } else {
      return("ZeroVerified_NoAffil")
    }
  }
  
  return(NA_character_)
}

# NEW: Enhanced query function with better affiliation handling
enhanced_do_query <- function(name_term, affiliation_analysis, source = c("AU","FAU"), 
                             existing_do_query_function) {
  source <- match.arg(source)
  
  # Get affiliation variants based on strategy
  affil_variants <- affiliation_analysis$variants
  use_affil_primary <- affiliation_analysis$use_affil_primary
  use_affil_fallback <- affiliation_analysis$use_affil_fallback
  
  # Track results across all attempts
  best_result <- NULL
  best_score <- 0
  
  # Strategy 1: Try with primary affiliation if available
  if (use_affil_primary && length(affil_variants) > 1) {
    for (variant_name in names(affil_variants)) {
      if (variant_name == "none") next  # Skip no-affiliation for primary
      
      affil_block <- affil_variants[[variant_name]]
      # Modify the name_term to include affiliation
      term_with_affil <- if (nzchar(affil_block)) paste(name_term, "AND", affil_block) else name_term
      
      log_line("Enhanced query (primary):", term_with_affil)
      
      # Use existing do_query function but with modified term
      result <- existing_do_query_function(term_with_affil, TRUE, source)
      if (!is.null(result) && result$verified_total > best_score) {
        best_result <- result
        best_score <- result$verified_total
      }
      
      # If we found good results, don't try weaker variants
      if (best_score >= 3) break
    }
  }
  
  # Strategy 2: Try without affiliation (always)
  log_line("Enhanced query (no affil):", name_term)
  
  result_no_affil <- existing_do_query_function(name_term, FALSE, source)
  if (!is.null(result_no_affil) && result_no_affil$verified_total > best_score) {
    best_result <- result_no_affil
    best_score <- result_no_affil$verified_total
  }
  
  # Strategy 3: Try fallback affiliation if available and primary didn't work well
  if (use_affil_fallback && best_score < 2 && length(affil_variants) > 1) {
    for (variant_name in names(affil_variants)) {
      if (variant_name == "none") next
      
      affil_block <- affil_variants[[variant_name]]
      term_with_affil <- if (nzchar(affil_block)) paste(name_term, "AND", affil_block) else name_term
      
      log_line("Enhanced query (fallback):", term_with_affil)
      
      result <- existing_do_query_function(term_with_affil, TRUE, source)
      if (!is.null(result) && result$verified_total > best_score) {
        best_result <- result
        best_score <- result$verified_total
      }
    }
  }
  
  # Return best result or empty result
  if (!is.null(best_result)) {
    return(best_result)
  } else {
    return(list(
      term_used = name_term,
      raw_n = 0L,
      verified_total = 0L,
      verified_first = 0L,
      verified_last = 0L,
      pmids_verified = character(0),
      pmids_firstauthor = character(0),
      pmids_lastauthor = character(0),
      pmids_ambiguous = character(0),
      used_affil = FALSE
    ))
  }
}

# PATCH: Enhanced decision tree function
enhanced_run_decision_tree <- function(row, existing_functions) {
  last_norm  <- toupper(ifelse(is.na(row$last_name_norm), "", row$last_name_norm))
  first_tok  <- toupper(ifelse(is.na(row$first_name_token), "", row$first_name_token))
  middle_tok <- toupper(ifelse(is.na(row$middle_name_token), "", row$middle_name_token))
  risk       <- row$risk_surname
  
  city  <- toupper(ifelse(is.na(row$city_token),  "", row$city_token))
  state <- toupper(ifelse(is.na(row$state_token), "", row$state_token))
  org   <- toupper(ifelse(is.na(row$org_token),   "", row$org_token))
  
  # NEW: Enhanced affiliation analysis
  affiliation_analysis <- enhanced_affiliation_analysis(city, state, org)
  
  log_line("Affiliation analysis for", row$NPI, ":", 
           "strategy=", affiliation_analysis$strategy,
           "score=", affiliation_analysis$score,
           "city=", affiliation_analysis$has_city,
           "state=", affiliation_analysis$has_state,
           "org=", affiliation_analysis$has_org)
  
  date_filter  <- existing_functions$.build_date_filter(row$EnumerationDate)
  allowed_set  <- existing_functions$build_allowed_author_set(last_norm, first_tok, middle_tok)
  
  # Aggregators to expose the bottleneck explicitly
  affil_raw_max <- 0L;  affil_ver_max <- 0L
  noaff_raw_max <- 0L;  noaff_ver_max <- 0L
  
  strict_fm <- row$name_strict_fm
  relaxed_f <- row$name_relaxed_f
  
  # NEW: Enhanced query strategy
  # 1) FAU-first with enhanced affiliation handling
  fau_variants <- existing_functions$build_fau_variants(last_norm, first_tok, middle_tok)
  for (fau_full in fau_variants) {
    res <- enhanced_do_query(fau_full, affiliation_analysis, source = "FAU", existing_functions$do_query)
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
      return(c(list(
        match_tier = 1, 
        ambiguous_reason = NA_character_,
        raw_n_with_affil = affil_raw_max, 
        raw_n_without_affil = noaff_raw_max,
        verified_with_affil_total = affil_ver_max,
        verified_without_affil_total = noaff_ver_max,
        affiliation_strategy = affiliation_analysis$strategy,
        affiliation_score = affiliation_analysis$score
      ), res))
    }
  }
  
  # 2) AU with enhanced affiliation handling
  res <- enhanced_do_query(strict_fm, affiliation_analysis, source = "AU", existing_functions$do_query)
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
    return(c(list(
      match_tier = 2, 
      ambiguous_reason = NA_character_,
      raw_n_with_affil = affil_raw_max, 
      raw_n_without_affil = noaff_raw_max,
      verified_with_affil_total = affil_ver_max,
      verified_without_affil_total = noaff_ver_max,
      affiliation_strategy = affiliation_analysis$strategy,
      affiliation_score = affiliation_analysis$score
    ), res))
  }
  
  # 3) Relaxed AU with enhanced affiliation handling
  res <- enhanced_do_query(relaxed_f, affiliation_analysis, source = "AU", existing_functions$do_query)
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
    return(c(list(
      match_tier = 3, 
      ambiguous_reason = NA_character_,
      raw_n_with_affil = affil_raw_max, 
      raw_n_without_affil = noaff_raw_max,
      verified_with_affil_total = affil_ver_max,
      verified_without_affil_total = noaff_ver_max,
      affiliation_strategy = affiliation_analysis$strategy,
      affiliation_score = affiliation_analysis$score
    ), res))
  }
  
  # NEW: Enhanced final classification
  final_reason <- enhanced_classify_ambiguity(
    max(affil_raw_max, noaff_raw_max), 
    max(affil_ver_max, noaff_ver_max), 
    affiliation_analysis
  )

  list(
    match_tier = NA_integer_, 
    term_used = NA_character_, 
    raw_n = 0L,
    verified_total = 0L, 
    verified_first = 0L, 
    verified_last = 0L,
    pmids_verified = character(0), 
    pmids_firstauthor = character(0), 
    pmids_lastauthor = character(0),
    ambiguous_reason = final_reason,
    raw_n_with_affil = affil_raw_max, 
    raw_n_without_affil = noaff_raw_max,
    verified_with_affil_total = affil_ver_max,
    verified_without_affil_total = noaff_ver_max,
    affiliation_strategy = affiliation_analysis$strategy,
    affiliation_score = affiliation_analysis$score
  )
}

# PATCH: How to integrate this into your existing script
# 1. Add these functions to your script
# 2. Replace the existing classify_ambiguity function with enhanced_classify_ambiguity
# 3. Replace the existing run_decision_tree function with enhanced_run_decision_tree
# 4. Add the new columns to your output: affiliation_strategy, affiliation_score

cat(
  "ZEROHITS_NOAFFIL_FIX.R loaded. This patch provides:\n",
  "- Enhanced affiliation analysis and scoring\n",
  "- Multi-tier affiliation query strategies\n",
  "- Better handling of missing affiliation data\n",
  "- More nuanced ambiguity classification\n",
  "- Detailed affiliation diagnostics\n",
  "To use: integrate these functions into your main script\n",
  sep = ""
)
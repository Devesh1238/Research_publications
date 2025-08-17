# PubMed Query Script Patches Applied

## Summary
Successfully applied three surgical patches to `03_run_pubmed_smart_query_v2_http.R` to implement FAU-first querying and FAU Gate filtering for improved precision in author matching.

## Patches Applied

### PATCH 1: FAU Gate Helpers (Lines 213-270)
**Location**: Added after existing helper functions in the matching & scoring helpers section

**New Functions Added**:
- `.VERY_COMMON_LAST`: List of common surnames requiring stricter matching
- `.jw()`: Jaro-Winkler similarity wrapper
- `.best_fau_match()`: Finds best FAU candidate with same last name and first initial
- `.fau_gate_pass()`: Main gate function that decides pass/fail for records

**Purpose**: Provides strict name similarity checking to filter out false positives, especially for common surnames like "WANG", "KIM", "LEE", etc.

### PATCH 2: FAU Gate Integration (Lines 432-438)
**Location**: Inside the `do_query()` function's record processing loop

**Change**: Added FAU Gate check before existing scoring logic:
```r
# ---- [PATCH 2] FAU Gate (ADD THIS BLOCK) --------------------------------
# Reject AU-derived candidates unless a FAU in this record looks like our surgeon
if (!.fau_gate_pass(fau, last_norm, first_tok, source, risk)) {
  if (!is.na(pmid)) pmids_fail <- c(pmids_fail, pmid)
  next
}
# ---- [END PATCH 2] -------------------------------------------------------
```

**Purpose**: Filters records early in the process, rejecting those that don't pass the FAU Gate before expensive scoring calculations.

### PATCH 3: FAU-First Tier Reordering (Lines 461-498)
**Location**: Replaced the existing tier logic in `run_decision_tree()`

**New Tier Structure**:
1. **Tier 1**: FAU + Affiliation + Date window (most precise)
2. **Tier 2**: FAU only + Date window (no affiliation constraint)
3. **Tier 3**: AU with affiliation + FAU Gate filtering
4. **Tier 4**: AU without affiliation (STD risk only, ≤5 results)

**Purpose**: Prioritizes precise FAU queries over broader AU queries, significantly reducing false positives.

## Function Signature Changes

### `do_query()` Function
**Before**: `do_query(name_term, use_affil)`
**After**: `do_query(name_term, use_affil, source = "AU")`

**New Parameter**: `source` - indicates whether query originated from FAU or AU search, affecting gate strictness.

## Key Benefits

### For "Rosalind Kaplan" Problem
- **Tier 1/2**: Direct FAU query `"KAPLAN ROSALIND"[FAU]` will only return Rosalind's papers
- **Result**: Robert Kaplan papers completely eliminated from consideration

### For "Jennifer vs Jing Wang" Problem  
- **FAU Gate**: Rejects records where FAU names don't closely match "JENNIFER" (Jaro-Winkler < 0.93/0.95)
- **Common Surname Handling**: Extra strict threshold (0.95) for "WANG" and other common surnames
- **Result**: "Jing", "Jiahao", "Jinjiao" variants filtered out early

## Backward Compatibility
- All existing configuration variables preserved
- Existing helper functions unchanged
- Same input/output CSV format
- `run_phase2_http()` function signature unchanged

## Files Created
- `03_run_pubmed_smart_query_v2_http.R` - Updated script with patches
- `03_run_pubmed_smart_query_v2_http.R.bak` - Backup of original
- `PATCH_SUMMARY.md` - This documentation

## Usage
The script maintains the same usage pattern:
```r
# Set API key
Sys.setenv(NCBI_API_KEY="your_key_here")

# Run the pipeline
result <- run_phase2_http()
```

The FAU-first approach and gate filtering will now automatically provide much higher precision in author matching while maintaining the same external interface.
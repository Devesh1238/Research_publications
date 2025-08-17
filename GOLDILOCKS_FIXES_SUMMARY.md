# Goldilocks Fixes - Solving the Zero Results Problem

## 🎯 **Problem Analysis**
After implementing the FAU Gate, we successfully eliminated false positives (no more "J Wang" issues) but created the opposite problem - too many zeros due to overly strict filtering.

### **Root Causes Identified:**
1. **Critical Bugs**: `modal_first` NA values causing `"missing value where TRUE/FALSE needed"` errors
2. **FAU Gate Too Strict**: Single threshold (0.93/0.95) for all contexts
3. **Poor Tier 3 Recovery**: AU + affiliation queries failing due to strict gating
4. **Insufficient Error Handling**: Crashes preventing legitimate matches

## 🛠️ **Goldilocks Fixes Applied**

### **GOLDILOCKS FIX 1: Safe NA Handling**
**Location**: `verify_record_score()` function
**Problem**: NA values in `modal_first` causing crashes
**Solution**: 
- Added comprehensive NA filtering: `fnames[nzchar(fnames) & !is.na(fnames)]`
- Safe `init_ok()` function with length and NA checks
- Robust similarity calculations with proper NA handling
- Safe fallback: `if (length(sims) > 0) max(sims, 0, na.rm = TRUE) else 0`

### **GOLDILOCKS FIX 2: Graduated FAU Gate**
**Location**: `.fau_gate_pass()` function  
**Problem**: Single strict threshold for all contexts
**Solution**: **Tier-specific thresholds**
- **Tier 3 (AU + Affiliation)**: More lenient (0.83/0.88)
- **Tier 4 (AU no affiliation)**: Strict (0.93/0.95)
- **Common surnames**: Extra strict (+0.05 to base threshold)
- **Context-aware**: Strong affiliation = lower name requirements

```r
if (identical(tier, "AU_WITH_AFFIL")) {
  # Tier 3: More lenient for AU + Affiliation
  base_thr <- if (lastU %in% .VERY_COMMON_LAST || identical(risk_last, "HIGH")) 0.88 else 0.83
} else {
  # Tier 4: Stricter for AU without affiliation  
  base_thr <- if (lastU %in% .VERY_COMMON_LAST || identical(risk_last, "HIGH")) 0.95 else 0.93
}
```

### **GOLDILOCKS FIX 3: Enhanced Tier Structure**
**Location**: `run_decision_tree()` function
**Problem**: Poor recovery at Tier 3, insufficient fallbacks
**Solution**: **Multi-tier recovery strategy**

1. **Tier 1-2**: FAU queries (unchanged, work well)
2. **Tier 3**: AU + Affiliation with **LENIENT** FAU Gate (0.83/0.88)
3. **Tier 3.5**: **NEW** - Expanded recovery for strong affiliation contexts
4. **Tier 4**: AU without affiliation - **STRICT** gate (0.93/0.95)

**Key Innovation - Tier 3.5**:
```r
# Tier 3.5: EXPANDED AU + Affiliation recovery for edge cases
if (has_affil && nchar(affil_blk) > 20) {  # Strong affiliation context
  permissive_au <- sprintf('%s %s[AU]', last_norm, substr(first_tok, 1, 1))
  res <- try(do_query(permissive_au, use_affil = TRUE, source = "AU", tier_name = "AU_WITH_AFFIL"), silent = TRUE)
  if (!inherits(res, "try-error") && res$verified_total > 0 && res$verified_total <= 20) {
    return(c(list(match_tier = 3), res))
  }
}
```

### **GOLDILOCKS FIX 4: Comprehensive Error Handling**
**Location**: Main execution loop
**Problem**: Crashes preventing legitimate matches
**Solution**: 
- **Pre-validation**: Check essential fields before processing
- **Safe execution**: `try()` blocks around all tier queries
- **Better diagnostics**: Enhanced error messages and logging
- **Graceful degradation**: Continue processing even after errors

## 🎯 **Expected Outcomes**

### **The Goldilocks Zone**: Not too strict, not too loose

1. **Bug Resolution**: No more `"missing value where TRUE/FALSE needed"` errors
2. **Tier 3 Recovery**: AU + affiliation should now work much better with lenient gate (0.83 vs 0.93)
3. **Strong Affiliation Boost**: Tier 3.5 catches edge cases with good institutional context
4. **Maintained Precision**: Tier 4 still strict to avoid false positives
5. **Better Diagnostics**: Clear error messages for debugging

### **Expected Result Patterns**:
- **More Tier 1-2 hits**: FAU queries working well
- **Significantly more Tier 3 hits**: Lenient gate allows legitimate AU + affiliation matches
- **Some Tier 3.5 hits**: Edge cases with strong affiliation context
- **Fewer Tier 4 attempts**: Better recovery upstream
- **Fewer errors**: Robust NA handling prevents crashes

## 🔍 **Key Threshold Changes**

| Context | Old Threshold | New Threshold | Impact |
|---------|---------------|---------------|---------|
| AU + Affiliation (Tier 3) | 0.93/0.95 | **0.83/0.88** | +12% more lenient |
| AU no Affiliation (Tier 4) | 0.93/0.95 | 0.93/0.95 | Unchanged (strict) |
| FAU queries | 0.85 | 0.85 | Unchanged |

## 🧪 **Testing Strategy**
The fixes should be tested on your 100-surgeon cohort to validate:
1. **Error reduction**: Fewer `"Error_Query"` entries
2. **Tier 3 recovery**: More matches at Tier 3 with verified_n_total > 0  
3. **Maintained precision**: No return of "J Wang" style false positives
4. **Overall coverage**: Higher percentage of surgeons with verified_n_total > 0

This represents a **surgical precision fix** - targeted changes that address the specific issues while maintaining the gains from the original FAU Gate implementation.
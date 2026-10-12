# Run an R reference script and save the result of every top-level svyby/svytotal/
# svymean/svyratio/svyquantile/svyglm call to <out_prefix>_NN_<fn>.csv, for comparison
# with the Python port using compare_to_r.py.
#
# Usage: Rscript run_r_reference.R <script.R> <meps_dir> <out_prefix>
#  - "C:/MEPS" paths in the script are replaced by <meps_dir>
#  - install.packages()/install_github() calls are skipped
args <- commandArgs(trailingOnly = TRUE)
src <- readLines(args[1])
src <- gsub("C:/MEPS", args[2], src, fixed = TRUE)
src <- gsub("C:\\\\MEPS", args[2], src)
src <- src[!grepl("install.packages|install_github", src)]
.captured <- list(); .depth <- 0
.wrap <- function(fname) {
  f <- getExportedValue("survey", fname)
  function(...) {
    .depth <<- .depth + 1; on.exit(.depth <<- .depth - 1)
    r <- f(...)
    if (.depth == 1) .captured[[length(.captured) + 1]] <<- list(fn = fname, res = r)
    r
  }
}
for (fn in c("svyby", "svytotal", "svymean", "svyratio", "svyquantile", "svyglm")) assign(fn, .wrap(fn))
exprs <- parse(text = src)
t0 <- Sys.time()
for (e in exprs) { r <- withVisible(eval(e, globalenv())); if (r$visible) print(r$value) }
cat("Elapsed:", format(Sys.time() - t0), "\n")
for (i in seq_along(.captured)) {
  cp <- .captured[[i]]; res <- cp$res
  out <- tryCatch({
    if (inherits(res, "svyby")) {
      df <- as.data.frame(res); df
    } else if (inherits(res, "svyglm")) {
      s <- summary(res)$coefficients; data.frame(term = rownames(s), s, check.names = FALSE)
    } else {
      data.frame(term = names(coef(res)), estimate = as.numeric(coef(res)), se = as.numeric(SE(res)))
    }
  }, error = function(e) NULL)
  if (!is.null(out)) write.csv(out, sprintf("%s_%02d_%s.csv", args[3], i, cp$fn), row.names = FALSE)
}

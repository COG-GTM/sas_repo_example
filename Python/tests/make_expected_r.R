library(survey); options(survey.lonely.psu="adjust")
df <- read.csv("synthetic_design.csv")
d <- svydesign(id=~VARPSU, strata=~VARSTR, weights=~WT, data=df, nest=TRUE)
fmt <- function(x, stat, by, sub) { out <- data.frame(stat=stat, by=by, subpop=sub, level=as.character(if (by=="none") "all" else x[[1]]), estimate=as.numeric(coef(x)), se=as.numeric(SE(x))); rownames(out)<-NULL; out }
res <- rbind(
  fmt(svytotal(~Y, d), "total", "none", "none"),
  fmt(svymean(~Y, d), "mean", "none", "none"),
  { r <- svyratio(~Y, ~X, d); data.frame(stat="ratio", by="none", subpop="none", level="all", estimate=as.numeric(coef(r)), se=as.numeric(SE(r))) },
  fmt(svyby(~Y, ~GRP, d, svytotal), "total", "GRP", "none"),
  fmt(svyby(~Y, ~GRP, d, svymean), "mean", "GRP", "none"),
  fmt(svyby(~Y, ~GRP, subset(d, FLAG==1), svytotal), "total", "GRP", "FLAG"),
  fmt(svyby(~Y, ~GRP, subset(d, FLAG==1), svymean), "mean", "GRP", "FLAG"),
  { r <- svyby(~Y, ~GRP, denominator=~X, subset(d, FLAG==1), svyratio); data.frame(stat="ratio", by="GRP", subpop="FLAG", level=as.character(r$GRP), estimate=as.numeric(coef(r)), se=as.numeric(SE(r))) }
)
rownames(res) <- NULL
write.csv(res, "synthetic_expected_r.csv", row.names=FALSE)
print(res)

# Quantiles (svyquantile defaults: qrule="math", interval.type="mean", df=degf(design))
q_all <- svyquantile(~Y, d, quantiles=0.5)$Y
q_by <- svyby(~Y, ~GRP, subset(d, FLAG==1), svyquantile, quantiles=0.5)
q <- rbind(
  data.frame(by="none", level="all", estimate=q_all[1, "quantile"], se=q_all[1, "se"]),
  data.frame(by="GRP", level=as.character(q_by$GRP), estimate=q_by$Y, se=q_by$se.Y)
)
rownames(q) <- NULL
write.csv(q, "synthetic_expected_quantile_r.csv", row.names=FALSE)
print(q)


# formatting and scaling
reverse <- function(x){
  x <- (max(x, na.rm=T)-x)-max(x, na.rm=T)
}
normalize <- function(x){
  x <- (x-min(x, na.rm=T))/(max(x, na.rm=T)-min(x, na.rm=T))
  return(x)
}
standardize <- function(x){
  x <- (x-mean(x, na.rm=T))/sd(x, na.rm=T)
  return(x)
}
rankify <- function(x){
  qfunc <- ecdf(x)
  return(qfunc(x))
} 

# derive one tsFA
signal <- function(x){
  x <- data.frame(x)
  
  factors = tsfa::estTSFmodel(as.matrix(x), 1)
} 

# derive one cross sectional factor
factors <- function(x){
  x <- data.frame(x)
  factors = psych::fa(as.matrix(x), 1)
  return(factors$scores) 
} 

# functions for modeling
slice.time <- function(dates, tr.dates, ts.dates, fixedWindow=T){
  dates = unique(dates)
  slicepoints <- dates[order(dates)] 
  
  rows <- length(slicepoints)
  steps  <- floor((rows-tr.dates)/ts.dates)
  push <- rows-(tr.dates+(steps*ts.dates))+1
  slicepoints <- slicepoints[push:rows] ### pushing forward
  
  tmsl <- caret::createTimeSlices(y=slicepoints, initialWindow=tr.dates, horizon=ts.dates, fixedWindow = T, ts.dates-1)
  ceil <- length(tmsl$train)
  return(list(tmsl=tmsl, slicepoints=slicepoints, ceil=ceil))
} 

# takes training df as input and generates training timing parametres
timing <- function(train){
  cvs <- ceiling((1.2)^log(nrow(train)))
  rows <- nrow(train) 
  dyn.time <- max(c(floor(rows/cvs)), na.rm=T)
  skips <- floor((rows-dyn.time)/cvs)
  push <- rows-(dyn.time+(cvs*skips))+1
  return(list(cvs=cvs, dyn.time=dyn.time, skips=skips, push=push))
} 

# golden cut
goldn.it <- function(data, x1, x2, y, sample=.05, cens=2, mid.1=NULL, mid.2=NULL, ylim=c(-1, 1)){
  d <- na.omit(data.table::data.table(data)[,c(x1, x2, y), with=F])
  d <- d[!is.infinite(d[[x1]]) & !is.infinite(d[[x2]]),]
  
  if (sd(d[[x1]], na.rm=T)<.66){r.1 <- 1} else {r.1 <- 0} 
  if (sd(d[[x2]], na.rm=T)<.66){r.2 <- 1} else {r.2 <- 0} 
  
  if (sd(d[[x1]], na.rm=T)<.1){r.1 <- 2} 
  if (sd(d[[x2]], na.rm=T)<.1){r.2 <- 2} 
  
  if (sd(d[[x1]], na.rm=T)<.01){r.1 <- 3} 
  if (sd(d[[x2]], na.rm=T)<.01){r.2 <- 3} 
  
  if (is.null(mid.1)){mid.1 <- round(mean(d[[x1]], na.rm=T), r.1)}  
  if (is.null(mid.2)){mid.2 <- round(mean(d[[x2]], na.rm=T), r.2)}  
  print(paste0("mid 1 / 2: ", mid.1, " / ", mid.2))
  
  d[get(x1)<mid.1 & get(x2)<mid.2, LL := get(x1)-get(x2)]   
  d[get(x1)<mid.1 & get(x2)>mid.2, LH := get(x1)-get(x2)]   
  d[get(x1)>mid.1 & get(x2)<mid.2, HL := get(x1)-get(x2)]   
  d[get(x1)>mid.1 & get(x2)>mid.2, HH := get(x1)-get(x2)]   
  
  d$region <- NA 
  d[, region := ifelse(as.numeric(!is.na(LL))==1, "1.LL", region)]   
  d[, region := ifelse(as.numeric(!is.na(LH))==1, "2.LH", region)]   
  d[, region := ifelse(as.numeric(!is.na(HL))==1, "3.HL", region)]   
  d[, region := ifelse(as.numeric(!is.na(HH))==1, "4.HH", region)]   
  print(table(d$region))
  
  try(m <- glm(as.formula(paste0(y, "~region")), data=d))
  
  p <- list()
  try(p$LL <- graph.it(d[abs(LL-round(mean(LL, na.rm=T)))<cens], "LL", y, sample, print=F, ylim=ylim))
  try(p$LH <- graph.it(d[abs(LH-round(mean(LH, na.rm=T)))<cens], "LH", y, sample, print=F, ylim=ylim))
  try(p$HL <- graph.it(d[abs(HL-round(mean(HL, na.rm=T)))<cens], "HL", y, sample, print=F, ylim=ylim))
  try(p$HH <- graph.it(d[abs(HH-round(mean(HH, na.rm=T)))<cens], "HH", y, sample, print=F, ylim=ylim, 
                       note = paste0("\n", x1, "-", x2)))
  
  plot <- ggpubr::ggarrange(plotlist=p)
  print(plot)
  try(print(round(coef(summary(m)), 3)))
} 

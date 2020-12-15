
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

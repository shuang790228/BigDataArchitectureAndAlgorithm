predict.naiveBayes <- function(object,
                               newdata,
                               type = c("class", "raw"),
                               threshold = 0.001,
                               ...) {
    type <- match.arg(type)
    newdata <- as.data.frame(newdata)
    attribs <- match(names(object$tables), names(newdata))
    isnumeric <- sapply(newdata, is.numeric)
    newdata <- data.matrix(newdata)
    L <- sapply(1:nrow(newdata), function(i) {
        ndata <- newdata[i, ]
        L <- log(object$apriori) + apply(log(sapply(seq_along(attribs),
            function(v) {
                nd <- ndata[attribs[v]]
                if (is.na(nd)) rep(1, length(object$apriori)) else {
                
                  if (dim(object$tables[[v]])[2] < 2) {
                 	 prob<-vector(mode="numeric",length=0)
					for(i in 1:dim(msd)[1])
					{
  						prob[i] <- 0
					} 
                  	
                  	
                  } else {
                  	prob <- if (isnumeric[attribs[v]]) {
                   	 	msd <- object$tables[[v]]
                    	msd[, 2][msd[, 2] == 0] <- threshold
                    	dnorm(nd, msd[, 1], msd[, 2])
                  	} 	else object$tables[[v]][, nd]
                  }
                 	prob[prob == 0] <- threshold
                	prob
                }
                
                
                
            })), 1, sum)
        if (type == "class")
            L
        else {
            ## Numerically unstable:
            ##            L <- exp(L)
            ##            L / sum(L)
            ## instead, we use:
            sapply(L, function(lp) {
                1/sum(exp(L - lp))
            })
        }
    })
    if (type == "class")
        factor(object$levels[apply(L, 2, which.max)], levels = object$levels)
    else t(L)
}
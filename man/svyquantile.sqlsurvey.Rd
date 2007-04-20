\name{svyquantile.sqlsurvey}
\alias{svyquantile.sqlsurvey}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{Probability-weighted quantiles}
\description{
Estimates quantiles from data in a \code{sqlsurvey} object. 
}
\usage{
svyquantile.sqlsurvey(x, design, quantiles, build.index = FALSE, ...)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{x}{Model formula specifying variables }
  \item{design}{\code{sqlsurvey} object }
  \item{quantiles}{Quantiles to be estimated }
  \item{build.index}{Build a temporary index (may increase speed) }
  \item{\dots}{For future expansion}
}
\details{
Currently this function does not compute confidence intervals, and
requires \code{quantiles} to be a single number. Both limitations will
be relaxed in future versions.
}
\value{
 A vector of quantiles.
}
\references{ }

\seealso{\code{\link{sqlsurvey}}}
\examples{
sqclus1<-sqlsurvey(id="dnum", fpc="fpc", weights="pw", strata="fpc",
   data=system.file("apiclus1.db",package="surveyNG"),
   table.name="clus1", key="snum")

sqclus1
svyquantile(~api99+api00, design=sqclus1, quantiles=0.5)
close(sqclus1)

}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{survey}


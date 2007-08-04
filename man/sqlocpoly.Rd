\name{sqlocpoly}
\alias{sqlocpoly}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{Smoothing}
\description{
This function performs weighted density estimation and smoothing, using the binned local polynomial smoothers in the \code{KernSmooth} package.
}
\usage{
sqlocpoly(formula, design, bandwidth, M = 401)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{formula}{One-sided formula for density estimation, two-sided for smoothing }
  \item{design}{ \code{sqlsurvey} object }
  \item{bandwidth}{ Smoothing bandwidth }
  \item{M}{Number of grid points}
}
\details{
The binning is performed in SQL, so for large data sets the computation time is roughly linear in \code{M} and in the number of observations. \code{M} should be a few times larger than \code{range(x)/bandwidth}, but can often be reduced from the default value.
}
\value{
  An object of class \code{svysmooth}
}

\seealso{\code{\link{sqlsurvey}} }
\examples{
sqclus1<-sqlsurvey(id="dnum", fpc="fpc", weights="pw", strata="fpc",
   data=system.file("apiclus1.db",package="surveyNG"),
   table.name="clus1", key="snum")

dens <- sqlocpoly(~api00, sqclus1, bandwidth=50)
smooth <- sqlocpoly(api00~api99, sqclus1, bandwidth=50)

dens
plot(dens)
plot(smooth)

close(sqclus1)
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{survey}
\keyword{hplot}% __ONLY ONE__ keyword per line

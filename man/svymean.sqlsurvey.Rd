\name{svymean.sqlsurvey}
\alias{svymean.sqlsurvey}
\alias{svytotal.sqlsurvey}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{Means and totals}
\description{
Horvitz-Thompson estimator of means and totals for a database-backed
survey object. Computes cell proportions and counts for factor variables.
}
\usage{
\method{svymean}{sqlsurvey}(x, design, na.rm = FALSE, byvar = NULL,
se = FALSE, keep.estfun = FALSE, ...)
\method{svytotal}{sqlsurvey}(x, design, na.rm = FALSE, byvar = NULL,
se = FALSE, keep.estfun = FALSE, ...)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{x}{model formula }
  \item{design}{\code{sqlsurvey} object }
  \item{na.rm}{Remove missing values (treat as domain)}
  \item{byvar}{Formula with grouping variables}
  \item{se}{Compute variance-covariance matrix?}
  \item{keep.estfun}{Keep the SQL table of estimating functions after
    the standard errors are computed.}
  \item{\dots}{For future expansion }
}
\value{
  A data frame with a column for each variable and a row for each group,
  with the covariance matrix in the \code{"var"} attribute.
}

\seealso{ \code{\link{sqlmodelmatrix}}}
\examples{
sqclus1<-sqlsurvey(id="dnum", fpc="fpc", weights="pw", strata="fpc",
   data=system.file("apiclus1.db",package="surveyNG"),
   table.name="clus1", key="snum")

sqclus1
svymean(~api99+stype, design=sqclus1)
svytotal(~enroll+stype, design=subset(sqclus1, api99 > 500))
close(sqclus1)

}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{survey}


\name{svylm}
\alias{svylm}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{Linear regression models}
\description{
Fits a linear regression model to a \code{sqlsurvey} object using SQL code.
}
\usage{
svylm(formula, design)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{formula}{Model formula }
  \item{design}{\code{sqlsurvey} object }
}
\value{
 Vector of regression coefficients with attribute \code{"var"} containing variance matrix.
}
\seealso{ \code{\link{sqlmodelmatrix}}}
\examples{
sqclus1<-sqlsurvey(id="dnum", fpc="fpc", weights="pw", strata="fpc",
   data=system.file("apiclus1.db",package="surveyNG"),
   table.name="clus1", key="snum")
svylm(api00~api99+stype*comp_imp, design=sqclus1)
close(sqclus1)
}
\keyword{survey}

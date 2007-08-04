\name{sqlhexbin}
\alias{sqlhexbin}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{Hexagonal binning}
\description{
Draws a scatterplot in which points are replaced by hexagons indicating the population weight in regions of the plot. Requires the \code{"hexbin"} package from Bioconductor
}
\usage{
sqlhexbin(formula, design, xlab = NULL, ylab = NULL, ..., chunksize = 5000)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{formula}{Formula specifying the two variables to plot }
  \item{design}{\code{sqlsurvey} object }
  \item{xlab}{x-axis label }
  \item{ylab}{y-axis label }
  \item{\dots}{Other arguments to \code{gplot.hexbin} }
  \item{chunksize}{Number of observations to fetch at a time }
}
\details{
Unlike most functions for \code{sqlsurvey} objects, this fetches all observations for the specified variables, and so would be slow over a network link.
}
\value{
  A object of S4 class \code{hexbin}, invisibly.
}
\references{ Carr, D. B. et al. (1987) Scatterplot Matrix Techniques for Large N. JASA 83, 398, 424-436. }

\seealso{\code{\link{sqlsurvey}}, \code{\link{sqlocpoly}}}
\examples{
\dontrun{
sqclus1<-sqlsurvey(id="dnum", fpc="fpc", weights="pw", strata="fpc",
   data=system.file("apiclus1.db",package="surveyNG"),
   table.name="clus1", key="snum")

sqlhexbin(api00~api99, design=sqclus1,style="centroid")-

close(sqclus1)
}
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{survey}
\keyword{hplot}% __ONLY ONE__ keyword per line

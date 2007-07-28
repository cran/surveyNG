\name{sqlsurvey}
\alias{sqlsurvey}
\alias{open.sqlsurvey}
\alias{close.sqlsurvey}
\alias{dim.sqlsurvey}
\alias{open.sqlmodelmatrix}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{Survey design based on SQL database}
\description{
Specifies a survey data set based on a connection to a SQL (currently SQLite)
database.  The data may be in an existing database or in a data frame
that will be written out to a database. If data are in an existing
database they should be in a single table in denormalized form.
}
\usage{
sqlsurvey(id, strata = NULL, weights = NULL, fpc = "0", data,
table.name = basename(tempfile("_tbl_")), key = "row_names")
\method{close}{sqlsurvey}(con, tidy=TRUE, ...)
\method{open}{sqlsurvey}(con, db=NULL,...)
\method{open}{sqlmodelmatrix}(con, design,...)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{id}{vector of strings with names of sampling unit identifiers}
  \item{strata}{vector of strings with names of stratum identifies }
  \item{weights}{string with name of weighting variable }
  \item{fpc}{string with name of population size variable (variable may
    be zero for sampling with replacement or infinite population size)}
  \item{data}{Either a data frame or the name of a SQLite database file}
  \item{table.name}{Name for the data table containing the survey data
    and design variables}
  \item{key}{name of a variable that can be used as a unique key }
  \item{con}{survey object or survey model matrix object to be
    disconnected from or reconnected to the database}
  \item{tidy}{Clear any pending results?}
  \item{db}{An existing connection (optional, allows multiple objects to
    be reconnected to the same database connection)}
  \item{design}{A \code{sqlsurvey} object with an active database
    connection}
  \item{...}{For future expansion}
}

\value{
  \code{sqlsurvey} returns an object of class \code{sqlsurvey}
  
}

\seealso{}
\examples{
library(survey)
data(api)
sqclus2<-sqlsurvey(id=c("dnum","snum"), fpc=c("fpc1","fpc2"),
  weights="pw", data=apiclus2)
sqclus2
svymean(~api99+api00, design=sqclus2)

sqclus1<-sqlsurvey(id="dnum", fpc="fpc", weights="pw", strata="fpc",
   data=system.file("apiclus1.db",package="surveyNG"),
   table.name="clus1", key="snum")

sqclus1
svymean(~api99+api00, design=sqclus1)
close(sqclus1)
close(sqclus2)

}

\keyword{survey }


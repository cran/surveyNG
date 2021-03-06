

make.zdata<-function(db, table, factors=9){
  rval<-dbGetQuery(db,sqlsubst("select * from %%tbl%% limit 1",
                            list(tbl=table)))
  if (length(factors)==0) return(rval[FALSE,])
  rval<-as.list(rval) 	## lists are faster
  if(is.character(factors)){
    for(f in factors){
      levs<- dbGetQuery(db,sqlsubst("select distinct %%v%% from %%tbl%% order by %%v%%",
                        list(v=f,tbl=table)))[[1]]
      rval[[f]]<-factor(rval[[f]],  levels=levs)
    }
    class(rval)<-"data.frame"
    return(rval[FALSE,])
  } else { ##numeric limit on levels
    for(f in names(rval)){
      if (!is.numeric(factors) || length(factors)>1)
        stop("invalid specification of 'factors'")
      levs<- dbGetQuery(db,
                        sqlsubst("select distinct %%v%% from %%tbl%% order by %%v%% limit %%n%%",
                                 list(v=f, tbl=table, n=factors+2)))[[1]]
      if (length(na.omit(levs))<=factors)
        rval[[f]]<-factor(rval[[f]],  levels=levs)
    }
    class(rval)<-"data.frame"
    return(rval[FALSE,])
  }
  
}

close.sqlsurvey<-function(con, tidy=TRUE,...){
  gc() ## make sure any dead model matrices are finalized.
  if(!isIdCurrent(con$conn)){
    warning("connection already closed")
    return(TRUE)
  }
  if (tidy){
    pending<-dbListResults(con$conn)
    if (length(pending))
      lapply(pending, dbClearResult)
  }
  dbDisconnect(con$conn)
}

open.sqlsurvey<-function(con, db=NULL, ...){
  library(RSQLite)
  sqlite<-dbDriver("SQLite")
  if (is.null(con$dbname))
    stop("design used a temporary database")
  if (is.null(db)){
    if(!file.exists(con$dbname))
       stop("database file",con$dbname,"not found")
    con$conn<-dbConnect(sqlite, dbname=con$dbname)
  }else{
    if (isIdCurrent(db))
      con$conn<-db
    else
      stop("'db' is an expired connection")
  }
  if (!is.null(con$subset)){
    con$subset$conn<-con$conn
  }
  con
}

open.sqlmodelmatrix<-function(con, design,...){
  library(RSQLite)
  if (isIdCurrent(design$conn))
    con$conn<-design$conn
  else
    stop("design has an expired connection")
  con
}

finalizeSubset<-function(e){
  dbGetQuery(e$conn, sqlsubst("drop table %%tbl%%",list(tbl=e$table)))
}


linbin<-function(x,table,design,M=401,lim=NULL,y=NULL){
  if (M>800) stop("too many grid points")
  if (is.null(design$subset)){
    tablename<-table
    wtname<-design$weights
  } else {
    tablename<-sqlsubst(" %%tbl%% inner join %%subset%% using(%%key%%) ",
                        list(tbl=table, subset=design$subset$table, key=design$key))
    wtname<-design$subset$weights
  }
  if(is.null(lim)){
	xrange<-dbGetQuery(design$conn, sqlsubst("select min(%%x%%) as low, max(%%x%%) as up from %%tbl%%",
					list(x=x,tbl=tablename)))
	lim=c(xrange[1,"low"],xrange[1,"up"])
  }
  gridp<-seq(lim[1], lim[2], length=M)
  lownames<-paste("low",1:M,sep="")[-M]
  upnames<-paste("up",1:M, sep="")[-1]
  lowexpr<-paste("(%%x%% -",gridp[-M],")")
  upexpr<-paste("(",gridp[-M],"- %%x%%)")
  innames<-paste("in",1:M,sep="")[-M]
  bintable<-basename(tempfile("_bin_"))
  if (is.null(y)){
    query<-sqlsubst("create table %%new%% as select %%low%%, %%up%%, %%wt%%, %%x%% from %%old%%",
  		list(new=bintable, old=tablename, low=paste(lowexpr,lownames,sep=" as "),
   			up=paste(upexpr, upnames, sep=" as "), wt=wtname,x=x))
   } else {
    query<-sqlsubst("create table %%new%% as select %%low%%, %%up%%, %%wt%%, %%x%%, %%y%% from %%old%%",
  		list(new=bintable, old=tablename, low=paste(lowexpr,lownames,sep=" as "),
   			up=paste(upexpr, upnames, sep=" as "), wt=wtname,x=x,y=y))
   	}
  dbGetQuery(design$conn, query)
  on.exit(dbGetQuery(design$conn,paste("drop table ",bintable)),add=TRUE)
  dbGetQuery(design$conn, sqlsubst("create index %%idx%% on %%tbl%% (%%x%%)",
  	list(idx=basename(tempfile("_idx_")), x=x,tbl=bintable)))
  lbinname<-paste("lbin",1:M,sep="")[-M]
  rbinname<-paste("rbin",1:M,sep="")[-1]
  if (is.null(y)){
  	lbinquery<-paste("sum(",upnames,"*%%wt%%*(%%x%% between ",gridp[-M],"AND",gridp[-1],")) as ",lbinname)
  	rbinquery<-paste("sum(",upnames,"*%%wt%%*(%%x%% between ",gridp[-M],"AND",gridp[-1],")) as ",rbinname)
  } else{
  	lbinquery<-paste("sum(",upnames,"*%%wt%%*%%y%%*(%%x%% between ",gridp[-M],"AND",gridp[-1],")) as ",lbinname)
  	rbinquery<-paste("sum(",upnames,"*%%wt%%*%%y%%*(%%x%% between ",gridp[-M],"AND",gridp[-1],")) as ",rbinname)
  	}
  bins<-dbGetQuery(design$conn, sqlsubst("select %%lquery%%, %%rquery%% from %%bintable%%",
  	list(lquery=lbinquery, rquery=rbinquery, wt=wtname, tbl=tablename, 
       bintable=bintable, x=x, y= if(!is.null(y)) y else "")))
  bins<-as.vector(as.matrix(bins))
  bins<-bins[1:(M-1)]+bins[M:(2*M-2)]
  N<-dbGetQuery(design$conn, sqlsubst("select sum(%%wt%%) from %%tbl%%", 
     list(wt=wtname,tbl=tablename)))[[1]]
  
  list(grid=gridp, counts = -as.vector(as.matrix(bins))/diff(gridp), sumwt=N)
}


sqlocpoly<-function(formula, design, bandwidth, M=401){
	if (length(formula)==2){
	   x<-attr(terms(formula),"term.labels")
	   if(length(x)>1) stop('only one variable')
	   bins<-linbin(x, table=design$table, design=design,M=M)
	   xbinned<-with(bins,counts/(sumwt*diff(grid)))
	   rval<-list(locpoly(rep(1,M-1), xbinned ,binned=TRUE,
	   	bandwidth=bandwidth,range.x=range(bins$grid)))
	   attr(rval,"ylab")<-"Density"
	   names(rval)<-x
	} else {
	   if(length(formula[[3]])>1) stop('only one variable')
	   x<-attr(terms(formula),"term.labels")
	   if(length(x)>1) stop('only one variable')
	   xbins<-linbin(x, table=design$table, design=design,M=M)
	   y<-deparse(formula[[2]])
	   ybins<-linbin(x, table=design$table, design=design,M=M,y=y)
	   rval<-list(locpoly(xbins$counts, ybins$counts,binned=TRUE,bandwidth=bandwidth,
	     range.x=range(xbins$grid)))
	   names(rval)<-x
	   attr(rval,"ylab")<-y
   }
   class(rval)<-"svysmooth"
   attr(rval,"call") <- sys.call()
   attr(rval,"density") <- (length(formula)==2)
   rval
}

sqlexpr<-function(expr, design){
   nms<-new.env(parent=emptyenv())
   assign("%in%"," IN ", nms)
   assign("&", " AND ", nms)
   assign("|"," OR ", nms)
   out <-textConnection("str","w",local=TRUE)
   inorder<-function(e){
     if(length(e) ==1) {
       cat(e, file=out)
     } else if (length(e)==2){
       nm<-deparse(e)
       if (exists(nm, nms)) nm<-get(nms)
       cat(deparse(e), file=out)
     } else if (deparse(e[[1]])=="c"){
       cat("(", file=out)
       for(i in seq_len(length(e[-1]))) {
         if(i>1) cat(",", file=out)
         inorder(e[[i+1]])
       }
       cat(")", file=out)
     } else {
         cat("(",file=out)
         inorder(e[[2]])
         nm<-deparse(e[[1]])
         if (exists(nm,nms)) nm<-get(nm,nms)
         cat(nm,file=out)
         inorder(e[[3]])
         cat(")",file=out)
       }

   }
   inorder(expr)
   close(out)
   paste("(",str,")")

}

subset.sqlsurvey<-function(x,subset,...){

  subset<-substitute(subset)
  rval<-new.env()
  rval$subset<-sqlexpr(subset)

  rval$table<-basename(tempfile("_sbs_"))
  rval$idx<-basename(tempfile("_idx_"))
  rval$weights<-"_subset_weight_"
  query<-sqlsubst("create table %%tbl%% as select %%key%%, %%wt%%*%%subset%% as _subset_weight_ from %%base%%",
                  list(tbl=rval$table, key=x$key, wt=x$weights, subset=rval$subset, base=x$table )
                  )
  dbGetQuery(x$conn, query)
  dbGetQuery(x$conn,sqlsubst("create unique index %%idx%% on %%tbl%%(%%key%%)",
                             list(idx=rval$idx,tbl=rval$table, key=x$key)))
  rval$conn<-x$conn
  reg.finalizer(rval, finalizeSubset)
  x$subset<-rval
  x$call<-sys.call(-1)
  x
}

sqlsurvey<-function(id, strata=NULL, weights=NULL, fpc="0",
                       data, table.name=basename(tempfile("_tbl_")),
                       key="row_names"){
library(RSQLite)
sqlite<-dbDriver("SQLite")
if (is.character(data) && length(data)==1)
  db<-dbConnect(sqlite,dbname=data)
else{
  db<-dbConnect(sqlite)
  dbWriteTable(db,table.name,data)
}
if (is.character(data) && length(data)==1){
  zdata<-make.zdata(db,table.name)
} else {
  zdata<-data[numeric(0),]
  names(zdata)<-make.db.names(db,names(zdata))
  dbGetQuery(db, sqlsubst("create unique index %%idx%% on %%tbl%%(%%key%%)",
                          list(idx=basename(tempfile("idx")), tbl=table.name,key=key)))
}
  rval<-list(conn=db, table=table.name,
           id=id, strata=strata,weights=weights,fpc=fpc,
           call=sys.call(), zdata=zdata, key=key
           )
if (is.character(data) && length(data)==1)
  rval$dbname<-data
else
  rval$dbname<-NULL
class(rval)<-"sqlsurvey"
rval
}

sqlsubst<-function(strings, values){
  for(nm in names(values)){
  	if (is.null(values[[nm]])) next
    if (length(values[[nm]])>1) values[[nm]]<-paste(values[[nm]],collapse=", ")
    strings<-gsub(paste("%%",nm,"%%",sep=""),values[[nm]], strings)
  }
strings
}

print.sqlsurvey<-function(x,...){
  cat("SQLite survey object:\n")
  print(x$call)
  invisible(x)
}

dim.sqlsurvey<-function(x){
  if(is.null(x$subset))
    nrows<-dbGetQuery(x$conn, sqlsubst("select count(*) from %%table%%", list(table=x$table)))[[1]]
  else
    nrows<-dbGetQuery(x$conn, sqlsubst("select count(*) from %%table%% where %%wt%%>0",
                                       list(table=x$subset$table, wt=x$subset$weights)))[[1]]
  ncols<-ncol(x$zdata)
  c(nrows,ncols)
}


svymean.sqlsurvey<-function(x, design, na.rm=FALSE,byvar=NULL,se=FALSE, keep.estfun=FALSE,...){

  tms<-terms(x)

  ##handle subpopulations
  if (is.null(design$subset)){
    tablename<-design$table
    wtname<-design$weights
  } else {
    tablename<-sqlsubst(" %%tbl%% inner join %%subset%% using(%%key%%) ",
                        list(tbl=design$table, subset=design$subset$table, key=design$key))
    wtname<-design$subset$weights
  }

  ## use sqlmodelmatrix if we have factors or interactions
  basevars<-rownames(attr(tms,"factors"))
  if (any(sapply(design$zdata[basevars], function(v) length(levels(v)>0)))
      || any(attr(tms,"order")>1)){
    mm<-sqlmodelmatrix(x,design, fullrank=FALSE)
    termnames<-mm$terms
    tablename<-sqlsubst("%%tbl%% inner join %%mm%% using(%%key%%)",
                        list(tbl=tablename,mm=mm$table, key=design$key))
  } else {
    termnames<-attr(tms,"term.labels")
  }
  vars<-paste("sum(",wtname,"*",termnames,")")

  if (is.null(byvar)){
    query<-sqlsubst("select %%vars%%, sum(%%wt%%) from %%table%%",
                    list(vars=vars, wt=wtname,table=tablename))
  }else{
    byvar<-attr(terms(byvar),"term.labels")
    query<-sqlsubst("select %%vars%%, %%byvars%%, sum(%%wt%%) from %%table%% group by %%byvars%%",
                    list(vars=vars, byvars=byvar, wt=wtname, table=tablename))    
  }
  result<-dbGetQuery(design$conn, query)
  p<-length(termnames)
  totwt<-result[[ncol(result)]]
  result[1:p]<-result[1:p]/totwt
  result<-result[-ncol(result)]
  names(result)<-c(termnames,byvar)
  if(se){
    utable<-basename(tempfile("_U_"))
    if (is.null(byvar)){
      means<-unlist(result[1,])
      unames<-paste("_",termnames,sep="")
      query<-sqlsubst("create table %%utbl%% as select %%key%%, %%vars%% from %%table%%",
                      list(utbl=utable, key=design$key,
                           vars=paste(termnames,"-",means," as ",unames,sep=""),
                           table=tablename))
    } else {
      means<-as.vector(t(as.matrix(result[,1:p,drop=FALSE])))
      bycols<-result[,-(1:p),drop=FALSE]
      qbycols<-lapply(bycols, function(v) if(is.character(v)) lapply(v,adquote) else v)
      bynames<-do.call(paste, c(mapply(paste, names(bycols),"=",qbycols,SIMPLIFY=FALSE), sep=" & "))
      vnames<-paste("(",termnames,"-",means,")",sep="")
      uexpr<-paste(vnames,"*(",rep(bynames,each=p),")")
      unames<-paste("_",as.vector(outer(termnames, do.call(paste, c(bycols,sep="_")), paste,sep="_")),sep="")
      query<-sqlsubst("create table %%utbl%% as select %%key%%, %%vars%% from %%table%%",
                      list(utbl=utable, key=design$key,
                           vars=paste(uexpr,"as",unames),
                           table=tablename))
      
    }
    
    dbGetQuery(design$conn, query)
    query<-sqlsubst("create unique index %%idx%% on  %%utbl%% (%%key%%)",
                    list(utbl=utable, key=design$key, idx=basename(tempfile("idx"))))
    dbGetQuery(design$conn, query)
    if (!keep.estfun)
      on.exit(dbGetQuery(design$conn, sqlsubst("drop table %%utbl%%",list(utbl=utable))))
    vmat<-sqlvar(unames,utable,design)
    attr(result,"var")<-vmat/tcrossprod(rep(totwt,each=p))
  }
  
 
  result
  
}

svytotal.sqlsurvey<-function(x, design, na.rm=FALSE,byvar=NULL,se=FALSE,keep.estfun=FALSE,...){

  tms<-terms(x)

  ## handle subpopulations
  if (is.null(design$subset)){
    tablename<-design$table
    wtname<-design$weights
  } else {
    tablename<-sqlsubst(" %%tbl%% inner join %%subset%% using(%%key%%) ",
                        list(tbl=design$table, subset=design$subset$table, key=design$key))
    wtname<-design$subset$weights
  }
  
  ## use modelmatrix if we have factors or interactions
  basevars<-rownames(attr(tms,"factors"))
  if (any(sapply(design$zdata[basevars], function(v) length(levels(v)>0)))
      || any(attr(tms,"order")>1)){
    mm<-sqlmodelmatrix(x,design, fullrank=FALSE)
    termnames<-mm$terms
    tablename<-sqlsubst("%%tbl%% inner join %%mm%% using(%%key%%)",
                        list(tbl=tablename,mm=mm$table, key=design$key))
  } else {
    termnames<-attr(tms,"term.labels")
  }

  vars<-paste("sum(",wtname,"*",termnames,")",sep="")
  if (is.null(byvar)){
    query<-sqlsubst("select %%vars%% from %%table%%",
                    list(vars=vars, wt=wtname,table=tablename))
  }else{
    byvar<-attr(terms(byvar),"term.labels")
    query<-sqlsubst("select %%vars%%, %%byvars%%  from %%table%% group by %%byvars%%",
                    list(vars=vars, byvars=byvar, wt=wtname, table=tablename))    
  }
  result<-dbGetQuery(design$conn, query)
  p<-length(termnames)
  names(result)<-c(termnames, byvar)

  if(se){
    utable<-basename(tempfile("_U_"))
    if (is.null(byvar)){
      unames<-paste("_",termnames,sep="")
      query<-sqlsubst("create table %%utbl%% as select %%key%%, %%vars%% from %%table%%",
                      list(utbl=utable, key=design$key,
                           vars=paste(termnames,"as",unames),
                           table=tablename))
    } else {
      bycols<-result[,-(1:p),drop=FALSE]
      qbycols<-lapply(bycols, function(v) if(is.character(v)) lapply(v,adquote) else v)
      bynames<-do.call(paste, c(mapply(paste, names(bycols),"=",qbycols,SIMPLIFY=FALSE), sep=" & "))
      uexpr<-as.vector(outer(termnames, bynames, function(i,j) paste(i,"*(",j,")",sep="")))
      unames<-paste("_",as.vector(outer(termnames, do.call(paste, c(bycols,sep="_")), paste,sep="_")),sep="")
      query<-sqlsubst("create table %%utbl%% as select %%key%%, %%vars%% from %%table%%",
                      list(utbl=utable, key=design$key,
                           vars=paste(uexpr,"as",unames),
                           table=tablename))
      
    }
    dbGetQuery(design$conn, query)
    query<-sqlsubst("create unique index %%idx%% on  %%utbl%% (%%key%%)",
                    list(utbl=utable, key=design$key, idx=basename(tempfile("idx"))))
    dbGetQuery(design$conn, query)
    if (!keep.estfun)
      on.exit(dbGetQuery(design$conn, sqlsubst("drop table %%utbl%%",list(utbl=utable))))
    vmat<-sqlvar(unames,utable,design)
    attr(result,"var")<-vmat
    
  }
  
  result
  
}


svyquantile.sqlsurvey<-function(x,design, quantiles,build.index=FALSE,...){
  SMALL<-1000 ## 20 for testing. More like 1000 for production use
  if (is.null(design$subset))
    tablename<-design$table
  else
    tablename<-sqlsubst("%%tbl%% inner join %%sub%% using(%%key%%)",
                        list(tbl=design$table, sub=design$subset$table,
                             key=design$key))
  bisect<-function(varname,wtname,  low, up, plow,pup, nlow,nup,quant,W){
    if (up==low) return(up)
    if (nup-nlow < SMALL){
      query<-sqlsubst("select %%var%%, %%wt%% from %%table%% where %%var%%>%%low%% and %%var%%<=%%up%%",
                      list(var=varname,wt=wtname,table=design$table,
                           low=low,up=up))
      data<-dbGetQuery(design$conn, query)
      return(bisect.in.mem(data, plow, pup,quant,W))
    }
    mid<-((pup-quant+0.5)*low+(quant-plow+0.5)*up)/(pup-plow+1)
    query<-sqlsubst("select total(%%wt%%), count(*), max(%%var%%) from %%table%% where %%var%%<=%%mid%%",
                    list(var=varname,mid=mid,wt=wtname,table=design$table))
    result<-dbGetQuery(design$conn, query)
    mid<-result[[3]]
    nmid<-result[[2]]
    pmid<-result[[1]]/W
    if (mid==up && pmid>quant) return(mid)
    if (mid==low){
      query<-sqlsubst("select total(%%wt%%)from %%table%% where %%var%%==%%mid%% ",
                      list(var=varname, mid=mid,table=design$table, wt=wtname))
      pexactmid<-dbGetQuery(design$conn,query)
      if (pmid+pexactmid>quant) return(up)
    }
    if (pmid>quant)
      bisect(varname,wtname,low,mid,plow,pmid,nlow,nmid,quant,W)
    else
      bisect(varname,wtname,mid,up,pmid, pup, nmid, nup, quant,W)
  }

  bisect.in.mem<-function(data, plow,pup,quant,W){
    data<-data[order(data[,1]),]
    p<-cumsum(data[,2])/W
    p<-p+plow
    p[length(p)]<-p[length(p)]+(1-pup)
    data[min(which(p>=quant)),1]
  }

  qsearch<-function(varname, quant){
    ll<-levels(design$zdata[[varname]])
    if (!is.null(ll) && length(ll)<100){
      tbl<-svytable(formula(paste("~",varname)),design)
      cdf<-cumsum(tbl[,2])/sum(tbl[,2])
      return(tbl[,1][min(which(cdf>=quant))])
    }
    if(build.index){
      idxname<-basename(tempfile("idx"))
      dbGetQuery(design$conn, sqlsubst("create index %%idx%% on %%tbl%%(%%var%%)",
                                       list(idx=idxname, tbl=design$table,var=varname)))
      on.exit(dbGetQuery(design$conn, sqlsubst("drop index %%idx%%",list(idx=idxname))))
    }
    lims<-dbGetQuery(design$conn,
                     sqlsubst("select min(%%var%%), max(%%var%%), count(*), sum(%%wt%%) from %%table%%",
                              list(var=varname, wt=design$weights, table=design$table)))
    bisect(varname,design$weights, lims[[1]],lims[[2]],0,1,0,lims[[3]],quant,lims[[4]])
  }

  if(length(quantiles)>1) stop("only one quantile")
  tms<-terms(x)
  rval<-sapply(attr(tms,"term.labels"), qsearch,quant=quantiles)
  names(rval)<-attr(tms,"term.labels")
  rval
}




svylm<-function(formula, design){
  tms<-terms(formula)
  yname<-as.character(attr(tms,"variables")[[2]])
  ## handle subpopulations
  if (is.null(design$subset)){
    tablename<-design$table
    wtname<-design$weights
  } else {
    tablename<-sqlsubst(" %%tbl%% inner join %%subset%% using(%%key%%) ",
                        list(tbl=design$table, subset=design$subset$table, key=design$key))
    wtname<-design$subset$weights
  }
  

  mm<-sqlmodelmatrix(formula, design, fullrank=TRUE)
  termnames<-mm$terms
  tablename<-sqlsubst("%%tbl%% inner join %%mm%% using(%%key%%)",
                        list(tbl=tablename,mm=mm$table, key=design$key))

   p<-length(termnames)
   mfy<-basename(tempfile("_y_"))
   sumxy<-paste("sum(",termnames,"*%%mfy%%*%%wt%%) as _xy_",termnames,sep="")
   sumsq<-outer(termnames,termnames, function(i,j) paste("sum(",i,"*",j,"*%%wt%%)",sep=""))

   qxwx<-sqlsubst("select %%sumsq%% from %%table%%" ,
                     list(sumsq=sumsq, table=tablename, wt=wtname)
                     )
   xwx<-matrix(as.matrix(dbGetQuery(design$conn, qxwx)),p,p)
   qxwy <- sqlsubst("select %%sumxy%% from %%tablename%% inner join (select %%y%% as %%mfy%%, %%key%% from %%mf%%) using(%%key%%)", 
        list(sumxy = sumxy, y = yname, key = design$key, tablename = tablename, 
        mf=mm$mf, wt=wtname, mfy=mfy))
   xwy<-drop(as.matrix(dbGetQuery(design$conn, qxwy)))
   beta<-solve(xwx,xwy)

   xytab<-basename(tempfile("_xyt_"))
   muname<-basename(tempfile("_mu_"))
   qmu<-paste("(",paste(termnames,"*",formatC(beta,format="fg",digits=16),collapse="+"),") as ",muname)
   qxytab<-sqlsubst("create table %%xytab%% as select %%x%%, %%y%%, %%qmu%%, %%key%% from (select %%y%%, %%key%% from %%mf%%) inner join %%mm%% using(%%key%%)", list(xytab=xytab, x=termnames,
    y=yname, key=design$key,qmu=qmu,mf=mm$mf,mm=mm$table))       
   dbGetQuery(design$conn, qxytab)
   on.exit(dbGetQuery(design$conn, paste("drop table ",xytab)),add=TRUE)
   
   Utable<-basename(tempfile("_U_"))
   unames<-paste("_U_", termnames, sep="")
   u<-paste(termnames,"*(",yname,"-",muname,") as ",unames)
   qu<-sqlsubst("create table %%utable%% as select %%u%%, %%key%% from %%xytab%%",
   	list(key=design$key, utable=Utable, u=u,xytab=xytab))  
   dbGetQuery(design$conn, qu)
   on.exit(dbGetQuery(design$conn,paste("drop table ",Utable)),add=TRUE)

   Uvar<-sqlvar(unames,Utable, design)
   xwxinv<-solve(xwx)
   v<-xwxinv%*%Uvar%*%xwxinv
   names(beta)<-termnames
   dimnames(v)<-list(termnames, termnames)
   attr(beta, "var")<-v
   beta
}

dim.sqlmm<-function(x){
	n<-dbGetQuery(x$conn, sqlsubst("select count(*) from %%table%%",list(table=x$table)))[[1]]
	p<-length(x$terms)
	c(n,p) 	
}

sqlvar<-function(U, utable, design){
   nstages<-length(design$id)
   units<-NULL
   strata<-NULL
   results<-vector("list",nstages)
   stagevar<-vector("list",nstages)
   p<-length(U)
   if (is.null(design$subset)){
    tablename<-design$table
    wtname<-design$weights
  } else {
    tablename<-sqlsubst(" %%tbl%% inner join %%subset%% using(%%key%%) ",
                        list(tbl=design$table, subset=design$subset$table, key=design$key))
    wtname<-design$subset$weights
  }
   sumUs<-sqlsubst(paste(paste("sum(",U,"*%%wt%%) as _s_",U,sep=""),collapse=", "),list(wt=wtname))
   Usums<-paste("_s_",U,sep="")
   avgs<-paste("avg(",Usums,")",sep="")
   avgsq<-outer(Usums,Usums, function(i,j) paste("avg(",i,"*",j,")",sep=""))
   for(stage in 1:nstages){
     oldstrata<-strata
     strata<-unique(c(units, design$strata[stage]))
     units<-unique(c(units, design$strata[stage], design$id[stage]))

     query<-sqlsubst("select %%avgs%%, %%avgsq%%, count(*) as _n_, %%fpc%% as _fpc_, %%strata%%
                      from 
                            (select %%strata%%, %%sumUs%%, %%fpc%% from %%basetable%% inner join %%tbl%% using(%%key%%) group by %%units%%)  as r_temp
                      group by %%strata%%" ,
                     list(units=units, strata=strata, sumUs=sumUs, tbl=utable,avgs=avgs,
                          avgsq=avgsq,fpc=design$fpc[stage],
                          basetable=tablename, key=design$key
                          )
                     )
     result<-dbGetQuery(design$conn, query)
     result<-subset(result, `_fpc_`!=`_n_`) ## remove certainty units
     if (is.null(oldstrata)){
       result$`_p_samp_`<-1
     } else {
       index<-match(result[,oldstrata], results[[stage-1]][,oldstrata])
       keep<-!is.na(index)
       result<-result[keep,,drop=FALSE]
       index<-index[keep]
       result$`_p_samp_`<-results[[stage-1]][index,"_n_"]/results[[stage-1]][index,"_fpc_"] ##assumes p = n/N (missing data?)
     }
     means<-as.matrix(result[,1:p])
     ssp<-as.matrix(result[,p+(1:(p*p))])
     meansq<-means[,rep(1:p,p)]*means[,rep(1:p,each=p)]
     nminus1<-(result$`_n_`-1)
     if (any(nminus1==0)){
       if (getOption("survey.lonely.psu")=="remove")
         nminus1[nminus1==0]<-Inf
       else
         stop("strata with only one PSU at stage ",stage)
     }
     stagevar[[stage]]<-((ssp-meansq) * (result$`_n_`^2)/nminus1)*result$`_p_samp_`

     if (any(result$`_fpc_`>0)) {## without-replacement
       stagevar[[stage]][result$`_fpc_`>0]<-stagevar[[stage]][result$`_fpc_`>0]*((result$`_fpc_`-result$`_n_`)/result$`_fpc_`)[result$`_fpc_`>0]
     }
     results[[stage]]<-result
 }
   vars<-lapply(stagevar, function(v) matrix(colSums(v),p,p))
   rval<-vars[[1]]
   for(i in seq(length=nstages-1)) rval<-rval+vars[[i+1]]
   dimnames(rval)<-list(U,U)
   rval
}

svytable.sqlsurvey<-function(formula, design,...){
  tms<-terms(formula)
  if (is.null(design$subset))
    tablename<-design$table
  else
    tablename<-sqlsubst("%%base%% inner join %%sub%% using(%%key%%) where %%wt%%>0",
                        list(base=design$table, sub=design$subset$table,
                             key=design$key, wt=design$subset$weights))
  query<-sqlsubst("select %%tms%%, sum(%%wt%%) from %%table%% group by %%tms%%",
                  list(wt=design$weights,
                       table=tablename,
                       tms=attr(tms,"term.labels")))
  dbGetQuery(design$conn, query)
}


adquote<-function(s) paste("\"",s,"\"",sep="")

sqlmodelmatrix<-function(formula, design, fullrank=TRUE){
  mmcol<-function(variables,levels, name.only=FALSE){
    if (length(variables)==0){
      if(name.only) return("_Intercept_") else return("1 as _Intercept_")
    }
    rval<-paste(variables,"==",adquote(levels),sep="")
    termname<-paste(variables,levels,sep="")
    if (length(rval)>1){
      rval<-paste(paste("(",rval,")",sep=""),collapse="*")
      termname<-paste(termname,collapse="_")
    }
    if (name.only)
      make.db.names(design$conn, termname)
    else 
      paste(rval,"as",make.db.names(design$conn, termname))
  }
  if (!all(all.vars(formula) %in% names(design$zdata)))
    stop("some variables not in database")
  ok.names<-c("~","I","(","-","+","*")
  if (!all( all.names(formula) %in% c(ok.names, names(design$zdata))))
    stop("unsupported transformations in formula")
  tms<-terms(formula)
  mf<-model.frame(formula,design$zdata)
  mm<-model.matrix(tms, mf)
  ntms<-max(attr(mm,"assign"))
  mftable<-basename(tempfile("_mf_"))
  mmtable<-basename(tempfile("_mm_"))

  dbGetQuery(design$conn, sqlsubst("create table %%mf%% as select %%key%%, %%vars%% from %%table%%",
                               list(mf=mftable, vars=all.vars(formula), table=design$table,
                                    id=design$id, strata=design$strata,key=design$key)))
  dbGetQuery(design$conn, sqlsubst("create unique index %%idx%% on %%tbl%%(%%key%%)",
                                   list(idx=basename(tempfile("idx")), tbl=mftable,
                                        key=design$key)))


  patmat<-attr(tms,"factors")
  nms<-attr(tms,"term.labels")
  orders<-attr(tms, "order")
  if (fullrank)
    contrastlevels<-function(f) {levels(f)[-1]}
  else
    contrastlevels<-levels
  
  mmterms<-lapply(1:ntms,
                    function(i){
                      vars<-rownames(patmat)[as.logical(patmat[,nms[i]])]
                      if (orders[i]==1 && is.null(levels(mf[[vars]])))
                        return(list(paste(vars," as _",vars,sep="")))
                      levs<-as.matrix(expand.grid(lapply(mf[vars],contrastlevels)))
                      lapply(split(levs,row(levs)),
                             function(ll) mmcol(vars,ll))
                    })
  if (fullrank)  mmterms<-c(mmterms, list(mmcol(NULL,NULL)))
  
  mmnames<-lapply(1:ntms,
                    function(i){
                      vars<-rownames(patmat)[as.logical(patmat[,nms[i]])]
                      if (orders[i]==1 && is.null(levels(mf[[vars]])))
                        return(list(paste("_",vars,sep="")))
                      levs<-as.matrix(expand.grid(lapply(mf[vars],contrastlevels)))
                      lapply(split(levs,row(levs)),
                             function(ll) mmcol(vars,ll,TRUE))
                    })
  if (fullrank) mmnames<-c(mmnames, list(mmcol(NULL,NULL,TRUE)))

  
  mmquery<-sqlsubst("create table %%mm%% as select %%key%%, %%terms%% from %%mf%%",
                    list(mm=mmtable, id=design$id, strata=design$strata,
                         terms=unlist(mmterms), mf=mftable,
                         key=design$key))
  dbGetQuery(design$conn, mmquery)
  dbGetQuery(design$conn, sqlsubst("create unique index %%idx%% on %%tbl%%(%%key%%)",
                                   list(idx=basename(tempfile("idx")), tbl=mmtable,
                                        key=design$key)))
  rval<-new.env(parent=emptyenv())
  rval$table<-mmtable
  rval$mf<-mftable
  rval$formula<-formula
  rval$terms<-unlist(mmnames)
  rval$call<-sys.call()
  rval$conn<-design$conn
  reg.finalizer(rval, sqlmmDrop)
  class(rval)<-"sqlmm"
  rval
}

sqlmmDrop<-function(mmobj){
  dbGetQuery(mmobj$conn,sqlsubst("drop table %%mm%%", list(mm=mmobj$table))) 
  dbGetQuery(mmobj$conn,sqlsubst("drop table %%mf%%", list(mf=mmobj$mf)))
  invisible(NULL)
}

head.sqlmm<-function(x,n=6,...) dbGetQuery(x$conn,
     sqlsubst("select * from %%mm%% limit %nn%", list(mm=x$table,nn=n)))

hexbinmerge<-function(h1,h2){
	h<-h1
	h@cID<-NULL
	
	extra<-!(h2@cell %in% h1@cell)
	cells<-sort(unique(c(h1@cell, h2@cell)))
	i1<-match(h1@cell,cells)
	i2<-match(h2@cell,cells)
	i2new<-match(h2@cell[extra], cells)
	i2old<-match(h2@cell[!extra], cells)
	
	n<-length(cells)

	count<-integer(n)
	count[i1]<-h1@count
	count[i2new]<-h2@count[extra]
	count[i2old]<-count[i2old]+h2@count[!extra]
	h@count<-count
		
	xcm<-numeric(n)
	xcm[i1]<-h1@xcm
	xcm[i2new]<-h2@xcm[extra]
	xcm[i2old]<-xcm[i2old]*(1-h2@count[!extra]/count[i2old])+h2@xcm[!extra]*(h2@count[!extra]/count[i2old])
	h@xcm<-xcm
	
	ycm<-numeric(n)
	ycm[i1]<-h1@ycm
	ycm[i2new]<-h2@ycm[extra]
	ycm[i2old]<-ycm[i2old]*(1-h2@count[!extra]/count[i2old])+h2@ycm[!extra]*(h2@count[!extra]/count[i2old])
	h@ycm<-ycm
	
	h@n<-as.integer(sum(count))
	h@ncells<-n	
	h@cell<-cells
	h
	}

sqlhexbin<-function(formula, design, xlab=NULL,ylab=NULL, ...,chunksize=5000){
	require("hexbin")
	tms<-terms(formula)
	x<-attr(tms,"term.labels")
	if (length(x)>2) stop("only one x variable")
	y<-deparse(formula[[2]])
	if (is.null(design$subset)){
   		tablename<-design$table
    	wtname<-design$weights
  	} else {
    	tablename<-sqlsubst(" %%tbl%% inner join %%subset%% using(%%key%%) ",
                        list(tbl=design$table, subset=design$subset$table, key=design$key))
    	wtname<-design$subset$weights
  	}

	query<-sqlsubst("select min(%%x%%) as xmin, max(%%x%%) as xmax, min(%%y%%) as ymin, max(%%y%%) as ymax from %%tbl%% where %%wt%%>0", list(x=x,tbl=tablename,y=y, wt=wtname))
	ranges<-dbGetQuery(design$conn, query)
	xlim<-with(ranges,c(xmin,xmax))
	ylim<-with(ranges,c(ymin,ymax))
	N<-dbGetQuery(design$conn, sqlsubst("select count(*) from %%tbl%% where %%wt%%>0",
	   list(tbl=tablename,wt=wtname)))[[1]]
	
	query<-sqlsubst("select %%x%% as x, %%y%% as y, %%wt%% as _wt from %%tbl%% where %%wt%% > 0",
		list(x=x,y=y,tbl=tablename,wt=wtname))
	result<-dbSendQuery(design$conn, query)
	on.exit(dbClearResult(result))

    got<-0
    htotal<-NULL
    while(got<N){
    	df<-fetch(result, chunksize)
    	if (nrow(df)==0) break
    	h<-hexbin(df$x,df$y,ID=TRUE,xbnds=xlim,ybnds=ylim)
    	h@count<-as.vector(tapply(df[["_wt"]],h@cID,sum))
    	if (is.null(htotal)){
    		htotal<-h
    	} else 
    		htotal<-hexbinmerge(htotal,h) 
    	
    }
    if (is.null(xlab)) xlab<-x
    if (is.null(ylab)) ylab<-y
    
    gplot.hexbin(htotal,xlab=xlab, ylab=ylab, ...)
    invisible(htotal)
	}
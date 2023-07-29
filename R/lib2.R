#' @export
chkpcode <- function(pc='EC2R 8AH') {
  #composed of correct chars
  #grepl(patt='[^a-zA-Z0-9]/',x=pc,perl=TRUE)
  #right length
  nch <- sapply(pc,nchar)
  stopifnot(all(nch<=8))
  #max one space
  stopifnot(unlist(lapply(gregexpr(patt=' ',pc),length))==1)
  #is either 1-part or 2-part
  x <- strsplit(pc,split=' ')
  #stopifnot(all(lapply(x,length)==1)||all(lapply(x,length)==2))
  #1-part always starts with alpha cap
  if(length(x[[1]])==1) {
    stopifnot(all(unlist(gregexpr(pc,patt='^[A-Z,a-z]'))==1))
  }
  #2-part always starts with number
  if(length(x[[1]])==2) {
    pcin <- lapply(x,'[[',2)
    if(!all(unlist(gregexpr(pcin,patt='^[0-9]'))==1)) browser()
    stopifnot(all(unlist(gregexpr(pcin,patt='^[0-9]'))==1))
  }
  TRUE
}

#' @export
parsepcode <- #parse a vector of 'irregular' (normal) postcode
  function(pc=c('AL1 1AD','AL1 1BD','AL1 1CD')) {
    x <- lapply(pc,ppc)%>%
      lapply(.,data.table)%>%
      lapply(.,t)%>%
      Reduce(rbind,.)%>%
      data.frame(.)%>%
      lapply(.,unlist)%>%
      suppressWarnings(.)
    x <- lapply(x,`names<-`,NULL)
    names(x) <- names(ppc(pc[1]))
    x
  }

#' @export
ppc <-  #parse a single 'irregular' (normal) postcode
  function(pc='EC2R 8AH') {
  if(nchar(pc)<2) return(list(area=ifelse(grepl('[A-Z,a-z]',pc),paste0(toupper(pc),'--'),''),district='',sector='',unit=''))
  chkpcode(pc)
  pc <- toupper(pc)
  gg <- gregexpr(patt=' ',pc)
  x <- strsplit(pc,split=' ')
  out <- unlist(lapply(x,'[[',1))
  nout <- nchar(out)
  inum <- as.numeric(regexpr("[0-9]",out))
  area <- pc
  sector <- unit <- district <- rep('',length(pc))
  area[inum==2] <- substr(out[inum==2],1,1)
  area[inum==3] <- substr(out[inum==3],1,2)
  district[inum==2] <- substring(out[inum==2],2)
  district[inum==3] <- substring(out[inum==3],3)
  if(any(lapply(x,length)>1)) { #inbound code exists
    stopifnot(all(lapply(x,length)==2)) #exists for all
    inb <- unlist(lapply(x,'[[',2))
    nin <- nchar(inb)
    sector <- substr(inb,1,1)
    unit <- substring(inb,2,nin)
  }
  list(area=area,district=district,sector=sector,unit=unit)
}

#' @export
rcs <- function(){'-'}
#rcs <- function(){'.'}

#' @export
escrcs <- function(){paste0("\\",rcs())}

#' @export
subdollar <- function(x) {
  gsub(patt=escrcs(),rep=" ",x=x)
}

#' @export
regpcode <- #parse 'irregular' (normal) postcode to 'regular' 12-character
  function(rawcode=c('AL1 1AD','AL1 1BD','AL1 1CD'),x=parsepcode(rawcode)) {
    rawcode <- gsub(patt='  ',rep=' ',rawcode)
    Reduce(paste0,lapply(x,pad1))
  }

#' @export
pad1 <- function(x) {
  n1 <- nchar(x)
  x[n1==1] <- paste0(x[n1==1],paste(collapse ='',rep(rcs(),2)))
  x[n1==2] <- paste0(x[n1==2],rcs())
  x
}

#' @export
strips <- function(x) {
  gsub(patt=' ',rep='',x)
}

#' @export
irregpcode <- function(x) {
  x1 <- substr(x,1,pmin(6,nchar(x)))
  x2 <- substr(x,pmin(7,nchar(x)),nchar(x))
  gsub(patt=' $',rep='',x=paste(gsub(patt='\\-',rep='',x=x1),gsub(patt='\\-',rep='',x=x2)))
}

#' @export
splitpcode <- function(x) {
  if(length(gregexpr(patt=' ',x))>0) x <- gsub(patt=' ',rep='',x=x)
  nn <- nchar(x)
  paste(substr(x,1,nn-3),substr(x,nn-2,nn))
}

#' @export
getallv <- function(v=1:3) {
  ll<- list(length(v),ty='list')
  for(i in seq_along(v)) {
    ll[[i]] <- getrdatv(v=v[i])
  }
  ll
}

#' @export
readcsv <- function(x,...) {data.table(read.csv(x,...,stringsAsFactors=F))}

#' @export
setdir <- function(x='d:/gdrive/clients/PPD') {
  x1 <- setkey(setnames(readcsv(paste0(x,'/boroughnumber.csv')),old='file.',new='fnum')[,fnum:=zeroprepend(fnum,nt=2)][,bc:=abbrev(Borough,5)],fnum)
  x2 <- setkey(data.table(fn=dir(paste0(x,"/london boroughs")))[,nn:=substr(fn,11,12)],nn)
  x3 <- x1[x2][,dir:=paste0(x,"/london boroughs")]
  putrdatv(x3,ty='bo')
  x3
}

#' @export
readppd <- function(x=getrdatv(ty='dir')[1,]) {
  names <- c("unique_id", "price_paid", "deed_date", "postcode", "property_type",
             "new_build", "estate_type", "saon", "paon", "street", "locality",
             "town", "district", "county", "linked_data_uri")
  setnames(readcsv(x[,paste0(dir,'/',fn)]),names)[]
}

#' @export
getpp <- function(bo=gett('bo')) {
  res <- vector('list',nrow(bo))
  for(i in 1:nrow(bo)) { #read and save the raw data
    print(i)
    x0 <- readppd(bo[i,])[3<nchar(postcode)]
    x1 <- data.table(data.frame(lapply(x0,abbrev,nospace=F)))[,id:=paste(regpcode(postcode),saon,paon,street,sep='.')]
    x1 <- unique(setkey(x1,id,deed_date))[,price_paid:=as.numeric(price_paid)]
    res[[i]] <- copy(x1)
  }
  dt <- unique(setkey(rbindlist(res),id,deed_date))[,ntr:=.N,id]
  putrdatv(dt,ty='getppd')
  dt
}

#' @export
edpp <- function(getppd=gett('getppd')) {
  j <- c('price_paid','deed_date','postcode','property_type','new_build','estate_type','id','ntr')
  edppd <- getppd[,j,with=F]
  putt(edppd)
}

#' @export
prunepp <- function(x=gett('edppd'),nmin=8000,nminhard=3000) {
  #xx <- setkey(copy(x)[,unique_id:=1:nrow(x)],unique_id)
  xx <- copy(x)
  xx[,rcode:=regpcode(postcode)][,ircode:=rcode] #35s for London
  for(l in 4:0) {
    if(xx[,any(nchar(rcode)==3*l)]) xx[nchar(rcode)==3*l,N:=.N,by=rcode][N<nmin,rcode:=substr(rcode,1,3*(l-1))]
  }
  setkey(xx,rcode)
  tt<-table(xx['',substr(ircode,1,3)]) #now check {'area' which is char1:3 in rcode} against nminhard
  while(any(nminhard<tt)) {
    j <- names(tt)[which.max(tt)]
    xx[rcode==''&substr(ircode,1,3)==j,rcode:=substr(ircode,1,3)]
    tt <- tt[-match(j,names(tt))]
  }
  xx[rcode=='',rcode:='drop']
  xx[,N:=.N,by=rcode]
  putrdatv(xx,ty='pruneppd')
  xx
}

#' @export
getpco <- function(dd='d:/gdrive/clients/PPD/codepo',ff=dir(dd)) {
  ll <- vector('list',length(ff))
  for(i in seq_along(ff)) {
    x <- setnames(data.table(read.csv(file=paste0(dd,'/',ff[i]),header=F))[,c(1,3,4),with=F],c('messcode','ee','nn'))
    x <- x[,pcode:=splitpcode(messcode)][,messcode:=NULL]
    x <- x[,pcode:=toupper(pcode)][,rcode:=regpcode(pcode),pcode]
    ll[[i]] <- x
  }
  eenn <- rbindlist(ll)
  putrdatv(eenn,ty='getpcod',v=0)
  eenn
}

#' @export
agpco <- function(eenn=getrdatv(ty='getpcod',v=0)) {
  ll <- vector('list',4)
  ll[[1]] <- eenn[,list(x=rcode,eex=ee,nnx=nn)]
  ll[[2]] <- copy(eenn)[,x:=substr(rcode,1,9)][,list(eex=round(mean(ee)),nnx=round(mean(nn))),x]
  ll[[3]] <- copy(eenn)[,x:=substr(rcode,1,6)][,list(eex=round(mean(ee)),nnx=round(mean(nn))),x]
  ll[[4]] <- copy(eenn)[,x:=substr(rcode,1,3)][,list(eex=round(mean(ee)),nnx=round(mean(nn))),x]
  x <- setkey(setnames(rbindlist(ll)[,pc:=irregpcode(x)],old='x',new='rc'),pc)
  putrdatv(x,ty='agpcod')
  x
}

#' @export
seg <- function(x1=gett('pruneppd')[rcode!='drop']) {
  x2 <- setkey(x1[,ntr:=.N,id][,year:=substr(deed_date,1,4)][,list(price_paid,year,deed_date,property_type,id,ntr,rcode)],id,deed_date)
  x3 <- x2[1<ntr]
  x3 <- x3[,startdate := c("",deed_date[-.N]) ,by=id][,startprice := c(NA,price_paid[-.N]) ,by=id][startdate!=''][deed_date>startdate,r:=log(price_paid/startprice)]
  x3 <- x3[,pa:=365.25*r/as.numeric(as.Date(deed_date)-as.Date(startdate))][!is.na(r)]
  x3 <- setkey(x3[(-1<pa)&(pa<1)],id,deed_date)
  setcolorder(x3,c('id','property_type','ntr','rcode','startdate','startprice','deed_date','price_paid','year','r','pa'))
  x3[,deed_date:=as.Date(deed_date)] #move this upstream?
  x3[,startdate:=as.Date(startdate)]
  #x3[,id:=paste0(id,'.',deed_date)]
  putrdatv(x3,ty='segd')
  x3
}


#' @export
newperiods <- function(d1=gett('segd')[,min(deed_date)],d2=gett('segd')[,max(deed_date)]+90,period='year',find='last',settolast=F,d0=as.Date('1995-04-01'),...) {
  if(period=='quarter')  {
    calq <- seq.Date(from=d0,to=d2+85,by='quarter')-1 #revised version is calendar quarters
    da <- calq[d1<=calq]
    # da <- extractDates(seq.Date(from=d1,to=d2,by=1),period='month',find=find,...)
    # if(settolast) {
    #   da <- rev(da[seq(from=length(da),to=1,by=-3)]) #end on final date
    # } else {
    #   da <- da[seq(from=1,to=length(da),by=3)] #start on first date
    # }
  } else if(period=='week') {
    da <- extractDates(seq.Date(from=d1,to=d2,by=1),period=period,...)
  } else {
    da <- extractDates(seq.Date(from=d1,to=d2,by=1),period=period,find=find,...)
  }
  da <- structure(da,names=as.character(seq_along(da)))
  #print(da)
  #putrdatv(da,v=0,ty='period')
  da
}

#takes col vector of dates, period dates, returns 3 matrices with period in cols: start in period end in period, ninperiod
#' @export
accrue <- function(pdate = newperiods(...),
                   segd = gett('segd'),
                   ...) {
  da1a <- structure(outer(pdate, segd[, deed_date], `-`)
                    , class = 'numeric')
  da1a[] <- pmax(0, da1a[]) #numeric matrix
  da0a <-   structure(outer(pdate, segd[, startdate], `-`)
                      , class = 'numeric')
  da0a[] <- pmax(0, da0a[])
  structure(cbind(t(da0a[1, , drop = F]), t(diff(da0a - da1a)))  ,
            dimnames=list(segd[, iddate(id, deed_date)], as.character(pdate[])))
}
#' @export
accruechki <- function(segd=gett('segd')) {
  c(
    segd[,which(startdate==min(startdate))][1],
    segd[,which(startdate==max(startdate))][1],
    segd[,which(deed_date==min(deed_date))][1],
    segd[,which(deed_date==max(deed_date))][1],
    segd[,which(startdate==median(startdate))][1],
    segd[,which(deed_date==median(deed_date))][1]
  )
}
#' @export
accruechk <- function(pdate=gett('hfcad'),hfacd=gett('hfacd'),segd=gett('segd'),ichk=accruechki(segd=segd)) {
  #select segd
  segd1 <- segd[ichk,]
  stopifnot(segd[,all(startdate<deed_date)])
  stopifnot(nrow(hfacd)==nrow(segd))
  hfacd1 <- hfacd[ichk,which(colnames(hfacd)!='const')] #don't assume const
  #startperiod, accrued days at start
  j1withaccrual <- apply(hfacd1,1,function(x){min(which(x>0))})
  d1withaccrual <- as.Date(colnames(hfacd1)[j1withaccrual])
  tt <- rep(NA,nrow(segd1))
  for(i in 1:nrow(segd1)) {
    tt[i] <- (d1withaccrual[i] - hfacd1[i,j1withaccrual[i]]) == segd1[i,startdate]
  }
  stopifnot(all(tt))
  #endperiod, accrued days at end
  j1withaccrual <- apply(hfacd1,1,function(x){max(which(x>0))})
  d1withaccrual <- as.Date(colnames(hfacd1)[j1withaccrual])
  tt <- rep(NA,nrow(segd1))
  for(i in 1:nrow(segd1)) {
    tt[i] <- d1withaccrual[i] - (7-hfacd1[i,j1withaccrual[i]])  == segd1[i,deed_date]
  }
  stopifnot(all(tt))
  # days on each segment
  stopifnot(segd1[,deed_date-startdate] == apply(hfacd1,1,sum))
  # number of segments on each segment
  stopifnot(all(apply(t(apply(hfacd1>0,1,diff)),1,max) + as.numeric(hfacd1[,1]>0)==1)) #all have one 'start' or are 'start period 1'
  stopifnot(all(-apply(t(apply(hfacd1>0,1,diff)),1,min) + as.numeric(hfacd1[,ncol(hfacd1)]>0)==1)) #all have one 'end' or 'end at final period'
}



#method 1
#pr prep - returns named list of dt holding yx, rows are return 'segments' , explan cols are accruals
#' @export
yxpr1 <- function(segd=gett('segd'),...) {
  x <- setkey(segd[,te:=gsub(patt=' ',rep=rcs(),rcode)],te)
  grp <- x[,unique(te)]
  ll <- structure(vector('list',length(grp)),names=grp)
  for(j in 1:length(grp)) {
    xx <- setkey(data.table(accrue(segd=x[grp[j]],...),keep.rownames=T),rn)#[,segid:=paste0(rn,deed_date)]
    yy <- setkey(x[grp[j],list(id=iddate(id,deed_date),r)][,unity:=1],id)
    unity <- setnames(copy(yy[,list(r)]),'unity')[,unity:=1]
    yx <- yy[xx]
    rownames(yx) <- yx[,id]
    ll[[j]] <- yx[,id:=NULL]
  }
  setda(ll)
  putrdatv(ll,ty='yxpr1d')
  ll
}

#' @export
setda <- function(yxpr1d=getrdatv(ty='yxpr1d')) {
  j <- colnames(yxpr1d[[1]])
  j <- j[(nchar(j)==10)]
  setdad <- j[(as.character(as.Date(j))==j)]
  putt(setdad)
}

#' @export
extda <- function(setdad=gett('setdad'),nper=100) {
  setdad <- as.Date(setdad)
  d1=max(setdad)
  dd <- mean(diff(as.numeric(setdad)))
  if(29<dd && dd<32) {
    period <- 'month'
  } else  if(89<dd && dd<93) {
    period <- 'quarter'
  } else if(364<dd && dd<366) {
    period <- 'year'
  }
  extdad <- newperiods(d1=d1,d2=d1+(nper+2)*dd,period=period,find='last')[2:(nper+1)]
  putt(extdad)
}

#' @export
preda <- function(extdad=gett('extdad'),varpd=gett('varpd'))

  #re regression - returns a single 2-column {cell,field}
  #' @export
yxre1 <- function(yxpr1d=gett('yxpr1d')) {
  grp <- names(yxpr1d)
  ll <- structure(vector('list',length(grp)),names=grp)
  for(j in 1:length(grp)) {
    ll[[j]] <- summary(lm(r ~ . -1,data=yxpr1d[[j]]))
  }
  putrdatv(ll,ty='yxre1d')
  ll
}
#fo reformat one column of 'coefficients' matrix into a 2-col dt
#' @export
yxfo1 <- function(yxre1d=gett('yxre1d'),jstat=c('estimate','stderror','tvalue')) {
  jstat <- match.arg(jstat)
  grp <- names(yxre1d)
  x <- lapply(lapply(lapply(lapply(yxre1d,'[[','coefficients'),'[',,j=1:3),mattotab,rclabel=c("date", "stat")),data.table)
  for(j in seq_along(x))  {
    x[[j]][,stat:=tolower(abbrev(stat))]
    x[[j]] <- x[[j]][stat==jstat,]
    x[[j]][,cell:=paste(grp[j],'_',gsub(patt='`',rep='',x=date),sep='')][,date:=NULL][,stat:=NULL]
  }
  res <- rbindlist(x)[,list(cell,est=field)]
  putrdatv(res,ty='yxfo1d')
  res
}

#method 2
#prep
#' @export
yxpr2 <- function(segd=getrdatv(ty='segd'),...) {
  x <- setkey(segd[,te:=gsub(patt=' ',rep='',rcode)],te)
  grp <- x[,unique(te)]
  ll <- structure(vector('list',length(grp)),names=grp)
  for(j in 1:length(grp)) {
    xx <- setkey(data.table(accrue(segd=x[grp[j]],...),keep.rownames=T),rn)
    yy <- x[grp[j],r,id]
    ll[[j]] <- cbind(yy,xx)[id==rn][,id:=NULL][,rn:=NULL]
    setnames(ll[[j]],c(colnames(ll[[j]])[1],paste(grp[j],colnames(ll[[j]])[-1],sep='_')))
  }
  putrdatv(ll,ty='yxpr2d')
  ll
}
#regress
#' @export
yxre2 <- function(yxpr2d=getrdatv(ty='yxpr2d')) {
  yx <- as.matrix(rbindlist(yxpr2d,fill=T))
  yx[is.na(yx)] <- 0
  yx <- data.frame(yx)
  res <- summary(lm(r ~ . -1,data=yx))
  putrdatv(res,ty='yxre2d')
  res
}

#reformat
#' @export
yxfo2 <- function(yxre2d=gett('yxre2d'),jstat=c('estimate','stderror','tvalue')) {
  jstat <- match.arg(jstat)
  co <- yxre2d[['coefficients']]
  colnames(co) <- tolower(abbrev(colnames(co)))
  res <- data.table(cell=gsub(patt='\\.',rep='-',x=rownames(co)),est=co[,jstat])
  putrdatv(res,ty='yxfo2d')
  res
}

#' @export
yxpr3 <- function(yxpr1d=gett('yxpr1d')){  #},save=TRUE) { #nb the timesampling comes from v1!
  nti <- ncol(yxpr1d[[1]])-1 #all regressors including unity
  nzo <- length(yxpr1d)
  nn <- nti*nzo
  xx <- matrix(0,nn,nn,dimnames=list(1:nn,1:nn))
  xy <- matrix(0,nn,1,dimnames=list(1:nn,1))
  for(i in 1:nzo) {
    ij1 <- (i-1)*nti+1:nti
    mm <- as.matrix(data.frame(yxpr1d[[i]]))
    xx[ij1,ij1] <- crossprod(mm[,-1,drop=FALSE])
    xy[ij1,1] <- crossprod(mm[,-1,drop=FALSE],mm[,1,drop=FALSE])
    nam <- paste(names(yxpr1d)[i],colnames(yxpr1d[[i]])[-1],sep='_')
    rownames(xy)[ij1] <- colnames(xx)[ij1] <- rownames(xx)[ij1] <- nam
  }
  yxpr3d <- list(xx=xx,xy=xy)
  #if(save) { putrdatv(yxpr3d,ty='yxpr3d')}
  yxpr3d
}


#' @export
yxfo3 <- function(yxpr3d=gett('yxpr3d'),priored=gett('priored')) {  #,save=TRUE) {
  if(!is.null(priored)) {
    stopifnot(all(dim(yxpr3d)==dim(priored)))
    yxpr3d$xx <- yxpr3d$xx+priored
  }
  res <- solve(yxpr3d$xx,yxpr3d$xy)
  yxfo3d <- data.table(cell=rownames(res),est=res[,1])
  #if(save) { putt(yxfo3d) }
  yxfo3d
}

#' @export
yxfi3 <- function(yxpr1d=gett('yxpr1d'),yxfo3d=gett('yxfo3d')) {
  ntot <- sum(unlist(lapply(yxpr1d,nrow)))
  yy <- fi <- matrix(0,ntot,1,dimnames=list(as.character(1:ntot),NULL))
  i0 <- 0
  for(i in seq_along(yxpr1d)) { #through zones
    x <- as.matrix(yxpr1d[[i]][,-1,with=F])
    b <- as.matrix(yxfo3d[ncol(x)*(i-1)+(1:ncol(x)),2,with=F])
    ii <- i0+(1:nrow(x))
    fi[ii,1] <- x%*%b
    if(any(is.na( fi[ii,1]))) browser()
    yy[ii,1] <- as.matrix(yxpr1d[[i]][,1,with=F])
    rownames(yy)[ii] <- rownames(yxpr1d[[i]])
    i0 <- i0+nrow(x)
  }
  yxfi3d <- data.table(y=as.numeric(yy),yfit=as.numeric(fi),id=substr(rownames(yy),1,12))
  #if(save) { putt(yxfi3d) }
  yxfi3d
}



#' @export
getset <- function() {
  x <- setkey(rbindlist(lapply(as.list(ddv1()[,as.numeric(num)]),getrd)),lev,zone,tp,sp)
  unique(setkey(x[,list(MSR=mean(MSR),MSE=mean(MSE),N=max(N)),'zone,tp,sp'],zone,tp,sp))
}


#' @export
zone <- function(agpcod=gett('agpcod'),segd=gett('segd')) {
  x <- setkey(agpcod,rc)[segd[,unique(rcode)]]
  putrdatv(x,ty='zoned')
  x
}



#' @export
neare <- function(num = getpv('neare', 'num') #order changed because this is called with lapply in coho
                  ,
                  distanced=gett('distanced')
                  ,
                  maxsep = getpv('neare', 'maxsep')) {
  setkey(distanced, rc)[, rr := rank(dist), rc][1 < rr][rr <= (num + 1.5)][dist <maxsep][,dist:=rr][] #ties both get included
}


#' @export
np <- function() {
  npd <- rbindlist(list(
    list(fn='prunepp',par='nmin',value='30000'),
    list(fn='prunepp',par='nminhard',value='15000'),
    list(fn='neare',par='num',value='3'),
    list(fn='neare',par='maxsep',value='100'),
    list(fn='priore',par='sstrength',value='0'),
    list(fn='priore',par='tstrength',value='0'),
    list(fn='yxpr1',par='period',value='year')
  ))
  #putt(npd)
}




#' @export
gettoken <- function(x,split='_',i=1) {
  unlist(lapply(strsplit(x,split=split),'[[',i))
}

#one equation for all time, so ncol is number of zones
#constants are prefixed with zone so they automatically get linked
#returns a crossproduct (unlike tpriore, which must be windowed first)
#' @export
spriore <- function(yxpr3d=gett('yxpr3d'),linked=gett('linked')) {
  z <- sort(unique(gsub(patt='\\.',rep='-',x=gettoken(colnames(yxpr3d$xx)))))
  nz  <- length(z)
  neq <- nrow(linked)
  nt <- ncol(yxpr3d$xx)/nz
  sprior <- matrix(0,neq,nz,dimnames=list(linked[,rc],z))
  spriored <- yxpr3d$xx*0
  mindist <- 2
  linked[,rc:=gsub(patt='\\.',rep='-',x=rc)][,other:=gsub(patt='\\.',rep='-',x=other)][,link:=gsub(patt='\\.',rep='-',x=link)]
  stopifnot(identical(sort(union(linked[,rc],linked[,other])),z))
  for(i in 1:nrow(linked)) {
    j1 <- grep(patt=paste0('^',linked[i,rc],'$'),x=z)
    j2 <- grep(patt=paste0('^',linked[i,other],'$'),z)
    if(!length(j1)==length(j2)) browser()
    sprior[i,j1] <-  1/max(mindist,linked[i,dist])
    sprior[i,j2] <- -1/max(mindist,linked[i,dist])
  }
  spriored0 <- crossprod(sprior)
  stopifnot(nrow(spriored0)==nz&all(0<apply(abs(spriored0),1,sum))&all(0<apply(abs(spriored0),2,sum)))
  for(i in 1:nrow(spriored0)) { #distributed into identical temporal blocks
    ii <- (i-1)*nt + 1:nt
    for(jcol in 1:ncol(spriored0)) {
      jj <- (jcol-1)*nt + 1:nt
      spriored[ii,jj] <- diag(spriored0[i,jcol],nt)
    }
  }
  spriored
}


#tpx is a replacement for tpriore
#it can be trimmed on the dates in rownames, colnames
#it is not the crossproduct because needs to be windowed first
#' @export
tpx <- function(
  nz=20
  ,
  da=gett('setdad')
  ,
  slopescale=2
) {
  #pcu <- rbind(-slopescale*sdlslope(length(da))[1,],sdlcurv(length(da)),slopescale*sdlslope(length(da))[length(da)-1,])
  pcu <- sdlcurv(length(da)+4)[,3:(length(da)+2)] #mod 20180429
  #pcu
  #pcu[1,] <- pcu[1,]+slopeweight*sdlslope(length(da))
  cn <- rep.int(c('const',as.character(da)),nz)
  rn <- rep.int(as.character(c('',da,'')),nz)
  stopifnot(length(rn)==nrow(pcu)*nz & length(cn)==(ncol(pcu)+1)*nz)
  x <- matrix(0,length(rn),length(cn),dimnames=list(rn,cn)) #+1 for constants
  for(i in 1:nz) {
    ii <- (i-1)*nrow(pcu)+(1:nrow(pcu))
    jj <- (i-1)*(ncol(pcu)+1)+1+(1:ncol(pcu)) #+1 for constants, and all offset by 1 for initial constant
    x[ii,jj] <- pcu
  }
  #browser()
  x
}


#tpx1(.) is a replacement for tpx(.)[,-1] which has wrong pattern
#it can be trimmed on the dates in rownames, colnames
#it is not the crossproduct because needs to be windowed first
#' @export
tpx1 <- function(
  nz=1 #not used
  ,
  da=gett('pxmodad')[1:5]
  ,...
  ) {
  n <- length(da)
  structure(rbind(-sdlslope(n)[1,],sdlcurv(n),sdlslope(n)[n-1,]),dimnames=list(as.character(da),as.character(da)))
}

#' @export
tpx2 <- function(nz,da=gett('pxmodad')[1:10],edgeprior=c(2,1,1)) { #edgeprior is additional to minimum, ordered by tau; nz not used
  stopifnot(0<(length(da)-2*length(edgeprior))) #edges cannot consume all rows
  x1 <- tpx1(,da)
  n1 <- nrow(x1)+1
  x2 <- x1[-c(1:length(edgeprior),n1-(1:length(edgeprior))),]
  for(i in rev(seq_along(edgeprior))) {
    x2 <- rbind(
      (1+edgeprior[i])*x1[i,],
      -edgeprior[i]   *x1[i,],
      x2,
      (1+edgeprior[i])*x1[n1-i,],
      -edgeprior[i]   *x1[n1-i,]
    )
  }
  stopifnot(all(apply(x2,1,sum)==0)) #constraints
  stopifnot(all(apply(x2,2,sum)==0))
  x2[0<(apply(abs(x2),1,sum)),]
}

#NOT USED postcode: prune from level lo returning all possible parents
#' @export
hitolo <- function(lo=2,agpcod=gett('agpcod')) {
  locode <- unique(setkey(agpcod[,x1:=substr(x,1,3*lo)],x1)) #all codes down to lo
  x <- vector('list',lo)
  for(i in 1:lo) { #at each level, generate parents at that level, take unique
    genparent <- setkey(locode[,parent:=substr(x,1,3*i)][nchar(parent)==i*3],x1)
    x[[i]] <- unique(genparent)[,list(x1,parent)]
  }
  hitolod <- rbindlist(x)[nchar(x1)==3*lo]
  setkey(hitolod[,child:=strips(x1)][,parent:=strips(parent)][,x1:=NULL],parent)
}





#' @export
pcl <- function() {list(Areas=1,Districts=2,Sectors=3,unit=4)}

#' @export
dezo <- function(yxtad=gett('yxtad'),da=index(yxtad)[nrow(yxtad)-1]) {
  dezod <- data.table(de=as.numeric(coredata(yxtad[da,])),zo=colnames(yxtad),key='zo')[,lev:=nchar(zo)/3]
  putt(dezod)
}

#this function was used in ppdsgv but its output now would screw up the new jomaFun so do not use this!
#' @export
joma <- function(wimad=gett('wimad'),dezod=gett('dezod')) {
  #browser()
  setkey(wimad[,rc:=regpcode(id)],rc)
  x3 <- setkey(wimad[,rc:=substr(regpcode(id),1,9)],rc)[dezod[lev==3]]
  x2 <- setkey(wimad[,rc:=substr(regpcode(id),1,6)],rc)[dezod[lev==2]]
  x1 <- setkey(wimad[,rc:=substr(regpcode(id),1,3)],rc)[dezod[lev==1],allow=T]
  i3 <- x3[,unique(id)]
  itaken <- i3
  i2 <- setdiff(x2[,unique(id)],itaken)
  itaken <- c(itaken,i2)
  i1 <- setdiff(x1[,unique(id)],itaken)
  itaken <- c(itaken,i1)
  stopifnot(!any(duplicated(itaken)))
  jomad <- setkey(rbindlist(list(setkey(x1,id)[i1],setkey(x2,id)[i2],setkey(x3,id)[i3])),id)
  jomad
}


#' @export
newme <- function(app='cityresi',n=100) {
  setv(app=app)
  pax <- setkey(rbindlist(list(
    list(fn='prunepp',par='nmin',value='15000'),
    list(fn='prunepp',par='nmin',value='100000'),
    list(fn='priore',par='sstrength',value='1e4'),
    list(fn='priore',par='sstrength',value='1e7'),
    list(fn='priore',par='tstrength',value='1e4'),
    list(fn='priore',par='tstrength',value='1e6'),
    list(fn='yxpr1',par='period',value='quarter'),
    list(fn='yxpr1',par='period',value='year')
  )),fn,par)
  pax <- pax[1:min(n,nrow(pax))]
  pa0 <- np()
  nextv <- 1+maxver()
  me <- data.table(ver=nextv:(nextv+(nrow(pax)-1)),done=FALSE,ipa=idxrd()+(1:nrow(pax)))
  putrdatv(me,ty='me',v=0)
  for(i in 1:nrow(pax)) {
    pa <- unique(setkey(rbindlist(list(pax[i,],pa0)),fn,par))
    setv(ver=me[i,ver])
    putt(pa)
  }
}

#' @export
getme <- function() {getrdatv(app='cityresi',v=0,type='me')}

#' @export
mex <- function(me=getme()) { #execute a set of jobs in meta
  me <- me[done==FALSE]
  for(ime in me[,seq_along(as.numeric(ver))]) {
    print(paste0(ime,"/",nrow(me)))
    setv(app='cityresi',ver=me[ime,ver])
    pa <- setkey(gett('pa'),fn,par)

    nmin <- as.numeric(pa[list('prunepp','nmin'),value])
    system.time(pruneppd <- prunepp(edppd,nmin=nmin)) #these execution times coul be logged
    system.time(segd <- seg()) #45s

    period <- pa[list('yxpr1','period'),value]
    system.time(yxpr1(per=period,part=T))
    system.time(yxpr3()) # 50s


    sstrength <- as.numeric(pa[list('priore','sstrength'),value])
    tstrength <- as.numeric(pa[list('priore','tstrength'),value])
    zoned <- zone(agpcod=getlast('agpcod'))
    distanced <- distance()
    neared <- neare(num=2,maxsep=40)
    linked <- linke()
    spriored <- spriore()
    tpriored <- tpriore()
    priored <- priore(tstrength=tstrength,sstrength=sstrength)


    system.time(yxfo3(prior=priored))# 79
    system.time(yxfi3())# 2
    system.time(yxta(3))


    merd <- getrdatv(ty='me',v=0)
    merd[ver==getv()$ver,done:=T]
    putrdatv(merd,ty='me',v=0)
  }
}

#' @export
summe <- function(me=getme()[done==TRUE]) {
  r2 <- ver <- me[,ver]
  palist <- vector('list',length(r2))
  for(i in seq_along(ver)) {
    setv(ver=ver[i],ty='yxfi3d')
    ird <- ddv1()[,as.numeric(num)]
    print(ird)
    fi <- getrd(ird)
    r2[i] <- cor(as.matrix(fi))[1,2]^2
    setv(ver=ver[i],ty='pa')
    palist[[i]] <- setkey(rbind(getrd(ddv1()[,as.numeric(num)]),data.table('r2','r2',r2[i]),use.names=F),fn,par)
  }
  summed <- palist[[1]]
  for(i in 2:length(palist)) summed<-summed[palist[[i]]]
  putrdatv(summed,ver=0,ty='summed')
  summed
}

#' @export
delme <- function(me=getme()) {
  for(i in 1:nrow(me)) {
    delrd(ddv1("*","*",v=me[i,ver])[,as.numeric(num)])
  }
}


#' @export
#cross-validation MSE(tp,sp)
yxxv <- function(yxpr1d=gett('yxpr1d'),
                 nfold=5,
                 pp=c(1,4,7),
                 maxp=8
) {
  nz <- length(yxpr1d)
  ntotlist <- lapply(yxpr1d,nrow)
  ntot <- sum(unlist(ntotlist))
  dofold <- function(len,nf=nfold) {rep(1:nf,length.out=len)} #[order(runif(len))]} #removed this randomisation; actually want systematically to keep 'stratified sampling' on rd
  dofoldd <- lapply(ntotlist,dofold) #list of vector of fold, each of length nrow(yxpr1d[[i]])
  xout <- structure(vector('list',nz),names=names(yxpr1d))
  xin <- structure(vector('list',nz),names=names(yxpr1d))
  stren <- 10^(pp) #always some, just to condition it
  spriored <- gett('spriored')
  tpriored <- gett('tpriored')
  itest <- 0
  for(ifold in 1:nfold) {
    iout <- lapply(dofoldd,'==',ifold) #only ifold is out...
    iin <- lapply(iout,'!') #...rest is in
    for(iz in 1:nz) { #partition this fold
      xin[[iz]] <- yxpr1d[[iz]][iin[[iz]],]
      rownames(xin[[iz]]) <- rownames(yxpr1d[[iz]])[iin[[iz]]]
      xout[[iz]] <- yxpr1d[[iz]][iout[[iz]],]
      rownames(xout[[iz]]) <- rownames(yxpr1d[[iz]])[iout[[iz]]]
    }
    yxpr3din  <- yxpr3(yxpr1d=xin ) #not a function of prior
    fold <- NULL
    ii<-1
    for(js in seq_along(pp)) {
      is <- pp[js]
      for(jt in seq_along(pp)){ #~22s, mostly on yxfo3
        #         for(is in 1:maxp) {
        #       for(it in 1:maxp){ #~22s, mostly on yxfo3
        it <- pp[jt]
        yxfo3din <- yxfo3(yxpr3d=yxpr3din,prior=priore(ts=stren[jt],ss=stren[js],spriored=spriored,tpriored=tpriored))
        yxfi3dout <- yxfi3(yxpr1d=xin,yxfo3d=yxfo3din)[,se:=(yfit-y)^2]
        yxfi3din  <- yxfi3(yxpr1d=xout,yxfo3d=yxfo3din)[,se:=(yfit-y)^2]
        fold[[ii]] <- rbindlist(list(
          yxfi3dout[,list(se=mean(se)),list(pc=substring(id,0,0))],
          yxfi3dout[,list(se=mean(se)),list(pc=substring(id,0,3))],
          yxfi3dout[,list(se=mean(se)),list(pc=substring(id,0,6))]
        ))[,infold:=F][,fold:=ifold][,it:=it][,is:=is]
        ii<-ii+1
        fold[[ii]] <- rbindlist(list(
          yxfi3din[,list(se=mean(se)),list(pc=substring(id,0,0))],
          yxfi3din[,list(se=mean(se)),list(pc=substring(id,0,3))],
          yxfi3din[,list(se=mean(se)),list(pc=substring(id,0,6))]
        ))[,infold:=T][,fold:=ifold][,it:=it][,is:=is]
        ii<-ii+1
      }
    }
    putrdatv(rbindlist(fold),type=paste0('fold',ifold))
    print(ifold)
  }
}

#get the cross-validation MSE results as a single table
#' @export
xv <- function(ff='fold[1-5]',oos=TRUE,pcx='^$') {
  setv(type=ff)
  x <- rbindlist(lapply(as.list(ddv1()[,as.numeric(num)]),getrd))
  x <- setkey(x[,list(mse=mean(se)),by='pc,infold,it,is'],infold,pc,it,is)
  if(oos) x <- x[infold==TRUE]
  xvd <- x[grepl(patt=pcx,x=x[,pc])]
  putt(xvd)
}

#convert table to matrix
#' @export
xvm <- function(xvd=gett('xvd'),dorank=T,domean=T,rescale=TRUE) {
  if(dorank) xvd[,mse:=rank(mse)/nrow(xvd)][]
  if(domean) xvd <- xvd[,list(mse=mean(mse)),'is,it'][]
  if(rescale) xvd[,mse:=rank(mse)/.N]
  xvmd <- tabtomat(data.frame(xvd[,list(is,it,mse)]))
  putt(xvmd)
}

#plots of the MSE matrix
#' @export
xvxvp <- function(xvmd=gett('xvmd')) {
  par(mfrow=c(1,2))
  imin <- which.min(apply(xvmd,1,min))
  jmin <- which.min(apply(xvmd,2,min))
  image(t(xvmd),xlab='temporal',ylab='spatial')
  plot(zoo(cbind(xvmd[imin,],xvmd[,jmin])),scr=1,col=rainbow(2),ylim=c(0,max(xvmd)),ty='b',pch=19,xlab='smoothing',ylab='mean squared error')
  legend(col=rainbow(2),pch=19,x='topleft',leg=c('temporal','spatial'),bty='n')
  grid()
}

#find a 3x3 segment with lowest mean mse
#' @export
xvij <- function(xvmd=gett('xvmd')) {
  f1 <- function(i=2,j=2,x=xvmd) {
    ij <- sweep(as.matrix(expand.grid(-2:1,-2:1)),FUN='+',MAR=2,STATS=c(i,j))
    mean(x[ij])
  }
  x <- outer(3:9,3:9,FUN=Vectorize(f1))
  xvijd <- c(which.min(apply(x,1,min)),which.min(apply(x,2,min)))+1
  putt(xvijd)
}

#' @export
ijse <- function(is=0,it=0,xvijd=gett('xvijd')) {
  yxfi3d <- gett(paste0('fi',xvijd[1]+is,xvijd[2]+it))
  yxfo3d <- gett(paste0('fo',xvijd[1]+is,xvijd[2]+it))
  putt(yxfi3d) #} these 2 are not 'canonical' ie are side-effects of this fn, but done for 'safety' in case they are assumed optimal (somewhere)
  putt(yxfo3d) #}
  ijsed <- yxfo3d[gettoken(yxfo3d[,cell],,2)!='unity'] #this is the difference between ijsed and yxfo3d
  putt(ijsed) #the naming is a bit wierd
}

#daily return
#' @export
yxta <- function(ijsed=gett('ijsed')) {
  yxtad <- mz(t(tabtomat(data.frame(t(data.frame(strsplit(ijsed[,cell],split='_'))),ijsed[,est]))))
  putt(yxtad)
}

#period return
#' @export
yxtap <- function(yxtad=gett('yxtad')) {
  x <- as.numeric(diff(index(yxtad)))
  ndays <- c(mean(x),x)
  yxtapd <- sweep(yxtad,STAT=ndays,MAR=1,FUN=`*`)
  putt(yxtapd)
}

#order
#' @export
o1 <- function(yxtapd=gett('yxtapd'),cumulate=T,exponentiate=F,percent=F,refdate=index(yxtapd)[min(which(index(yxtapd)>as.Date('2009-01-02')))],reorder=T) {
  #if(period) { z1 <- yxtad*as.numeric(mean(diff(index(yxtad)))) } else { z1 <- yxtad }
  refdate <- index(yxtapd)[which.min(abs(refdate-index(yxtapd)))]
  z1 <- yxtapd
  if(cumulate) { z2 <- cumsum(z1) } else { z2 <- z1 }
  if(!is.null(refdate)) {
    z3 <- sweep(z2,MAR=2,STAT=z2[refdate],FUN='-')
  } else {
    z3<-z2
  }
  if(exponentiate) {z4<-exp(z3)-1} else { z4 <- z3 }
  if(reorder) {z5 <- z4[,order(z4[nrow(z4)]),drop=F]} else {z5 <- z4}
  if(percent) {z6<-z5*100} else {z6 <- z5}
  o1d <- z6
  putt(o1d)
}


#plot
#' @export
p1 <- function(o1d=gett('o1d'),addlegend=T,col=rainbow(ncol(o1d)),...) {
  plot(o1d,col=col,scr=1,...)
  if(addlegend) legend(x='topleft',col=rev(col),lty=1,leg=rev(colnames(o1d)),cex=.7)
  grid()
}



#solve for a set of tp, sp, but without the xv, this time full sample
#' @export
xv9 <- function(xvijd=gett('xvijd')) {
  is <- 2:9#xvijd[1]+(-2:1)
  it <- 2:9#xvijd[2]+(-2:1)
  for(i in seq_along(is)) {
    for(j in seq_along(it)) {
      yxfi3(yxfo3=yxfo3(priored=priore(ss=10^is[i],ts=10^it[j])))
      putrdatv(gett('yxfo3d'),type=paste0('fo',is[i],it[j]))
      putrdatv(gett('yxfi3d'),type=paste0('fi',is[i],it[j]))
    }
  }
}


#' @export
ldgscale <- function(ldgxd=gett('ldgxd'),yxtapd=gett('yxtapd'),scoxd=gett('scoxd'),decd=gett('decd'),annualise=F,percent=F) {
  ldgxd <- setkey(ldgxd,pcode)
  ppa <- round(365/mean(as.numeric(diff(index(yxtapd))))) #ie periods per annum

  allvcv1 <- sum(c(ppa,acf(as.matrix(scoxd),lag.max=(ppa-1),plot=F)$acf[,1,1][-1]*2*((ppa-1):1)))
  allvcv2 <- sum(c(ppa,acf(as.matrix(scoxd),lag.max=(ppa-1),plot=F)$acf[,2,2][-1]*2*((ppa-1):1)))
  allvcvr <- sum(c(ppa,acf(as.numeric(as.matrix(rnorm(decd$rer))),lag.max=(ppa-1),plot=F)$acf[,,1][-1]*2*((ppa-1):1)))
  allvcvt <- sum(c(ppa,acf(as.numeric(as.matrix(rnorm(decd$ret))),lag.max=(ppa-1),plot=F)$acf[,,1][-1]*2*((ppa-1):1)))
  if(annualise) {
    annfac <- sqrt(c(allvcv1,allvcv2,allvcvr,allvcvt))
    annfacm <- ppa
  } else {
    annfac <- rep(1,4) #sqrt(c(ppa,ppa))
    annfacm <- 1
  }
  percfac <- ifelse(percent,100,1)
  bothfac <- annfac*percfac
  ldgxd[,loadings1:=loadings1*bothfac[1]][,loadings2:=loadings2*bothfac[2]][,residvol:=residvol*bothfac[3]][,sdev:=sdev*bothfac[4]][,meanret:=meanret*annfacm*percfac]
}


#' @export
cex <- function(celid=gett('celid'),cncd=gett('cncd'),FUN=scoce) { #rolling apply FUN to ret
  scoxd <- vector('list',length(celid)+2)
  iinit <- index(cncd)<min(celid[[1]]$estwin)
  if(any(iinit)) iw0 <- index(cncd)[iinit] else iw0 <- NULL #the 'run-in'
  j <- buice(celid[[1]])
  if(0<length(iw0)) scoxd[[1]] <- do.call(FUN,list(x=celid[[1]],ret=cncd[iw0,j]))
  if(1<length(celid)) {
    da <- as.Date(names(celid))
    for(i in 1:length(celid)) { #this assumes more than 1 date in celid
      iw <- celid[[i]]$estwin
      if(0<length(iw0)) {iw <- iw[max(iw0)<iw]}
      iw0 <- c(iw0,iw)
      j <- buice(celid[[i]])
      scoxd[[i+1]] <- do.call(FUN,list(x=celid[[i]],ret=cncd[iw,j]))  #scoce(celid[[i]],cncd[iw,])
    }
    #    browser()
    iw1 <- index(cncd)[max(iw)<index(cncd)] #run-out
    if(0<length(iw1)) {
      j <- buice(celid[[length(celid)]])
      scoxd[[length(celid)+2]] <-  do.call(FUN,list(x=celid[[length(celid)]],ret=cncd[iw1,j])) #scoce(celid[[length(celid)]],cncd[iw1,])
    } else {
      scoxd <- scoxd[!unlist(lapply(scoxd,is.null))]
    }
  }
  scoxd
}


#' @export
dec1 <- function(celid=gett('celid'),cncd=gett('cncd'),nona=T) {
  x<-cex(celid=celid,cncd=cncd,FUN=face)
  x <- x[!unlist(lapply(x,is.null))]
  #x <- x[!unlist(lapply(x,is.na))]
  decd <- x[[1]]
  for(i in seq_along(names(x[[1]]))) {
    decd[[i]] <- mz(tabtomat(Reduce(rbind,lapply(lapply(x,'[[',i),mattotab))))
    j <- match(colnames(cncd),colnames(decd[[i]]))
    decd[[i]] <- decd[[i]][,colnames(cncd)[!is.na(j)]]
    #decd[[i]] <- Reduce(rbind,lapply(x,'[[',i))
  }
  if(nona) { decd <- decd[keep(decd)] }
  decd
  #putt(decd)
}



#' @export
scox <- function(celid=gett('celid'),yxtapd=gett('yxtapd')) {
  scoxd <- Reduce(rbind,cex(celid,yxtapd,scoce))
  scoxd
  #putt(scoxd)
}


#' @export
vz <- function(x) {is.zoo(x)}

#commented 20180103 to reuse name
#' #' @export
#' dec <- function(celid=gett('celid'),yxtapd=gett('yxtapd')) {
#'   x<-cex(celid=celid,yxtapd=yxtapd,FUN=face)
#'   decd <- x[[1]]
#'   for(i in seq_along(names(x[[1]]))) {
#'     decd[[i]] <- Reduce(rbind,lapply(x,'[[',i))
#'   }
#'   decd
#'   #putt(decd)
#' }


#' @export
ab <- function(x=aacol1()) {paste('[',paste(paste(paste("'",x,"'",sep=''),collapse=',',sep=''),']',sep=''),sep='')}






#this function references an old jomad which was written by function joma
#that was non-standard usage ie should have been called jomaFun
#then in ppdsgv2, jomaFun does exist but probably writes a different object so
#this will be broken if run again ever
#' @export
coropng <- function(yxtad=o1(gett('yxtapd'),cum=F,perc=T,ref=NULL,reo=F),ylim=NULL,rootdir='./data/',skip=1,main='main') {
  da <- as.Date(gett('setdad'))
  for(i in seq(from=1,to=length(da),by=skip)) {
    dezod <- dezo(yxtad,da[i])
    if(is.null(ylim)) {
      jomad <- joma()[,fill:=de]
    } else {
      jomad <- joma()[,fill:=pmin(pmax(de,min(ylim)),max(ylim))]
    }
    png(file=paste0(rootdir,da[i],'.png'))
    df <- data.frame(jomad)
    bnames <- setkey(copy(jomad)[,list(long=mean(long),lat=mean(lat),rc,id),id],rc)
    plot(ggplot(df) +
           geom_polygon(aes(long, lat, group = group,fill=fill),alpha=.5,color='blue') +
           geom_text(data=bnames, aes(long, lat, label = id,  map_id =NULL), size=4,col='blue') +
           scale_fill_gradient(low="blue", high="orange",limits=ylim) +
           labs(fill='%')  +
           ggtitle(main))
    dev.off()
  }
}

#' @export
corodec <- function(decd=gett('decd'),comp=list(market='re1',quality='re2',residual='rer',total='ret'),skip=5) {
  #ylim <- list(re1=c(200,500),re2=c(-10,10),re3=c(-30,30),ret=c(200,400))
  for(i in seq_along(comp)) {
    coropng(yxtad=o1(decd[[comp[[i]]]],cum=T,perc=T,ref=NULL,reo=F,exp=T),rootdir=paste0('./image/cu/',comp[i],'/'),skip=skip,main=names(comp)[i])
  }
  for(i in seq_along(comp)) {
    coropng(yxtad=o1(decd[[comp[[i]]]],cum=F,perc=T,ref=NULL,reo=F,exp=T),rootdir=paste0('./image/nocu/',comp[i],'/'),skip=skip,main=names(comp)[i])
  }
}



#' @export
datecols <- function(dt) {
  j <- colnames(dt)
  j <- j[nchar(j)==10]
  j <- j[grepl(patt='-',x=j)]
  j[j==as.character(as.Date(j))]
}


#' @export
yield <- function(rawdlyz=getrd(max(greprd('yields'))),da=index(gett('yxtad')),cn=c('term.premium','inflation',"earn.yield")) { #daily 3 series, dtlocf(x,weekday=0:6)
  #yields <- getrd(greprd('yields'))[da]
  yields <- rawdlyz[da,]
  y1 <- (yields[,'GUKG10 Index',drop=F] -yields[,'BP0003M index',drop=F])/100
  y2 <- (yields[,'GUKG10 Index',drop=F] -yields[,'GUKGIN10 Index',drop=F])/100
  y3 <- (yields[,'EARN_YLD',drop=F])/100
  yieldd <- cbind(y1,y2,y3)
  colnames(yieldd) <- cn
  putt(yieldd)
}

#' @export
varyx <- function(scoxd=gett('scoxd'),yieldd=gett('yieldd'),norm='ts') {
  varyxd <- cbind(scoxd,yieldd)
  if(norm!='') {
    varyxd <- zoonorm(varyxd,norm)
  }
  putt(varyxd)
}



#' @export
yxfan <- function(varpd=gett('varpd'),
                  colors = NULL,
                  cis = NULL,
                  name = names(varpd$fcst),
                  main = name,
                  ylab = '', #this seems to have a bug
                  xlab = 'quarters',
                  col.y = NULL,
                  nc=2,
                  plot.type = 'multiple',
                  mar = par("mar"),
                  oma = par("oma"),
                  tail=20,
                  style=c('fanchart','line')) {
  force(varpd)
  force(name)
  style <- match.arg(style)
  varpd$endog <- tail(varpd$endog,tail)
  if(style=='fanchart') {
    fanchart(varpd,colors=colors,cis=cis,name=name,main=main,ylab=ylab,xlab=xlab,col.y=col.y,nc=nc,plot.type=plot.type,mar=mar,oma=oma)
  } else {
    plot(varpd,colors=colors,cis=cis,name=name,main=main,ylab=ylab,xlab=xlab,col.y=col.y,nc=nc,plot.type=plot.type,mar=mar,oma=oma)
  }
}


#' @export
buifo <- function(scofod=gett('scofod'),ldgxd=gett('ldgxd'),decd=gett('decd')) {
  tt <- ldgxd[,max(time)]
  ldgmat <- structure(t(as.matrix(ldgxd[time==tt,list(loadings1,loadings2)])),dimnames=list(NULL,ldgxd[time==tt,pcode]))
  rer <- decd$rer
  M <- zoo(scofod[,1,drop=F]%*%ldgmat[1,,drop=F],index(scofod))
  S <- zoo(scofod[,2,drop=F]%*%ldgmat[2,,drop=F],index(scofod))
  iwin <- format(index(rer),'%Y')>=as.numeric(format(max(index(rer)),'%Y'))-5
  R <- mz(structure(t(matrix(rep(apply(rer[iwin],2,mean),nrow(scofod)),ncol(M),nrow(scofod))),dimnames=list(as.character(index(scofod)),colnames(M))))
  buifod <- list(M=M,S=S,R=R,T=M+S+R)
  putt(buifod)
}

#this called by buifoFun - not sure about the above
#' @export
buifoX <- function(scofod,ldgxd,decd,rmom) {
  tt <- ldgxd[, max(time)]
  ldgmat <-
    structure(t(as.matrix(ldgxd[time == tt, grep('loadings', names(ldgxd)), with =
                                  F])), dimnames = list(NULL, ldgxd[time == tt, pcode]))
  rer <- decd$rer
  M <- zoo(scofod[, 1, drop = F] %*% ldgmat[1, , drop = F], index(scofod))
  S <- zoo(scofod[, -1, drop = F] %*% ldgmat[-1, , drop = F], index(scofod))
  iwin <-
    format(index(rer), '%Y') >= as.numeric(format(max(index(rer)), '%Y')) -
    rmom
  R <-
    mz(structure(t(matrix(
      rep(apply(rer[iwin], 2, mean), nrow(scofod)), ncol(M), nrow(scofod)
    )), dimnames = list(as.character(index(
      scofod
    )), colnames(M))))
  buifod <- list(M = M,
                 S = S,
                 R = R,
                 T = M + S + R)
  buifod
}


#' @export
hless <- function(varyxd=gett('varyxd'),da=max(index(varyxd)),...) {
  col <- aacol1(ncol(varyxd))
  plot(sweep(varyxd,STAT=varyxd[da,],MAR=2,FUN='-'),scr=1,col=col,lwd=2,xlab='',ylab='',...)
  grid()
  legend(x='topleft',col=col,lty=1,leg=colnames(varyxd))
}

#' @export
buicex <- function(x){
  rownames(x$full)
}

#' @export
jpp <- function(jinc=c("unique_id", "price_paid", "deed_date", "postcode", "property_type",
                       "new_build", "estate_type", "saon", "paon", "street", "locality",
                       "town", "district", "county", "linked_data_uri", "id", "rcode"),
                jdrop=c("unique_id","saon","paon","street","locality","town","district","county","linked_data_uri")) {
  setdiff(jinc,jdrop)
}

#' @export
narep <- function(x,nasub='') {
  jn <- colnames(x)
  for(j in seq_along(jn)) {
    i <- which(is.na(x[,j,with=FALSE]))
    x[i,(j):=nasub]
    #x[i,j:=nasub,with=F]
  }
  x[]
}

#' @export
imgdir <- function() {
  suppressWarnings(nodir <- shell('dir image'))
  if(nodir) {
    # system('mkdir image')
    # system('mkdir image/cu')
    # system('mkdir image/cu/re1')
    # system('mkdir image/cu/re2')
    # system('mkdir image/cu/rer')
    # system('mkdir image/cu/ret')
    # system('mkdir image/nocu')
    # system('mkdir image/nocu/re1')
    # system('mkdir image/nocu/re2')
    # system('mkdir image/nocu/rer')
    # system('mkdir image/nocu/ret')
    shell('mkdir image')
    shell('mkdir image/cu')
    shell('mkdir image/cu/re1')
    shell('mkdir image/cu/re2')
    shell('mkdir image/cu/rer')
    shell('mkdir image/cu/ret')
    shell('mkdir image/nocu')
    shell('mkdir image/nocu/re1')
    shell('mkdir image/nocu/re2')
    shell('mkdir image/nocu/rer')
    shell('mkdir image/nocu/ret')
  }
}


#' @export
allcode <- function(celid=gett('celid')) {rownames(gett('celid')[[1]]$full)}

#' @export
coho <- function(target = 'MK-43-',
                 neared = gett('neared'),
                 nmin = 20) {
  cohort <- list(target)
  cohod <- data.table(rc = target, ilev = 1)
  i <- 1
  while ((length(unlist(cohort)) < nmin) & (i < nmin)) {
    i <- length(cohort)
    cohort[[i + 1]] <-
      setdiff(neared[cohort[i]][, unique(other)], unlist(cohort[1:i]))
    if (0 < length(cohort[[i + 1]]))
      cohod <- rbind(cohod, data.table(rc = cohort[[i + 1]], ilev = i + 1))
  }
  cohod
}



#' @export
coname <- function(ico=1,tag='co') {paste0(zeroprepend(ico,4),tag)}


#' @export
keep <- function(x) {unlist(lapply(x,function(x){!all(is.na(x))}))}

#' @export
pm1 <-
  function(jomaxd = jomax1(j = 'meanret'),
           low = "purple",
           high = "red",
           annotate = F,
           leg = '',
           nlabel = 30,
           focusrc = extremarc[1],
           setscale=T,
           ...) {
    #browser()
    ggp <- ggplot(data.frame(jomaxd)) +
      geom_polygon(aes(long, lat, group = group, fill = de), color = NA)
    if(setscale) { #NOTE THIS COMMENTED OUT 2017-07-09 as it freezes on s_f_g call wtf
      ggp <- ggp +
        suppressWarnings(scale_fill_gradient2(
          low = low,
          mid = "white",
          high = high,
          midpoint = 0,
          space = "rgb",
          na.value = "grey50",
          guide = "colourbar"
        )) +
        labs(fill = leg) + coord_fixed(ratio = 1 / latlong())
    }
    if (annotate) {
      uniquelevels <-
        setkey(jomaxd, de)[data.table(jomaxd[, unique(de)]), mult = 'first']
      i <-
        intersect(1:nrow(uniquelevels), unique(c((1:(
          nlabel / 2
        )), nrow(uniquelevels) - (0:(
          nlabel / 2
        )))))
      extremarc <- uniquelevels[i, rc]
      if (focusrc %in% jomaxd[, unique(rc)])
        extremarc <- c(extremarc, focusrc)
      bnames <-
        setkey(copy(jomaxd)[, list(long = mean(long), lat = mean(lat), rc, id), id], rc)[extremarc]
      ggp <-
        ggp + geom_text(
          data = bnames,
          aes(long, lat, label = id),
          size = 4,
          col = 'black'
        )
    }
    ggp <- ggp + theme(legend.title=element_blank()) #added 2017-04-24 for brexit charts
    plot(ggp)
  }


#' @export
pm <- function(j='meanret',zomaxd=gett('zomad'),dezod=gett('dezod'),annotate=F,rcx='SE-22-',rad=.0003,window=F,...) {
  while('de'%in%colnames(dezod)) dezod[,de:=NULL]
  setnames(dezod,j,'de')
  x<-setkey(copy(dezod)[,de:=de*100],zo)
  jomad <- joma1(dezod=x,zomad=zomaxd) #was Fun
  if(window) {
    lmad <- jomad[,list(eex=mean(long),nnx=mean(lat),pc='-'),rc]
    distanced <- setkey(dist1(lmad,sf=8),rc)[rcx,allow=T] #was Fun
    jomad <- setkey(jomad,rc)[setkey(distanced,other)][dist<rad]
  }
  pm1(jomad,annotate=annotate&window,leg=j,...)
}


#' @export
px <- function(nn='jomaxd',annotate=F,...) {
  getgd(nn)
  pm1(jomaxd,annotate=annotate,...)
}

#all permutations
#' @export
dist1 <- function(zoned=gett('zoned'),sf=3) {
  pairs <- setkey(zoned[,one:=1],one)
  distanced <- pairs[pairs,allow=T][,dist:=round(.001*((i.eex-eex)^2+(i.nnx-nnx)^2)^.5,sf)][,list(rc,pc,other=i.rc,eex,nnx,dist)]
}

#distances from rc1
#' @export
distx <- function(zoned=gett('zoned'),sf=3,rc1=zoned[1,rc]) {
  pairs <- setkey(zoned[,one:=1],one)
  #distanced <- pairs[rc==rc1][pairs,allow=T][,dist:=round(.001*(((i.eex-eex))^2+(i.nnx-nnx)^2)^.5,sf)][,list(rc,pc,other=i.rc,eex,nnx,dist)]
  distanced <- pairs[grepl(rc1,rc)][pairs,allow=T][,dist:=round(.001*(((i.eex-eex))^2+(i.nnx-nnx)^2)^.5,sf)][,list(rc,pc,other=i.rc,eex,nnx,dist)]
}

#' @export
latlong <- function(latitude=52) {
  cos((latitude/90)*(pi/2))
}

#' @export
kmdeg <- function() {
  1e4/90
}



#' @export
rotest <- function(celirawd=gett('celirawd')[[1]],distanced=gett('distanced'),rcx='WC-',rad=30) {#},win=T) {
  #rc <- jomax1(rcx=rcx,rad=rad,win=win)[,unique(rc)] #nb this covertly accesses zomad and dezod and probably should not as they are later steps
  rc <- distanced[rc==rcx|other==rcx][dist<rad][,union(rc,other)]
  r1 <- eigen(crossprod(ldgce(celirawd)[intersect(rc,buice(celirawd)),]))$vectors
  rot <- sweep(r1,STAT=sign(diag(r1)),FUN=`*`,MAR=2)
  rot
}


#' @export
rotapply <- function(celirawd=gett('pxcecelirawd'),rot=eigen(crossprod(ldgce(celirawd[[1]])))$vectors) {
  celid <- copy(celirawd)
  lapply(celid,rotapp,rot=rot)
}

#' @export
rotapp <- function(x,rot=eigen(crossprod(ldgce(x)))$vectors) {
  x1 <- copy(x)
  x1$loadings <- x$loadings%*%rot
  if('fcp'%in%names(x1)) {x1$fcp <- x$fcp%*%rot}
  x1$fmp <- x$fmp%*%rot
  colnames(x1$loadings) <- colnames(x$loadings)
  colnames(x1$fmp) <- colnames(x$fmp)
  class(x1) <- 'ce'
  x1
}



#this used to be a step jomaxFun
#' @export
jomax1 <- function(j='meanret',zomad=gett('zomad'),dezodg=gett('dezod'),rcx='SE-22-',rad=.0003,window=F,addparents=F,noshow='TD-') {
  dezod <- setkey(setnames(copy(dezodg),j,'de')[,deperc:=de*100],zo)
  jomad <- joma1(dezod=dezod,zomad=zomad)
  if(window) {
    lmad <- setkey(jomad[,list(eex=mean(long)*kmdeg()*1000*latlong(),nnx=mean(lat)*kmdeg()*1000,pc='-'),rc],rc)
    rcx1 <- lmad[grepl(rcx,rc),rc][1] #find first rc that part-matches
    distanced <- setkey(distx(lmad,sf=8,rc1=rcx1),rc)[rcx1,allow=T] #changed from step to fn
    jomad <- setkey(jomad,rc)[setkey(distanced,other)][dist<rad | ( (addparents) & (grepl(paste0(substr(rcx1,1,3),'$'),datakey))  )]#nearby or (addparents and isparent)
  }
  jomaxd <- jomad[(!is.na(datakey))&!(rc%in%noshow)]
  jomaxd
}

#this de-stepped for jomax1()
#' @export
joma1 <- function(zomad=gett('zomad'),dezod=gett('dezod')) {
  jomad <- setnames(dezod[zomad],'zo','datakey')[,lev:=NULL]
  jomad
}


#' @export
similar <- function(da=max(index(varyxd)),varyxd=gett('varyxd'),before=4,j=1:3) {
  varyxd <- varyxd[,j]
  i <- apply(sweep(varyxd,STAT=varyxd[da,],FUN='-',MAR=2)^2,1,sum)[-(nrow(varyxd)+0:-before)]
  da0 <- index(varyxd)[which.min(i)]
  colnames(varyxd) <- varlabl()
  rownames(varyxd) <- as.character(index(varyxd))
  varyxd[c(da0,da),]
}

#' @export
varlabl <- function() {
  labl <- c('fmp1'='factor 1','fmp2'='factor 2','fmp3'='factor 3','term.premium'='yield curve slope','inflation'='breakeven inflation','earn.yield'='equity earnings yield')
}

#' @export
rereta <- function(x) {
  j1 <- jdz()
  j2 <- unlist(j1[match(names(x),names(j1))])
  j3 <- j2[!is.null(j2)]
  setnames(x[,zo:=irregpcode(zo)][,names(j3),with=F],j3)[]
}

#' @export
jdz <- function() {
  #list(zo='postcode',loadings1='factor 1',loadings2='factor 2',loadings3='factor 3',sdev='vol',value='price',beta='beta',dd1='07-08',dd2='from 07',meanret='mean total',meanrer='mean residual',vafo='forecast',fprem='factor premium',irp='historical IR',irf='forecast IR',rho='correlation')#,delmean='delta mean',delvol='delta vol')
  #list(zo='postcode',loadings1='factor 1',loadings2='factor 2',loadings3='factor 3',sdev='vol',median='median',p25='quartile1',beta='beta',dd1='07-08',dd2='from 07',r1y='last 12m',trans='3m sales',meanret='mean total',meanrer='residual',vafo='forecast',fprem='factor',rho='correlation',PV='value Bn',ppm2='price/m2')#,delmean='delta mean',delvol='delta vol')
  list(zo='postcode',loadings1='factor 1',loadings2='factor 2',loadings3='factor 3',sdev='vol',median='median',p25='quartile1',beta='beta',dd1='07-08',dd2='from 07',r1y='last 12m',trans='12m sales',turn='12m sales/stock',meanret='mean total',meanrer='residual',vafo='forecast',rho='correlation',ppm2='price/m2',dist='km radius')#,delmean='delta mean',delvol='delta vol')
}

#' @export
invlist <- function(x=jdz()) {
  structure(as.list(names(x)),names=unlist(x))
}

#CUTCUT
#' @export
gvisformats <- function() {
  list('07-08'="#.#%",
       'from 07'='#.#%',
       'factor 1'='#.#%',
       'factor 2'='#.#%',
       'factor 3'='#.#%',
       'vol'='#.#%',
       'beta'='#.#',
       'mean total'='#.#%',
       'residual'='#.#%',
       'forecast'='#.#%',
       'median'='#',
       'quartile1'='#',
       'correlation'='#%',
       'sales/12m'='#%',
       'last 12m'='#.#%',
       'price/m2'='###'
  )
}

#' @export
p2score <- function(o1d=gett('o1d'),addlegend=T,col=rainbow(ncol(o1d)),...) {
  o2d <- data.table(mattotab(o1d))[,date:=as.Date(date)][,factor:=substr(bui,4,4)][,bui:=NULL]
  p <- ggplot(data.frame(o2d),aes(date,field,col=factor))
  p + geom_line(size=1.) + theme(axis.title.x = element_blank()) + ylab('cumulative score')  +ggtitle('factor performance')
}

#' @export
p2perf <- function(o1d=gett('o1d'),addlegend=T,col=rainbow(ncol(o1d)),...) {
  o2d <- data.table(mattotab(o1d))[,date:=as.Date(date)][,postcode:=irregpcode(bui)][,bui:=NULL]
  p <- ggplot(data.frame(o2d),aes(date,field,col=postcode))
  p + geom_line(size=1.) + theme(axis.title.x = element_blank()) + ylab('cumulative log return')  +ggtitle('postcode index')
}

#' @export
compnam <- function(){c('factor 1','factor 2','factor 3','total','residual')}


#' @export
pdecd <- function(decd=gett('decd'),dec2xd=gett('dec2xd'),j=c('M--30-','W--2--'),cc=ggpalette8(),...) {
  ii <- c(1,5,2,4,3)#seq_along(compnam())
  pp <- decd
  #f1 f2 f3 / loc12 locr total
  #f1 loc12 f3 locr f3 total
  ii <- 1:6
  compnam <- function(){c('factor 1', 'local factors', 'factor 2', 'residual', 'factor 3', 'total')}
  pp <- list(decd[[1]],dec2xd[[1]]+dec2xd[[2]],decd[[2]],dec2xd[[4]],decd[[3]],decd[[4]])
  j <- sort(j)
  dd <- lapply(lapply(pp,'[',,j,drop=F),o1,...)
  yr <- range(as.numeric(unlist(dd)))
  x<-lapply(lapply(dd,p2perf),`+`,expand_limits(y=range(as.numeric(unlist(dd)))))
  x<-lapply(x,`+`,theme(legend.position="none"))
  x[[1]] <- x[[1]]+
    theme(legend.justification=c(1,0), legend.position=c(1,0), legend.title=element_blank(),
          legend.key = element_rect(colour = NA, fill = NA))
  x[-(1:2)]<-lapply(x[-(1:2)],`+`,ylab(""))
  for(i in seq_along(x)) x[[i]]<-x[[i]]+ggtitle(compnam()[i])+scale_fill_manual(values=cc) + scale_colour_manual(values=cc)
  multiplot(plotlist=x[ii],cols=3)
}




#' @export
ntile <- function(x,n=3) {as.numeric(cut(x, breaks = quantile(x, probs = (0:n)/n),include.lowest = TRUE, labels = 1:n))}

#' @export
iddate <- function(id,deed_date) {paste0(id,'.',deed_date)}

#not used
#' @export
finalcompleteperiod <- function() {
  as.character(max(index(gett('varyxd'))))
}
#' @export
finalpartialperiod <- function() {
  as.character(max(names(gett('celid'))))
}

#map rc to zone
#' @export
rczo <- function(
  rc='NG-1--'
  ,
  zoned=gett('zoned')
) {
  rcx <- rc
  zo <- zoned[,unique(rc)]
  while(!(rcx%in%zo) & 3<nchar(rcx)) rcx <- substr(rcx,1,nchar(rcx)-3)
  if(!(rcx%in%zo))  stop(paste0('failed to find regular code: ',rc))
  rcx
}

#' @export
varpplot <-
  function(varpd = gett('varpd')) {
    fanchart(
      varpd,
      name = 'fmp1',
      main = 'Factor 1 : the market rate of growth',
      ylab = 'quarter score',
      xlab = 'quarter from 1995'
    )
  }

#' @export
vindis <- function(vind=gett('vind'),rc='SW-6--',j0='sold.bought') {
  x <- copy(vind)[,mu:=mu][,se:=se]
  setkey(setnames(x,'y0','sold.bought'),rcode,sold.bought,y1)
  x1 <- lapply(lapply(list(
    mu=dcast(x[rc],sold.bought~y1,value.var='mu'),
    se=dcast(x[rc],sold.bought~y1,value.var='se'),
    n=dcast(x[rc],sold.bought~y1,value.var='n')
  ),t),data.table,keep.rownames=T)
  x2 <- data.table(t(c(rn='subtotal.resold',apply(x1$n[c(-1,-nrow(x1$n)),-1],2,sum,na.rm=T))))
  x1$n <- rbind(x1$n,x2)

  for(i in seq_along(x1)) {
    colnames(x1[[i]]) <- as.character(x1[[i]][1,])
    colnames(x1[[i]])[1] <- j0
    x1[[i]] <- x1[[i]][-1]
  }
  x0 <- copy(x1$n)
  n<-nrow(x0)
  x1$n <- x0[c(1:(n-2),n,n-1),]
  x1
}

#2017-02-24 prune down all elements of cod to the segments ending on or before daend
#' @export
codx <- function(
  ico=1
  ,
  daend='2005-06-30'
  ,
  cod=cocoget(ico)   #gett(coname(ico,'co')) #no longer keep cod like this - see coco
) {
  stopifnot(daend%in%colnames(cod[[1]]))
  jend <- which(colnames(cod[[1]])==daend) #all have same cols
  if(jend==ncol(cod[[1]])) return(cod) #no extraction needed
  codxd <- lapply(cod,`[`,0,0) #empty copy
  for(i in seq_along(cod)) {
    ipend <- apply(cod[[i]][,(jend+1):ncol(cod[[i]])],1,sum)==0 #pending periods
    codxd[[i]] <- cod[[i]][ipend,1:jend] #select rows with no pending accrual after daend
  }
  codxd
}
codxchk <- function() {
  cod <- cocoget(1) #gett(coname(1))
  daend <- '2005-06-30'
  cod1 <- codx(daend=daend,cod=cod)
  x <- unlist(lapply(cod,ncol))-unlist(lapply(cod1,ncol))
  stopifnot(all(0<x & all((x-max(x))==0)))
  stopifnot(all(colnames(cod1[[1]])[-(1:2)]<=daend))
  stopifnot(max(cod1[[1]])[-(1:2)]==daend)
  T
}

#2017-02-24 accessor for trprior(nz), prepared in tpriorputFun
#' @export
tpriorget <- function(nz=cohod[,.N,ico][,min(N)],tpxputd=gett('tpxputd')) {
  tpxputd[[as.character(nz)]]
}


#2017-02-24 remove dates from priored row, col, that exceed the end date
#' @export
priorex <- function(spriored=gett('spriored'),daend='2005-06-30',useconstant=F) {
  #stopifnot(all(rownames(spriored)%in%colnames(spriored))) #stetted in this form 2016-02-26 - should work for both t & s
  cn <- colnames(spriored)
  de <- nchar(cn)
  ds <- de-9
  isconst <- grepl('unity',cn)|grepl('const',cn)
  i <- which(!isconst)
  ic <- which(isconst)
  ix <- i[as.Date(substr(cn[i],ds[i],de[i])) <= as.Date(daend)]
  if(useconstant) ix <- c(ix,ic)
  jkeep <- cn[sort(ix)]
  #ikeep <- which(rownames(spriored)%in%jkeep)  #no longer one row/priorequation per date 20180502
  ikeep <- !grepl('unity|const',rownames(spriored))
  spriored[ikeep,jkeep] #spriored[ikeep,jkeep]
}
priorexchk <- function() {
  spriored <- priorex(useconstant=T)
  rn <- rownames(spriored)
  cn <- colnames(spriored)
  #stopifnot(identical(rn,cn))
  de <- nchar(rn)
  ds <- de-9
  stopifnot(any(grepl('unity',rn)&!grepl('const',rn)))
  i <- which(!grepl('unity',rn)&!grepl('const',rn))
  all(is(as.Date(substr(rn[i],ds[i],de[i])),'Date'))
}

#for f' prior: remove rows that do not reference the extremal periods
#' @export
priori <- function(tprior1ed) {
  dax <- range(rownames(tprior1ed))
  ix <- rownames(tprior1ed)%in%dax
  tprior1ed[ix,,drop=F]
}


#extractor
#' @export
getptc <- function(ptcd=gett('ptcd'),daestx=ptcd[,max(daest)]) {
  x1 <- setkey(ptcd,daest)[daestx]
  x2 <- dcast(x1,form=da~rc,value.var='pret')
  x3 <- zm(zoo(x2[,-1,with=F],x2[,as.Date(da)]))
  x3
}


#replacement to stc, solves
#' @export
stc1 <- function(iseqx=1
                 ,
                 yxsub = cocoget(icox,path=rootx) #gett(coname(icox))
                 ,
                 zoned = setkey(gett('zoned'), rc)
                 ,
                 cohod = gett('cohod')
                 ,
                 prime1rcd = gett('prime1rcd')
                 ,
                 tpxputd=gett('tpxputd')
                 ,
                 setdad=gett('setdad')
                 ,
                 deltatp=0
                 ,
                 deltasp=0
                 ,
                 deltatpce=-3 #the 'additional solution' for ce estimation
                 ,
                 startdad=max(setdad) #for no backtest
                 ,
                 primeonly=F
                 ,
                 rootx='coco/'
) {
  #rc1 <- names(yxsub)
  #all(rc1%in%zoned[,rc]) #F
  #setdiff(names(yxsub),zoned[,rc])
  #browser()
  stopifnot(iseqx%in%prime1rcd[,iseq])
  icox <- prime1rcd[iseq==iseqx,unique(ico)]
  stopifnot(length(icox)==1)
  #linked <- linkeFun(neareFun(distanced=distanceFun(zoned[names(yxsub)],save=F),save=F),save=F)
  linked <- linke( neared=neare( distanced=distance( zoned=setkey(zoned,rc)[names(yxsub)] ) ) )
  #spriored <- spriore(yxpr3Fun(yxsub, save = F), linked=linked) #this includes crossprod
  spriored <- spriore(yxpr3(yxpr1d=yxsub), linked=linked) #this includes crossprod
  tpriored <- tpriorget(nz=length(yxsub),tpxputd) #get precalced tprior sized for this nz
  dayper <- as.numeric(mean(diff(as.Date(setdad)))) #days/period - strictly this depends on the period
  da <- setdad[startdad<=setdad]
  isit <- unique(prime1rcd[ico==icox,.(ico,iseq,is,it)])
  x3 <- as.list(1:length(da)*nrow(isit))
  ix3 <- 0
  cod <- copy(yxsub)
  prc1 <- paste(paste0('^',primerc(cohod)[ico==icox,rc],'_'),collapse='|') #grep pattern for prime.rc(icox) jcore1
  for(i in seq_along(da)) { #end date
    daend <- da[i]
    yxsub <- codx(1,daend,cod) #remove later data
    #yxpr3d <- yxpr3Fun(yxsub, save = F) #take crossproducts
    yxpr3d <- yxpr3(yxpr1d=yxsub) #take crossproducts
    iunity <- grep('unity',rownames(yxpr3d$xy))
    x2 <- list(xx=yxpr3d$xx[-iunity,-iunity],xy=yxpr3d$xy[-iunity,,drop=F])
    sp <- priorex(spriored,daend) #trim dates and remove {unity, const}
    tp <- crossprod(priorex(tpriored,daend))
    #browser()
    stopifnot(identical(dim(tp),dim(sp)))

    #jcore <- grep(prc1,colnames(sp)) #identify cols for core rc jcore2,3
    #x2j <- list(xx=yxpr3d$xx[-iunity,-iunity][jcore,jcore],xy=yxpr3d$xy[-iunity,,drop=F][jcore,,drop=F])
    #tpx0 <- priorex(tpriored,daend) #remove const and trim cols to daend

    #-----------------loop for per-zone tune
    for(i1 in 1:nrow(isit)){
      print(isit[i1,])
      ix3 <- ix3+1
      isx <- isit[i1,is]
      itx <- isit[i1,it]
      iseqx <- isit[i1,iseq]
      #priort <- tp*(10 ^ (itx+deltatp))
      # #------------------------------------------- dynamic scaling of tp for xx density, just on core to save time
      # xxdi <- diag(solve( (x2j$xx+priort[jcore,jcore]) )) #unpriored xx + tprior_tuned
      # psc <- rep(1,ncol(sp))  #unity for the non-core cols jcore5
      # psc[jcore] <- exp(log(xxdi)-mean(log(xxdi))) #priorscale cols get dynamic scaling jcore6
      # tpx1 <- sweep(tpx0,MAR=1,STAT=psc,FUN=`*`)
      # tpx <- crossprod(tpx1) #slow eg 7s - is there any shortcut to rowscaling prior to taking column products??
      # priored <- tpx+sp*(10 ^ (isx+deltasp))
      # #-------------------------------------------
      priored <- tp*(10 ^ (itx+deltatp)) + sp*(10 ^ (isx+deltasp))
      # x3[[ix3]] <- yxfo3Fun(x2, priored, save = F) #solve
      #x3a <- yxfo3Fun(x2, priored, save = F) #solve
      #x3b <- yxfo3Fun(x2, priored=tp*(10 ^ (itx+deltatp+deltatpce)) + sp*(10 ^ (isx+deltasp)), save = F)
      x3a <- yxfo3(yxpr3d=x2, priored) #solve
      x3b <- yxfo3(yxpr3d=x2, priored=tp*(10 ^ (itx+deltatp+deltatpce)) + sp*(10 ^ (isx+deltasp)))
      x3[[ix3]] <- x3a[x3b[,.(cell,ceret=est)],on='cell']
      x3[[ix3]][,daest:=daend][,ico:=icox][,iseq:=iseqx][,is:=isx][,it:=itx] #units are daily
    }
  }
  x4 <- rbindlist(x3)
  cellx <- strsplit(x4[,cell],'_')
  x5 <- x4[,pret:=est*dayper][,ceret:=ceret*dayper][,rc:=unlist(lapply(cellx,'[',1))][,da:=unlist(lapply(cellx,'[',2))]#[,prime:=ico==icox]
  x6 <- setkey(x5,ico,rc,is,it)[setkey(prime1rcd[ico==icox],ico,rc,is,it)]
  x7 <- setkey(x6[,.(ico,iseq,daest,da,rc,pret,ceret,is,it)],rc,da,daest) #no longer has prime; added is,it; returns only 'prime' zones
  x7
}


#removed 21-06-15
# stc <- function(icox = 2
#                 ,
#                 yxsub = cocoget(icox) #gett(coname(icox))
#                 ,
#                 zoned = setkey(gett('zoned'), rc)
#                 ,
#                 cohod = gett('cohod')
#                 ,
#                 xvcod = setkey(gett('xvcod'), ico)
#                 ,
#                 xvexd = gett('xvex1d') #note change of ver
#                 ,
#                 tpxputd=gett('tpxputd')
#                 ,
#                 setdad=gett('setdad')
#                 ,
#                 deltatp=0
#                 ,
#                 deltasp=0
#                 ,
#                 startdad=max(setdad) #for no backtest
#                 ,
#                 primeonly=F
# ) {
#   linked <- linkeFun(neareFun(distanced=distanceFun(setkey(zoned,rc)[names(yxsub)],save=F),save=F),save=F)
#   spriored <- spriore(yxpr3Fun(yxsub, save = F), linked=linked) #this includes crossprod
#   tpriored <- tpriorget(nz=length(yxsub),tpxputd) #get precalced tprior sized for this nz
#   dayper <- as.numeric(mean(diff(as.Date(setdad)))) #days/period - strictly this depends on the period
#   da <- setdad[startdad<=setdad]
#   x3 <- setNames(as.list(da), as.list(da))
#   cod <- yxsub
#   for(i in seq_along(da)) { #end date
#     daend <- da[i]
#     yxsub <- codx(1,daend,cod) #remove later data
#     yxpr3d <- yxpr3Fun(yxsub, save = F) #take crossproducts
#     sp <- priorex(spriored,daend) #trim dates and remove {unity, const}
#     tp <- crossprod(priorex(tpriored,daend))
#     stopifnot(identical(dim(tp),dim(sp)))
#     priorraw <- tp*(10 ^ (xvexd[ico == icox, it1]+deltatp))+sp*(10 ^ (xvexd[ico == icox, is1]+deltasp))
#     print(deltatp)
#     iunity <- grep('unity',rownames(yxpr3d$xy))
#     x2 <- list(xx=yxpr3d$xx[-iunity,-iunity],xy=yxpr3d$xy[-iunity,,drop=F])
#     #this line replaces the ones commented
#     priored <- priorraw
#     #the following lines do not belong here because they are in abandoned fork now named stc1
#     # #------------------------------------------- dynamic scaling of priored for xx density
#     # xxdi <- diag(solve(x2$xx+priorraw))
#     # xx <- yxfo3Fun(x2, priorraw, save = F) #solve
#     # xxdid <- data.table(xx[,.(cell,da)],xxdi)
#     # xxdid[,priorscale:=log(xxdi/mean(xxdi))][,priorscale:=exp(priorscale)]
#     # priorscaled <- tcrossprod(xxdid[,priorscale])
#     # priored <- priorraw*priorscaled
#     # #-------------------------------------------
#     x3[[i]] <- yxfo3Fun(x2, priored, save = F) #solve
#     x3[[i]][,daest:=daend][,ico:=icox] #units are daily
#   }
#   x4 <- rbindlist(x3)
#   cellx <- strsplit(x4[,cell],'_')
#   x5 <- x4[,pret:=est*dayper][,rc:=unlist(lapply(cellx,'[',1))][,da:=unlist(lapply(cellx,'[',2))]#[,prime:=ico==icox]
#   x6 <- copy(x5)[,prime:=F]
#   rcprime <-  primerc(cohod)[ico==icox,rc]
#   x6 <- setkey(x6,rc)[rcprime,prime:=T]
#   if(primeonly) { x6 <- x6[prime==T] }
#   # x6 <- setkey(x5,rc)[primerc(cohod)[ico==icox,rc]] #select just the prime zones
#   # rcprime <-  primerc(cohod)[ico==icox,rc]
#   # x6 <- copy(x5)[,prime:=F]
#   # x6 <- setkey(x6,rc)[rcprime,prime:=T]
#   #
#   # setkey(x5,rc)[x5[]]
#   #
#   # x6 <- setkey(x5,rc)[primerc(cohod)[,prime:=ico==icox]] #select just the prime zones
#   x7 <- setkey(x6[,.(ico,daest,da,rc,pret,prime)],rc,da,daest) #added prime
#   x7
# }

#VAR------------------------------------------
#' @export
scofo1 <- function(
  varyxd=gett('varyxd'),
  rscod=gett('rscod'),
  n.ahead=8,
  useeco=T #for version 2, turn off eco vbles
) {
  eco <- varyxd[,c('term.premium','inflation','earn.yield')]
  dad <- rscod[,sort(unique(daest))]
  ll <- as.list(dad)
  for(i in seq_along(dad)) {
    sco <- dcast(rscod[daest==dad[i]],da~factor,value.var='value')
    endo <- zoo(sco[,2:4,with=F],sco[,as.Date(da)])
    if(useeco) endo <- cbind(endo,eco[index(endo)])
    varfd <- VAR(endo, p=1)
    x <- predict(varfd,n.ahead=n.ahead)
    ll[[i]] <- melt(data.table(
      da=dad[(i+1):(i+n.ahead)],
      tau=1:n.ahead,
      f1=x$fcst$X1[,'fcst'],
      f2=x$fcst$X2[,'fcst'],
      f3=x$fcst$X3[,'fcst']
    ),id.var=c('da','tau'))[,daest:=dad[i]][]
  }
  scofo1d <- rbindlist(ll)
  scofo1d
}

#arma------------------------------------------
#' @export
scofo3 <- function(
  rscod=gett('rscod')
  ,
  n.ahead=8
) {
  setkey(rscod,daest,factor,da)
  dad <- rscod[,sort(unique(daest))]
  ll <- as.list(dad)
  for(i in seq_along(dad)) {
    ll[[i]] <- melt(data.table(
      da=dad[(i+1):(i+n.ahead)],
      tau=1:n.ahead,
      f1=predict(auto.arima(rscod[data.table(daest=dad[i],factor=1),value],max.d=0),n.ahead=n.ahead)$pred,
      f2=predict(auto.arima(rscod[data.table(daest=dad[i],factor=2),value],max.d=0),n.ahead=n.ahead)$pred,
      f3=predict(auto.arima(rscod[data.table(daest=dad[i],factor=3),value],max.d=0),n.ahead=n.ahead)$pred
    ),id.var=c('da','tau'))[,daest:=dad[i]][]
  }
  scofo3d <- rbindlist(ll)[,value:=as.numeric(value)]
  scofo3d
}

#holtwinters------------------------------------
#' @export
scofo4 <- function(
  rscod=gett('rscod')
  ,
  n.ahead=8
  ,
  gamma=F
) {
  setkey(rscod,daest,factor,da)
  dad <- rscod[,sort(unique(daest))]
  ll <- as.list(dad)
  for(i in seq_along(dad)) {
    ll[[i]] <- melt(data.table(
      da=dad[(i+1):(i+n.ahead)],
      tau=1:n.ahead,
      f1=as.numeric(predict(HoltWinters(ts(rscod[data.table(daest=dad[i],factor=1),value],start=1995,freq=4),gamma=gamma),n.ahead=n.ahead)),
      f2=as.numeric(predict(HoltWinters(ts(rscod[data.table(daest=dad[i],factor=2),value],start=1995,freq=4),gamma=gamma),n.ahead=n.ahead)),
      f3=as.numeric(predict(HoltWinters(ts(rscod[data.table(daest=dad[i],factor=3),value],start=1995,freq=4),gamma=gamma),n.ahead=n.ahead))
    ),id.var=c('da','tau'))[,daest:=dad[i]][]
  }
  scofo4d <- rbindlist(ll)
  scofo4d
}

#johansen---------------------------------------
#' @export
scofo5 <- function(  rscod=gett('rscod')
                     ,
                     gdp=data.table(gett('gdp'))
                     ,
                     n.ahead=8
                     ,
                     doplot=F
) {
  setkey(rscod,daest,factor,da)
  dad <- rscod[,sort(unique(daest))]
  gdp[nrow(gdp),date:=as.Date(max(dad))]
  i <-1
  ll <- as.list(dad)
  for(i in 1:length(dad)) {
    x1 <- dcast(rscod[daest==dad[i]],da~factor)
    x2 <- zoo(x1[,-1,with=F],x1[,da])
    x3 <- cbind(cumsum(x2),zoo(gdp[,.(px_last)],gdp[,as.character(date)])[-1,])
    x4 <- coredata(x3)[!is.na(x3[,1]),,drop=F]
    x5 <- ca.jo(x4,K=2,ecd='const')
    x6 <- vec2var(x5)
    x7 <- predict(x6,n.ahead=n.ahead)
    if(doplot) fanchart(x7)
    ll[[i]] <- data.table(
      da=dad[(i+1):(i+n.ahead)],
      tau=1:n.ahead,
      f1=diff(c(x4[nrow(x4),1],x7$fcst$X1[,'fcst'])),
      f2=diff(c(x4[nrow(x4),2],x7$fcst$X2[,'fcst'])),
      f3=diff(c(x4[nrow(x4),3],x7$fcst$X3[,'fcst']))
    )[,daest:=dad[i]][]
  }
  x8 <- rbindlist(ll)
  melt(x8,id.vars=c('da','tau','daest'))[,.(da,tau,variable,value,daest)]
}

#de-Fun 2017-07-27 but untested
#' @export
xvt1 <- function( #yxxvFun <- function(
  ico=2
  ,
  yxsubd=cocoget(ico) #gett(coname(ico, 'co'))
  ,
  nfold=getpv('yxxv','nfold')
  ,
  maxp=getpv('yxxv','maxp')
  ,
  zoned=gett('zoned') #not meaningful
  ,
  tpxputd=gett('tpxputd') #not meaningful
  ,
  slevel=c(6,7)#,8) #lev
  ,
  tlevel=c(2,3,4)#,5)
) {#cohod
  #browser()
  set.seed(123) #attempt to make cross-validation repeatable otherwise tuning can have varying results
  linked <- linke( neared=neare( distanced=distance( zoned=setkey(zoned,rc)[names(yxsubd)] ) ) )
  spriored <- spriore(yxpr3(yxpr1d=yxsubd), linked=linked) #this includes crossprod
  tpriored <- tpriorget(nz=length(yxsubd),tpxputd) #get precalced tprior sized for this nz

  daend <- rev(colnames(yxsubd[[1]]))[1]
  sp <- priorex(spriored,daend) #remove {unity, const}
  tp <- crossprod(priorex(tpriored,daend))
  stopifnot(identical(dim(tp),dim(sp)))
  pp <- 1:maxp
  nz <- length(yxsubd)
  ntotlist <- lapply(yxsubd,nrow) #number of segments in each zone, because each zone gets folded at the segment level
  ntot <- sum(unlist(ntotlist))
  dofold <- function(len,nf=nfold) {rep(1:nf,length.out=len)} #[order(runif(len))]} #removed this randomisation; actually want systematically to keep 'stratified sampling' on rd
  dofoldd <- lapply(ntotlist,dofold) #list of vector of fold, each of length nrow(yxsubd[[i]])
  xout <- structure(vector('list',nz),names=names(yxsubd))
  xin <- structure(vector('list',nz),names=names(yxsubd))
  folds <- as.list(1:nfold)
  lmsfun <- function(yyfit){ #function to deliver just the desired oos reg stats: rsq and beta
    x <- yyfit[,summary(lm(y~yfit-1))]
    list(rsq=x$r.squared,coef=x$coefficients[1,1])
  }
  itest <- 0
  ifold <- 1
  for(ifold in 1:nfold) {
    iout <- lapply(dofoldd,'==',ifold) #only ifold is out... 1/maxp are true
    iin <- lapply(iout,'!') #...rest is in, most are true, so these are 'in sample' for this fold
    junity <- grepl('unity',colnames(yxsubd[[1]])) #this should be done upstream e.g. in cocoget

    for(iz in 1:nz) { #partition this fold, and remove unity cols at same time
      xin[[iz]] <- yxsubd[[iz]][iin[[iz]],!junity,with=F]
      rownames(xin[[iz]]) <- rownames(yxsubd[[iz]])[iin[[iz]]]
      xout[[iz]] <- yxsubd[[iz]][iout[[iz]],!junity,with=F]
      rownames(xout[[iz]]) <- rownames(yxsubd[[iz]])[iout[[iz]]]
    }
    yxpr3din  <- yxpr3(yxpr1d=xin) #not a function of prior

    fold <- NULL
    ii<-1
    js <- 1
    for(js in seq_along(slevel)) {
      jt <- 1
      for(jt in seq_along(tlevel)){ #~22s, mostly on yxfo3
        priored <- tp*10^tlevel[jt] + sp*10^slevel[js]  #tp*stren[jt]+sp*stren[js]

        yxfo3din <- yxfo3(yxpr3d=yxpr3din, priored) #solve
        yxfi3din <- yxfi3(yxpr1d=xin,yxfo3d=yxfo3din)[,se:=(yfit-y)^2] #CHANGE 20170312 in stays in, out stays out (had this reversed)

        yxfi3dout  <- yxfi3(yxpr1d=xout,yxfo3d=yxfo3din)[,se:=(yfit-y)^2]
        evt <- 'c(list(mse=mean(se),sse=sum(se)),lmsfun(.SD))'
        fold[[ii]] <- rbindlist(list(
          yxfi3dout[,eval(parse(text=evt)),list(pc=substring(id,0,0))], #CHANGE 20170312, added sse
          yxfi3dout[,eval(parse(text=evt)),list(pc=substring(id,0,3))],
          yxfi3dout[,eval(parse(text=evt)),list(pc=substring(id,0,6))],
          yxfi3dout[,eval(parse(text=evt)),list(pc=substring(id,0,9))]
        ))[,infold:=F][,fold:=ifold][,it:=tlevel[jt]][,is:=slevel[js]]
        ii<-ii+1
        fold[[ii]] <- rbindlist(list(
          yxfi3din[,eval(parse(text=evt)),list(pc=substring(id,0,0))],
          yxfi3din[,eval(parse(text=evt)),list(pc=substring(id,0,3))],
          yxfi3din[,eval(parse(text=evt)),list(pc=substring(id,0,6))],
          yxfi3din[,eval(parse(text=evt)),list(pc=substring(id,0,9))]
        ))[,infold:=T][,fold:=ifold][,it:=tlevel[jt]][,is:=slevel[js]]
        ii<-ii+1
      }
    }
    bindfold <- rbindlist(fold)
    folds[[ifold]] <- bindfold
  }
  rbindlist(folds)[,ico:=ico]
}

#ce(dt) : convert dt to basic ce
#' @export
cedtx <- function (cetab)#, dat = cetab[, max(date)])
{
  #stopifnot(length(dat) == 1)
  #cetab <- data.frame(cetab[date == dat])
  cetab <- data.frame(cetab)
  jbui <- grep("bui", colnames(cetab))
  jloadings <- grep("loadings", colnames(cetab))
  jfmp <- grep("fmp", colnames(cetab))
  jhpl <- grep("hpl", colnames(cetab))
  jmethod <- grep("method", colnames(cetab))
  jfull <- grep("full", colnames(cetab))
  juniqueness <- grep("uniqueness", colnames(cetab))
  jsdev <- grep("sdev", colnames(cetab))
  jqua <- grep("qua", colnames(cetab))
  bui <- cetab[, jbui, drop = TRUE]
  attributes(bui) <- NULL
  ifull <- which(cetab[, jfull] == "1")
  factors <- 1:ncol(cetab[, jloadings, drop = FALSE])
  ans <-
    list(
      loadings = as.matrix(cetab[, jloadings, drop = FALSE]),
      fmp = as.matrix(cetab[, jfmp, drop = FALSE]),
      hpl = as.matrix(cetab[,jhpl, drop = FALSE]),
      method = as.matrix(cetab[,jmethod, drop = FALSE]),
      full = as.matrix(cetab[,jfull, drop = FALSE]),
      uniqueness = as.matrix(cetab[,juniqueness, drop = FALSE]),
      sdev = as.matrix(cetab[,jsdev, drop = FALSE]),
      qua = as.matrix(cetab[, jqua,drop = FALSE]),
      weight = NULL, call = "restored")
  mode(ans$full) <- "numeric"
  mode(ans$full) <- "logical"
  dimnames(ans$loadings) <- list(bui, psz("loadings", factors))
  dimnames(ans$fmp) <- list(bui, psz("fmp", factors))
  dimnames(ans$hpl) <- list(bui, psz("hpl", factors))
  dimnames(ans$method) <- list(bui, psz("method", factors))
  dimnames(ans$full) <- list(bui, "full")
  dimnames(ans$uniqueness) <- list(bui, "uniqueness")
  dimnames(ans$sdev) <- list(bui, "sdev")
  class(ans) <- "ce"
  ans
}

#dt(ce): conversion of basic unaugmented ce to dfr
#' @export
dtcex <- function (x)#, dat = max(index(x)))
{
  colnames(x$loadings) <- 1:ncol(x$loadings)
  colnames(x$fmp) <- 1:ncol(x$fmp)
  colnames(x$hpl) <- 1:ncol(x$fmp)
  colnames(x$method) <- 1:ncol(x$method)
  res <- data.frame(
    #date = dat,
    bui = as.matrix(rownames(x$loadings)),
    sdev = x$sdev, uniqueness = x$uniqueness, full = as.character(as.numeric(x$full)),
    loadings = x$loadings, fmp = x$fmp, hpl = x$hpl, method = metce(x),
    qua = x$qua)
  names(res) <- gsub("\\.", "", names(res))
  data.table(res)
}


#' @export
ce2 <- function(
  icox=140
  ,
  decd=gett('decd')
  ,
  cohod=gett('cohod')
  ,
  win=getpv('celi', 'win')
  ,
  k=2
  ,
  center = getpv('celi', 'center')
) {
  j <- cohod[ico==icox,rc]
  i <- nrow(decd$rer)-(0:(4*win-1))
  rer1 <- decd$rer[i,j]
  x <- rollapplyr(rer1, 4, sum, partial = T)
  ce2 <- fms2(x, range = k, center = center)
  ce2d <- dtcex(ce2)[,ico:=icox]
  ce2d
}

#' @export
dec2 <- function(
  icox=1
  ,
  decd=gett('decd') #list of 5 zoo
  ,
  cohod=gett('cohod')
  ,
  ce2d=gett('ce2d')
  ,
  prime1rcd=gett('prime1rcd') #dt: rc minilev ico       sse        mse is it
) {
  #browser()
  j <- cohod[ico==icox,rc]#sort(rc)] #20180503, stetted the following as it messed up dec2: |'had to add the sort 20180423'
  rer1 <- decd$rer[,j]
  ce <- cedtx(ce2d[ico==icox])
  dec2d <- face(ce,rer1)[c("re1","re2","ret","rer")]
  ll <- lapply(lapply(lapply(dec2d,coredata),melt),data.table)
  for(i in seq_along(ll)) {
    setnames(ll[[i]],c('da','rc','value'))[,eval(parse(text=paste0("comp:='",names(ll)[i],"'")))][,ico:=icox]
  }
  rbindlist(ll)
}

#' @export
ppdfreq <- function() {
  x1 <- setnames(data.table(t(rppd[,.(saon=mean(saon!=''),paon=mean(paon!=''),street=mean(street!=''),locality=mean(locality!=''))]),keep.rownames=T),c('field','populated'))[]
  x <- setnames(rbind(
    setkey(rppd[saon!='',.N,saon],N)[.N,][,field:='saon'],
    setkey(rppd[paon!='',.N,paon],N)[.N,][,field:='paon'],
    setkey(rppd[street!='',.N,street],N)[.N,][,field:='street'],
    setkey(rppd[locality!='',.N,locality],N)[.N,][,field:='locality'],use.names=F
  ),c('most common example','instances of example','field'))[,c(3,1,2),with=F]
  x[x1,on=c(field='field')][,populated:=round(populated,4)]
}

#' @export
segscr <- function(
  izo=1
  ,
  yxpr1d=gett('yxpr1d') #list of dt
  ,
  cncd=gett('cncd')
) {
  yxpr1d <- yxpr1d[izo] #unary list of dt
  yx <- as.matrix(yxpr1d[[1]])
  dimnames(yx) <- list(rownames(yxpr1d[[1]]),colnames=names(yxpr1d[[1]]))
  dax <- colnames(yx)[-(1:2)]
  y <- yx[,1,drop=F]
  fit <- yx[,dax]%*%cncd[as.Date(dax),names(yxpr1d),drop=F]/(365.25/4)
  #plot(as.matrix(yx[,-(1:2)])%*%cncd[,names(yxpr1d)[izo],drop=F]/(365.25/4),as.matrix(yx[,1,drop=F]))
  #lm(y ~ fit -1)
  #apply(cbind(y,fit),2,mean)
  idx <- substr(rownames(yx),1,nchar(rownames(yx))-11)
  data.table(id=idx,r=as.numeric(y),fit=as.numeric(fit),pass=as.numeric(abs(y-fit))<1)
}

#zone mapped to sector
#' @export
zose <- function(
  x
  ,
  pcod=getlast('pcod')
) {
  x1 <- unique(pcod[,.(xs=substr(rcode,1,9),xd=substr(rcode,1,6),xa=substr(rcode,1,3))])
  x2 <- x[,.(pop=unique(substr(rc,1,3)))] #populated areas
  x3 <- x1[x2,on=c(xa='pop'),allow=T] #{xa, xd, xs} for populated areas
  xxs <- setkey(setnames(x[x3,on=c(rc='xs'),nomatch=0,allow=T],old='rc',new='xs'),xs)
  xxd <- setkey(setnames(x[x3,on=c(rc='xd'),nomatch=0,allow=T],old='rc',new='xd'),xs)
  xxa <- setkey(setnames(x[x3,on=c(rc='xa'),nomatch=0,allow=T],old='rc',new='xa'),xs)
  xxds <- setkey(rbind(xxs,xxd[!xxs])[,names(xxa),with=F],xs)
  x4 <- rbind(xxds,xxa[!xxds])[,xarea:=irregpcode(xa)][,xdistrict:=irregpcode(xd)][,xsector:=irregpcode(xs)][,xs:=NULL][,xa:=NULL][,xd:=NULL]
  x4
}

#' @export
coco <-
  function(
    ii = cohod[, sort(unique(ico))],
    cohod = gett('cohod'),
    yxpr1d = gett('yxpr1d')
  ) {
    stopifnot(0==nrow(badrd()))
    i <- ii
    for (i in ii) {
    if((i%%100)==1) print(i)
    x <- yxpr1d[cohod[ico == i, sort(rc)]]
    save(x,file=cocofp(i))
    cocod <- data.table(ico = i, nseg = sum(unlist(lapply(x, nrow))),key='ico')
    }
    cocod
  }

#' @export
steps <- function() {
  rr <-   c(
    'yielddl',
    'pp',
    'rpp',
    'ppscr',
    'rtoz',
    'prpp',
    'segraw',
    'ppscr',
    'seg',
    'stn',
    'vin',
    'yxpr1',
    'setda',
    'extda',
    'zone',
    'refzone',
    'distance',
    'neare',
    'linke',
    'coho',
    'tpxput',
    'size',
    'coco',
    'xvco',
    'xvt1',
    'prime1rc',
    'ptc1',
    'cnc',
    'segscr',
    'cncadj',
    'pvid',
    'pvrc',
    'hvrc',
    'refzone',
    'celiraw',
    'cero',
    'celi',
    'ldgx',
    'sco',
    'dec',
    'ce2',
    'dec2',
    'dec2x',
    'dd',
    'lmsco',
    'vcvdec',
    'varyx',
    'varf',
    'varp',
    'varco',
    'vcfmt',
    'scofo',
    'dec',
    'buifo',
    'ldgx1',
    'vafo',
    'pplp',
    'ppsum',
    'dezocombo',
    'dezoj',
    'rma',
    'dezo',
    'zoma',
    'rma',#this a bit out of place, only used on next step
    'joma',
    'jomax',
    'stn',
    'stnevo'
  )
  rr
}

#' @export
restart1 <- function(stepd=steps(),to=stepd[length(stepd)]) { #standalone - works, but slowly
  stopifnot(to%in%stepd)
  rr <- paste0(stepd,'d')[1:which(stepd==to)]
  for(i in seq_along(rr)) {
    assign(x=rr[i],value=gett(rr[i]),envir=globalenv())
  }
}
#' @export
restart2 <- function(stepd=steps()) { #could modify this to just get ones not in tables()
  rr <- paste0(stepd,'d')
  ll <- as.list(stepd)
  for(i in seq_along(rr)) {
    ll[[i]] <- gett(rr[i])
  }
  ll
}
#' @export
restart3 <- function(stepd=steps()) { #could modify this to just get ones in ddv()
  ll <- sfLapplyWrap(as.list(steps()),restart2) #this will fail
  rr <- paste0(stepd,'d')
  for(i in seq_along(ll)) {
    assign(x=rr[i],value=ll[[i]],envir=globalenv())
  }
}

#' @export
hull <- function(ce=rev(gett('celid'))[[1]]) {
  ldgd <- ldgce(ce)
  xx <- as.matrix(ldgd)
  rownames(ldgd)[sort(unique(as.numeric(surf.tri(xx,delaunayn(xx)))))]
}

#' @export
qpf <- function(
  rc=buice(ce)[1], #code to solve for
  hulld=hull(ce=ce), #codes forming convex hull in loadings
  ce=rev(gett('celid'))[[1]], #
  eps=0
) {
  stopifnot(!rc%in%hulld)
  ldgd <- data.table(ldgce(ce),keep.rownames=T)
  ii <- c(rc,hulld)
  dvec <- matrix(1,length(ii),1,dimnames=list(ii,'1'))
  dvec[1] <- 0 #target
  xc <- setkey(ldgd,rn)[ii]
  diagi <- diag(length(ii))
  diagi[1,1] <- 0 #no positivity constraint for target (it is constrained zero in 1st eqn)
  z <- 1
  eps <- eps
  Amat <- cbind(as.numeric(ii==rc),as.matrix(xc[,.(z*loadings1,z*loadings2,z*loadings3)]),diagi)
  bv <- cbind(0,as.matrix(ldgd[rc,.(z*loadings1,z*loadings2,z*loadings3)]),t(rep(-eps,length(ii))))
  meq <- ncol(ldgd) #target=0; loadings equalities; remainder are positivity
  cex <- extractce(ce,ii)#subset ce on ii using local lib function extractce
  Amat <- Amat[,-(ncol(ldgd)+1)]  #remove the all-zero (already zapped) positivity constraint on target
  bv <- bv[,-(ncol(ldgd)+1),drop=F] #ditto
  constr <- list(Am=Amat,bv=bv,meq=meq)
  sol <- tgt.solve.QP(
    ce=ce,         #ce
    dvec=dvec,       #return, colnames(dvec) in ce
    constr=constr,     #constraints list(Am,bv,meq) - nrows(constr$Am)=ncol(dvec)
    tgt=1,     #target for vol or gross
    ttyp='gross',   #target type
    tol=1e-4,      #tolerance
    vcomp='T'
  )$solution
  stopifnot(max(abs(sol$solution%*%as.matrix(ldgd[ii,-1,with=F]) - ldgd[rc,-1,with=F]))<1e-6)
  print(sum(abs(sol$solution)))
  x <- data.table(ii,c('target',rep('solution',length(hulld))),zapsmall(sol$solution))
  x[-1,][,target:=rc][,.(rc=ii,target,solution=V3)][]
}

#' @export
qpfloop <- function(ce=rev(gett('celid'))[[1]],ii=1:nrow(ce$loadings)) {
  ll <- setNames(as.list(buice(ce)),buice(ce))[ii]
  hulld <- hull(ce=ce)
  for(i in seq_along(ii)) {

    ll[[i]] <- tryCatch(qpf(rc=names(ll)[i],hulld=hulld,ce=ce), error = function(e) {data.table(rc='',target=names(ll)[i],solution=NA*0)})
    ll[[i]][,qpstatus:=ifelse(!is.na(solution),'feas','infeas')]
  }
  ll[]
}


#' @export
dezop1 <- function(
  celid=gett('celid')
  ,
  grpd=gett('grpd')
  ,
  bench=grpd[grep('^1.',rname),.(zo,w=PV/sum(PV))]
  ,
  rcb=distanced[rc%in%bench[,zo]&other%in%bench[,zo],totdist:=sum(dist^2),rc][which.min(totdist),rc]
  ,
  distanced=gett('distanced')
) {
  vcv <- vcvce(celid[[length(celid)]])$T
  #stopifnot(all(colnames(vcv)%in%grpd[,zo]))
  jname <- intersect(colnames(vcv),grpd[,zo])
  j <- match(jname,grpd[,zo])
  j1 <- match(jname,colnames(vcv))
  x1 <- as.matrix(grpd[,.(dd1,dd2,r1y,loadings1,loadings2,loadings3,value,beta,meanret,meanrer,meanrems,vafo,median,p25,p75,PV,post)])
  x2 <- as.matrix(grpd[,.(residvol=residvol^2)])[j,]
  w1 <- as.matrix(grpd[,PV/sum(PV),drop=F])
  w2 <- w1[j,,drop=F]
  pf <- data.table(t(w1)%*%x1)[,PV:=PV*nrow(grpd)]
  cov <- t(as.matrix(bench[,w]))%*%vcv[bench[,zo],j1]%*%w2
  var1 <- t(as.matrix(bench[,w]))%*%vcv[bench[,zo],bench[,zo]]%*%(as.matrix(bench[,w]))
  var2 <- t(w2)%*%vcv[j1,j1]%*%w2
  pf[,rad:=distanced[rc==rcb&other%in%grpd[,zo],mean(dist)]]
  pf[,rho:=cov/sqrt(var1*var2)]
  pf[,residvol:=(t(w1[j]^2)%*%x2)^.5]
  pf[,sdev:=(var2)^.5][]
}

#timeseries line
#' @export
gggrp <- function(grptd=gett('grptd')) {
  x <- setkey(grptd,da,grp)[!is.na(da)][,da:=as.Date(da)]
  x[,cs:=cumsum(V1),grp]
  ggplot(data=x, aes(x=da, y=cs)) +
    geom_line(aes(color = grp, group=grp), size=1.) + #the equivalent of stroke
    theme(legend.title=element_blank()) + #get rid of legend title
    ylab("cumulative log return") + #y axis
    xlab("") #x axis
}

#' @export
pxg <- function(
  grp=c('cname','rname')
  ,
  grpd=gett('grpd')
  ,
  celid=gett('celid')
  ,
  zomad=gett('zomad')
  ,
  dezocombod=gett('dezocombod')
  ,
  rad=300,
  rcx='AL-') {
  grp <- match.arg(grp)
  #x0 <- grpd[,.(zo,grp=eval(parse(text=grp)))][dezocombod,on=c(zo='zo')]
  if(grp=='cname') {#not v nice!
    x0 <- grpd[,.(zo,grp=cname)][dezocombod,on=c(zo='zo')]
  } else {
    x0 <- grpd[,.(zo,grp=rname)][dezocombod,on=c(zo='zo')]
  }
  fmode <- function(x){
    x1 <- tail(names(sort(table(x))), 1)
    if(is.numeric(x)) {x1 <- as.numeric(x1)}
    x1
  }
  dezod <- dezoxFun(x0,fun=fmode)[,grp:=as.factor(grp)]
  jo <- jomax1(j='grp',zomad=zomad,dezodg=dezod,rcx=rcx,rad=rad,window=T)
  px(jo=jo,annotate=F,focusrc=rcx,setscale=F)  #as.numeric(input$region)<.0003
}

#apply a function crank to (last letter found) for each k and da; output to next letter
#' @export
six <- function(
  fun=crank
  ,
  sixd=gett('sixd')
  ,
  ...
) {
  a <- max(asig(sixd))
  k <- ksig(sixd[,which(grepl(a,names(sixd))),with=F])
  j0 <- paste0(a,k)
  j1 <- paste0(letters[match(a,letters)+1],k)
  sixd <- setkey(sixd,da)[getkey(sixd),(j1):=lapply(.SD,fun),.SDcols=j0,by=.EACHI][]
  sixd[]
  #putt(sixd)
}

#-delog, xsection aggregate, relog
#' @export
si6 <- function(sixd=gett('sixd')) {
  #sixd[,f1:=e1*pret][e1!=0,.(pe=cumsum(f1),sig=sign(e1)),'rc,da'][]
  sixd[,f1:=exp(pret)-1]
  si6d <- rbind(
    sixd[0>e1,.(pe=-sum(e1*pret),gross=sum(abs(e1)),net=sum(e1)),'da']
    ,
    sixd[0<e1,.(pe=sum(e1*pret),gross=sum(abs(e1)),net=sum(e1)),'da']
    ,
    sixd[,.(pe=log(1+sum(e1*pret)),gross=sum(abs(e1)),net=sum(e1)),'da']
  )[,net:=zapsmall(net)]  [pe!=0]
  putt(si6d)
}

#cumulative:  sig         da            pe          cpe           ff
#' @export
si7 <- function(si6d=gett('si6d')) {
  x <-setkey(si6d,net,da)[,cpe:=cumsum(pe),net][,ff:=as.factor(c('underperform','relative','outperform')[round(net+2)])][]
  si7d <- x[,.(sig=net,da,pe,cpe,ff)]
  putt(si7d)
}

#' @export
si0f <- function(ptc1d=gett('ptc1d'),wgtx=3,gross=2) {
  si0d <- six(cnorm,six(cnorm,six(crank,si1f(ptc1d)))[,d1:=c1+wgtx*c2])[,.(rc,da,pret,xar,e1=e1*gross)]
  si0d
}
#' @export
rglx <- function(
  nn=c('kmd','pcla1d','grpd')
  # kmd=gett('kmd')
  # ,
  # pcla1d=gett('pcla1d')
  # ,
  # grpd=gett('grpd')
  ,
  gtyp='cluster'#not actually used
  ,
  cex=.7
) {
  getgd(nn)
  x0 <- grpd[kmd,on=c(zo='pcode')]
  x <- pcla1d[x0,on=c(area='zo')][,lab:=paste(irregpcode(area),la)]
  x[,eval(parse(text=paste0('grp:=',gtyp)))]
  x[,plot3d(loadings1,loadings2,loadings3,cex.axis=.01,col=as.numeric(substr(grp,1,1)))]
  x[,text3d(loadings1,loadings2,loadings3,cex=cex,col=as.numeric(substr(grp,1,1)),texts=lab)]
}


#' @export
rr1 <- function(mu,j=2) {
  th <- atan2(-mu[j],mu[1])
  r1 <- matrix(c(cos(th),-sin(th),sin(th),cos(th)),2,2)
  x2 <- diag(length(mu))
  x2[c(1,j),c(1,j)] <- r1
  x2
}


#20170505 rotate all mean into f1
#' @export
rr2 <- function(x=sco,tpd=tpd) {
  r <- as.list(1:ncol(x))
  r[[1]] <- diag(ncol(x))
  da <- rownames(coredata(x))
  for(i in 2:ncol(x)) {
    ida <- tpd[k==i,tp1]<=da & da<=tpd[k==i,tp2]
    r[[i]] <- rr1(mu=apply(coredata(x)[ida,],2,mean),j=i)
    x <- coredata(x)%*%r[[i]]
  }
  Reduce(`%*%`,r)
}
chkrr2 <- function(x=do.call(fun,args=list(x=matrix(rnorm(12),4,3))),fun=rr2) {
  #https://en.wikipedia.org/wiki/Rotation_matrix
  #Rotation matrices are square matrices, with real entries. More specifically, they can be characterized as orthogonal matrices with determinant 1; that is, a square matrix R is a rotation matrix if and only if RT = R−1 and det R = 1.
  # square
  stopifnot(is.matrix(x) & nrow(x)==ncol(x))
  # real entries
  stopifnot(!is.complex(x))
  # orthogonal &   RT = R−1
  stopifnot(isTRUE(all.equal(tcrossprod(x),diag(ncol(x)))))
  # determinant 1
  stopifnot(isTRUE(all.equal(det(x),1)))
  T
}

#' @export
sse1 <- function(prime1rcd,xvt1d,ver,foldx=FALSE,noco=F) {
  x0 <- c(pc='rc',ico='ico',is='is',it='it')
  if(noco) x0 <- x0[x0!='ico']
  for(i in seq_along(ver)) {
    x1 <- xvt1d[prime1rcd[,.(rc,ico,is,it)],on=x0][infold==foldx][,.(mse=mean(mse),sse=sum(sse),rsq=mean(rsq),coef=mean(coef),infold=F,fold=0,it=mean(it),is=mean(is),ico=max(ico)),pc]
    x2 <- x1[,.(mse=mean(mse),sse=sum(sse),rsq=mean(rsq),coef=mean(coef),it=mean(it),is=mean(is),rho=cor(is,it))][,ver:=ver[i]] #n.b. not weighted.mean()
  }
  x2
}


#this function deprecated - better to follow the design pattern and have a step storing this for each version, plus a summary function across versions
#key results by version - out-of-fold
#' @export
ssescan<-function(ver=15:16,foldx=FALSE) {
  x <- setNames(as.list(ver),ver)
  for(i in seq_along(ver)) {
    setv(ver=ver[i])
    prime1rcd <- gett('prime1rcd')
    xvt1d <- gett('xvt1d')
    x1 <- xvt1d[prime1rcd[,.(rc,ico,is,it)],on=c(pc='rc',ico='ico',is='is',it='it')][infold==foldx][,.(mse=mean(mse),sse=sum(sse),rsq=mean(rsq),coef=mean(coef),infold=F,fold=0,it=mean(it),is=mean(is),ico=max(ico)),pc]
    x[[i]] <- x1[,.(mse=mean(mse),sse=sum(sse),rsq=mean(rsq),coef=mean(coef),it=mean(it),is=mean(is),rho=cor(is,it))][,ver:=ver[i]] #n.b. not weighted.mean()
  }
  rbindlist(x)
}

#one cohort per zone
#' @export
cohoz <- function(
  distanced = gett('distanced')
  ,
  nco = 5
) {
  setkey(distanced, rc)
  co <- distanced[, .(rc = sort(unique(rc)))][, i := .I]
  ll <- as.list(1:nrow(co))
  for (i in 1:nrow(co)) {
    print100(i)
    x <- setkey(distanced[co[i, rc]], dist)[1:nco]
    ll[[i]] <- x[, .(rc = other, ilev = 2, ico = i)][1, ilev := 1]
  }
  zoco <- rbindlist(ll)
}

#' @export
cptc <- function(
  ptc1d=gett('ptc1d')
  ,
  start='2000-03-31'
) {
  allda <- ptc1d[start<=da,sort(unique(da))]
  #allda <- allda[1:4]
  ll <- as.list(allda)
  for(i in seq_along(allda)) {
    ll[[i]] <- copy(ptc1d[da<=allda[i]])[,daest:=allda[i]][]
  }
  cptcd <- rbindlist(ll)
  cptcd
}

#' @export
cocomkd <- function(type='coco') {
  shell(paste0("rd /s /q .\\",type),intern=T)
  mkdirn(type)
}

#' @export
cocofp <- function(i=1,type='coco') {
  paste0(type,'/',coname(i,type),'.RData')
}

#' @export
cocoget <- function(i=1,type='coco',path=type) {
  load(paste0(path,'/',coname(i,type),'.RData'))
  x
}


#qualifying names equal length ending numeric
#' @export
jsig <- function(x=gett('sigge1d')) {
  x0 <- names(x)[grep('[0-9]$',names(x))]
  n1 <- sapply(x0,nchar)
  allequal <- function(x){
    x <- setNames(x,NULL)
    isTRUE(all.equal(x, rep(x[1], length(x))))
  }
  stopifnot(allequal(n1))
  x0
}


#' @export
ksig <- function(x=gett('sigge1d')) {
  x0 <- jsig(x)
  n1 <- nchar(x0)[1]
  jn <- sort(unique(as.numeric(sapply(x0,substr,n1,n1))))
  jn
}

#return alpha prefixes
#' @export
asig <- function(x=gett('sigge1d')) {
  x0 <- jsig(x)
  n1 <- nchar(x0)[1]
  an <- sort(unique(sapply(x0,substr,1,n1-1)))
  an
}

#' @export
crank=function(x,n){
  ((rank(x)-1)/(length(x)-1) -.5)*2
}

#' @export
cnorm=function(x) {
  x1<-x-mean(x)
  x1/(sum(abs(x))+.Machine$double.eps)
}

# #' @export
# csum=function(x,w=rep(1,length(x))){
#   browser()
# }

#plot backtest performance log and non-log
#' @export
sig <- function(ptc1d=gett('ptc1d'),sid=sif(ptc1d,gross=gross,wgt=wgt),size=1,gross=2,wgt=3) {
  ll <- as.list(1:2)
  ll[[1]] <- ggplot(sid,aes(as.Date(da),cpe))+geom_line(aes(color=ff),size=size) +
    theme(legend.title=element_blank()) + theme(legend.position=c(.2, .9)) +
    ylab("log return") +
    xlab("")

  ll[[2]] <- ggplot(sid,aes(as.Date(da),100*(exp(cpe)-1)))+geom_line(aes(color=ff),size=size) +
    theme(legend.position="none") +
    ylab("return (%)") +
    xlab("")
  multiplot(plotlist=ll,cols=2)
}

#one-liner
#' @export
sif <- function(ptc1d=gett('ptc1d'),wgtx=3,gross=2) {
  si7(si6(si0f(ptc1d=ptc1d,wgtx=wgtx,gross=gross)))
}

#' @export
dezof <- function (dezojd, fun = mean)
{
  missingl1 <- dezojd[, setdiff(substr(zo, 1, 3), zo)]
  avg <- copy(dezojd)[, `:=`(zo, substr(zo, 1, 3))][zo %in%  missingl1][, lapply(.SD, fun), by = zo]
  if ("grp" %in% names(avg))
    avg[, `:=`(grp, as.numeric(grp))]
  avg[, `:=`(lev, 1)]
  setkey(rbind(dezojd, avg), zo)[]
}


#----------------------------from epc 2017-07-07
#regularise: non-alphanumeric to space, space contraction, lead/trail space removal, space substitution, pasteup
#' @export
reg1 <- function(x1=ext1d) {
  x <- copy(x1)
  j <- 'a0' #names(x)[grep('^a[0-9]',names(x))]
  patt <- c(
    ' +'        =   ' ', #repeated space
    ' - '       =   '-', #flat ranges with ' - '
    '^ '        =   '',  #leading space
    ' $'        =   '',  #trailing space
    ' '         =   '-', #space
    '^FLATS?-'  =   '',  #remove
    '^APARTMENTS?-'='',  #remove
    '^STUDIOS?-'=   '',  #remove
    '^MAISONETTES?-'=''  #remove
  )
  for(i in seq_along(j)) {
    cmd <- gsub(patt='XXX',rep=j[i],x="XXX:=gsub(patt='[^\\\\w\\\\s\\\\-]',rep='',XXX,perl=T)")
    x[,eval(parse(text=cmd))] #eliminate non-alpha, non-whitespace
    for(i1 in seq_along(patt)) {
      cmd <- paste0(j[i],":=gsub(patt='",names(patt)[i1],"',rep='",patt[i1],"',x=",j[i],",perl=T)")
      x[,eval(parse(text=cmd))]
    }

  }
  x
}

#viewkey
#' @export
vk <- function(x=e3d) {
  setkey(x,rc12,a3,a2,a1)[]
}
#join unique key
#' @export
juk <- function(x=e3d) {
  x1 <- setkey(x,rc12,a0)
  x1[unique(getkey(x1)),mult='first'][]
}

#summarise uniqueness
#' @export
i1f <- function(x=i1steps()[[1]]) {
  getgd(x[,step])
  s1 <- copy(get(x[,step]))
  tkn <- paste0('testid:=',x[,id])
  s1[,eval(parse(text=tkn))]
  cbind(x,s1[,.(unique=length(unique(testid)),duplicated=sum(duplicated(testid)),total=length(testid))])
}

#data argument for i1
#' @export
i1steps <- function(type=c('epc','ppd')) {
  type <- match.arg(type)
  if(type=='epc') {
    list(
      data.table(step='e1d',id='BUILDING_REFERENCE_NUMBER'),
      data.table(step='e2d',id='brn'),
      data.table(step='e3d',id='brn'),
      data.table(step='e4aad',id='brn'),
      data.table(step='e5d',id='brn')
    )
  } else if(type=='ppd'){
    list(
      data.table(step='prppd',id='id'),
      data.table(step='segd',id='id'),
      #data.table(step='p2d',id='id'),# has no id field
      data.table(step='p3d',id='id')
    ) #else if(type=='seg') {}
  }

  #)
}

#' @export
ppm2a <- function(by='type',ppm1d=gett('ppm1d')) {
  setkey(ppm1d[,.(n=.N,pv=sum(pv,na.rm=T),m2=sum(median,na.rm=T)),by=by][,pm2:=pv/m2][pv!=0&m2!=0],pm2)
}


#' @export
ppm2x <- function(ppm2d=gett('ppm2d'),typex='pm2',catx='estate') {
  x0 <- catdt()[ppm2d,on=c(cat='cat',catvalue='catvalue')]
  x1 <- dcast(x0[cat==catx&variable==typex],postcode~desc,value.var='value')
  x1
}


#' @export
ppm2g <- function(rcx=c('NG-1--','W--2--'),ppm2td=gett('ppm2td'),size=1.,cc=ggpalette8()) {
  xt <- copy(ppm2td)[,pc:=irregpcode(rcode)]
  #xtfinal <- xt[date==max(date)]
  xt <- setkey(xt,rcode)[data.table(rcx)]
  xt <- setkey(xt[,dum:=-as.numeric(date)],dum)
  xt <- xt[,pcx:=reorder(pc,ppm2t,FUN=`[`,i=1)]#[,colx:=reorder(pc,ppm2t)]
  p1 <- ggplot(xt,aes(date,ppm2t))+
    geom_line(aes(col=pcx),size=size)+
    coord_trans(y="log2") +
    #theme(legend.title=element_blank()) + #legend title
    ylab("price/m2") + #y axis
    xlab("") +#x axis
    theme(legend.position="none")+ #legend title
    scale_colour_manual(values=cc) +
    theme(text = element_text(size=20))
  #theme(axis.text.x = element_text(size=20))

  p2 <- ggplot(xt[date==max(date)],aes(pcx,ppm2t,fill=pcx))+
    geom_bar(stat='identity') +
    ylab("price/m2") + #y axis
    xlab("") +#x axis
    theme(legend.position="none")+ #legend title
    theme(axis.text.x = element_text(size=15))  +
    scale_fill_manual(values=cc)+
    theme(text = element_text(size=20))
  multiplot(p1,p2,cols=2)
}



#' @export
catdt <- function() {
  x <- structure(list(catvalue = c("O", "F", "T", "D", "S", "L", "F","N", "Y"),
                      cat = c("type", "type", "type", "type", "type", "estate","estate", "newbuild", "newbuild"),
                      desc=c("Commercial", "Flat", "Terrace", "Detached", "SemiDetached",  "Leasehold", "Freehold","Resale", "NewBuild")),
                 row.names = c(NA, -9L), class = c("data.table","data.frame"), .Names = c("catvalue","cat",'desc'))
  data.table(x)
}

#' @export
cnc <- function(x,value.var='pret') {
  dt1 <- dcast(x,da~rc,value.var=value.var)
  xx <- zm(zoo(dt1[,-1,with=T],dt1[,as.Date(da)]))
  plot(cumsum(xx[-1,1:5]),col=1:5,scr=1)
  xx
}

#' @export
jomaf <- function(dezod,zomad) {
  setnames(dezod[zomad], 'zo', 'datakey')[, lev := NULL]
}

#summarise step - to be stored, the used as reference data with a checking utility (not written yet)
#' @export
stepsum <- function(step='si1') {
  nn <- paste0(step,'d')
  getgd(nn)
  j <- names(get(nn))
  cl <- unlist(lapply(get(nn),class))
  na <- unlist(lapply(lapply(get(nn),is.na),any))
  data.table(j,cl,na)[,st:=step][]
}

#prime rc identifies the 'prime' cohort for each rc, such that lev(rc) is minimised
#' @export
primerc <- function(
  cohod#=gett('cohod') #illegal
) {
  kk <- cohod[,.(minilev=min(ilev)),rc]
  setkey(setnames(setkey(cohod,rc,ilev)[kk,mult='first'],old='ilev',new='minilev'),rc)[]
}
#' @export
primercchk <- function(x=primerc(),cohod=gett('pxzocohod')) {
  stopifnot(x[,all(minilev<3)])
  stopifnot(all(cohod[,unique(rc)]%in%x[,rc]))
}

#requires arg to be a zone, returns 'primary zone in cohort' otherwise null
#' @export
primeico <- function(cc='PE-31-',cohod) {
  icox <- primerc(cohod)[minilev==1][primerc(cohod)[cc],on=c(ico='ico')][,rc]
  icox[!is.na(icox)]
}



#' @export
scoplstn <- function(
  scofostncd
  ,
  stnd
  ,
  setdad
  ,
  extdad
  ,
  io=90
  ,
  nper=20
  ,
  yo=io
  ,
  title=NULL
  ) {
  #browser()
  alldad <- c(as.Date(setdad),extdad)
  win <- alldad[(-nper:nper)+io]
  dao <- alldad[yo]
  xy1 <- setkey(copy(scofostncd)[,da:=as.Date(da)][,col:=as.factor(factor)],da)[data.table(win)]
  xy <- xy1[da==dao][,.(factor,ref=y)][xy1,on=c(factor='factor')][,y:=y-ref]
  ggplot()+
    geom_line(data=xy[type=='actual'],aes(da,y,color=col),size=1)+
    geom_point(data=xy[type=='forecast'],aes(da,y,color=col),size=1.5)+
    geom_line(data=xy[type=='forecast'],aes(da,y,color=col),linetype='dotted',size=.5)+
    xlim(c(min(win),max(win)))+ ##remove x axis label and relabel key
    #ylim(-7,10)+xlab('')+
    labs(x='',y='cumulative factor',title=title)+
    # ylab('cumulative score')+
    # xlab('') +
    theme(legend.title=element_text())
}



#' @export
stnbui <- function(qtrs=4,nn=c('scoxd','extdad','ldgxd'),labely=F) {
  if(exists('ldgxd')) rm('ldgxd',envir=globalenv())
  getgd(nn)
  scofod <<- zm(zoo(scoxd[nrow(scoxd)+(1:20)-66,],extdad[1:20]))#invent forecasts from -66
  buifoFun() #calc buifo
  pe <- sort(apply(buifod$T[1:qtrs],2,sum))[c(ncol(buifod$T)-(2:0),1:3)]
  x1 <- data.table(rc=names(pe),pe)
  x1[,rcx:=reorder(rc,pe)]
  ggplot(x1,aes(rcx,pe))+geom_bar(stat='identity',fill='lightblue')+coord_flip() +
    scale_x_discrete(name="") + scale_y_continuous(limits=c(-.1,.8),name=ifelse(labely,"cumulative log return",""))+
    ggtitle(paste0(months(as.Date(rownames(buifod$T)[qtrs])+1),' ',substr(rownames(buifod$T)[qtrs],1,4)))+theme_light()
}

#hedonic
#' @export
hed1 <- function(catx=c('estate','type'),ppm1d,ppmxd) {
  ppm1d <- ppm1d[!is.na(rcode)] #this should be done upstream
  catx <- match.arg(catx)
  x1 <- ppmxd[ppm1d,on=c(rcode='rcode')][,mv:=median*ppm2][,resid:=log(pv/mv)] #model the current residual value
  xy <- x1[type!='O'][estate!='U'][,ppm2:=resid][,ac:=substr(rcode,1,3)]
  #print(xy[,.N,'estate,newbuild,type,ac'])
  xy[,newbuild:=as.factor(newbuild)][,type:=as.factor(type)][,estate:=as.factor(estate)]
  if(catx=='type') {#this is not nice but constructing formula as.formula(paste(...)) not works due to scoping
    x3 <- xy[,data.table(summary(lm(ppm2~newbuild+type))$coefficients[,1,drop=F],keep.rownames=T),ac][rn=='(Intercept)',rn:='ppm2']
  } else {
    x3 <- xy[!is.na(ac),data.table(summary(lm(ppm2~newbuild+estate))$coefficients[,1,drop=F],keep.rownames=T),ac][rn=='(Intercept)',rn:='ppm2']
  }
  #x4 <- melt(dcast(x3,ac~rn,value.var='Estimate')[,.SD/ppm2,ac],id.vars='ac')
  x4 <- setnames(x3,c('rn','Estimate'),c('variable','value'))
  ac1 <- x4[,unique(ac)]
  x5a <- rbind(x4[grepl('newbuild',variable)],data.table(ac=ac1,variable='newbuildN',value=0))
  #x5b <- rbind(x4[grepl(catx,rn)],data.table(ac=ac1,variable=ifelse(catx=='estate','estateF','typeD'),value=0))
  x5b <- rbind(x4[grepl(catx,variable)],data.table(ac=ac1,variable=ifelse(catx=='estate','estateF','typeD'),value=0))
  ren <- c(detached='typeD',flat='typeF',semidetached='typeS',terraced='typeT',freehold='estateF',leasehold='estateL',resale='newbuildN',newbuild='newbuildY')
  x6 <- data.table(new=names(ren),old=ren)
  x7 <- unfactordt(x5a[x5b,on=c(ac='ac'),allow=T][,prem:=value+i.value][,.(ac,variable,i.variable,prem)][,cat:=catx])
  hed1d <<- x6[x6[x7,on=c(old='i.variable')],on=c(old='variable')][,.(variable=new,i.variable=i.new,cat,ac,prem)]
  setkey(hed1d,variable,i.variable)
}

#' @export
hed2 <- function(acx='AL-',hed1x,nn='hed1d') {
  getgd(nn)
  ren <- c(detached='typeD',flat='typeF',semidetached='typeS',terraced='typeT',freehold='estateF',leasehold='estateL',resale='newbuildN',newbuild='newbuildY')
  rendt <- data.table(new=names(ren),old=ren)
  if(missing(hed1x)) hed1x <- hed1d
  rendt[rendt[hed1x,on=c(old='i.variable')],on=c(old='variable')]
  setnames(dcast(hed1x[ac==acx],variable~i.variable,value.var='prem'),old='variable',new=irregpcode(acx))[]
}

#timeseries graph
#' @export
gg0 <- function(xy,exp=800,start=xy[x==min(x),is.na(sd(y))||sd(y)>.02*xy[,max(y)]],cex=.5,dotfuture=T) {
  #browser()
  xy[,g:=as.factor(g)]
  if(dotfuture) {
    xy[,future:=Sys.Date()<x]
    x <- ggplot()+
      geom_line(data=xy[!future==T],aes(x,y,color=g),size=1)+
      geom_point(data=xy[future==T],aes(x,y,color=g),size=1.5)+
      geom_line(data=xy[(Sys.Date()-90<x)==T],aes(x,y,color=g),size=.5,linetype="dotted")+
      scale_x_date(expand=c(0, exp),name='') +
      geom_dl(data=xy,aes(x,y,label = g), method = list(dl.trans(x = x ), "last.points", cex = cex))
  } else {
    x <- ggplot(xy,aes(x,y))+geom_line(aes(color=g),size=1)#+
    # scale_colour_discrete(guide = 'none') + #commented these 20180723 because prev line already made useless
    #   scale_x_date(expand=c(0, exp),name='') +
    #   geom_dl(data=xy,aes(x,y,label = g), method = list(dl.trans(x = x ), "last.points", cex = cex))
  }
  if(start) {
    x <- x+ geom_dl(data=xy,aes(x,y,label = g), method = list(dl.trans(x = x ), "first.points", cex = cex))
  }
  x #+ theme(legend.position="none")
}



#similar to now
#' @export
cycch1 <- function(stnevod,varyxd) {
  relabel <- data.table(mnem=c('fmp1','fmp2','fmp3','term.premium','inflation','earn.yield'),label=c('factor 1','factor 2','factor 3','term premium %','expected inflation %','equity earnings yield %'))
  x1 <- rbind(data.table(tail(varyxd,1))[,period:=as.character(max(index(varyxd)))],data.table(head(stnevod,1))[,period:=as.character(min(index(stnevod)))])
  x2 <- melt(x1,id.vars='period')
  x3 <- relabel[x2,on=c(mnem='variable')][,j:=reorder(label,1:nrow(x2))]
  ggplot(x3,aes(x=period,y=value,fill=j))+
    geom_bar(stat='identity', width=0.4, position=position_dodge(width=0.5))+#geom_bar(width=0.4, position = position_dodge(width=0.5))
    scale_x_discrete(name="") + scale_y_continuous(name="")+ theme(legend.title=element_blank())
}

#' @export
aappdc <- function(i=1,name='Set3',nbrew=12) {
  aacol1(m=nbrew,name=name,nbrew=nbrew)[(i%%nbrew)+1]
}

#' @export
cbpalette8 <- function(){c("#000000", "#E69F00", "#56B4E9", "#009E73", "#F0E442", "#0072B2", "#D55E00", "#CC79A7")}

#' @export
ggpalette8 <- function() {
  n=8
  hues = seq(15, 375, length = n + 1)
  hcl(h = hues, l = 65, c = 100)[c(1,4,7,2,5,8,3,6)]
}

#' @export
gg_colour_hue <- function(n) {
  hues = seq(15, 330, length = n + 1)
  hcl(h = hues, l = 65, c = 100)[1:n]
}

#' @export
ggpalette8 <- function() {
  n=8
  hues = seq(15, 375, length = n + 1)
  hcl(h = hues, l = 65, c = 100)[c(1,4,7,2,5,8,3,6)]
}


#' @export
strong8 <- function() {
  setNames(
  c("#FF0000","#00FF00","#0000FF","#00FFFF","#FF00FF","#FFFF00","#A020F0","#FFA500")
  ,
  c('red','green','blue','cyan','magenta','yellow','purple','orange')
  )
}

#' @export
ppdpals <- cbpalette8
#' @export
ppdpalp <- ggpalette8
#' @export
ppdpalf <- strong8

#' @export
ads3 <- function(
  nn=c('ads2d'),
  bins=5,
  pal="YlOrRd",
  vbl='ppm2',
  unit=' £/m<sup>2',
  sig=3,
  scalefactor=100
) {
  getgd(nn)
  ads2d@data$de <- ads2d@data[[vbl]]*scalefactor #hack to be able to use ~cb(de) later
  cb <- colorBin(pal, domain = ads2@data$de, bins = bins)
  labels <- sprintf(
    paste0("<strong>%s</strong><br/>%g",unit,"</sup>"),
    ads2d@data[,'pc'], signif(ads2@data$de,sig)
  ) %>% lapply(htmltools::HTML)
  leaflet::leaflet(data = ads2d,options = leafletOptions(minZoom = 10)) %>%
    addProviderTiles(providers$OpenStreetMap, group = "OSM") %>% #essential to specify OSM!!!
    addPolygons(
      fillColor = ~cb(de),
      weight = 1,
      opacity = .5,
      color = "white",
      dashArray = "3",
      fillOpacity = 0.4,
      highlight = highlightOptions(
        weight = 5,
        color = "#666",
        dashArray = "",
        fillOpacity = 0.7,
        bringToFront = TRUE),
      label = labels,
      labelOptions = labelOptions(
        style = list("font-weight" = "normal", padding = "3px 8px"),
        textsize = "15px",
        direction = "auto")) #%>%
    #setView(-1.5,52.5,10)

}


#' @export
es <- function(x,n=1000) {
  edit(x[sample(nrow(x),min(n,nrow(x)),rep=F)])
}

#lon - distance from wc2
#' @export
lmlon <- function(nn=c('distanced','refzoned')) {
  getgd(nn)
  x1 <- setkey(distanced[other==refzoned,.(rc,lon=dist)],rc)[]
  stopifnot(isTRUE(all.equal(setkey(x1,rc)[,rc],icod[,sort(unique(ico))]))) #1:1 with ico
  x1
}

#den - #/ha [requires polygon area]
#' @export
lmden <- function(
  nn=c('rdo2d','adszod','zoned')
  ) {
  getgd(nn)
  j <- c('d','s')
  x0 <- rdo2d[2:3] #corresponding to j [has different naming convention]
  ll <- as.list(j)
  for(i in seq_along(j)) { #areas, sectors
    ll[[i]] <- data.table(m2=geosphere::areaPolygon(x0[[i]]),rc=regpcode(x0[[i]]@data$name))[unique(adszod[,j[i],with=F]),on=c(rc=j[i])][!is.na(m2)&rc!=''][,.(rc,m2)]
  }
  x1 <- setkey(setnames(rbindlist(ll),c('zo','m2')),zo)
  #x1 <- data.table(m2=geosphere::areaPolygon(rmased),rc=regpcode(rmased@data$name))[adszod,on=c(rc='s')][!is.na(m2)]
  x2 <- setkey(x1[,.(m2=sum(m2)),zo],zo)[zoned[,unique(rc)]] #area
  x3 <- setkey(unique(prppd[,.(id,rcode)])[,.N,rcode],rcode)[zoned[,unique(rc)]] #number
  x4 <- setkey(x2[x3,on=c(zo='rcode')][,.(den=1e4*N/m2,rc=zo,N)],rc)[]
  stopifnot(isTRUE(all.equal(setkey(x4,rc)[,rc],icod[,sort(unique(ico))])))
  x4
}

#den - #/ha [requires polygon area]pxosrdo2d pxg2adszod pxzozoned
# fuden <- function(  #ALREADY EXISTS AS A STEP
#   nn=c('pxosrdo2d','pxg2adszod','pxzozoned')
# ) {
#   getgd(nn)
#   j <- c('d','s')
#   x0 <- pxosrdo2d[2:3] #corresponding to j [has different naming convention]
#   ll <- as.list(j)
#   for(i in seq_along(j)) { #areas, sectors
#     ll[[i]] <- data.table(m2=geosphere::areaPolygon(x0[[i]]),rc=regpcode(x0[[i]]@data$name))[unique(pxg2adszod[,j[i],with=F]),on=c(rc=j[i])][!is.na(m2)&rc!=''][,.(rc,m2)]
#   }
#   x1 <- setkey(setnames(rbindlist(ll),c('zo','m2')),zo)
#   #x1 <- data.table(m2=geosphere::areaPolygon(rmased),rc=regpcode(rmased@data$name))[pxg2adszod,on=c(rc='s')][!is.na(m2)]
#   x2 <- setkey(x1[,.(m2=sum(m2)),zo],zo)[pxzozoned[,unique(rc)]] #area
#   x3 <- setkey(unique(prppd[,.(id,rcode)])[,.N,rcode],rcode)[pxzozoned[,unique(rc)]] #number
#   x4 <- setkey(x2[x3,on=c(zo='rcode')][,.(den=1e4*N/m2,rc=zo,N)],rc)[]
#   stopifnot(isTRUE(all.equal(setkey(x4,rc)[,rc],icod[,sort(unique(ico))])))
#   x4
# }



#met - distance from an area with dense and large districts [requires den + aggregation] does not work well on small sets with few a!
#' @export
lmmet <- function(nn=c('distanced','icod')) {
  getgd(nn)
  x1 <- lmden()[,a:=substr(rc,1,3)]
  #x1[,sumrank:=rank(5*rank(-den)+rank(-N))<=2]
  x2 <- x1[den>7,.(n=sum(N)),a][n>8e3] #met areas ~60
  x3 <- x1[x2[,.(a)],on=c(a='a')][,.SD[which.max(den)],a]
  x4 <- setkey(distanced[x3,on=c(other='rc')],rc,dist)
  x5 <- setkey(x4[,.SD[1:2,.(met=sum(dist))],rc],rc)[]
  stopifnot(isTRUE(all.equal(setkey(x5,rc)[,rc],icod[,sort(unique(ico))])))
  x5
}

#dynamic------
#re1 - segments starting 'new' and ending 'resale'
#' @export
lmre1 <- function(
          nn='segd'
          ,
          d2=format(Sys.Date())
          ) {
  x1 <- setkey(seglxd[deed_date<as.Date(d2),.(re1=mean(startnew=='Y')),parent][,.(re1,rc=parent)],re1)[]
}
#----------------------got to here but lmre1 broken and some messing on the next one too - need the data tidied so rerun v36
#fre - freehold fraction [assumed unchanging]
#' @export
lmfre <- function(nn=c('seglxd','icod'),d2=format(Sys.Date())) {
  x1 <- setkey(seglxd[deed_date<as.Date(d2),.(fre=mean(estate_type=='F')),parent][,.(fre,rc=parent)],rc)[]
  stopifnot(isTRUE(all.equal(setkey(x1,rc)[,rc],icod[,sort(unique(ico))])))
  x1
}

#fli - flip fraction
#' @export
lmfli <- function(nn='vind',d2=format(Sys.Date())) {
  getgd(nn)
  setkey(segd[deed_date<as.Date(d2),.(fli=mean((deed_date-startdate)<365)),rcode][,.(fli,rc=rcode)],rc)[]
}

#gro - delta m2 !over epc period!
#' @export
lmgro <- function(nn='ppm1d',d2=format(Sys.Date())) {
  getgd(nn)
  setkey(ppm1d[,rat:=gro/median][rat<.3&rat>-.3][,.(agro=sum(gro)/sum(median)),rcode][,.(gro=agro,rc=rcode)],rc)[]
}

#new - new/total transactions
#' @export
lmnew <- function(nn='prppd',d2=format(Sys.Date())) {
  getgd(nn)
  x <- setkey(setkey(prppd,rcode)[deed_date<as.Date(d2),.(new=mean(new_build=='Y')),rcode][,.(new,rc=rcode)],rc)[]
}

#res - resold ever
#' @export
lmres <- function(nn=c('vind','zoned','icod')) {
  getgd(nn)
  x0 <- copy(vind)#[rcode=='BS-16-']
  x1 <- x0[y1!='TOTAL',.(resale=sum(n,na.rm=T)),rcode]
  x2 <- x0[y1=='TOTAL',.(total=sum(n,na.rm=T)),rcode]
  x3 <- x1[x2,on=c(rcode='rcode')][,res:=resale/total]
  x4 <- setkey(x3,rcode)[zoned[,rc]]
  x5 <- setkey(x4[,.(rc=rcode,res)],rc)[]
  stopifnot(isTRUE(all.equal(setkey(x5,rc)[,rc],icod[,sort(unique(ico))])))
  x5
}


#pm2 - opening £/m2
#' @export
lmpm2a <- function(nn=c('ppm2td','icod'),d2=format(Sys.Date()-getpv('celi', 'win')*365.25)) {
  getgd(nn)
  x1 <- setkey(setkey(ppm2td[date<d2],rcode,date)[ppm2td[,unique(rcode)],.(rcode,da=date,pm2=ppm2t),mult='first'][,.(pm2,rc=rcode)],rc)
  stopifnot(isTRUE(all.equal(setkey(x1,rc)[,rc],icod[,sort(unique(ico))])))
  x1
}

#pmr - pm2 relative to cohort
#' @export
lmpmr <- function(nn=c('cohod','prime1rcd')) {
  getgd(nn)
  x0 <- lmpm2a()
  x1 <- setkey(x0[cohod,on=c(rc='rc')][,.(rc,pmr=pm2/median(pm2)),ico][prime1rcd[,.(rc,ico)],on=c(rc='rc',ico='ico')][,ico:=NULL],rc)
  stopifnot(isTRUE(all.equal(setkey(x1,rc)[,rc],icod[,sort(unique(ico))])))
  x1
}

#pm2 - opening £/m2
#' @export
lmpm2 <- function(nn='ppm2td',d2=format(Sys.Date()-getpv('celi', 'win')*365.25)) {
  getgd(nn)
  setkey(lmpm2a()[cohod,on=c(rc='rc')][,.(rc,pm2=median(pm2)),ico][prime1rcd[,.(rc,ico)],on=c(rc='rc',ico='ico')][,ico:=NULL],rc)
}

#tstat and rsq table for lhs
#' @export
lmregtt <- function(nn='lmregtd') {
  x1 <- dcast(lhsdt()[,eqn:=paste0(code,'.all')][lmregtd,on=c(eqn='eqn')][,value:=ifelse(stat=='tstat',round(value,1),round(value,2))],driver+stat~desc)
  rhsdt()[,.(code,desc)][x1,on=c(code='driver')][,code:=NULL][stat!='tstat',desc:='.'][is.na(desc),desc:='const'][]
}

#pmc - closing £/m2 [this pretty useless as it is pm2 - factor returns; will therefore be a trivial explanatory vble]
#' @export
lmpmc <- function(nn='ppm2td',d2=format(Sys.Date())) {
  getgd(nn)
  setkey(setkey(ppm2td[date<d2],rcode,date)[ppm2td[,unique(rcode)],.(rcode,da=date,pm2=ppm2t),mult='last'],rc)
}

#ret - quarterly
#' @export
lmret <- function(nn=c('ptc1d','setdad','icod'),win=getpv('celi', 'win')) {
  getgd(nn)
  da1 <- '2002-06-30' #setdad[length(setdad)-win*4]
  x1 <- setkey(ptc1d[da<=da1,.(ret=mean(pret)),rc],rc)
  stopifnot(isTRUE(all.equal(setkey(x1,rc)[,rc],icod[,sort(unique(ico))])))
  x1
}

#lhs---------
#loadings + mean resid
#' @export
lmlhs <- function(nn='ldgxd',d2=format(Sys.Date())) {
  getgd(nn)
  x <- ldgxd[time==max(time),.(pc=pcode,rer=meanrer,l1=loadings1,l2=loadings2,l3=loadings3)]
  rownames(x) <- x[,pc]
  x[,pc:=NULL]
}

#z transform
#' @export
zf <- function(x) {
  x1 <- -as.data.table(lapply(lapply(lapply(x,rank),`/`,(nrow(x)+1)),qnorm,lower.tail=F))
  rownames(x1) <- rownames(x)
  x1
}

#join on rownames
#' @export
jrn <- function(a,b) {
  stopifnot(isTRUE(all.equal(sort(rownames(a)),sort(rownames(b)))))
  x <- copy(a)[,.tmp:=rownames(a)][copy(b)[,.tmp:=rownames(b)],on=c(.tmp='.tmp')]
  rownames(x) <- x[,.tmp]
  x[,.tmp:=NULL]
}

#' @export
dtpwr <- function(x,pwr=2) {
  x2 <- x
  if(1<pwr) { for(i in 2:pwr) {x2 <- x2*x}  }
  rownames(x2) <- rownames(x)
  colnames(x2) <- paste0(colnames(x2),pwr)
  x2
}

#test standardising lhs, rhs and adding powers to rhs [conclusion: no transform, 3 or 4 powers]
#' @export
lmpoly <- function(nn=c('lmlhsd','lmrhsd'),stat='adj.r.squared') {
  getgd(nn)
  jrhs <- setdiff(names(lmrhsd),'rc')
  jlhs <- setdiff(names(lmlhsd),'pc')
  xl <- lmlhsd #un-transformed
  xr <- lmrhsd
  zr <- zf(xr) #standardise to N(0,1)
  zl <- zf(xl)
  xx <- jrn(xl,xr)
  xz <- jrn(xl,zr)
  zz <- jrn(zl,zr)
  xx2 <- jrn(xx,dtpwr(xr)) #add squares
  xz2 <- jrn(xz,dtpwr(zr))
  zz2 <- jrn(zz,dtpwr(zr))
  xx3 <- jrn(xx2,dtpwr(xr,3)) #add cubes
  xz3 <- jrn(xz2,dtpwr(zr,3))
  zz3 <- jrn(zz2,dtpwr(zr,3))
  xx4 <- jrn(xx3,dtpwr(xr,4)) #add cubes
  xz4 <- jrn(xz3,dtpwr(zr,4))
  zz4 <- jrn(zz3,dtpwr(zr,4))
  x5 <- matrix(NA,12,4,dimnames=list(c('xx','xz','zz','xx2','xz2','zz2','xx3','xz3','zz3','xx4','xz4','zz4'),jlhs))
  for(j in seq_along(jlhs)) {
    f1 <- paste0(jlhs[j],' ~ ',paste0(jrhs,collapse='+'))
    f2 <- paste0(f1,'+',paste0(paste0(jrhs,'2'),collapse='+'))
    f3 <- paste0(f2,'+',paste0(paste0(jrhs,'3'),collapse='+'))
    f4 <- paste0(f3,'+',paste0(paste0(jrhs,'4'),collapse='+'))
    x5[1,j] <- summary(lm(data=xx,formula=as.formula(f1)))[[stat]]
    x5[2,j] <- summary(lm(data=xz,formula=as.formula(f1)))[[stat]]
    x5[3,j] <- summary(lm(data=zz,formula=as.formula(f1)))[[stat]]
    x5[4,j] <- summary(lm(data=xx2,formula=as.formula(f2)))[[stat]]
    x5[5,j] <- summary(lm(data=xz2,formula=as.formula(f2)))[[stat]]
    x5[6,j] <- summary(lm(data=zz2,formula=as.formula(f2)))[[stat]]
    x5[7,j] <- summary(lm(data=xx3,formula=as.formula(f3)))[[stat]]
    x5[8,j] <- summary(lm(data=xz3,formula=as.formula(f3)))[[stat]]
    x5[9,j] <- summary(lm(data=zz3,formula=as.formula(f3)))[[stat]]
    x5[10,j] <- summary(lm(data=xx4,formula=as.formula(f4)))[[stat]]
    x5[11,j] <- summary(lm(data=xz4,formula=as.formula(f4)))[[stat]]
    x5[12,j] <- summary(lm(data=zz4,formula=as.formula(f4)))[[stat]]
  }
  x5
}

#centre and add powers
#' @export
lmcen <- function(nn=c('lmlhsd','lmrhsd','lmrhsmd'),pwr=4,scale=F) {
  getgd(nn)
  xr <- as.data.table(lapply(lmrhsd,scale,scale=scale)) #same as the following which is clearer
  rownames(xr) <- rownames(lmrhsd)
  colnames(xr) <- colnames(lmrhsd)
  x1 <- jrn(lmlhsd,dtpwr(xr,1))
  if(1<pwr) {
  for(i in 2:pwr) { #add powers
    x1 <- jrn(x1,dtpwr(xr,i))
    #x1 <- suppressWarnings(jrn(x1,dtpwr(xr,i)))
  }
  }
  x1
}

#poly regress univariate /multivariate SEE fulmuni  BELOW
#' @export
lmuni <- function(
            nn=c('lmlhsd','lmrhsd','lmcend')
            ,
            uni=F
            ,
            col=1
            ){#},pwr=4,uni=F) {
  getgd(nn)
  j1 <-setdiff(names(lmcend),names(lmlhsd)) #2 lines to infer pwr
  f1 <- function(x){substr(x,nchar(x),nchar(x))}
  pwr <- max(unlist(lapply(lapply(j1[j1!='const'],f1),as.numeric)),na.rm=T)
  x1 <- copy(lmcend)#lmcen(pwr=pwr)
  ll <- setNames(as.list(names(lmlhsd)),names(lmlhsd))
  for(j in seq_along(lmlhsd)) {
    if(uni) {
      for(k in seq_along(lmrhsd)) {
        frml <- lmfrml(jlhs=names(lmlhsd)[j],names(lmrhsd)[k],pwr)
        x3 <- lm(data=x1,formula=frml)
        x4 <- data.table(summary(x3)$coefficients[,col,drop=F])
        names(x4) <- names(lmrhsd)[k]
        #x4[,rhsx:=names(lmrhsd)]#[,lhs:=name
        x4[,rhs:=paste0(names(lmrhsd)[k],0:pwr)]#[,lhs:=names(lmlhsd)[j]]
        x4[,eqn:=paste0(names(lmlhsd)[j],'.',names(lmrhsd)[k])]
        if(k==1) x2 <- x4 else x2 <- rbind(x2,x4,use.names=F)
      }
      x2 <- x2[grepl('...0',rhs),rhs:='(Intercept)']
      x2[,`:=`('lhs',names(lmlhsd)[j])]
      #x2[,pwr:=0:4]
      ll[[j]] <- setnames(x2,c('coef','rhs','eqn','lhs'))[,method:='univ']
    } else {
      frml <- lmfrml(jlhs=names(lmlhsd)[j],names(lmrhsd),pwr)
      x3 <- lm(data=x1,formula=frml)
      x4 <- setnames(data.table(summary(x3)$coefficients[,col,drop=F],keep.rownames=T),c('rhs','coef'))[,`:=`(lhs,names(lmlhsd)[j])]
      x4[,eqn:=paste0(names(lmlhsd)[j],'.all')]
     ll[[j]] <- x4[,method:='mult']
    }
    ll[[j]][,rhsx:=substr(rhs,1,3)]
    ll[[j]][rhsx=='(In',rhsx:='const']
    setcolorder(ll[[j]],c('method','eqn','lhs','rhs','rhsx','coef'))
  }
  x <- lmpwr(rbindlist(ll))
  x[grepl('(Intercept)',rhs),rhs:='const']
}


#' @export
fulmuni <- function(
  lmlhsd,lmrhsd,lmcend
  ,
  uni=F
  ,
  col=1
){#},pwr=4,uni=F) {
  j1 <-setdiff(names(lmcend),names(lmlhsd)) #2 lines to infer pwr
  f1 <- function(x){substr(x,nchar(x),nchar(x))}
  pwr <- max(unlist(lapply(lapply(j1[j1!='const'],f1),as.numeric)),na.rm=T)
  x1 <- copy(lmcend)#lmcen(pwr=pwr)
  ll <- setNames(as.list(names(lmlhsd)),names(lmlhsd))
  for(j in seq_along(lmlhsd)) {
    if(uni) {
      for(k in seq_along(lmrhsd)) {
        frml <- lmfrml(jlhs=names(lmlhsd)[j],names(lmrhsd)[k],pwr)
        x3 <- lm(data=x1,formula=frml)
        x4 <- data.table(summary(x3)$coefficients[,col,drop=F])
        names(x4) <- names(lmrhsd)[k]
        #x4[,rhsx:=names(lmrhsd)]#[,lhs:=name
        x4[,rhs:=paste0(names(lmrhsd)[k],0:pwr)]#[,lhs:=names(lmlhsd)[j]]
        x4[,eqn:=paste0(names(lmlhsd)[j],'.',names(lmrhsd)[k])]
        if(k==1) x2 <- x4 else x2 <- rbind(x2,x4,use.names=F)
      }
      x2 <- x2[grepl('...0',rhs),rhs:='(Intercept)']
      x2[,`:=`('lhs',names(lmlhsd)[j])]
      #x2[,pwr:=0:4]
      ll[[j]] <- setnames(x2,c('coef','rhs','eqn','lhs'))[,method:='univ']
    } else {
      frml <- lmfrml(jlhs=names(lmlhsd)[j],names(lmrhsd),pwr)
      x3 <- lm(data=x1,formula=frml)
      x4 <- setnames(data.table(summary(x3)$coefficients[,col,drop=F],keep.rownames=T),c('rhs','coef'))[,`:=`(lhs,names(lmlhsd)[j])]
      x4[,eqn:=paste0(names(lmlhsd)[j],'.all')]
      ll[[j]] <- x4[,method:='mult']
    }
    ll[[j]][,rhsx:=substr(rhs,1,3)]
    ll[[j]][rhsx=='(In',rhsx:='const']
    setcolorder(ll[[j]],c('method','eqn','lhs','rhs','rhsx','coef'))
  }
  x <- lmpwr(rbindlist(ll))
  x[grepl('(Intercept)',rhs),rhs:='const']
}

#' @export
lmfrml <- function(jlhs,jrhs,pwr=4) {
  f1 <- paste0(jlhs,' ~ ',paste0(paste0(jrhs,'1'),collapse='+'))
  if(1<pwr) {
  for(i in 2:pwr) {
    f1 <- paste0(f1,'+',paste0(paste0(jrhs,i),collapse='+'))
  }
  }
  as.formula(f1)
}

#' @export
lmpwr <- function(x,jcoef='rhs',jconst=c('const','(Intercept)')) {
    x1 <- copy(x)
  x1[,i.:=eval(parse(text=jcoef))]
  x1[(i.%in%jconst),pwr:=0]
  x1[!(i.%in%jconst),pwr:=as.numeric(substr(rhs,nchar(rhs),nchar(rhs)))]
  x1[,i.:=NULL]
  x1
}

#' @export
rhscode <- function(){
  c('km from London'='lon',' #/Ha'='den','km from city'='met','resold from new'='re1','freehold fraction'='fre',' £/m2'='pm2',' £/m2 relative'='pmr','avg log quarter return'='ret')#,'resold'='res') #,'(delta m2)/m2'='gro' #resold not works due to assumptions re augment() and many:one
}
#' @export
rhsdt <- function(){
  x <- data.table(
    code=rhscode(),
    units=names(rhscode()),
    desc=c('From London','Density','Rurality','First Resale','Freehold','Price','Price relative','Return 95-02')#,'Resold') #,'Capex'
  )
  rbind(x,data.table(code='all',units='-',desc='all fundamentals'))
}
#' @export
lhsdt <- function(){
  data.table(
    code=c('rer','l1','l2','l3'),
    desc=c('residual','factor 1','factor 2','factor 3')
  )
}

#' @export
lmpolyunico <- function(nn=c('lmcend','lmfitd','lmrhsd','lmrhsmd'),lhs='l2') {
  getgd(nn)
  #rhs <- rhscode()
  ll <- as.list(rhsdt()[,code])
  #yabsmax <- max(abs(unlist(lmfitd[,paste0(lhs,'.',rhscode()),with=F])))
  yfit <- unlist(lmfitd[,paste0(lhs,'.',rhsdt()[,code]),with=F])
  yrange <- quantile(yfit,c(.01,.99))
  n25 <- round(nrow(lmcend)*.05)
  n75 <- round(nrow(lmcend)*.95)
  plotorder <- NULL
  rhsuni <- setdiff(rhsdt()[,code],'all')
  for(i in seq_along(rhsuni)) { #copy of the code below to resequence by slope of line
    jrhs=rhsuni[i]#rhsdt()[i,code]
    # xy <- setnames(cbind(lmcend[,paste0(jrhs,1),with=F],lmfitd[,paste0(lhs,'.',jrhs),with=F]),c('x','y'))
    # setkey(xy[,x:=x+unlist(lmrhsmd[,rhsdt()[i,code],with=F])],x)
    xy <- setkey(setnames(cbind(lmrhsd[,jrhs,with=F],lmfitd[,paste0(lhs,'.',jrhs),with=F]),c('x','y')),x)
    up <- xy[n25,y]<xy[n75,y]
    plotorder[i] <- abs(xy[n25,y]-xy[n75,y])
  }
  rhssort <- rhsdt()[rev(order(plotorder))]
  cols <- 3
  layout <- t(matrix(seq(1, cols * ceiling(nrow(rhssort)/cols)),
                     ncol = cols, nrow = ceiling(nrow(rhssort)/cols)))
  #rowser()
  for(i in seq_along(rhssort[,code])) {
    jrhs=rhssort[i,code]
    #xy <- setnames(cbind(lmcend[,paste0(jrhs,1),with=F],lmfitd[,paste0(lhs,'.',jrhs),with=F]),c('x','y'))
    #setkey(xy[,x:=x+unlist(lmrhsmd[,rhssort[i,code],with=F])],x)
    xy <- setkey(setnames(cbind(lmrhsd[,jrhs,with=F],lmfitd[,paste0(lhs,'.',jrhs),with=F]),c('x','y')),x)
    xwindow <- xy[n25:n75,range(x)]
    ywindow <- xy[n25:n75,range(y)]
    up <- xy[n25,y]<xy[n75,y]
    tit <- if(up) paste0(rhssort[i,desc],' ->') else  paste0('<- ',rhssort[i,desc])
    x1 <- ggplot(xy[seq(from=n25,to=n75,by=10)],aes(x,y))+geom_point(size=.1)+#geom_bar(stat='identity')+
      #ylim(ywindow)+xlim(xwindow)+
      scale_y_continuous(limits = yrange) + #c(-yabsmax, yabsmax)) +
      xlab(rhssort[i,units]) + ggtitle(tit)
    if(!up) x1 <- x1+scale_x_reverse() #generates warnings but works correctly
    if(i==1) x1 <- x1 + ylab('loading(x)') else x1 <- x1 + ylab('')
    ll[[i]] <- x1
  }
  multiplot(plotlist=ll,cols=cols,layout=layout)
}


#' @export
lmex1 <- function(nn=c('lmcend','lmfitd'),jlhs='l2',jrhs='all') {
  cbind(lmcend[,jlhs,with=F],lmfitd[,paste0(jlhs,'.',jrhs),with=F])
}

#the top-level accessor for fit scatterplots
#' @export
lmfitx <- function(jlx=c('l1','l2','l3','rer'),nn=c('lmcend','lmfitd'),jrx=c('all',rhscode()),rng=c(-.04,.12),...) {
  getgd(nn)
  jlx <- match.arg(jlx)
  jrx <- match.arg(jrx)
  lmex1 <- function(nn=c('lmcend','lmfitd'),jlhs=jlx,jrhs=jrx) {
    getgd(nn)
    setnames(cbind(lmfitd[,paste0(jlhs,'.',jrhs),with=F],lmcend[,jlhs,with=F]),c('x','y'))
  } # x=fit, y=act
 #browser()
  #rho <- lmregtd[stat=='rsquared'&substr(eqn,1,2)==jlx,value^.5]
  xy <- lmex1(jlhs=jlx,jrhs=jrx)
  ggplot(xy,aes(x,y)) + geom_point(...) +
    ggtitle(paste0(lhsdt()[code==jlx,desc],'  correlation ',round(cor(xy)[1,2],2))) +
    xlab(paste0('fit(',rhsdt()[code==jrx,desc],')')) + ylab('actual') +
    stat_smooth(method = "lm", col = "dark grey")+
    ylim(rng)+xlim(rng)
}

#list of barplot of tstats annotated with R2, intended for a multiplot()
#' @export
lmregtg <- function(eqnx=lmregtd[,unique(eqn)],nn=c('lmregtd','lmregd')) {
  ll <- as.list(eqnx)
  y1 <- lmregtd[stat=='tstat'&driver!='const',max(value)]
  ymax <- ceiling(y1/10)*10
  for(i in seq_along(eqnx)) {
    x1 <- lmregtd[stat=='tstat'&eqn==eqnx[i]&driver!='const'][,y:=value]
    x2 <- rhsdt()[x1,on=c(code='driver')][,x:=reorder(desc,lmregtd[stat=='tstat'&driver!='const',mean(value),driver][,V1])]
    ll[[i]] <- ggplot(x2,aes(x,y,color=NULL,fill=x)) +
      geom_bar(stat='identity') +
      scale_x_discrete(name=paste0('R2 = ',lmregtd[stat=='adj.r.squared'&eqn==eqnx[i],round(value,2)])) +
      coord_flip() +
      theme(legend.position="none") +
      scale_y_continuous(limits=c(0,ymax),name=ifelse(i==length(eqnx),"t-stat","")) +
      ggtitle(lhsdt()[code==lmregd[eqn==eqnx[i],lhs[1]],desc])
  }
  ll
}

#list of barplot of tstats annotated with R2, intended for a multiplot()
#' @export
lmrcx2g <- function(rcx2='W--2--',x=lmrcx2(rcx=rcx2),nn=c('lmnrcd')) {
  x0 <- melt(x[,-(2:4)],variable.factor=F,id.var='code')[code!='const']
  eqnx <- x0[,unique(variable)]
  ll <- as.list(eqnx)
  y1 <- x0[,range(value)]
  for(i in seq_along(eqnx)) {
    x1 <- x0[variable==eqnx[i]][,y:=value]
    x2 <- lmrhsord()[x1,on=c(code='code')][,x:=descfactor]
    ll[[i]] <- ggplot(x2,aes(x,y,color=NULL,fill=x)) +
      geom_bar(stat='identity') +
      scale_x_discrete(name='') +
      coord_flip() +
      theme(legend.position="none") +
      scale_y_continuous(limits=y1,name=ifelse(i==length(eqnx),"contribution to fitted value (%)","")) +
      ggtitle(gsub(patt=' %',rep='',x=eqnx[i]))
  }
  ll
}

#see fulm1rc for compliant version below
#' @export
lm1rc <- function(rcx='W--2--',nn=c('lmcontd','lmlhsd')) {
  x1 <- dcast(lmcontd[rc==rcx],rhs~eqn,value.var='contrib')
  f1 <- function(x0) {
    j1 <- lapply(x0,is.numeric)
    x1 <- as.list(seq_along(x0))
    for(i in seq_along(x0)) {
      x1[[i]] <- ifelse(j1[[i]],sum(x0[[i]]),'Fit Total')
    }
    setnames(as.data.table(x1),names(x0))[]
  }
  x2 <- rbindlist(list(x1,f1(x1),lmlhsd[rownames(lmlhsd)==rcx,][,.(rhs='Actual',l1,l2,l3,rer)]))
  x3 <- rbind(x2,data.table(rhs=' Actual-Fit',x2[rhs=='Actual',-1,with=F]-x2[rhs=='Fit Total',-1,with=F]))[,rc:=rcx]
  x3
}

#' @export
fulm1rc <- function(rcx='W--2--',lmcontd,lmlhsd) {
  x1 <- dcast(lmcontd[rc==rcx],rhs~eqn,value.var='contrib')
  f1 <- function(x0) {
    j1 <- lapply(x0,is.numeric)
    x1 <- as.list(seq_along(x0))
    for(i in seq_along(x0)) {
      x1[[i]] <- ifelse(j1[[i]],sum(x0[[i]]),'Fit Total')
    }
    setnames(as.data.table(x1),names(x0))[]
  }
  x2 <- rbindlist(list(x1,f1(x1),lmlhsd[rownames(lmlhsd)==rcx,][,.(rhs='Actual',l1,l2,l3,rer)]))
  x3 <- rbind(x2,data.table(rhs=' Actual-Fit',x2[rhs=='Actual',-1,with=F]-x2[rhs=='Fit Total',-1,with=F]))[,rc:=rcx]
  x3
}


#' @export
lmrcx <- function(rcx='W--2--',nn='lmnrcd',scale=100,decp=2,append='') {
  x0 <- copy(lmnrcd)[,value:=round(scale*value,decp)]
 # x1 <- dcast(x0[rc==rcx],rhs~variable)
  x1 <- dcast(setkey(x0,rc)[rcx],rc+rhs~variable)
  setnames(x1,old=paste0(lhsdt()[,code],'.all'),new=paste0(lhsdt()[,desc],append))
  x1
}

#' @export
lmrcx1 <- function(rcx='W--2--',nn='lmnrcd',scale=100,decp=2,...) {
  lmrcx(rcx,nn,scale,decp,...)[5<nchar(rhs)]
}

#' @export
lmrcx2 <- function(
  rcx='W--2--'
  ,
  scale=100
  ,
  decp=2
  ,
  nn=c('lmrhsd','lmcend','lmnrcd')
  ) {
  x1 <- melt(lmrcx(rcx[1],nn,scale,decp)[-(1:3),!'rc',with=F],id.var='rhs')
  x2a <- copy(lmrhsd)[,a:=NULL] #added this 20180325, not sure why a is in here
  x2 <- melt(x2a[rownames(x2a)==rcx],variable.name='rhs',measure.vars=names(x2a))[,.(rhs,variable='exposure',value)][]
  x3 <- data.table(rhs='const',variable='exposure',value=1)
  x23 <- rbind(x2,x3)[,value:=signif(value,3)]
  x4 <- melt(lmcend[rownames(x2a)==rcx],variable.name='rhs',measure.vars=names(lmcend))[substr(rhs,4,4)=='1',.(rhs=substr(rhs,1,3),variable='standardised',value)][x2[,.(rhs)],on=c(rhs='rhs')]
  x5 <- data.table(rhs='const',variable='standardised',value=0)
  x45 <- rbind(x4,x5)[,value:=signif(value,2)]
  x6 <- setcolorder(dcast(rbindlist(list(x1,x23,x45)),rhs~variable),c('rhs','exposure','standardised','factor 1','factor 2','factor 3','residual'))
  setnames(x6,c('rhs','exposure','standardised','factor 1 %','factor 2 %','factor 3 %','residual %'))
  x7 <- rhsdt()[x6,on=c(code='rhs')][]
  x8 <- setnames(x7[is.na(desc),desc:='average'],old='desc',new='.')[,units:=NULL]
  setkey(x8,code)[lmrhsord()[,c('const',code)]][]
}

#' @export
lmrhsord <- function(nn='lmregtd') {
  getgd(nn)
  x1 <- rhsdt()[setkey(lmregtd[stat=='tstat'&driver!='const',.(meant=-mean(value)),driver],meant)[,.(rhs=driver,x=-meant)],on=c(code='rhs')]
  x1[,codefactor:=reorder(as.factor(code),x)][,descfactor:=reorder(as.factor(desc),x)]
  x1[]
}

#' @export
lmrcg1 <- function(rcx=c('W--2--')) {
  x1 <- melt(setnames(lmrcx1(rcx=regpcode(rcx),append='')[,rc:=irregpcode(rc)],old='rc',new='zone')[],id.vars=c('zone','rhs'))
  x1[,tmp:=nchar(as.character(rhs))][,rhs:=reorder(as.factor(rhs),tmp)]
  ggplot(x1,aes(variable,value)) + geom_bar(aes(fill=rhs), position='dodge', stat='identity') +
    scale_x_discrete(name="") + scale_y_continuous(name="loading (%)") +
    ggtitle(irregpcode(rcx)) + guides(fill=guide_legend(title=""))
}

#' @export
rcodeA <- function(levelx=c('d','da','s'),nn='inczod'){
  getgd(nn)
  setkey(inczod,level)[levelx,sort(unique(pruneto))]
}


#' @export
segl <- function(i,segd,x2) {
  segd[grepl(x2[i,perl],id)]
}
#'grpd','cinoued','cinoupd','jomad'
#steps for export to gui - pars is also exported
#' @export
dst1 <- function() {
  x1 <- c('si1d','ppm2d','ppm2td','pcla1d','ppmxd','ptc1d','cohod',
          'dec2xd','varfd','vcvdecd','lmscod','zomad','scoxd','vcfmtd',
          'decd','cncd','varyxd','dezocombod','dezoint1d','celid',
          'yieldd','vind','zoned','kmd','dfinald','hed1d','sty1d',
          'stnevod','scofostnd','scofostncd','buifod','stnd','setdad',
          'extdad','buifostnd','lmcend','lmfitd','lmrhsd','lmrhsmd','lmregtd',
          'lmregd','lmnrcd','dezospdfd','frsud','mejosfd','typed','cdfd'
          )
  x1 <- setdiff(x1,'yieldd')
  x2 <- gsub('d$','',x1)
  x3 <- setkey(data.table(seq=seq_along(x2),step=x2),step)
  x3
}

#existence in versions
#' @export
dst3 <- function(verx=28,dst=dst1()) {
  dst[,ver:=verx][,des:=descrdatv(type=paste0(step,'d'),ver=verx)][]
  x1 <- ddv(ver=verx)[]
  x2 <- dst[]
  x1[x2,on=c(des='des')][]
}

#classes match across versions
#ver step col class
#' @export
dst4 <- function(verx=28,dst=dst3(verx=verx),nmax=3) {
  x2 <- setkey(dst[1:min(nmax,nrow(dst))],ver,step)
  x2[getkey(x2),.(class(getrdatv(ver=ver,type=paste0(step,'d')))[1]),by=.EACHI][]
}

#utility to reduce size - needs parameterise of tables
#' @export
ppsub <- function(vin=29
                  ,
                  vout=34
                  ,
                  rcx='^S--|^DA-'    #'^S--|^W--|^WC-|^ZE-|^SM-|^DA-'
                  ,
                  dt=c('ppd','pcrcd','pars','pcod','agpcod','e1d')
) {
  #dt <- c('ppd','pcrcd','pars','pcod','agpcod','e1d')
  on.exit(setv(v=vout))
  for(i in seq_along(dt)) {
    if(exists(dt[i],envir=globalenv())) {
      rm(list=dt[i],envir=globalenv())
    }
  }
  setv(ver=vin)
  getgd(dt)
  pcrcd <<- pcrcd[grepl(rcx,rcode)]
  ppd <<-  setnames(pcrcd[ppd,on=c(pcode='pc')][!is.na(rcode)][,rcode:=NULL],old='pcode',new='pc') #this achieves the selection via pcrcd
  #rdo3d <<- rdo3d[grepl(rcx,rcode)]
  pcod <<- pcod[grepl(rcx,rcode)]
  agpcod <<- agpcod[grepl(rcx,rc)]
  e1d <<- e1d[grepl(rcx,rcode)]
  setv(ver=vout)
  putt(ppd)
  putt(pcrcd)
  putt(pars)
  #putt(rdo3d)
  putt(pcod)
  putt(agpcod)
  putt(e1d)
}

#not sure I want a prefix for codes
#' @export
iprefix <- function(){''}

imgdirdelete <- function() { #these lines found adrift (not in function) 20190617 - preventing pkg build - are they needed as part of deploy???
  imgpath <- "..\\pkgx\\img"
  if(dir.exists(imgpath)) {
    shell(paste0("rd /s /q ",imgpath))
  }
  dir.create(imgpath)
}

#hard delete and replace of rd, img, mejo
#' @export
deploy <- function(typed=c(dst1()[,paste0(step,'d')],'pars'),outdir='..\\gui1\\',dn=c('rd','img','mejo'),subdir=paste0(dn,'\\')) {
  aatopselect('aappd')
  indir <- paste0(root.global,'rd/')
  #delete output dirs
  sapply(subdir,outdir=outdir,function(subdir,outdir){if(length(dir(paste0(outdir,subdir)))) shell(paste0("rd /s /q ",paste0(outdir,subdir)[1]))})
  #stopifnot(!('rd'%in%dir(outdir)|'mejo'%in%dir(outdir)|'img'%in%dir(outdir)))
  #make new dirs
  for(i in 1:3) if(!dn[i]%in%dir(outdir)) shell(paste0('mkdir ',outdir,subdir)[i])
  #copy
  for(i in 2:3) shell(paste0('cp ./',dn[i],'/*.* ',outdir,dn[i],'/'))
  #selective copy for rd
  dd <- dir(indir)
  ff <- dd[grepl(paste0(descrdatv(type=typed),collapse='|'),dd)]
  cc <- paste0('cp ',indir,ff,' ',paste0(outdir,'rd/'))
  for(i in seq_along(ff)) {
    shell(cc[i])
  }
  setdiff(sort(dir(paste0(outdir,subdir[1]))),sort(ff))
  stopifnot(all.equal(sort(dir(paste0(outdir,subdir[1]))),sort(ff)))
}





#' @export
km <- function(ldgxd,hulld,k=4,level=2,yearx=ldgxd[,max(time)],nstart=1000) {
  hpc <- hulld[year==yearx,rc]
  if('loadings4'%in%colnames(ldgxd)) { #not v nice - this is when hull4 has been used
    x <- setkey(ldgxd[time==yearx],pcode)[hpc,.(pcode,loadings1,loadings2,loadings3,loadings4)]
  } else {
    x <- setkey(ldgxd[time==yearx],pcode)[hpc,.(pcode,loadings1,loadings2,loadings3)]
  }
  ll <- as.list(k)
  for(i in seq_along(k)) {
    ll[[i]] <- copy(x)
    gg <- kmeans(x[,-1,with=F],cent=k[i],nstart=nstart)$cluster
    ll[[i]][,cluster:=gg][,nk:=k[i]]
    #ll[[i]][,stype:=tail(names(sort(table(substr(pcode,1,3*level)))), 1),'nk,cluster'] #random selection - so do this after lp when weight available
  }
  kmd <-rbindlist(ll)
  kmd
}

#' @export
vin <- function(seglxd,prppd,augd,trim=.05) {
  s2 <- seglxd[,.(rcode=parent,y0=as.integer(format(startdate,'%Y')),y1=format(deed_date,'%Y'),r,pa)] #y1 was as.integer
  v1 <- suppressWarnings(s2[,.(mu=mean(pa,trim=trim,na.rm=T),se=sd(pa,na.rm=T),n=.N),by='rcode,y0,y1'][,se:=se/sqrt(n-1)])
  prppd[,year:=substr(deed_date,1,4)]#[,area:=substr(id,1,3)] #not clear if these exist already
  s2b <- prppd[,.(rcode,y1=as.character(year))] #now the total transaction count in y0
  v2 <- prppd[,.(n=.N),'year,rcode'][,.(y0=as.integer(year),rcode,y1='TOTAL',mu=NA,se=NA,n)]
  setkey(v2,rcode)[augd[,unique(pruneto)],n:=NA] #20171008: these set NA because the many:one relation in augment() is not recognised int the logic above.. will this screw up regression?
  rbind(v1,v2)
}

# linked <- linkeFun(neared=neared)     #remove lower diagonal, and compose 'link' string
#--linkeFun     #remove lower diagonal, and compose 'link' string
#' @export
linke <- function(neared=gett('neared')
                  ,
                  delim = getpv('linke', 'delim')
) {
  f1 <- function(a, b) {
    sort(c(a, b))
  }
  neared[, link := paste0(delim, f1(rc, other)[1], delim, f1(rc, other)[2], delim), 'rc,other']
  linked <- unique(setkey(neared, link))
  linked
}

#' @export
distance <- function(zoned=gett('zoned')
                     ,
                     save = T
                     ,
                     sf = 3) {
  pairs <- setkey(zoned[, one := 1], one)
  distanced <-
    pairs[pairs, allow = T][, dist := round(.001 * ((i.eex - eex) ^ 2 + (i.nnx -
                                                                           nnx) ^ 2) ^ .5, sf)][, list(rc, pc, other = i.rc, eex, nnx, dist)]
  distanced
}

#' @export
savepng <-
  function(nn=c('zomad','dezod'),
           npx = 1000,
           j=as.character(invlist()[setdiff(as.character(unlist(jdz())), c('postcode','km radius'))])
  ) {
    getgd(nn)
    #imgpath <- "..\\gui1\\img"
    imgpath <- "..\\pkgx\\img"
    if(dir.exists(imgpath)) {
      shell(paste0("rd /s /q ",imgpath))
    }
    dir.create(imgpath)
    stopifnot(dir.exists(imgpath) && length(dir(imgpath))==0)
    for (i in seq_along(j)) {
      jo <-
        jomax1(
          #          j = as.character(invlist()[j[i]]),
          j=j[i],
          zomad = zomad,
          dezod = dezod,
          window = F
        )
      fn <- paste0(imgpath,"\\", as.character(j[i]), '.png')
      print(fn)
      png(fn, npx, npx)
      pm1(jo = jo, leg = j[i])
      dev.off()
    }


  }

#--px()
#' @export
px <- function(jomaxd=gett('jomaxd'),annotate=F,...) {
  pm1(jomaxd,annotate=annotate,...)
}

#' @export
si1f <- function(ptc1d,k=3,wgt=3) {
  setkey(ptc1d,rc,da)
  setkey(ptc1d,rc)
  ptc1d <- ptc1d[getkey(ptc1d),.(da,pret,a1=data.table::shift(cumsum(pret),fill=0),a2=data.table::shift(rollsumr(pret,na.pad=T,fill=0,k=k))),.EACHI]
  ptc1d[is.na(a2),a2:=0][,xar:=(wgt*a2*4/k+a1*365/as.numeric((as.Date(da)-as.Date('1994-12-31'))))/(wgt+1)]
}

#' @export
jdzlabel <- function(){setNames(list('pv','m2','pm2','n'),c('present value','area m2','price/m2','n assets'))}

#choropleth
#has no units in the label popup, and no key
#' @export
cl1 <- function(dezo2d,dex='ppm2',palette="YlGnBu", probs=c(0,.1,.2,.4,.6,.8,.9,.95,1),width='800px',height='800px') {
  dezo2d@data[,'de'] <- dezo2d@data[,dex]
  sig <- 3
  unit <- ''# £/m<sup>2</sup>'
  #cb <- colorQuantile("YlOrRd", domain = dezo2d@data$de, n=20)
  cb <- colorQuantile(palette=palette, domain = dezo2d@data$de, probs=probs)
  labels <- sprintf(
    paste0("<strong>%s</strong><br/>%g",unit),
    dezo2d@data[,'pc'], signif(dezo2d@data$de,sig)
  ) %>% lapply(htmltools::HTML)

  leaflet(dezo2d,width=width,height=height,options=leafletOptions(minZoom=7)) %>%
    #  setView(lng = -5, lat = 55, zoom = 5) %>%
    addProviderTiles("CartoDB.Positron") %>%
    addPolygons(
      fillColor = ~cb(de)
      ,
      smoothFactor = 0, #this removes slivers of white between polygons https://gis.stackexchange.com/questions/176112/remove-sliver-gaps-between-polygons-with-r
      weight = 1,
      opacity = 0,
      color = "white",
      dashArray = "3",
      fillOpacity = 0.4,
      highlight = highlightOptions(
        weight = 5,
        color = "#666",
        dashArray = "",
        fillOpacity = 0.7,
        bringToFront = TRUE)
      ,
      label = labels,
      labelOptions = labelOptions(
        style = list("font-weight" = "normal", padding = "3px 8px"),
        textsize = "12px",
        direction = "auto")
    )
}
#' @export
cl2 <- function(dezo2d,dex='ppm2',palette="YlGnBu", probs=c(0,.1,.2,.4,.6,.8,.9,.95,1),width='800px',height='800px') {
  x1 <- cl1(dezo2d,dex,palette,probs,width,height)

}

#' @export
metro <- function() {c('N','E','SE','SW','NW','BN','CM','OX','RG','NG','AL','LU','RO','GU','KT','CR','HA')}


#cin protocol, find neighbours out to rank k1
#' @export
cinbr1 <- function(
  jtype='pxcippmd'
  ,
  nn=c('pxcicpxd')
  ,
  k1=20 #peers to check against
) {
  #browser()
  getgd(c(nn,jtype))
  z1 <- copy(get(jtype,envir=globalenv()))
  z2 <- z1[,.(ninunit=.N),rcunique][unique(pxcicpxd[,.(rcunique,EE,NN)]),on=c(rcunique='rcunique'),nomatch=0]
  z3 <- RANN::nn2(z2[,.(EE,NN)],k=k1)[[1]]
  z4 <- cbind(z2[,.(rcunique)],data.table(z3))
  z5 <- setkey(z2[,.(rcunique)][setkey(melt(z4,id.vars='rcunique',value.name='neighbrindex'),rcunique,variable),on=c(rcunique='rcunique')][,distncrank:=as.integer(substr(variable,2,3))][,variable:=NULL],rcunique,distncrank)[]
  z6 <- cbind(z5,z2[z5[,neighbrindex],.(neighbr=rcunique)])[,.(rcunique,distncrank,neighbr)]
  z7 <- setkey(z2[,.(rcneigh=rcunique,ninneigh=ninunit)][z6[0<distncrank],on=c(rcneigh='neighbr')],rcunique)[,cumul:=cumsum(ninneigh),rcunique]
  z8 <- z7[z7[,.(iend=min(which(cumul>=k1))),rcunique],on=c(rcunique='rcunique')][distncrank<=(iend)][,jtype:=jtype]
  z9 <- unique(pxcicpxd[,.(rcunique,zo)])[z8,on=c(rcunique='rcunique')]
  z9
}

#join x with cinbrd
#' @export
cinou <- function(x='pxcippmd',cinbrd=pxcinbrd,jrank='ppm2',k=100,p1=.02) {
  getgd(x)#c(x,nn))
  x1a <- setkey(cinbrd[jtype==x][get(x,envir=globalenv()),on=c(rcneigh='rcunique'),allow=T,nomatch=0],rcunique,distncrank)
  x1 <- x1a[,.SD[1:max(k,.SD[1,ninneigh])],rcunique][,inlier:=T]
  x2 <- setkeyv(x1,c('rcunique',jrank))[,rnk:=((1:.N)-1)/(.N-1),rcunique][,p:=pmin(rnk,(1-rnk))]
  x3 <- x2[p<p1,inlier:=F][,weight:=as.numeric(inlier)]
  x3[]
}
# cinou <- function(x='cippmd',nn='cinbrd',jrank='ppm2',k=100,p1=.02) {
#   getgd(c(x,nn))
#   x1a <- setkey(cinbrd[jtype==x][get(x,envir=globalenv()),on=c(rcneigh='rcunique'),allow=T,nomatch=0],rcunique,distncrank)
#   x1 <- x1a[,.SD[1:max(k,.SD[1,ninneigh])],rcunique][,inlier:=T]
#   x2 <- setkeyv(x1,c('rcunique',jrank))[,rnk:=((1:.N)-1)/(.N-1),rcunique][,p:=pmin(rnk,(1-rnk))]
#   x3 <- x2[p<p1,inlier:=F][,weight:=as.numeric(inlier)]
#   x3[]
# }
#SPs(SPDF) ie voronoi tesselation
#' @export
pcpoly <- function(pcud,eps=1e-9) {
  #browser()
  stopifnot(is(pcud,'data.table')&&c('EE','NN')%in%names(pcud))
  #spdf <- sp::SpatialPointsDataFrame(pcud[,.(EE,NN)],data=pcud[,.(EE)])
  x2 <- deldir::tile.list(deldir::deldir(pcud[,EE], pcud[,NN],eps=eps))
  x4 <- vector(mode="list", length=length(x2))
  #browser()
  for (i in seq(along=x4))
  {
    x3 <- cbind(x2[[i]]$x, x2[[i]]$y)
    x3 <- rbind(x3, x3[1,])
    x4[[i]] <- Polygons(list(Polygon(x3)), ID=as.character(i))
  }
  #browser()
  SPDF <- SpatialPolygonsDataFrame(SpatialPolygons(x4),data=pcud)
  SPDF
}

#' @export
pcpoly1 <- function(x,nn='ciard',eps=1e-9) {
  getgd(nn)
  pcpoly(x,eps=eps)[grep(grepstring(get(nn)),x[,rcunique]),]
}

#a summary of the steps in terms of .N by area - could be a general summary by a field which here is set to rcunique - could be a pre-set field eg 'byx'
#' @export
josum <- function() {
  jomned <- jomned[pre=='ci']
  setkey(jomned,level,fun)
  x3 <- NULL
  for(i in 1:nrow(jomned)) {
    x1 <- get(jomned[i,output],globalenv())
    if(is.data.table(x1)&&'rcunique'%in%names(x1)) {
      x2 <- setkey(setnames(x1[,.N,substr(rcunique,1,3)],c('area',jomned[i,output])),area)
      if(is.null(x3)) x3 <- x2 else x3 <- x3[x2]
    }
  }
  x3
}

#areas around a central area rcx
#' @export
nbrs <- function(rcx=distanced[1,rc],distanced=pxzodistanced) {
  rca <- substr(regpcode(rcx),1,3)
  x1 <- setkey(setkey(copy(distanced)[,x1:=substr(rc,1,3)],x1)[rca][,x2:=substr(other,1,3)][,mean(dist),x2],V1)
  x1[,x2]
}

#multi-area - saves each as a named step (not v nice)
#removed 210615
# mejo <- function(ax=nbrs('SW')[1:2],nn=c('depd','distanced')) {
#   getgd(nn)
#   ll <- list(NULL)
#   for(i in seq_along(ax)) {
#     print(ax[i])
#     ciarFun(ax[i])
#     jomneFun(depd=depd[!step%in%c('ciar','cicp')])
#     ll[[i]] <- copy(cinouad)
#     putrdatv(cijod,type=paste0('cijo',regpcode(ax[i]),'d'))
#   }
#   cinouad <<- rbindlist(ll) #this is a sideffect which is non-canonical form
#   putt(cinouad)
# }




#' mejosf <- function(ax=nbrs('SW'),nn=c('depd','distanced','setdad'),parallel=5<length(ax),ncpus=min(c(6,length(ax))),
#'                    keepx=F #keep=T keeps the intermediates for qc, and does not delete mejo/ just overwrites contents
#'                    ) {
#'   getgd(nn)
#'   if(!keepx) suppressWarnings(cocomkd('mejo')) #deletes this subdir upfront
#'   ll <- as.list(ax)
#'   sfInit(parallel=parallel,cpus=ncpus)
#'   #setkeep(keepx)
#'   ll1 <- sfLapplyWrap(ll,mejosf1,nn=nn,keepx=keepx) #the side-effect is to write cijoxxxd to rd
#'   mejosfd <<- rbindlist(ll1) #this relates to a different 'mejo which returned a 'job description' with fields like elapsed time etc... now lost???
#'   setkeep(T)
#'   putt(mejosfd)
#' }

#--used in 4map
#' @export
lflt2 <- function(x,sig=3,unit='',quantileNum=9,palette="YlGnBu",width='800px',height='800px',poly=T,line=T,legend=F,title=NULL,minzoom=7,lineopacity=.08,j=NULL,outlinercx=NULL,verbose=T,...) {
  #unit <- ''# £/m<sup>2</sup>'
  if(!is.null(j)) {
    x@data <- data.frame(data.table(x@data)[,eval(parse(text=paste0('de:=',j)))])
  }
  x1 <- leaflet(x,width=width,height=height,options=leafletOptions(minZoom=minzoom,...))
  #browser()
  #tweak probs for insufficient breaks https://github.com/rstudio/leaflet/issues/94
  #previously probs=c(0,.1,.2,.4,.6,.8,.9,.95,1) was an argument
  #now calc probs from quantilNum
  newObsVal <- x@data$de
  probs <- seq(0, 1, length.out = quantileNum + 1)
  bins <- quantile(newObsVal, probs, na.rm = TRUE, names = FALSE)
  while (length(unique(bins)) != length(bins)) {
    quantileNum <- quantileNum - 1
    probs <- seq(0, 1, length.out = quantileNum + 1)
    bins <- quantile(newObsVal, probs, na.rm = TRUE, names = FALSE)
  }
  x0 <- x@data$de
  if(length(probs)<4) { #generate pseudodata along the range of x0, for colorQuantile
    eps <- (max(x0)-min(x0))/3
    x3 <- seq(from=min(x0),to=max(x0)+eps,length.out=100)
    cb <- colorQuantile(palette=palette, domain = x3, probs=seq(from=0,to=1,length.out=8))
    #browser()
  } else {
    cb <- colorQuantile(palette=palette, domain = x@data$de, probs=probs)
  }

  labels <- sprintf(
    paste0("<strong>%s</strong><br/>%g",unit),
    x@data[,'pc'], signif(x@data$de,sig)
  ) %>% lapply(htmltools::HTML)
  if(poly) {
    x1 <- x1 %>% addPolygons(
      fillColor = ~cb(de),
      smoothFactor = 0, #this removes slivers of white between polygons https://gis.stackexchange.com/questions/176112/remove-sliver-gaps-between-polygons-with-r
      weight = 1,
      opacity = 0,
      color = "white",
      dashArray = "3",
      fillOpacity = 0.4,
      highlight = highlightOptions(
        weight = 5,
        color = "#666",
        dashArray = "",
        fillOpacity = 0.7,
        bringToFront = TRUE),
      label = labels,
      labelOptions = labelOptions(
        style = list("font-weight" = "normal", padding = "3px 8px"),
        textsize = "12px",
        direction = "auto")
    )
  }
  if(line) {
    x0 <- as.numeric(grepl(grepstring(outlinercx,dollar=T,caret=T),x@data$zo))
    if(!is.null(outlinercx) & 0<length(x0)) {
      w1 <- 1+2*x0
      l1 <- lineopacity*w1
      if(verbose) {
        print('outline width')
        print(w1)
      }
    } else {
      w1 <- 1
      l1 <- lineopacity
    }
    x1 <- addPolylines(x1,weight=w1,opacity=l1)
  }
  if(legend) {
    x1 <- addLegend(x1,title=title,position='topleft',pal=cb, values=range(x@data[,'de']))
  }
  x1
}

# lflt2 <- function(x,sig=3,unit='',quantileNum=9,palette="YlGnBu",width='800px',height='800px',poly=T,line=T,legend=F,title=NULL,minzoom=7,lineopacity=.08,j=NULL,...) {
#   #unit <- ''# £/m<sup>2</sup>'
#   if(!is.null(j)) {
#     x@data <- data.frame(data.table(x@data)[,eval(parse(text=paste0('de:=',j)))])
#   }
#   x1 <- leaflet(x,width=width,height=height,options=leafletOptions(minZoom=minzoom,...))
#   #browser()
#   #tweak probs for insufficient breaks https://github.com/rstudio/leaflet/issues/94
#   #previously probs=c(0,.1,.2,.4,.6,.8,.9,.95,1) was an argument
#   #now calc probs from quantilNum
#   newObsVal <- x@data$de
#   probs <- seq(0, 1, length.out = quantileNum + 1)
#   bins <- quantile(newObsVal, probs, na.rm = TRUE, names = FALSE)
#   while (length(unique(bins)) != length(bins)) {
#     quantileNum <- quantileNum - 1
#     probs <- seq(0, 1, length.out = quantileNum + 1)
#     bins <- quantile(newObsVal, probs, na.rm = TRUE, names = FALSE)
#   }
#   x0 <- x@data$de
#   if(length(probs)<4) { #generate pseudodata along the range of x0, for colorQuantile
#     eps <- (max(x0)-min(x0))/3
#     x3 <- seq(from=min(x0),to=max(x0)+eps,length.out=100)
#     cb <- colorQuantile(palette=palette, domain = x3, probs=seq(from=0,to=1,length.out=8))
#     #browser()
#   } else {
#     cb <- colorQuantile(palette=palette, domain = x@data$de, probs=probs)
#   }
#
#   labels <- sprintf(
#     paste0("<strong>%s</strong><br/>%g",unit),
#     x@data[,'pc'], signif(x@data$de,sig)
#   ) %>% lapply(htmltools::HTML)
#   if(poly) {
#     x1 <- x1 %>% addPolygons(
#       fillColor = ~cb(de),
#       smoothFactor = 0, #this removes slivers of white between polygons https://gis.stackexchange.com/questions/176112/remove-sliver-gaps-between-polygons-with-r
#       weight = 1,
#       opacity = 0,
#       color = "white",
#       dashArray = "3",
#       fillOpacity = 0.4,
#       highlight = highlightOptions(
#         weight = 5,
#         color = "#666",
#         dashArray = "",
#         fillOpacity = 0.7,
#         bringToFront = TRUE),
#       label = labels,
#       labelOptions = labelOptions(
#         style = list("font-weight" = "normal", padding = "3px 8px"),
#         textsize = "12px",
#         direction = "auto")
#     )
#   }
#   if(line) {
#     x1 <- addPolylines(x1,weight=1,opacity=lineopacity)
#   }
#   if(legend) {
#     x1 <- addLegend(x1,title=title,position='topleft',pal=cb, values=range(x@data[,'de']))
#   }
#   x1
# }


#---new version of lflt2
#' @export
jlfw <- function(x,j=NULL) {
  if(!is.null(j)) {
    x@data <- data.frame(data.table(x@data)[,eval(parse(text=paste0('de:=',j)))])
  }
  x
}

#' @export
lflfw <- function(x,width='800px',height='800px',minzoom=7,maxzoom=9,...) {
  leaflet(x,width=width,height=height,options=leafletOptions(minZoom=minzoom,maxZoom=maxzoom,...))
}

#' @export
colfw <- function(palette="YlGnBu",domain = x@data$de,...) {
  colorNumeric(palette=palette, domain = domain,...)
}

#' @export
polfw <- function(x,cb=colfw(),domain,lab) {
  x %>% addPolygons(
    fillColor = ~cb(domain),
    smoothFactor = 0, #this removes slivers of white between polygons https://gis.stackexchange.com/questions/176112/remove-sliver-gaps-between-polygons-with-r
    weight = 1,
    opacity = 0,
    color = "white",
    dashArray = "3",
    fillOpacity = 0.4,
    highlight = highlightOptions(
      weight = 5,
      color = "#666",
      dashArray = "",
      fillOpacity = 0.7,
      bringToFront = TRUE),
    label = lab,
    labelOptions = labelOptions(
      style = list("font-weight" = "normal", padding = "3px 8px"),
      textsize = "12px",
      direction = "auto")
  )
}

#' @export
lilfw <- function(x,lineopacity=.08) {
  addPolylines(x,weight=1,opacity=lineopacity)
}

#' @export
lelfw <- function(x,cb,domain,title=NULL,position='topleft') {
  leaflet::addLegend(x,title=title,position=position,pal=cb, values=range(domain))
}

#' @export
lflt3 <- function(x,critsum,j=NULL,sig=3,unit='',palette="YlGnBu",cbdomain=domain,domain = x@data$de,lab=x@data$pc,width='800px',height='800px',poly=T,line=T,legend=F,title=NULL,minzoom=7,maxzoom=16,lineopacity=.08,position='topleft',cb=colfw(palette,cbdomain)  ,...) {
  x1 <- jlfw(x,j)                           #select column
  x2 <- lflfw(x1,width,height,minzoom=minzoom,maxzoom=maxzoom,zoomControl=F)      #leaflet
               # cb <- colfw(palette,cbdomain,...)  was here and note it included ellipsis
  lab <- lalfw(domain,sig,unit,lab,critsum)         #label
  #browser()
  if(poly) x2 <- polfw(x2,cb,domain,lab)    #polygon ie fill
  if(line) x2 <- lilfw(x2,lineopacity)      #line ie outline
  if(legend) x2 <- lelfw(x2,cb,domain,title,position)   #legend
  x2
}

#critsum incorporates radius so needs to be dynamic... right now it is all reactive... how to do this reactive/step thing  right?

#' @export
lalfw <- function(domain = x@data$de,sig=3,unit='',lab=x@data[,'pc'],critsum,...) {
  sprintf(
    paste0(
      lab, critsum
    )) %>% lapply(htmltools::HTML)
}

#---

#' @export
majo <- function(ax) {
  ll <- as.list(ax)
  for(i in seq_along(ax)) {
    ll[[i]] <- getrdatv(ty=paste0('cijo',regpcode(ax[i]),'d'))
  }
  Reduce(rbind,ll)
}

#' @export
dec <- function(celid,cncd,cnced,...) {
  dec1d <- dec1(celid=celid,cncd=cncd,...) #these were gett('celid') - why??
  xx <- dec1(celid=celid,cncd=cnced,...)
  xx$ret <- dec1d$ret #total return from cncd
  xx$rer <- xx$ret-Reduce(`+`,xx[setdiff(names(xx),c('ret','rer','qua'))]) #rer is balancing item
  xx
}
# dec <- function(celid,cncd,cnced,...) {
#   dec1d <- dec1(celid=gett('celid'),cncd=cncd,...) WHY????  fixed (again) 210617
#   xx <- dec1(celid=gett('celid'),cncd=cnced,...)
#   xx$ret <- dec1d$ret #total return from cncd
#   xx$rer <- xx$ret-Reduce(`+`,xx[setdiff(names(xx),c('ret','rer','qua'))]) #rer is balancing item
#   xx
# }

#' @export
ciag <- function(cinoupd,cinoued,startyear) {
  x1 <- cinoued[as.character(startyear)<format(startdate),.(err=weighted.mean(err,weight*days,na.rm=T),fit=weighted.mean(fit,weight*days,na.rm=T),zo=max(zo)),rcunique]
  x2 <- cinoupd[,.(ppm2=weighted.mean(pv,weight,na.rm=T)/weighted.mean(median,weight,na.rm=T)),rcunique]
  x1[x2,on=c(rcunique='rcunique')][,pc:=irregpcode(rcunique)]
}

#' @export
cijo <- function(ciagd,civod) {
  x1 <- copy(civod)
  j <- c('rcunique','zo','pc','ppm2',names(ciagd)[grep('^[F,D,S,T].',names(ciagd))])
  x1@data <- data.frame(civod@data[ciagd,on=c(rcunique='rcunique'),nomatch=0][,j,with=F])#these should be the only fields needed
  #putrdatv(x1,) #why was this like this? 20180208
  x1
}

#' @export
agco <- function(yrsx=8,variablex=c('re1','re2'),cijod,frsud,cinouad) { #the first line tests that only rcunique,zo,ppm2 are needed
  cijod@data <- data.frame(data.table(cijod@data)[,.(rcunique,zo,ppm2,pc=irregpcode(rcunique),name=irregpcode(substr(zo,1,3)))])
  x0 <- setkey(frsud,zo,yrs,variable)[CJ(zo=unique(cijod@data[,'zo']),yrs=yrsx,variable=unique(frsud[,variable]))]
  x1 <- dcast(x0[,sum(value),'zo,variable'],zo~variable,value.var='V1')
  x2 <- data.table(cijod@data)[,.(rcunique,zo,name,pc,ppm2)][x1,on=c(zo='zo')]
  x3 <- cinouad[yrs==yrsx,.(rcunique,err)][x2,on=c(rcunique='rcunique')][is.na(err),err:=0]
  x4 <- x3[,eval(parse(text=paste0('aggret:=',paste0(variablex,collapse='+'))))]
  setkey(x4,rcunique)[cijod@data[,'rcunique']] #preserve order
}

#fraction/probability of property in price interval
#' @export
ppxint <- function(nn='cdfd',rcx='AL-1--',px1=400,px2=600) {
  getgd(nn)
  setkey(cdfd,zo)[rcx,pnorm(log(px2),mean=meanlog,sd=sdlog)-pnorm(log(px1),mean=meanlog,sd=sdlog)]
}


#fraction of property in category
#' @export
typen <-  function(nn=c('typed'),rcx=c('AL-1--','AL-2--'),select=list(newbuild='N')) {
  getgd(nn)
  x1 <- setkey(typed,rcode)[rcx]
  setkey(x1,cat,catvalue)[list(names(select),unlist(select)),sum(value)]/setkey(x1,cat)[names(select),sum(value)]
}

#---encode/decode counts
#encode 32 logicals in an integer
#' @export
enc1 <- function(x) {
  x1 <- unlist(x)
  stopifnot(is.logical(x1)&&length(x1)==32)
  as.integer(sum((x1[1:31])*(2^(0:30)))*ifelse(x1[32],1,-1))
}

#decode integer to 32 logicals
#' @export
dec32 <- function(x) {
  x1 <- unlist(x)
  stopifnot(is.integer(x1)&&length(x1)==1)
  c(as.logical(intToBits(abs(x1)))[1:31],0<x1)
}

#test...
# for(i in 1:1000) {
#   x1 <- sample(c(T,F),32,rep=T)
#   x2 <- enc1(x1)
#   x3 <- dec32(x2)
#   stopifnot(all(x3==x1))
# }



#33 price 'breaks' including 0 and inf -  make sure the 32nd empty because use of the sign bit is broken
#' @export
brk <- function() {  unique(c(0,seq(40,80,10),seq(100,400,20),450,seq(500,1000,100),2000,4000,10e10,1/0)) }

#encode summary of cinoupd for one type
#' @export
cicnt <- function(cinoupd,brkx=brk(),countthresh=c(0,3),typex=c('F','T','S','D')) {
  typex <- match.arg(typex)
  #getgd(nn)
  x3 <- cinoupd[rcunique==rcneigh] #remove the ringers (neighbours)
  x4 <- x3[,.(id,rcunique,type,pv,B=as.integer(cut(x3[,pv/1000],brkx)))] #B = bin(pv)
  stopifnot(all(x4[,B<33])&all(x4[,0<B]))
  for(ix in 1:32) { #assign to logical bin
    ass <- paste0("B",ix,":=(B==ix)&(type=='",typex,"')")
    x4[,eval(parse(text=ass))] #expressed as 32 logical columns
  }
  stopifnot(all(apply(x4[type==typex,-(1:5)],1,sum)==1)) #each row has a single bin column flagged true
  for(ix in 1:32) { #summarise Bin by Count by rcunique
    ass <- paste0('sum(B',ix,')')
    x0 <- setkey(setnames(x4[type==typex,eval(parse(text=ass)),rcunique],old='V1',new=paste0('C',ix)) ,rcunique)
    if(ix==1) {
      x5 <- x0
    } else {
      x5 <- x5[x0]
    }
  }
  stopifnot(all(apply(x5[,-1],1,sum)>0)) #each row has a single bin column flagged true
  stopifnot(all(x5[,C32]==0)) #the final bucket gets messed up so always choose brk to have it empty
  for(ix in seq_along(countthresh)) { #encode the counts into integer
    x6 <- setkey(cbind(x5[,1],countthresh[ix]<x5[,-1]),rcunique) #back to logical, this time count exceeding countthresh
    x7a <- setnames(x6[getkey(x6),.(cc=enc1(.SD)),.EACHI],old='cc',new=paste0('cc',ix)) #bins encoded into single column
    if(ix==1) {
      stopifnot(all(apply(x6[,-1],1,any))) #every rcunique has a value
      stopifnot(x7a[,all(0!=cc1)]) #the encoded count for threshold zero is therefore non-zero
      x7 <- copy(x7a)
    } else {
      x7 <- cbind(x7,x7a[,2,with=F])
    }
  }
  x7
}

#' @export
cicntn <- function(cinoupd,brkx=brk(),countthresh=c(0,3),typex=c('F','D','S','T')) {
  x3 <- cinoupd[,.(rcunique=sort(unique(rcunique)))]
  for(i in seq_along(typex)) {
    x1 <- cicnt(cinoupd,brkx,countthresh,typex=typex[i])
    x2 <- setnames(x1,old=c('cc1','cc2'),new=paste0(typex[i],countthresh))
    x3 <- x2[x3,nomatch=NA,on=c(rcunique='rcunique')]
  }
  x4 <- f_dowle3(x3)
  x4
}

#decodes int -> count(32bin) for one typex NOTE hardwired to breaks at 1,3
#' @export
cidec <- function(typex='D',cicntnd) {
  #getgd(nn)
  x1 <- setkey(copy(cicntnd),rcunique)
  x2 <- cbind(x1[,1],x1[getkey(x1),as.list(eval(parse(text=paste0('dec32(',typex,'0)')))),.EACHI][,-1]*1+x1[getkey(x1),as.list(eval(parse(text=paste0('dec32(',typex,'3)')))),.EACHI][,-1]*3)
  x3 <- x2[,-1]
  x4 <- as.data.frame(lapply(x3,as.raw))
  data.table()
  x2
}

#decodes int -> count(32bin), across multiple types
#' @export
cidecn <- function(typex=c('D','S','T','F'),cicntnd) {
  #getgd(nn)
  x1 <- lapply(typex,cidec,cicntnd=cicntnd)
  x2 <- lapply(x1,`[`,,j=-1)
  x3 <- data.table(Reduce(`+`,lapply(x2,data.frame)))
  x3
}

#---end encode/decode
#' @export
civo <- function(cicpxd,cicend,uproj1,uproj2,eps,nn='pxciard') {
  x0 <-cicpxd[cicend,on=c(rcunique='rcunique'),mult='first',nomatch=0][,.(rcunique,EE,NN,zo)]
  x1 <- pcpoly1(x0[!is.na(EE)],eps=eps,nn=nn) #problem with 'WC-1X-0--BQ-'
  proj4string(x1) <- CRS(uproj1)
  x2 <- sp::spTransform(x1, CRS(uproj2))
  x2
}

#modelled on deploy() - the two should be merged
#' @export
deployanest <-
  function(typed = c('frsud', 'mejosfd', 'dezojd'),
           outdir = '..\\20180125guitests\\',
           dn = c('rd', 'mejo'),
           subdir = paste0(dn, '\\')) {
    aatopselect('aappd')
    indir <- paste0(root.global, 'rd/')
    #delete output dirs
    del1 <- function(subdir, outdir) {
      #if(length(dir(paste0(outdir,subdir)))) {
      cmd <<- paste0("rd /s /q ", paste0(outdir, subdir)[1])
      shell(cmd)
      #}
    }
    sapply(dn, outdir = outdir, del1)
    stopifnot(!(
      'rd' %in% dir(outdir) | 'mejo' %in% dir(outdir) |
        'img' %in% dir(outdir)
    ))
    #make new dirs
    for (i in seq_along(dn))
      if (!dn[i] %in% dir(outdir))
        shell(paste0('mkdir ', outdir, subdir)[i])
    #copy
    for (i in which(dn != 'rd'))
      shell(paste0('cp ./', dn[i], '/*.* ', outdir, dn[i], '/'))
    #selective copy for rd
    dd <- dir(indir)
    ff <- dd[grepl(paste0(descrdatv(type = typed), collapse = '|'), dd)]
    cc <- paste0('cp ', indir, ff, ' ', paste0(outdir, 'rd/'))
    for (i in seq_along(ff)) {
      shell(cc[i])
    }
    llx <-
      c(
        'dezoint1d',
        'dezospdfd',
        'cdfd',
        'celid',
        'dezocombod',
        'dezojd',
        'frsud',
        'mejosfd',
        'scoxd',
        'dezoint2d',
        'aggbyaread'
      )
    getgd(llx)
    save(list = llx, file = paste0(outdir, 'x.RData'))
    stopifnot(all.equal(sort(dir(
      paste0(outdir, subdir[1])
    )), sort(ff)))
  }

#' @export
aggbyarea <- function(dezoint2d,dezospdfd) {
  x1a <- copy(dezoint2d)[,a:=substr(zo,1,3)][,zo:=NULL][,lapply(.SD,weighted.mean,w=PV),a]
  x1b <- copy(dezoint2d)[,a:=substr(zo,1,3)][,zo:=NULL][,lapply(.SD,sum),a]
  jsum <- match(c('PV','trans'),colnames(x1a))
  x1 <- cbind(x1a[,-jsum,with=F],x1b[,jsum,with=F])
  x2 <- dezospdfd
  x2@data <- data.frame(data.table(x2@data)[,a:=substr(rc,1,3)])
  x3 <- raster::aggregate(x2,by='a')
  x3@data <- data.frame(setkey(x1,a)[x3@data][,pc:=irregpcode(a)])
  x3
}

#' @export
revFun <- function( #data needed for backfill studies, not used yet
  nn='prppd',
  fnam2 = paste0(rdroot(), getpv('pp', 'fnam2'))
  ,
  nrows = -1 #for all
  ,
  d1='2016-06-30'
  ,
  d2='2018-01-31'

) {
  getgd(nn)
  da3 <-
    ceiling_date(seq.Date(
      from = as.Date(d1),
      to = as.Date(d2),
      by = 'month'
    ), 'month') - days(1)
  da4 <- format(da3, '%Y%m%d')
  x2 <- setkey(prppd[,.(id,pc,saon,paon,price_paid,deed_date)],pc,saon,paon,price_paid,deed_date)
  j <- colnames(read.csv(fnam2))
  i <- 1
  ll <- as.list(da4)
  for (i in seq_along(da4)) {
    fnam <- paste0(rdroot(), 'lr\\\\pp-complete-', da4[i], '.csv')
    x <-
      narep(setnames(data.table(
        read.table(
          fnam,
          stringsAsFactors = F,
          nrows = nrows,
          sep = ','
        )[, seq_along(j)]
      ), j)[, unique_id := NULL][, linked_data_uri := NULL])
    x[, deed_date := substr(deed_date, 1, 10)][, price_paid := as.numeric(price_paid)]
    x <-
      setkey(x[, postcode := abbrev(postcode, nosp = F)][, saon := abbrev(saon, nosp =
                                                                            F)][, paon := abbrev(paon, nosp = F)][, street := abbrev(street, nosp =
                                                                                                                                       F)][, locality := abbrev(locality, nosp = F)], postcode)
    x <- x[postcode != 'UNKNOWN' & postcode!='']
    setnames(x, 'postcode', 'pc')
    x1 <- x['2014-12-31' < deed_date][property_type!='O'][,.(pc,saon,paon,price_paid,deed_date)]
    setkey(x1,pc,saon,paon,price_paid,deed_date)
    x3 <- x2[x1,on=.(pc,saon,paon,price_paid,deed_date)][,filedate:=da4[i]]
    putrdatv(x3,type=paste0('rev1',substr(da4[i],1,7)))
    print(i)
  }
}


# sourcexx <- function(pkgx) {
#   x <- lapply(pkgx,function(x){source(paste0('../px',x,'/R/px',substr(x,3,4),'.R'))})
# }
#was current until 211006 but not used?
# sourcexx <- function(pkgx='01os',path='../aappdF/px') {
#   x <- lapply(pkgx,function(x){source(paste0(path,x,'/R/px',substr(x,3,4),'.R'))})
# }

#' @export
sourcexx <- function(pkgx='01os',path='../aappdF/px') {
  x <- lapply(pkgx,function(x){source(paste0(path,x,'/R/px',substr(x,3,4),'.R'))})
}

#' @export
installxx <- function(nnxx,path='../aappdF/px') {
  for(i in seq_along(nnxx)) {
    cmd <- paste0('R CMD INSTALL --no-multiarch ',path,nnxx[i])
    print(cmd)
    shell(cmd)
  }
}



#' @export
alldep <- function(stepcode=c('8'='ce'),...) {
  pp <- paste0('../px',zeroprepend(names(stepcode),2),stepcode,'/R/px',stepcode,'.R')
  source(pp)
  #source('../pkgx/R/splitstep.Rwip')
  #getgd('pcrcd') #not sure why this is here - it is now pxlrpcrcd and it's not used in depfun
  depFun(pre=paste0('px',stepcode),file=pp,...)#verb=F)
}

# alldep1 <- function(stepcode=c('08ce'),...) { #junk?
#   pp <- paste0('../px',stepcode,'/R/px',substr(stepcode,3,4),'.R')
#   source(pp)
#   lapply(as.list(stepcode))
#   depFun(pre=paste0('px',substr(stepcode,3,4)),file=pp,...)#verb=F)
# }

#pm2 - opening £/m2 pxpvppm2td pxzoicod ##was lmpm2a ##was in pxfu but not a step
#' @export
fulmpm2a <-
  function(#*#
    nn=c('pxpvppm2td','pxzoicod')
    ,#*#
    d2=format(Sys.Date()-getpv('celi', 'win')*365.25)
  ) {#*#
    getgd(nn)
    x1 <- setkey(setkey(pxpvppm2td[date<d2],rcode,date)[pxpvppm2td[,unique(rcode)],.(rcode,da=date,pm2=ppm2t),mult='first'][,.(pm2,rc=rcode)],rc)
    stopifnot(isTRUE(all.equal(setkey(x1,rc)[,rc],pxzoicod[,sort(unique(ico))])))
    x1
  }#*#

#converts modules to script stored in a table
#' @export
motab <-
  function(
    pxnnxx = nnxxd()[2]
    ,
    depd #dependency table
  ) {
    dd <- paste0('../', pxnnxx, '/R')
    x1 <- readLines(paste0(dd, '/px', substr(pxnnxx, 5, 6), '.R'))
    f1 <- function(x, pattern) { grepl(pattern, x) }
    x3 <- x1[which(!unlist(lapply(x1, f1, patt = '#\\*#|#.+@export')))]
    x4 <- data.table(irow = seq_along(x3), line = x3)[grep('Fun <-', line), fnum :=irow]
    setkey(x4[, fnum := as.numeric(as.factor(locf.local(as.matrix(fnum))))], fnum)
    x4[getkey(x4), iline := 1:.N, .EACHI]
    x5 <- x4[grep('Fun <-', line), .(step = gsub(' ', '', gsub('Fun <-?', '', line)), fnum)] #step fnum
    x6 <- x4[x5, on = c(fnum = 'fnum')]#[iline > 1] #executable: call arglist and body
    #x6[grep('Fun <-', line),line:=paste0('#----',gsub('.+<-.?$','',line),'----')]
    x6[grep('Fun <-', line),line:=paste0("print('--",gsub(" ","",gsub("<-","",line)),"')#------------------------")]
    x6[,line:=gsub(patt='match.arg',rep='aamatcharg',x=line)][]
    x6[,line:=gsub(patt='\\.\\.\\.',rep='',x=line)][,barestep:=gsub('Fun<-?','',step)]
    stepx <- setkey(copy(depd)[,depseq:=1:.N][grep(paste0(substr(pxnnxx, 1, 2), substr(pxnnxx, 5, 6)), step)],depseq)[, step]
    x7 <-  setkey(x6, barestep)[stepx,.(fnum,iline,step=barestep,line),mult = 'all'] #script in sequence
    x7
  }

#' @export
nnxxd <- function(){c('01os','02lr','03ep','04jo','05zo','06xv','07so','08ce','09pv','10g1','11fo','12hu','13g2','14fu','15ci','18pr')}

#' @export
aamatcharg <- function(x) {x[1]}

#' @export
optpri <- function(
  se1
  ,
  tipmd
  ,
  dammd
  ,
  trange = (2 ^ (2:10) * 10) #prior range
  ,
  k = 5
) {
  xx <- setkey(data.table(
    accrue(
      segd = se1,
      period = 'month',
      pdate = newperiods(
        d1 = se1[, min(startdate)],
        d2 = se1[, max(deed_date)],
        period = 'month'
      )
    ),
    keep.rownames = T
  ), rn)
  yy <-
    setkey(se1[, list(id = paste0(id, '.', deed_date), r)], id) #{id,r}
  yx <- yy[xx] #{id,r,T accruals}
  colnames(yx)[ncol(yx)] <-
    colnames(tipmd)[ncol(tipmd)] #set final column name to monthend
  yxphoney <-
    cbind(data.table(id = rep('dummy', nrow(tipmd)), r = rep(0, nrow(tipmd))), tipmd)[, -1, with = F]
  stopifnot(identical(colnames(yxphoney), colnames(yx)[-1]))
  tstr <- (nrow(yx) / 50000) * trange
  i1 <- 1 + (1:nrow(yx)) %% k
  i1[which(dammd[length(dammd) - 3] < yx[, substr(id, nchar(id) - 9, 99)])] <- 0 #zero for those ending later than Tfinal
  ll <- as.list(tstr)
  yx1 <- yx[, -1, with = F]
  for (j in seq_along(tstr)) {
    x1 <- 0
    for (i in 1:k) {
      insam <- rbind(yx1[i1 != i, ], yxphoney * tstr[j])
      outsam <- yx1[i1 == i, ]
      x1 <-
        x1 + crossprod(yx1[i1 == i, r] - predict(obj = lm(r ~ . - 1, data = insam), newdata =
                                                   outsam)) #cumulate oos sse
    }
    ll[[j]] <- data.table(tstra = tstr[j], sse = x1)[,tstrre:=tstr[j]/ mean(diag(crossprod(as.matrix(insam[,-1,with=F]))))]
  }
  rbindlist(ll)[,sol:=.I==which.min(sse.V1)][,found.min:=which(sol)!=1&which(sol)!=length(tstr)][,rc:=se1[1,rc]]
}

#' @export
ept <- function(
  x #selmd[[1]]
  ,
  dammd
  ,
  optprid
  ,
  keyx=c(month = 'month',tsc='tsc',tau='tau')
  ,...
) {
  x1 <- ept1a(se1=x,dammd=dammd,tpri=optprid[sol==T,.(rc,tstrx=round(tstra,-2),tstrre)],...)
  x2 <- ept2a(x=x1,keyx=keyx)
  x3 <- x2[x1,on=keyx][,deseas:=estimate-seasonal]
  x3[,sort(colnames(x3)),with=F]
}


#' @export
ept2a <- function(x,keyx=c(month = 'month',tsc='tsc',tau='tau')) {
  kx1 <- keyx[setdiff(names(keyx),'month')]
  x1 <- copy(x)[,month:=substr(date,6,7)]
  x2 <- x1[, .(monthmean = mean(estimate)), by=keyx][, seasonal := monthmean - mean(monthmean), by=kx1]#[,secumest:=0.*NA][,cumest:=0.*NA]
  x2
}


#' @export
ept1a <- function(
  se1=selmd[[1]]#[1:5000,] #seg rc
  ,
  tpri #tpri is {rc,tstrx,tstrre} where tstrx is unused, tstrre =tstra / mean(diag(crossprod(as.matrix(insam[,-1,with=F]))))
  ,
  dammd
  ,
  period='month'
  ,
  tstrength=100
  ,
  istr = -1:1
  ,
  istart=length(dammd)
  ,
  taux=c(1:12,18,24,36,48,60,120)-1
  ,
  keep.vcv=F
) {
  da <- as.character(dammd)
  rcx <- se1[,unique(rc)]
  stopifnot(length(rcx)==1 & 2<istart)
  xx <-
    setkey(data.table(
      accrue(
        segd = se1,
        period = period,
        pdate = newperiods(d1 = se1[, min(startdate)], d2 = se1[, max(deed_date)], period = period)
      ),
      keep.rownames = T
    ), rn)

  yy <- setkey(se1[, list(id = paste0(id, '.', deed_date), r)], id)
  yx <- yy[xx]
  colnames(yx)[ncol(yx)] <- as.character(max(dammd)) #set final column name to monthend (it is intramonth)

  iT <- istart:length(dammd)
  #iT <- length(dammd)
  ll <- as.list(rep(iT,each=length(istr)))
  for(i1 in seq_along(iT)) {

    daT <- as.character(dammd[iT[i1]])
    print(daT)
    yx1 <- yx[,1:match(daT,colnames(yx))]
    rescale <- mean(diag(crossprod(as.matrix(yx[,-1,with=F])))) #scaling applied in optprid for 'invariance'

    daysinT <- max(unlist(yx1[,ncol(yx1),with=F]))
    i2 <- which(unlist(yx1[,ncol(yx1),with=F])!=daysinT & 0<apply(yx1[,-(1:2),with=F],1,sum)) #segments ending prior to final day, daT
    #yx1[unlist(yx1[,ncol(yx1),with=F])==daysinT,] #are the ones erroneously excluded in the final period, final day

    #tipm calc
    #tipmd <- tpx(1, dammd[1:iT[i1]])[,-1] #replaced 20190309 because wrong
    tipmd <- tpx1(1, dammd[1:iT[i1]])

    #tstrength loop
    i3 <- 1
    for(i3 in seq_along(istr)) {
      print(i3)

      trialadjust <- 2^(istr[i3]) #adjustment in this loop
      tsc <- tpri[rc==rcx,tstrre] * rescale * trialadjust #scalar scaling

      print(tpri[rc==rcx])
      print(tsc)

      x1 <- cbind(data.table(id=rep('dummy',nrow(tipmd)),r=rep(0,nrow(tipmd))),tipmd* tsc) #tsc is applied to time prior rows

      stopifnot(all(colnames(x1)%in%names(yx1)))
      yx1 <- yx1[,colnames(x1),with=F]

      yx2 <- data.frame(rbind(yx1[i2,], x1 )[,-1,with=F]) #drop the id column
      print('start lm')
      rr <- lm(r ~ . - 1, data = yx2) #the only grunt work in this loop, checked it takes most of elapsed time


      print('end lm')
      x2a <- data.table(summary(rr)$coefficients[,1:2] * 365.25/12,keep.rownames=T)[,daest:=daT][,tsc:=tsc][,istr:=istr[i3]]
      x2 <- x2a[,date:=gsub('^X','',gsub('\\.','-',rn))][,rc:=rcx][,rn:=NULL]

      x3 <- setnames(copy(x2),gsub('^x','',gsub(' ','',tolower(gsub('Std.','std',names(x2)))))) #clean up
      x4 <- x3[,tau:=match(daest,da)-match(date,da)][,month:=substr(date,6,7)]
      ill <- (i1-1)*length(istr) + i3

      x5 <- x4[,.(rc,daest,date,month,tau,tsc,estimate,stderror,istr)][data.table(tau=taux),on=c(tau='tau')]

      #spreads section
      if(keep.vcv) { #optional augment x5 with residual~spread - requires: se1=first arg;yx1;rr$residuals
        se0 <- setkey(copy(se1)[,idtran:=paste0(id,'.',deed_date)],idtran)
        #browser()
        if(!all.equal(se0[,idtran],yx1[,sort(id)])) {browser()} else {print('yx1, se0 join ok')}
        stopifnot(all.equal(se0[,idtran],yx1[,sort(id)])) #strict 1:1 no unique()
        se1order <- se0[yx1[,id]] #construct transaction id and use this to join with yx
        spreadest <- flipcrit <- setNames(as.list(1:3),c('leasehold','terraced','new'))
        flipcrit[[1]] <- se1order[,.(flip=estate_type=='F',property_type)][,pola:=1][flip==T,pola:=-1] #lease vs free
        flipcrit[[2]] <- se1order[,.(flip=property_type=='T',property_type)][,pola:=-1][flip==T,pola:=1] #terraced vs other
        flipcrit[[3]] <- se1order[,.(flip=startnew=='Y',property_type)][,pola:=-1][flip==T,pola:=1] #new vs used
        for(k in seq_along(spreadest)) {
          x2 <- as.data.table(lapply(yx1[,-(1:2)],`*`,y=flipcrit[[k]][,pola])) #apply flip one col at a time
          x3 <- cbind(yx1[,1:2,with=F],x2) #put back id,r
          stopifnot(all.equal(dim(yx1),dim(x3)))
          #stopifnot(all(abs(lm(r ~ . - 1, data = rbind(yx1[i2,], x1 )[,r:=residuals(rr)][,-1,with=F])$coefficients)<1e-10)) #nice check but compute-expensive for a null result
          rr2 <- lm(r ~ . - 1, data = rbind(x3[i2,], x1 )[,r:=residuals(rr)][,-1,with=F])
          xspr <- setnames(data.table(gsub('`','',names(rr2$coefficients)),rr2$coefficients),c('date',names(flipcrit)[k]))
          spreadest[[k]] <- setkey(xspr,date)
        }
        spreads <- spreadest[[1]][spreadest[[2]]][spreadest[[3]]]
        x5 <- spreads[x5,on=c(date='date')]
      }

      #browser()
      if(keep.vcv) {
        vv <- vcov(rr)* (365.25/12)^2
        colnames(vv) <- rownames(vv) <- gsub('^X','',gsub('\\.','-',rownames(vv)))
        x6 <- setnames(data.table(vv,keep.rownames=T),old='rn',new='date')
        ll[[ill]] <- x6[x5,on=c(date='date')]
      } else {
        ll[[ill]] <- x5
      }
      #browser()
    }
  }
  #browser()
  x6 <- rbindlist(ll)
  x7 <- x6[,sort(names(x6)),with=F]
  x7
}

#' @export
epiab <- function(pi2=eptd,pi3=eptdcopy,taux=0:5) {
  epi2 <- setkey(copy(pi2)[istr==0,.(rc,date,i2=deseas)],rc,date) #2-sided
  epi3 <- setkey(copy(pi3)[istr==0,.(rc,date,tau,i3=deseas)],tau)[.(taux)] #rolling 1-sided
  dx <- sort(intersect(epi3[tau==0,date],epi3[tau==max(taux),date]))
  rcx <- epi3[,sort(unique(rc))]
  ll <- as.list(taux)
  for(j in seq_along(rcx)) {
    for(i in seq_along(taux)) {
      x6 <- epi2[epi3[.(taux[i])],on=c(date='date',rc='rc')][,.(rc,date,y=i2,x=i3)]#[,.(rc,date,y=i3,x=i2)]
      lmx <- summary(lm(y~x,data=x6[rc==rcx[j]]))
      k <- (j-1)*length(rcx)+i
      ll[[k]] <- as.data.table(c(setNames(as.list(lmx$coefficients[1:2,1:3]),c('a','b')),list(rsq=lmx$r.squared,tau=taux[i],rc=rcx[j])))
      ll[[k]][]
    }
  }
  x8 <- rbindlist(ll)
  x8
}

#apply linear debias to pi3, the one-sided estimate
# epi4 <- function(epiabd,epi3d) {
#   x1 <- setkey(epiabd,rc,tau)[setkey(epi3d,rc,tau),roll=T] #applies the max lag coeffs to the higher lags
#   #x1[,debias:=(deseas-a)/b]
#   x1[,debias:=a+b*deseas+m*]
#   x1[]
# }


#' @export
tabcre <- function(dt,fmt) {
  stopifnot(is.data.table(dt)&is.data.table(fmt)&names(fmt)==c('namex','isnumx','scalex','dpx','labx'))
  stopifnot(all.equal(fmt[,namex],names(dt)))
  return(list(dt=dt,fmt=fmt))
}

#' @export
tabfmt <- function(
  tab
  ,
  dt=tab$d
  ,
  fmt=tab$fmt
  ) {
  stopifnot(is.data.table(dt)&is.data.table(fmt)&names(fmt)==c('sc','dp','la'))
  x1 <- copy(dt)
  for(j in 1:ncol(dt)) {
    if(is.numeric(dt[[j]])) {
      x1[[j]] <- round(dt[[j]]*fmt[j,sc],fmt[j,dp])
    } else {
      x1[[j]] <- dt[[j]]
    }
  }
  lax <- fmt[,la]
  for(i in seq_along(lax)) {
    if(fmt[i,sc==1e-3]) lax[i] <- paste0(lax[i],'.k')
    if(fmt[i,sc==1e-6]) lax[i] <- paste0(lax[i],'.M')
    if(fmt[i,sc==1e-9]) lax[i] <- paste0(lax[i],'.B')
    if(fmt[i,sc==1e-12]) lax[i] <- paste0(lax[i],'.T')
  }
  fmt[,la:=lax]
  x2 <- setnames(data.table(as.data.frame(x1)),fmt[,la])
  x3a <- dt[,which(unlist(lapply(dt,is.numeric))),with=F]
  x3 <- rbind(
    as.data.table(lapply(x3a,mean))[,stat:='mean'],
    as.data.table(lapply(x3a,min))[,stat:='min'],
    as.data.table(lapply(x3a,quantile,prob=.05))[,stat:='q05'],
    as.data.table(lapply(x3a,quantile,prob=.95))[,stat:='q95'],
    as.data.table(lapply(x3a,max))[,stat:='max']
  )
  tab <- list(dt=dt,fmt=fmt,dtf=x2,summary=x3)
}

#' @export
capwords <- function(s, strict = FALSE) {
  cap <- function(s) paste(toupper(substring(s, 1, 1)),
                           {s <- substring(s, 2); if(strict) tolower(s) else s},
                           sep = "", collapse = " " )
  sapply(strsplit(s, split = " "), cap, USE.NAMES = !is.null(names(s)))
}


#--------------------------THETA
#' thecolFun <- function() { #moved to pxmo as it is now a step pxmolo
#'   thecold <<- thecol(unit=.1)
#'   putt(thecold)
#' }
#'
#' #' @export
#' thecol <- function(unit=1) {
#'   x1 <- seq(from=270+360,to=270+unit,by=-unit)%%360
#'   x2 <- rainbow(n=length(x1))
#'   x3 <- seq(from=log(50000),to=log(500),length=length(x1))
#'   setkey(data.table(thetad=x1,cra=x2,lppm2=x3),thetad)[]
#' }



#moved to pxmo as a step 'pxmoag0d'
# pxmoag0 <- function(nn='thecold') {
#   pxmoceFun()
#   pxmoroFun()
#   pxmorfFun()
#   x1 <- data.table(ldgce(pxmorfd[[1]]),keep.rownames=T)[,thetad:=floor(atanx(loadings3,loadings2))]
#   stopifnot(all(x1[,thetad]%in%thecold[,thetad]))
#   thecold[x1,on=c(thetad='thetad')][,.(l1=loadings1,l2=loadings2,l3=loadings3,rn,thetad,cra,memb=rn)][,craf:=as.factor(cra)]
# }

#' @export
ggsc1 <- function(x,textsize=3,title='',xlim=c(-.055,.04),ylim=c(-.03,.04)) {
  ggplot(x,aes(l2,l3))+
    geom_point(aes(colour=cra),size=4)+#,size=x[,l1]*40)+
    geom_segment(aes(y=0, x=xlim[1], xend=xlim[2], yend=0))+
    geom_segment(aes(y=ylim[1], x=0, xend=0, yend=ylim[2]))+
    theme(legend.position="none") +
    scale_color_manual(values = levels(x[,craf]))+
    scale_fill_manual(values = levels(x[,craf]))+
    geom_text(size=textsize,aes(label=(rn)),nudge_y=x[,l1/50],nudge_x=x[,l1/50], hjust = "inward") + xlab('factor 2 sensitivity') + ylab('factor 3 sensitivity') +
    ggtitle(paste(title,'factor sensitivitity'))
}

#' @export
atanx <- function(y,x,degrees=T) {#old version
  x1 <- atan2(y=y,x=x)#atan2(x,y)# #wrong way around wtf!
  x2 <- x1 + (x1<0)*2*pi
  if(degrees) {x2 <- x2 * 360/(2*pi)}
  x2
  #2*pi - (-x1 + (0<x1)*2*pi)
}

#noooooo
# atanx2 <- function(x,y,polarityx=-1) { #added polarity for 'new clockwise theta' which starts at 9:00 ie upwards from -ve x axis
#   x1 <- atan2(y=y,x=x*polarityx) #
#   x2 <- x1 + (x1<0)*2*pi
#   x3 <- x2 * 360/(2*pi)
#   x3
# }


#timeseries cumperf coded with their own theta
#' @export
grpe2 <- function(
  x1=pxmogrped[nper==180&k==8&grouptype=='cluster']
  ,
  y='deseas'
) {
  #x1=grped[nper==180&k==8&grouptype=='region']
  x1[,eval(parse(text=paste0('y:=',y)))]
  x2 <- setkey(x1,grouptype,k,typifier,date)[,.(date,cumsum(y)),'grouptype,k,typifier,thetad'][,col:=as.factor(typifier)][,date:=as.Date(date)]
  x3 <- pxmolo()[x2,on=c(thetad='thetad')][,craf:=reorder(as.factor(cra),thetad)]
  ggplot(x3,aes(date,V2,col=craf))+geom_line(size=1.)+
    scale_color_manual(values = levels(x3[,craf]),labels=setkey(unique(x3[,.(cra,typifier,thetad)]),thetad)[,typifier])+
    xlab('')+ylab('cumulative log performance')+
    theme(legend.justification=c(1,0), legend.position=c(1,0), legend.title=element_blank(),
    legend.key = element_rect(colour = NA, fill = NA))

}

#prep lookback data
#' @export
grpe3 <- function(x3=pxmogrped[nper==180&k==16&grouptype=='cluster'][,.(date,typifier,deseas,thetad,cra)],per=c(1,3,12),y='deseas') {
  ll <- as.list(per)
  x3[,eval(parse(text=paste0('y:=',y)))]
  for(i in seq_along(per)) {
    ll[[i]] <- x3[x3[,rev(sort(unique(date)))[per[i]]]<=date,.(perf=sum(y),thetad=max(thetad),cra=max(cra)),typifier][,months:=per[i]][]
  }
  setkey(rbindlist(ll),months,thetad)[]
}

#barchart clustered by period
#' @export
grpe4 <-function(pxmodad,x=pxmogrped[nper==180&k==8&grouptype=='cluster'],per=c(1,3,12,36),y='deseas') {
  x1 <- grpe3(x,per,y=y)
  #x2 <- x1[,months:=as.factor(months)]
  x2 <- x1[,months:=reorder(as.factor(months),thetad)][,craf:=reorder(as.factor(cra),thetad)]
  ggplot(x2,aes(months,perf,fill=craf)) +
    geom_bar(stat='identity', width=0.7, position=position_dodge(width=.8)) +
    coord_flip() +
    ylab('performance') + xlab(paste0('months lookback from ',max(pxmodad))) +
    scale_fill_manual(name='cluster',values = levels(x2[,craf]),labels=x2[,typifier])+
    theme(legend.justification=c(1,0), legend.position=c(1,0), legend.title=element_blank(),
          legend.key = element_rect(colour = NA, fill = NA))
}

#n-point filter - not nice at the edges, it extends last full-overlap point
# fil1 <- function(x=NULL,n2=1,type=c('lowpass','highpass','filter'),pwr=1) {
#   n <- 2*n2+1
#   type <- match.arg(type)
#   ff <- sin(pi * (1:n)/(n+1))^pwr
#   ff <- ff/sum(abs(ff))
#   if(type!='filter') {
#     x1 <- stats::filter(ts(x),ff)
#     x1[1:n2] <- x1[n2+1]
#     x1[(length(x1)-n2):length(x1)] <- x1[length(x1)-n2]
#     stopifnot(!any(is.na(x1)))
#   }
#   switch(type,
#          highpass=x[!is.na(x1)]-x1[!is.na(x1)],
#          lowpass=x1[!is.na(x1)],
#          filter=ff)
# }
#this version created 20190213 but causes errors in pxlrts
#' @export
fil1 <- function(x=NULL,n2=1,type=c('lowpass','highpass','filter'),pwr=1) {
  n <- 2*n2+1
  type <- match.arg(type)
  ff <- sin(pi * (1:n)/(n+1))^pwr
  ff <- ff/sum(abs(ff))
  if(type!='filter') {
    #browser()
    x1b <- unlist(x)
    x1a <- c(rep(x1b[1],n2),x1b,rep(x1b[length(x1b)],n2)) #pad
    x1 <- stats::filter(ts(x1a),ff)[(n2+1):(length(x1a)-n2)] #unpad
    stopifnot(length(x1)==length(x1b))
    stopifnot(!any(is.na(x1)))
  }
  switch(type,
         highpass=x[!is.na(x1)]-x1[!is.na(x1)],
         lowpass=x1[!is.na(x1)],
         filter=ff)
}



#' @export
dep1 <- function(
  filename='R/step.R'
  ,
  pre='ci' #pattern for grep is px|pre|Fun and pre is stored in each row of output table
  ,
  grepx=paste0('^px',pre)
  ,
  collapsex=','
) {
  stopifnot(length(pre)==1)
  #x0 <- setdiff(ls(pattern=paste(paste0(grepx,'.*Fun$'),collapse='|'),envir=globalenv()),'cidepFun') #recognise ci...Fun in globalenv
  x0 <- ls(pattern=paste(paste0(grepx,'.*Fun$'),collapse='|'),envir=globalenv())
  if(length(x0)==0) stop(paste0('no functions found for grepstring: ',grepx,'; depFun() requires pkgs to be sourced to globalenv(), not just in package'))
  res <- paste0(gsub(x0,patt='Fun',rep='d')) #output d.t
  x1 <- lapply(x0,formals)
  x2 <- lapply(x1,`[[`,'nn')
  x3 <- lapply(x2,as.character)
  x4 <- setNames(lapply(x3,function(x){setdiff(x,'c')}),res) #global d.t names listed in formal arg nn
  x5 <- lapply(x4,paste,collapse=collapsex) #comma separated single string
  depson <- unlist(lapply(lapply(x4,`%in%`,res),sum)) #zero iff 'all dependencies are outside this set' ie from other pkg
  #depson <- unlist(lapply(lapply(x4,`%in%`,res),all)) #complement ie 'all dependencies are inside this set'
  #depson
  isat <- setkey(data.table(step=res,satisfied=(depson==0),level=0),step) #table with rows per step
  #isat <- setkey(data.table(step=res,satisfied=depson,level=0),step) #table with rows per step
  while(isat[,any(!satisfied)]) {
    levelx <- isat[,max(level)]
    depson <- unlist(lapply(lapply(x4,`%in%`,isat[satisfied==F,step]),sum)) #find direct dependencies
    isat[step%in%names(depson)[depson==0]&satisfied==F,level:=levelx+1][level==levelx+1,satisfied:=T] #fill the table
  }
  print(filename)
  x6 <- getcomments(filename)
  x7 <- data.table(fun=names(x6),comment=gsub(patt='^\\s*-\\s*\\[ \\]','',x6))#[grep('ci.+Fun$',fun)]
  x8 <- x7[data.table(fun=x0,step=gsub('d$','',res),output=res,depends=unlist(x5))[isat,on=c(output='step')],on=c(fun='fun'),nomatch=NA]
  x9 <- x8[,pre:=pre][,pp:=substr(pre,1,2)][order(level)]
  x9
}

#' @export
pxxxtd <- function(stepx='pxmoag0') {
  x1 <- x2 <- stepx
  while(0<length(x2)) {
    x2 <- setdiff(dep1d[step%in%x1][,unlist(lapply(strsplit(depends,', '),function(x){substr(x,1,nchar(x)-1)}))],x1)
    x1 <- sort(union(x1,x2))
  }
  dep1d[step%in%x1][order(iseq)]
}

#eigen on subset
#' @export
eigsub <- function(x,names=c('WC-','NW-','SE-','CR-','CO-','B--','M--','BL-','TS-'),ref='M--') {
  stopifnot(all(names%in%rownames(x))&all(names%in%colnames(x)))
  x1 <- x[names,names]
  x2 <- eigen(x1)
  x3 <- sweep(x2$vectors,STAT=sign(x2$vectors[which(ref==names),]),FUN=`*`,MAR=2)
  x4 <- t(sweep(x[,names]%*%x3,MAR=2,STAT=x2$values,FUN=`/`))
  rownames(x4) <- paste0('evec',1:nrow(x4))
  x5 <- x*0
  x5[names,] <- x4
  x6 <- list(vectors0=x5,vectors=t(x4),values=x2$values,names=names)
  x6
}

#fmp: nxk 'factor portfolio weights'
#' @export
eigfmp <- function(
  x #from eigsub()
) {
  sweep(x$vectors[x$names,],MAR=2,STAT=x$values^.5,FUN=`/`)
}

#ldg
#' @export
eigldg <- function(
  x #from eigsub()
) {
  sweep(x$vectors,MAR=2,STAT=x$values^.5,FUN=`*`)
}

#fit
#' @export
eigfit <- function(
  xt #timeseries matrix ie set of col vectors
  ,
  names=c('WC-','NW-','SE-','CR-','CO-','B--','M--','BL-','TS-') #colnames to use
  ,
  k=3 #number factors
  ,
  returnx=c('fit','gma')
) { #x=eigsub(cov(xt),names=names(xt[c(1,4,8)]))
  returnx <- match.arg(returnx)
  stopifnot(k<=length(names))
  gma <- eigfmp(eigsub(cov(xt),names))[,1:k]%*%t(eigldg(eigsub(cov(xt),names))[,1:k])
  y1 <- as.matrix(xt[,names,with=F])%*%gma
  if(returnx=='fit') {
    return(y1)
  } else {
    return(gma)
  }
}

#default color
#' @export
gg_color_hue <- function(n) {
  hues = seq(15, 375, length = n + 1)
  hcl(h = hues, l = 65, c = 100)[1:n]
}

#based on dep1
#' @export
dep0a <- function(
  pre='ci' #pattern for grep is px|pre|Fun and pre is stored in each row of output table
  # ,
  # filename='R/step.R'
  ,
  grepx=paste0('^px',pre)
  # ,
  # collapsex=','
) {
  stopifnot(length(pre)==1)
  x0 <- ls(pattern=paste(paste0(grepx,'.*Fun$'),collapse='|'),envir=globalenv())
  if(length(x0)==0) stop(paste0('no functions found for grepstring: ',grepx,'; depFun() requires pkgs to be sourced to globalenv(), not just in package'))
  res <- paste0(gsub(x0,patt='Fun',rep='d')) #output d.t
  x1 <- lapply(x0,formals)
  x2 <- lapply(x1,`[[`,'nn')
  x3 <- lapply(x2,as.character)
  x4 <- setNames(lapply(x3,function(x){setdiff(x,'c')}),res) #global d.t names listed in formal arg nn
  x4[which(unlist(lapply(lapply(x4,nchar),length))==0)] <- '' #fix for functions with no dependencies (no nn argument)
  x5 <- data.table(step=names(x4))[,.(dep=x4[[.BY[[1]]]]),by=step]
  #x5[grep('pxmof23',step)]
  x5
}


#' @export
regpcodeP <- function(x,parallelx=T) {
  x0 <- min(ncpus(),ceiling(length(x)/5000))
  sfInit(parallel=parallelx,cpu=x0)
  x1 <- unlist(sfLapplyWrap(as.list(x),FUN=regpcode))
  sfStop()
  x1
}

#' @export
addsub <- function(x) {
  #add sector district area codes
  stopifnot(is.data.table(x)&all((c('pcode','rcode')%in%names(x))))
  x1 <- setkey(rbind(
    x[nchar(rcode)==12,.(rcode=unique(substr(rcode,1,9)))][,pcode:=irregpcode(rcode)]
    ,
    x[nchar(rcode)==12,.(rcode=unique(substr(rcode,1,6)))][,pcode:=irregpcode(rcode)]
    ,
    x[nchar(rcode)==12,.(rcode=unique(substr(rcode,1,3)))][,pcode:=irregpcode(rcode)]
  ),rcode)
  if('pcclean'%in%names(x)) {
    x1[,pcclean:=pcode]
  }
  if('pcraw'%in%names(x)) {
    x1[,pcraw:=pcode]
  }
  x2 <- setkey(rbind(x1,x),rcode)
  x2
}

#' @export
regpcodeL <- function( #a 'cached' version of regpcodeP using lookup table
    x, #vector of pc
    parallelx=length(x2)>10000,
    rcpc=NULL #optional rc-pc lut
) {
  if(is.null(rcpc)) { #compute all
    x4 <- data.table(
      pc=x,
      rc=regpcodeP(x=x,parallelx=parallelx)
    )
  } else { #reuse rows in lut
    stopifnot(is.data.table(rcpc)&sort(names(rcpc))==c('pc','rc'))
    x1 <- unique(intersect(x,rcpc[,pc])) #pc in lut
    x2 <- unique(setdiff(x,x1)) #pc not in lut
    if(length(x2)==0) {
      x4 <- rcpc[data.table(pc=x1),on=c(pc='pc')]
    } else {
      x3 <- data.table(
        pc=x2,
        rc=regpcodeP(x2,par=parallelx)
      )
      x4 <- rbind(
        x3,
        rcpc[data.table(pc=x1),on=c(pc='pc')]
      )
    }
  }
  x5 <- x4[data.table(pc=x),on=c(pc='pc'),mult='first']%>% #reorder
    .[,.(pc,rc)]
  stopifnot(nrow(x5)==length(x)&&all.equal(x,x5[,pc]))
  x5[,rc]
}


#' @export
hkeymd5 <- #constructor for hashkey from date,nx,rc6; for re-use
  function(date=.BY[[1]],nx=.BY[[2]],rc6) {
    openssl::md5( 
      paste0(
        date,
        '.',
        zeroprepend(nx,2),
        '.',
        paste0(sort(rc6), collapse='')
      )
    )
  }


#' @export
cenv0 <- function(x){
  min(x)+diff(range(x))/2
}

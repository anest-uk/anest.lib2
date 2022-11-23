#' @export
parsFun <- function(ver) {
  pars <<- setkey(data.table(read.csv(paste0('./progdata/parsv',ver,'.csv'))),pname) #using same pars (v10) for v12
  putt(pars) #note that none of these pars are passed as a list where using ttf; therefore defaults apply
}


#' @export
pcrcFun <- function(
  nn=c('pcrcd','ppd')
  ,
  pcx=ppd[,pc]
) {
  getgd(nn)
  if(is.null(pcrcd)) pcrcd <<- getrdatv('pcrcd',ver=0)
  x0 <- data.table(pcx = sort(unique(pcx)))
  if(!is.null(pcrcd)) {
    x1 <- fsetdiff(x0,pcrcd[,.(pcx=pcode)])
  } else {
    x1 <- x0
  }
  if(0<nrow(x1)) {
    sfInit(parallel=(10<nrow(x1)),cpus=min(10,ceiling(nrow(x1)/100)))
    system.time(x1[,rcx:=unlist(sfLapply(pcx,regpcode))])
    sfStop()
     pcrcd <<- unique(rbind(pcrcd,setnames(x1,c('pcode','rcode'))))
    putt(pcrcd)
  }
  setkey(pcrcd,pcode)
  # - [x] datatable
  pcrcd #for backward compatibility... should get rid
}

#' @export
pcoFun <-  function(
  pcodir = paste0(rdroot(), getpv('pco', 'pcodir'))
  ,
  ff = dir(pcodir)
  ,
  ver=0
) {
  ll <- setNames(as.list(ff),ff) #vector('list', length(ff))
  pcof <- function(fnam) {
    x <-
      setnames(data.table(read.csv(
        file = paste0(pcodir, '/', fnam), header = F
      ))[, c(1, 3, 4), with = F], c('messcode', 'ee', 'nn'))
    x[, pcode := splitpcode(messcode)][, messcode := NULL]
    x[, pcode := toupper(pcode)][, rcode := regpcode(pcode), pcode][]
  }
  sfInit(parallel=T,cpu=min(ncpus(),length(ll)/5))
  ll <- sfLapplyWrap(X=ll,FUN=pcof)
  pcod <<- rbindlist(ll)
  putt(pcod)
}

#' @export
agpcoFun <- function(
  nn='pcod'
) {
  getgd(nn)
  ll <- vector('list', 4)
  ll[[1]] <- pcod[, list(x = rcode, eex = ee, nnx = nn)]
  ll[[2]] <-
    copy(pcod)[, x := substr(rcode, 1, 9)][, list(eex = round(mean(ee)), nnx =
                                                    round(mean(nn))), x]
  ll[[3]] <-
    copy(pcod)[, x := substr(rcode, 1, 6)][, list(eex = round(mean(ee)), nnx =
                                                    round(mean(nn))), x]
  ll[[4]] <-
    copy(pcod)[, x := substr(rcode, 1, 3)][, list(eex = round(mean(ee)), nnx =
                                                    round(mean(nn))), x]
  agpcod <<-
    setkey(setnames(rbindlist(ll)[, pc := irregpcode(x)], old = 'x', new =
                      'rc'), pc)
  putt(agpcod)
  #putrdatv(agpcod, ty = 'agpcod', ver = ver)
}

#' @export
ju1Fun <- function(
  epcpath="C:/Users/Giles/AppData/Local/aabb/epc/"
  ,
  fnam='/certificates.csv'
) {
  ju1d <<- read.csv(paste0(epcpath,'fields transpose edited.csv'),header=F)[,1]
  putt(ju1d)
}

#read all and extract cols
#' @export
epcrFun <- function(
  nn='ju1d'
  ,
  epcpath="C:/Users/Giles/AppData/Local/aabb/epc/"
) {
  getgd(nn)
  dd0 <- list.dirs(epcpath)
  dd1 <- lapply(dd0[-(1:3)],paste0,'/certificates.csv')
  dd2 <- rbindlist(lapply(lapply(dd1,read.csv,header=T),data.table))
  epcrd <<- dd2[,ju1d,with=F]
  putt(epcrd)
}

#' @export
e1Fun <- function(
  nn=c('epcrd','pcrcd')
) {
  getgd(nn)
  if(!'pc'%in%names(epcrd)) epcrd[,pc:=POSTCODE] #would be better just to use symbol POSTCODE
  #pcrcd <- pcrcFun(pcrcd=pcrcd,ppd=epcrd)
  stopifnot(all(epcrd[,POSTCODE]%in%pcrcd[,pcode]))
  e1d <<- pcrcd[epcrd, on = c(pcode = "pc")]
  stopifnot(e1d[,all(nchar(rcode)==12)])
  e1d[,rc9:=substr(rcode,1,9)][,rc6:=substr(rcode,1,6)][,rc3:=substr(rcode,1,3)]
  putt(e1d)
}

#' @export
ppFun <- function(fnam = paste0(rdroot(), getpv('pp', 'fnam'))
                  ,
                  fnam2 = paste0(rdroot(), getpv('pp', 'fnam2'))
                  ,
                  nrows = getpv('pp', 'nrows')
                  ,
                  ver=8
                  ,
                  frac=getpv('pp', 'frac')
                  ,
                  code='.+' #eg 'SE22'
) {
  j <- colnames(read.csv(fnam2))
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
  x <- x[postcode != 'UNKNOWN'] #this is only one record in 2009 and is not included in any further analysis
  setnames(x, 'postcode', 'pc')
  x <- x[intersect(sample(1:.N,.N*frac,rep=F),grep(code,pc))]
  ppd <<- x[,idlr:=1:.N]
  putt(ppd)
  #putrdatv(ppd, v = ver, ty = 'ppd') #baffled as to why it was like this 2017-12-13
  #ppd
}
#' @export
rdFun <-
function() {
  if(!'rd'%in%dir(rdroot())) { #message("Error: no rd directory found : need to run newrd()")
    newrd()
  }
}

#' @export
yielddlFun <-
function(ver=0) {
  tkrs <- c("SPX INDEX", "ASX INDEX")
  x <-
    lapply(
      lapply(
        tkrs,
        bdh,
        field = "EARN_YLD",
        start.date = as.Date('1990-01-01'),
        include.non.trading.days = TRUE
      ),
      data.table
    )
  for (i in seq_along(tkrs))
    x[[i]][, bti := tkrs[i]]
  tkrs1 <- c("BP0003M INDEX", "GUKGIN10 Index", "GUKG10 Index")
  x1 <-
    lapply(
      lapply(
        tkrs1,
        bdh,
        field = "PX_LAST",
        start.date = as.Date('1990-01-01'),
        include.non.trading.days = TRUE
      ),
      data.table
    )
  for (i in seq_along(tkrs1))
    x1[[i]][, bti := tkrs1[i]]
  dt1 <- setcolorder(rbindlist(c(x, x1)), c(1, 3, 2))
  xx <-
    setkey(dt1[!is.na(EARN_YLD)], bti, date)[CJ(dt1[, (unique(bti))], dt1[, sort(unique(date))]), roll = T]
  xx1 <- dcast(xx, date ~ bti, value.var = 'EARN_YLD')
  xx2 <- zm(zoo(xx1[, -c(1, 2), with = F], xx1[, date]) / 100)
  colnames(xx2) <-
    lapply(strsplit(colnames(xx2), split = " "), "[[", 1)
  yielddld <<- xx2
  #putrdatv(xx2, ver = ver, ty = 'yielddld')
  #yielddld
  putt(yielddld)
}



#' @export
rppFun <-
  function(
    nn=c('ppd','pcrcd')
  )
  {
    #---------------------rpp1
    getgd(nn)
    pcrc(pc=ppd[,pc])
    rcs1 <- rcs() #code separator
    ppd[, pc := gsub(gsub(x = pc, patt = ' +$', rep = ''),
                     patt = '^ +$',
                     rep = '')]
    x1 <- pcrcd[ppd,on=c(pcode='pc')][, idtmp := paste(rcode, saon, paon, street, sep = rcs1)]
    setnames(x1,old='pcode',new='pc') #setnames(x,old='pc',new='pcode')
    ipass <- x1[,irregpcode(rcode)==pc] #cyclical check traps just two
    x2 <- x1[ipass]
    x3 <- x2[,.(rcode,a1=paon,a2=saon,a3=street,idtmp,deed_date,rc12=rcode,rc9=substr(rcode,1,9),rc6=substr(rcode,1,6),rc3=substr(rcode,1,3))] #column subset to do steps to x4
    x3[grepl('^FLAT$|^STUDIO$|^STUDIO FLAT$|^STUDIO APARTMENT$',a1),a1:='FLATUNIT'] #a placeholder must be left for these un-numbered flats
    x3[,a0:=paste(a1,a2,a3)]
    x4 <- unique(reg1(x3))[,id:=paste0(rcode,a0)]
    x5 <- unique(x4[,.(id,idtmp,rc12,rc9,rc6,rc3)])[x2,on=c(idtmp='idtmp')][,idtmp:=NULL][, ntr := .N, id]
    x6 <- x5[x5[,irregpcode(rc12)==pc]] #added this cyclical check 2017-03 due to 1 bad postcode
    x6[, N := .N, by = rc12]
    rppd <<- x6
    putt(rppd)
  }

#' @export
ppscrFun <-
function(
    nn = c('rppd')
    ,
    xx=unique(rppd[,.(idlr,id)])
    ,
    minprice = getpv('rpp', 'minprice')
    ,
    step=c('rpp','seg')
  ) {
    getgd(nn)
    step <- match.arg(step)
    setkey(xx,id)
    if(step=='rpp') {
      getgd('rppd')
      # -rpp screens
      xx[,pc1:=T]
      idx <- rppd[(6 > nchar(pc)|pc=='UNKNOWN'),.(id)]
      xx[idx,pc1:=F,mult='all']
      # minprice < price_paid
      xx[,mp2:=T]
      xx[rppd[(minprice > price_paid),.(id)],mp2:=F]
      # nchar(regpcode)==12
      xx[,rp3:=T]
      xx[rppd[(nchar(rcode)!=12),.(id)],rp3:=F]
      # as.character(as.Date(deed_date))==deed_date
      xx[,dd4:=T] #this is just too slow!
      #xx[rppd[as.character(as.Date(deed_date))!=deed_date,.(id)],dd4:=F]
      # paon+saon+street!=""
      xx[,st5:=T]
      xx[rppd[paste0(paon,saon,street)=='',.(id)],st5:=F]
      # price<top 0.1% by area,year
      x <- rppd[,area:=substr(rcode,1,3)][,year:=substr(deed_date,1,4)]
      idx <- x[x[,quantile(price_paid,.999),'area,year'],on=c(area='area',year='year')][price_paid>V1,.(id)]
      xx[,hp6:=T]
      xx[idx,hp6:=F]
      # price>bottom 0.1%by area,year
      x <- rppd[,area:=substr(rcode,1,3)][,year:=substr(deed_date,1,4)]
      idx <- x[x[,quantile(price_paid,.001),'area,year'],on=c(area='area',year='year')][price_paid<V1,.(id)]
      xx[,lp7:=T]
      xx[idx,lp7:=F]
      # No change in: property_type
      idx <- rppd[,length(unique(property_type)),id][V1>1,.(id)]
      xx[,dp8:=T]
      xx[idx,dp8:=F]
      # No change in: estate_type
      idx <- rppd[,length(unique(estate_type)),id][V1>1,.(id)]
      xx[,de9:=T]
      xx[idx,de9:=F]
    } else {
      #  seg screens
      #11 -1 < pa
      getgd('segrawd')
      xx[,la11:=T]
      xx[segrawd[-1>pa,.(id)],la11:=F]
      #12 pa < 1
      xx[,ha12:=T]
      xx[segrawd[pa>1,.(id)],ha12:=F]
      #13 -1 < r
      xx[,lr13:=T]
      xx[segrawd[-1>r,.(id)],lr13:=F]
      #14 (r < 3) | (grepl('WC-', id)
      xx[,hr14:=T]
      xx[segrawd[3<r,.(id)],hr14:=F]
      #15 is in segrawd
      xx[,rs15:=F]
      xx[segrawd[,.(unique(id))],rs15:=T]
      #16 not of type 'O'
      xx[,pt16:=T]
      xx[segrawd[property_type=='O',.(id)],pt16:=F]
    }
    ppscrd <<- xx
    putt(ppscrd)
  }

#' @export
inczoFun <-
function(
  nn='rppd'
  ,
  nmin = 3500#getpv('seg', 'nmin') #min for s, d
  ,
  nminhard = 50#getpv('seg', 'nminhard') #min for da
  ,
  check=F
) {
  getgd(nn)
  x0 <- unique(rppd[, .(rcode,id, ntr, s = substr(rcode,1,9), d=substr(rcode,1,6))]) #unique so one per id
  x1 <- x0[1<ntr] #this is the crucial bit where look only at repeats *
  xsing <- x0[ntr<2]
  nminaug <- nminhard # *
  xs <- x1[, N := sum((ntr-1)), by = s][N >= nmin] #sum ntr-1 per id, (=nseg) 83 for nmin=3500
  xs[,level:='s']
  xd <- x1[, N := sum((ntr-1)), by = d][N >= nmin] #1236 for nmin=3500
  xd[,level:='d']
  xda <- x1[ (N<nmin) & (nminaug<=N)] #in the grey zone, don't have quite enough in the district
  xda[,level:='da']    #157 districts
  xdrop <- x1[(N<nminaug)]  #880k ids, 949 districts
  xdrop[,level:='drop']
  x2 <- rbindlist(list(xs,xd,xda,xdrop))
  x3 <<- rbind(
    x2[level=='s',.(pruneto=s,level,id,rank=1,N)],
    x2[level=='d',.(pruneto=d,level,id,rank=2,N)],
    x2[level=='da',.(pruneto=d,level,id,rank=3,N)],
    x2[level=='drop',.(pruneto=d,level,id,rank=4,N)],
    xsing[,.(pruneto=d,level='ds',id,rank=5,N=1)] #singletons are flagged thus
    #xsing[,.(pruneto=rcode,level='drop',id,rank=5,N=1)] #singletons are flagged to drop
  )
  #stopifnot(identical(rppd[,sort(unique(id))],inczod[,sort(unique(id))])) #promises a 1:1
  inczod <<- x3[,rc6:=substr(id,1,6)][,rc9:=substr(id,1,9)][,rc12:=substr(id,1,12)]
  if(check) stopifnot(isTRUE(all.equal(setkey(rppd[,.(id=unique(id))],id),setkey(inczod[,.(id=unique(id))],id))))
  putt(inczod)
}

#' @export
prppFun <-
function(
            nn = c('rppd','inczod')
            ) {
  getgd(nn)
  #incza <- setkey(inczA(),s,rank)
  setkey(inczod,id,rank) #rank key matters because mult='first' on next line
  i <- inczod[inczod[,.(unique(id))],mult='first']
  #xx <- rppd[i[level != 'drop',.(pruneto,level,id)], on = c(id = 'id'),mult='all']
  xx <- rppd[i[,.(pruneto,level,id,rank)], on = c(id = 'id'),mult='all']
  setnames(xx, old = 'rcode', new = 'ircode')
  setnames(xx, old = 'pruneto', new = 'rcode')
  xx <- xx[xx[,irregpcode(ircode)==pc]] #added this cyclical check 2017-03 due to 1 bad postcode
  xx[, N := .N, by = rcode]
  prppd <<- xx
  putt(prppd)
}

#' @export
segrawFun <-
function(nn='prppd',
                      rmin = -1,
                      rmax = 3) {
  getgd(nn)
  x <- prppd#[rcode != 'drop']
  x[, ntr := .N, id]
  xx <-
    setkey(x[1 < ntr][, year := substr(deed_date, 1, 4)][, list(price_paid, year, deed_date,rcode,rc9,rc6,rc3, property_type, estate_type, new_build, id, ntr,rank)], id, deed_date)
  xx <-
    xx[, startdate := c("", deed_date[-.N]) , by = id][
      , startprice := c(NA, price_paid[-.N]) , by = id][
        , startnew := c(NA, new_build[-.N]) , by = id][
          startdate != ''][
          deed_date > startdate, r := log(price_paid / startprice)]
  xx <- xx[,colnames(xx)!='new_build',with=F]
  xx <- xx[, pa := 365.25 * r / as.numeric(as.Date(deed_date) - as.Date(startdate))][!is.na(r)]
  xx <- xx[rank<4]
  #browser()
  xx <-
    setkey(xx[(-1 < pa) &
                  (pa < 1) &
                  (rmin < r) &
                  ((r < rmax) |
                     (grepl('WC-', id)))], id, deed_date) #not very nice, but WC has only high return segments and must reject GL- busts
  x2 <- xx[,.(id,rcode,rc9,rc6,rc3,property_type,estate_type,startnew,ntr,startdate,startprice,deed_date,price_paid,year,r,pa)]
  x2[, deed_date := as.Date(deed_date)] #do this here because used character ops a few lines back
  x2[, startdate := as.Date(startdate)]
  #xx <- xx[rcode!='drop'] #not sure this is the right place to do it but rcode is assumed valid in following steps
  segrawd <<- x2
  putt(segrawd)
}


#' @export
segFun <-
function(
  nn=c('segrawd','ppscrd')
  ,
  step=c('seg','cnc')
) {
  getgd(nn)
  step <- match.arg(step)
  il <- Reduce(`&`,ppscrd[,!grep('^id',colnames(ppscrd)),with=F]) #logical index flags those passing
  idexc <- intersect(ppscrd[!il,unique(id)],segrawd[,id])
  idinc <- intersect(ppscrd[il,unique(id)],segrawd[,id])
  xx <- setkey(segrawd,id)[idinc]
  if(step=='cnc') { #this is a dependency on a later step - not fully implemented, would require a re-solve after cnc, segscr
    getgd('segscrd')
    idinc1 <- setdiff(xx[,id],segscrd[pass==F,id])
    xx <- setkey(xx,id)[idinc1]
  }
  segd <<- xx
  putt(segd)
}

#' @export
augFun <-
function(nn=c('inczod','agpcod','segd'),nmin=3500) {
  getgd(nn)
  #ll <- as.list(inczod[level=='da',unique(pruneto)])
  #ll <- as.list(segd[rank==3,unique(rcode)]) #have removed rcode from segd as not a property of it
  ll <- as.list(inczod[rank==3,sort(unique(rc6))]) #this assumes these are present in segd
  sectors <- agpcod[nchar(rc)==9]
  sn <- segd[,.(id)][,s:=substr(id,1,9)][,.N,s][100<N] #don't add sectors below threshold
  setkey(segd,rcode) #for da, this is a district
  for(i in seq_along(ll)) {
    cenx <- ll[[i]] #da to augment
    secx <- sectors[substr(rc,1,6)!=cenx] #all sectors not in cenx
    refx <- agpcod[rc==cenx] #coords of cenx
    deficit <- nmin-segd[cenx,.N]
    x1 <- sweep(as.matrix(secx[,.(eex,nnx)]),STAT=as.matrix(refx[,.(eex,nnx)]),MAR=2,FUN=`-`) #centre coords
    x2 <- setkey(data.table(x1)[,rc:=secx[,rc]][,dist:=sqrt(eex^2+nnx^2)/1e3],dist)[] #
    x3 <- setkey(sn[x2,on=c(s='rc')][!is.na(N),.(s,N,dist)],dist)[,cumN:=cumsum(N)][1:which(deficit<=cumN)[1]]
    x3[,chuck:=cumN-deficit]
    ll[[i]] <- x3[,.(pruneto=cenx,addsector=s,N,chuck,dist)]
  }
  augd <<- rbindlist(ll)
  putt(augd)
}

#' @export
ju1Fun <-
function(
  epcpath="C:/Users/Giles/AppData/Local/aabb/epc/"
  ,
  fnam='/certificates.csv'
) {
  ju1d <<- read.csv(paste0(epcpath,'fields transpose edited.csv'),header=F)[,1]
  putt(ju1d)
}

#' @export
e2Fun <-
function(nn='e1d',d0='2007-12-31',rcx='.') {
  #---
  #x1=NULL;for(i in seq_along(nn)) if(!exists(nn,envir=globalenv())) x1[length(x1)+1]=nn[i];x1
  getgd(nn)
  #---
  e2d <<- e1d[INSPECTION_DATE>d0&grepl(rcx,rcode,perl=T),.(brn=BUILDING_REFERENCE_NUMBER,a1=toupper(ADDRESS1),a2=toupper(ADDRESS2),a3=toupper(ADDRESS3),a4=toupper(ADDRESS),rc12=rcode,rc9,rc6,rc3)]
  e2d[grepl('^FLAT$|^STUDIO$|^STUDIO FLAT$|^STUDIO APARTMENT$',a1),a1:='FLATUNIT'] #a placeholder must be left for these un-numbered flats
  e2d[,a0:=paste(a1,a2,a3)]
  putt(e2d)
}

#' @export
e3Fun <-
function(nn='e2d') {
  getgd(nn)
  e3d <<- unique(reg1(e2d))
  putt(e3d)
}

#' @export
streetaFun <-
function(nn='prppd',nppd=min(1E7,nrow(prppd)*.5)) {
  getgd(nn)
  tkn <- unlist(lapply(lapply(lapply(lapply(lapply(prppd[sample(nrow(prppd),nppd),street],strsplit,' '),unlist),'[',-1),rev),'[',1))
  streetad <<- table(tkn)
  putt(streetad)
}

#' @export
streetFun <-
function(nn='streetad',minfreq=min(1E4,sort(streetad,decreasing=T)[min(20,length(streetad)/2)])) {
  getgd(nn)
  excl <- c("BANK","COTTAGES","CROFT","EAST","END","GATE","GREEN","MEAD","MEADOW","MEADOWS","MOUNT","NORTH","PARADE","PARK","ROW","SOUTH","VIEW","VILLAS","WEST",'COURT','GARDEN','TERRACE','WALK','GARDENS','HILL','VALE','ESTATE')
  streetd <<- setdiff(names(streetad[streetad>minfreq]),excl)
  putt(streetd)
}

#' @export
e4aFun <-
function(nn=c('e3d','streetd')) {
  getgd(nn)
  tkn <- paste0('-',streetd,'-')
  ll <- setNames(as.list(streetd),streetd)
  for(i in seq_along(ll)) {
    ll[[i]] <- e3d[grepl(tkn[i],a0)] #'street' is in a0 .28M avenue, 1.6M street
  }
  e4ad <<- ll#rbindlist(ll)
  putt(e4ad)
}

#' @export
e4aaFun <-
function(nn='e4ad'){
  getgd(nn)
  e4aad <<- rbindlist(e4ad)
  putt(e4aad)
}

#' @export
e4bFun <-
function(nn=c('e3d','e4ad','streetd'),minfreq=3) {
  getgd(nn)
  x3 <- x2 <- e4ad[unlist(lapply(e4ad,nrow)>0)]
  tkn1 <- names(x2) #'street'
  for(i in seq_along(tkn1)) {
    x0 <- strsplit(x2[[tkn1[i]]][,a0],paste0('-',tkn1[[i]],'-'))
    poststreetv <- unlist(lapply( x0  ,'[[',2))
    tt <- table(poststreetv)
    x3[[i]] <- setkey(data.table(token=tkn1[i],terminates=names(tt),freq=as.numeric(tt)),freq)
  }
  e4bd <<- setkey(setkey(rbindlist(x3),terminates)[is.na(suppressWarnings(as.numeric(substr(terminates,1,1))))][!names(x2)][freq>=minfreq],freq)
  putt(e4bd)
}

#' @export
e4cFun <-
function(nn='e4bd') {
  getgd(nn)
  tkn <- e4bd[,unique(token)]
  x0 <- copy(e4bd)
  for(i in seq_along(tkn))  {
    x0 <- x0[!( grepl(paste0('^',tkn[i],'-'),terminates) | grepl(paste0('-',tkn[i],'$'),terminates)) ]
  }
  e4cd <<- setkey(x0[,.(freq=sum(freq),ntoken=.N),terminates],freq)
  putt(e4cd)
}

#' @export
e4dFun <-
function(nn=c('e4cd'),freqmin=8,ntokenmin=2,pass=c('EAST','WEST','NORTH','SOUTH')) {
  getgd(nn)
  e4dd <<- setkey(e4cd[freqmin<freq&ntokenmin<=ntoken],terminates)[!pass]
  setkey(e4dd,freq,ntoken)
  putt(e4dd)
}

#' @export
e5Fun <-
function(nn=c('e3d','e4dd'),ncpu=8,kfold=1000,maxtokens=nrow(e4dd)) {
  getgd(nn)
  x0 <- as.list(copy(e3d)) # in case of DT related issues
  tx <- e4dd[,terminates][1:maxtokens] #vector of terminations
  tx1 <- paste(paste0('-',tx,'$'),collapse='|') #the or condition vectorises over tx
  e5p <- function(i,pattern,rep='',kfold=5,x0=x0) {
    stopifnot(i%in%(1:kfold))
    stopifnot(is.list(x0)&!(any(diff(unlist(lapply(x0,length))))))
    stopifnot('a0'%in%names(x0))
    i1 <- which(((seq_along(x0[[1]])%%kfold)+1)==i)
    x1 <- lapply(x0,`[`,i1)
    x1$a0 <- stringi::stri_replace_all_regex(str=x1$a0, pattern=pattern, rep=rep) #vectorised over a0
    x1
  }
  sfStop()
  sfInit(cpus=min(ncpus(),ncpu),parallel=(1e5<nrow(e3d))) #parallel criterion un-optimised
  tt <- system.time(xr <- sfLapplyWrap(X=as.list(1:kfold),FUN=e5p,x0=as.list(x0),pattern=tx1,kfold=kfold))
  x1 <- rbindlist(xr) #convrts to dt
  x2 <- setkey(e3d,brn,a0)[!x1[,.(brn,a0)]] #ie raw data where a0 has been modifed, just in case [nb where has NOT been modified, NOT in raw!]
  e5d <<- rbind(x2[,style:='raw'],x1[,style:='delocal'])#combine raw with subs
  putt(e5d)
}

#' @export
j1Fun <-
function(nn=c('e5d','prppd')) {
  getgd(nn)
  x1 <- setkey(prppd,rcode)[rcodeA()] #rcodeA() define the reprice, other transactions are un-allocated
  j1d <<- juk(e5d)[juk(x1[,.(rc12=ircode,a0=substr(id,13,999),id)])] #setkey to rc12, a0, take unique, join
  putt(j1d) #failed join have NA e5d fields eg brn
}

#' @export
ev1Fun <-
function(nn=c('e1d','j1d')) {
  getgd(nn)
  x <- copy(e1d)
  setnames(x,old=c('BUILDING_REFERENCE_NUMBER','TOTAL_FLOOR_AREA','INSPECTION_DATE'),new=c('brn','flarea','da'))
  ev1d <<- x[j1d[!is.na(brn),.(brn)],.(brn,flarea,da),on=c(brn='brn'),mult='all'][flarea>0]
  putt(ev1d)
}

#' @export
ev2Fun <-
function(nn=c('ev1d')) {
  getgd(nn)
  setkey(ev1d,brn,da)
  x1 <- ev1d[,.(min=min(flarea),max=max(flarea),median=median(flarea),n=.N),brn]
  x2 <- ev1d[,.(gro=(flarea[.N]-flarea[1])),brn] #careful interpreting: this is over the entire epc period 2007:present
  ev2d <<- x1[x2,on=c(brn='brn')]
  putt(ev2d)
}

#' @export
ch1Fun <-
function(nn='prppd') {
  getgd(nn)
  ch1d <<- prppd[,.(estate=min(estate_type),newbuild=min(new_build),type=min(property_type)),id]
  putt(ch1d)
}

#' @export
dfinalFun <-
function(nn='segd') {
  getgd(nn)
  dfinald <<- segd[,max(deed_date)]
  putt(dfinald)
}

#' @export
seglxFun <-
function(nn=c('augd','segd','inczod')) {
  getgd(nn)
  #native s
  i1 <- inczod[rank==1,.(rc=unique(pruneto))][,include:=rc][,parent:=rc][]
  #complete d
  i2 <- inczod[rank==2,.(rc=unique(pruneto))][,include:=rc][,parent:=rc][]
  #augment da
  i3 <- inczod[rank==3,.(rc=unique(pruneto))][,include:=rc][,parent:=rc][]
  #i4 <- inczod[rank==3,.(rc=unique(pruneto))][,include:=rc][augd,on=c(include='pruneto')][,.(rc,include=addsector)][,parent:=rc][]
  i4 <- inczod[rank==3,.(rc=unique(pruneto))][,include:=rc][augd[chuck<=0],on=c(include='pruneto')][,.(rc,include=addsector)][,parent:=rc][]
  #browser()
  i4a <- inczod[rank==3,.(rc=unique(pruneto))][,include:=rc][augd[0<chuck],on=c(include='pruneto')][,.(rc,include=addsector,keep=N-chuck)][,parent:=rc][]
  seglxd <<- rbind(
              segd[i1,on=c(rc9='include')],
              segd[i2,on=c(rc6='include')],
              segd[i3,on=c(rc6='include')],
              segd[i4,on=c(rc9='include')][,rc:=rc9][],
              segd[i4a,on=c(rc9='include')][,rc:=rc9][,.SD[sample(.N,keep)][,keep:=NULL],parent]
              )
  putt(seglxd)
}

#' @export
seglFun <-
function(nn='seglxd') {
  getgd(nn)
  setkey(seglxd,parent)
  x1 <- seglxd[,sort(unique(parent))]
  x2 <- setNames(as.list(x1),x1)
  for(i in seq_along(x2)) {x2[[i]] <- seglxd[x2[[i]]]}
  segld <<- x2
  putt(segld)
}

#' @export
icoFun <-
function(nn='seglxd') {
  getgd(nn)
  icod <<- seglxd[,.(id,ico=paste0(iprefix(),parent),nco=ifelse(nchar(parent)==9,9,ifelse(substr(id,1,6)==parent,6,9)))][,code:=substr(id,1,nco)][]
  putt(icod)
}

#' @export
vinjFun <-
function(nn=c('seglxd','prppd','j1d','augd')) {
  getgd(nn)
  idx <- j1d[!is.na(brn),id]
  setkey(seglxd,id)
  setkey(prppd,id)
  vind1 <- vin(seglxd=seglxd[idx][!is.na(deed_date)],prppd=prppd[idx][!is.na(deed_date)],augd=augd)
  vind2 <- vin(seglxd=seglxd,prppd=prppd,augd=augd)
  #browser()
  vv <- vind1[vind2,on=c(rcode='rcode',y0='y0',y1='y1')][,epcfraction:=n/i.n]
  vv1 <- vv[y1!='TOTAL'][,.(rstotal=sum(i.n,na.rm=T),epctotal=sum(n,na.rm=T)),'y0,y1'][,frac:=epctotal/rstotal]
  tt <- dcast(vv1,y0~y1,value.var='frac')
  m1 <- as.matrix(tt)
  m2 <- t(m1)
  m4 <- round(m2,3)
  colnames(m4) <-as.character(as.integer(round(m4[1,],0)))
  vinjd <<- m4[-1,]
  putt(vinjd)
}

#' @export
vinFun <-
function(nn=c('seglxd','prppd','augd'),trim=.05) {
  getgd(nn)
  vind <<- setkey(vin(seglxd=seglxd,prppd=prppd,augd=augd,trim=trim),rcode)[prppd[,unique(rcode)]] #20171008 added select on rc because stray codes eg YO-90-
  putt(vind)
}

#' @export
yxpr1Fun <-
function(nn=c('segld') #segld is seg split into a list named by zone, augmented to nmin
                     ,
                     period = getpv('yxpr1', 'period')
                     ,
                     ...) {
  getgd(nn)
  xx0 <- setNames(as.list(names(segld)),names(segld))
  newp <- newperiods(d1=segd[,min(startdate)],d2=segd[,max(deed_date)],period=period,...)
  for (j in 1:length(xx0)) {
    if((j%%100)==1|j<3) print(j)
    xx <-
      setkey(data.table(accrue(
        segd = segld[[j]], period = period, pdate=newp#, ...
      ), keep.rownames = T), rn)
    yy <-
      setkey(segld[[j]][, list(id = paste0(id, '.', deed_date), r)][, unity := 1], id)
    unity <- setnames(copy(yy[, list(r)]), 'unity')[, unity := 1]
    yx <- yy[xx]
    rownames(yx) <- yx[, id]
    xx0[[j]] <- yx[, id := NULL]
  }
  yxpr1d <<- xx0
  putt(yxpr1d)
}

#' @export
setdaFun <-
function(nn='yxpr1d') {
  getgd(nn)
  j <- colnames(yxpr1d[[1]])
  j <- j[(nchar(j) == 10)]
  setdad <<- j[(as.character(as.Date(j)) == j)]
  putt(setdad)
}

#' @export
zoneFun <-
function(nn=c('agpcod','inczod')) {
  getgd(nn)
  x <- setkey(agpcod, rc)[rcodeA(nn='inczod')]
  zoned <<- setkey(x[!is.na(rc),.(rc,eex,nnx,pc)],rc)
  putt(zoned)
}

#' @export
refzoneFun <-
function(nn=c('pcod','zoned','icod','segd'),ref=getpv('celi', 'ref')) {
  getgd(nn)
  if(!ref%in%icod[,unique(ico)]) ref <- segd[,median(price_paid),rcode][which.max(V1),rcode] #icod[1,ico] #most expensive in the universe
  x <- pcod[grepl(ref,rcode),.(ee=mean(ee),nn=mean(nn))]
  refzoned <<- zoned[,dist:=sqrt((eex-x[,ee])^2+(nnx-x[,nn])^2)][which.min(dist),rc]
  putt(refzoned)
}

#' @export
distanceFun <-
function(nn='zoned'
                        ,
                        save = T
                        ,
                        sf = 3) {
  getgd(nn)
  distanced <<- distance(zoned=zoned,save=save,sf=sf)
  putt(distanced)
}

#' @export
neareFun <-
function(
  num = getpv('neare', 'num') #order changed because this is called with lapply in coho
  ,
  nn='distanced'
  ,
  maxsep = getpv('neare', 'maxsep')
  ,
  save = TRUE) {
  getgd(nn)
  xx <- neare(num=num,distanced=distanced,maxsep=maxsep)
  #xx[,dist:=rr] #added this 2017-06-04
  neared <<- xx
  putt(neared, save = save,timer=F)
}

#' @export
linkeFun <-
function(nn='neared'
                     ,
                     delim = getpv('linke', 'delim')
                     ,
                     save = T) {
  getgd(nn)
  linked <<- linke(neared,delim)
  putt(linked, save = save,timer=F)
}

#' @export
cohoFun <-
function(nn=c('zoned','neared','distanced')
                    ,
                    num = getpv('neare', 'num')
                    ,
                    nco = getpv('coho', 'nco')
                    ,
                    method=c('adaptive','zoco')
                    ,
                    numfac=4 #arbitrary zone expansion factor
) {
  getgd(nn)
  method <- match.arg(method)
  if(method=='zoco') {
    cohod <- cohoz(distanced=distanced,nco=nco)
  } else {
    centre <- zoned[,meane:=mean(eex)][,meann:=mean(nnx)][,dist:=sqrt((eex-meane)^2+(nnx-meann)^2)][,meane:=NULL][,meann:=NULL][which.min(dist),rc]
    setkey(distanced, rc, other)
    setkey(neared, rc)
    allrc <- neared[, unique(rc)]
    coho1 <- coho(target = centre,
                  neared = neared,
                  nmin = nco)[, ico := 1]
    coho2 <- list(coho1)
    done <- coho1[ilev <= 2, rc]
    todo <- setdiff(allrc, done)
    nearedlist <-
      rev(lapply(rev(as.list(
        seq(
          from = 1,
          to = numfac * num,
          by = 1
        )
      )), neare,distanced=distanced))
    while (0 < length(todo) & length(coho2) < length(allrc)) {
      i <- length(coho2) + 1
      jscope <- num
      radius <- setkey(distanced,rc,other)[list(centre, todo)]
      ir <- radius[which.min(radius[, dist])]
      #if(!is.integer(ir)) browser()
      target <-
        radius[ir, list(other, dist),on=c(rc='rc',other='other')]
      coho1 <-
        coho(target = target[, other],
             neared = nearedlist[[jscope]],
             nmin = nco)[, ico := i]
      while (nrow(coho1) < nco) {
        jscope <- jscope + 1
        if(jscope>length(nearedlist)) error('insufficient links in cohort')
        if(jscope>10) print(paste0('incremented scope: ', jscope))
        if(jscope>20) browser()
        coho1 <-
          coho(target = target[, other],
               neared = nearedlist[[jscope]],
               nmin = nco)[, ico := i]
      }
      coho2[[i]] <- coho1
      todo <- setdiff(allrc, rbindlist(coho2)[ilev <= 2, rc])
    }
    xx <- rbindlist(coho2)
  }
  cohod <<- xx
  putt(cohod)
}

#' @export
tpxputFun <-
function(nn='cohod',setdad=gett('setdad'),...) {
  getgd(nn)
  nco <- cohod[,.N,ico][,sort(unique(N))]
  xx <- setNames(as.list(nco), as.list(nco))
  for(i in seq_along(nco)) {
    xx[[i]] <- tpx(nz=nco[i],da=setdad)
  }
  tpxputd <<- xx
  putt(tpxputd)
}

#' @export
sizeFun <-
function(nn=c('cohod','linked')) {
  getgd(nn)
  setkey(cohod, rc)
  setkey(linked, rc)
  sized <<-
    setkey(linked[cohod, allow = T][, list(km = sqrt(diff(range(eex, na.rm =
                                                                  T)) * diff(range(nnx, na.rm = T)) / 1e6)), ico], ico)[]
  putt(sized)
}

#' @export
cocoFun <-
function(
    nn=c('cohod','yxpr1d')
    ,
    ii = cohod[, sort(unique(ico))]
    ,
    parallel=T #because parallel does not work, mysteriously
  ) {#20170517 don't believe any of the following is needed
    getgd(nn)
    stopifnot(0==nrow(badrd()))
    suppressWarnings(cocomkd()) #delete coco/ and create new
    sfInit(par=parallel,cpus=ceiling(ncpus())) #mem intensive
    ll <- sfLapplyWrap(as.list(ii),coco,cohod=cohod,yxpr1d=yxpr1d)
    cocod <<- rbindlist(ll)
    putt(cocod)
  }

#' @export
xvcoFun <-
function(nn=c('cocod','sized','cohod')
                    ,
                    focusrc = getpv('seg', 'ipc') #these are included in xv
                    ,
                    suspectrc = c('CF-44-','CA-14-','PE-31-','SK-17-','TN-13-')
                    ,
                    lowrrc =NULL# setkey(primerc(),rc)[unique(setkey(gett('lmscod'),rsquared)[,.(rsquared,rc)])[1:8,unique(rc)]][,rc]
                    ,
                    rr = 11:20
                    ,
                    allxv=T #cross-validate every cohort
                    ,
                    chkminilev=F #check that every zone has a cohort in which it is core (may be F for testing)
) {
  getgd(nn)
  allrc <- sort(unique(primeico(  intersect(c(regpcode(focusrc),suspectrc,lowrrc),cohod[,unique(rc)])  ))) #bad boys
  x <-
    cocod[sized,on=c(ico='ico')][, rn := rank(nseg) / .N][, rk := rank(km) / .N][, doxv := F][, is :=
                                                                                                0 * NA][, it := 0 * NA]
  idoxv <- c(setkey(x, rn)[rr, ico], setkey(x, rk)[rr, ico]) #the ico capturing a range of attributes
  x1 <- setkey(primerc(cohod), rc) #a lookup from rc to ico
  if(chkminilev) {primercchk(x1)}
  iall <- x1[allrc, .(ico, rc, minilev)] #the motley collection brought as arguments
  i <- sort(union(idoxv, iall[, ico])) #a vector of ico
  x2 <- setkey(x, ico)[list(i), doxv := T]
  stopifnot(all(i %in% x2[doxv == T, ico])) #cohorts required for xv are flagged
  xx <-
    setkey(x2[x1[minilev == 1], on = c(ico = 'ico')][, c('ico', 'rc', 'nseg', 'km', 'rn', 'rk', 'doxv', 'is', 'it'), with =
                                                       F], ico)[]
  if(allxv) { xx[,doxv:=T] }
  xvcod <<- xx
  putt(xvcod)
}

#' @export
xvt1Fun <-
function(
  nn=c('xvcod','zoned','tpxputd')
  ,
  nfold=getpv('yxxv','nfold')
  ,
  maxp=getpv('yxxv','maxp')
  ,
  cpus=min(ceiling(ncpus()*1.5),length(ii))
  ,
  parallel=F
  ,
  ii=xvcod[doxv==T,ico]
) {
  getgd(nn)
  sfInit(par=parallel,cpus=cpus)
  ll <- sfLapplyWrap(as.list(ii),xvt1,nfold=nfold,maxp=maxp,zoned=zoned,tpxputd=tpxputd)
  xvt1d <<- rbindlist(ll)
  putt(xvt1d)
}

#' @export
prime1rcFun <-
function(
  nn=c('xvt1d','cohod')
) {
  getgd(nn)
  x3b <- setkey(setkey(xvt1d,pc,ico)[setkey(cohod[ilev<=2],rc,ico)][infold==F],ico,is,it) #sum sse over folds retaining key: ico, is, it; the number of levels is quite variable 3-10 but first 3 always exist for nco=20
  x3 <- setkey(x3b[getkey(x3b),.(sse=sum(sse),mse=mean(mse),rsq=mean(rsq),coef=mean(coef)),.EACHI],ico,sse)
  x4 <- x3[x3[,.(unique(ico))],mult='first']
  prime1rcd <<- primerc(cohod)[x4,on=c(ico='ico')][,.(rc,minilev,ico,sse,mse,rsq,coef,is,it)][,iseq:=as.numeric(as.factor(ico))]
  putt(prime1rcd)
}

#' @export
ptc1Fun <-
function(
  nn=c('cohod','prime1rcd','tpxputd','setdad','zoned')
  ,
  ii=prime1rcd[,sort(unique(iseq))]
  ,
  deltatp = 0
  ,
  deltasp = 0
  ,
  startdad=getpv('ptc','startdad',def=max(gett('setdad'))) #date where PIT begins
  ,
  cpu=min(ceiling(ncpus()),length(ii))
  ,
  parallel=T
  ,
  rollptc=getpv('ptc','rollptc') #simply copies the current estimates with a series of daest, no PIT
  ,
  deltatpce=getpv('ptc','deltatpce',def=-3)
  ,
  rootx='../pkgx/coco'
) {
  getgd(nn)
  stopifnot(startdad%in%setdad)
  sfInit(parallel=parallel,cpu=cpu)
  ll <- sfLapplyWrap(as.list(ii),stc1,cohod=cohod,prime1rcd=prime1rcd,tpxputd=tpxputd,zoned=zoned,setdad=setdad,deltatp=deltatp,deltasp=deltasp,startdad=startdad,deltatpce=deltatpce,rootx=rootx)
  sfStop()
  #putt(ll) #this for debug only
  xx <- rbindlist(ll)
  xx[da==max(da),ceret:=pret] #noise reduction, final period
  if(!rollptc) xx <- cptc(ptc1d=xx,start=startdad[1])
  ptc1d <<- xx
  putt(ptc1d)
}

#' @export
cncFun <-
function(nn='ptc1d') {
  getgd(nn)
  cncd <<- cnc(ptc1d,value.var='pret')
  putt(cncd)
}

#' @export
cnceFun <-
function(nn='ptc1d') {
  getgd(nn)
  cnced <<- cnc(ptc1d,value.var='ceret')
  putt(cnced)
}

#' @export
celirawFun <-
function(nn=c('cncd','refzoned')
                       ,
                       win = getpv('celi', 'win')
                       ,
                       k = getpv('celi', 'k')
                       ,
                       center = getpv('celi', 'center')
         ,
         flip=c(loadings2=1,loadings3=-1)

         ) {
  getgd(nn)
  cncd <- rollapplyr(cncd, 4, sum, partial = T)
  rownames(cncd) <- as.character(index(cncd))
  yearends <- extractDates(date = index(cncd),
                           find = 'l',
                           per = 'y')
  winyearends <- yearends[(win + 1):length(yearends)]
  ll <-
    structure(vector('list', length(winyearends)), names = as.character(winyearends))
  for (i in (win + 1):length(yearends)) {
    ida <- (index(cncd) <= yearends[i] & index(cncd) >= yearends[i - win])
    ce <- fms2(cncd[ida], range = k, center = center)

    pola <- sign(ldgce(ce)[refzoned, 2:k])*flip
    ce$loadings[, 2:k] <-
      sweep(ce$loadings[, 2:k],
            MAR = 2,
            STAT = pola,
            FUN = `*`)
    ce$fmp[, 2:k] <- sweep(ce$fmp[, 2:k],
                           MAR = 2,
                           STAT = pola,
                           FUN = `*`)
    ce$estwin <- index(cncd)[ida]
    ce$meanret <-
      apply(
        cncd[ida],
        MAR = 2,
        FUN = weighted.mean,
        w = seq(
          from = 1,
          to = 3,
          along.with = index(cncd)[ida]
        )
      )
    ce$meanrer <-
      apply(
        face(ce, cncd)$rer[ida],
        MAR = 2,
        FUN = weighted.mean,
        w = seq(
          from = 1,
          to = 3,
          along.with = index(cncd)[ida]
        )
      )
    ll[[i - win]] <- ce
  }
  celirawd <<- ll
  putt(celirawd)
}

#' @export
ceroFun <-
function(
                    nn=c('celirawd','cncd','scoxrawd','refzoned')
                    ,
                    rcx = refzoned #getpv('celi', 'rcx1')
                    ,
                    ceroda='2014-12-31'#min(names(celirawd))
                    ,
                    rad = 30
                    ,
                    rotate=getpv('cero','rotate',default='load')  #c('load','mean','identity')
                    ,
                    tpd=data.table(k=c(2,3),tp1=c('1995-12-31','1997-06-30'),tp2=c('2009-06-30','2014-06-30'))
                    ,
                    flip=c(loadings1=1,loadings2=-1,loadings3=-1)
) {
  if(rotate=='identity') { nn <- setdiff(nn,'scoxrawd') }
  getgd(nn)
  stopifnot(rotate%in%c('load','mean','identity') )
  if(!rcx%in%colnames(cncd)) rcx <- min(colnames(cncd)[grepl(rcx,colnames(cncd))])
  if(rotate=='load') { #use pca(loadings(centred on refpc))
    xx <- rotest(ce = celirawd[[as.character(ceroda)]],
                    rcx = rcx,
                    rad = rad)
  } else if(rotate=='mean') { #use means - rotate all mean into f1
#    scoxd <- scoce(x = celirawd[[as.character(ceroda)]],ret=cncd[(index(cncd)<=as.Date(ceroda))&(index(cncd)>=as.Date(ceroda0))])
# x1 <- scoxd[index(scoxd)<=as.Date(ceroda)&(index(scoxd)>=as.Date(ceroda0))]
# xx <- rr2(x1,tpd)
    xx <- rr2(scoxrawd,tpd)
    #browser()
    xx <- sweep(xx,MAR=2,STAT=flip*c(1,-sign(skewness((scoxrawd%*%xx)[,-1]))),FUN=`*`) #skew -ve apart from 1; flip applied again for 3
  } else if(rotate=='identity') {
    xx <- diag(ncol(ldgce(celirawd[[as.character(ceroda)]])))
  }
  cerod <<- xx
  putt(cerod)
}

#' @export
celiFun <-
function(nn=c('celirawd','cerod')
                    ,
                    rotate = T
) {
  getgd(nn)
  if (rotate) {
    xx <- rotapply(celirawd = celirawd, rot = cerod)
  } else {
    xx <- celirawd
  }
  celid <<- xx
  putt(celid)
}

#' @export
scoxFun <-
function(nn=c('celid','cncd')) {
  getgd(nn)
  scoxd <<- Reduce(rbind, cex(celid, cncd, scoce))
  putt(scoxd)
}


#' @export
scoxrawFun <-
  function(nn=c('celirawd','cncd')) {
    getgd(nn)
    scoxrawd <<- Reduce(rbind, cex(celirawd, cncd, scoce))
    putt(scoxrawd)
  }

#' @export
segscrFun <-
function(
    nn=c('yxpr1d','cncd')
    ,
    ii = seq_along(yxpr1d)
    ,
    parallel=F
  ) {
  getgd(nn)
    sfInit(par=parallel,cpus=ceiling(ncpus()))
    ll <- sfLapplyWrap(as.list(ii),segscr,yxpr1d=yxpr1d,cncd=cncd)
    segscrd <<- rbindlist(ll)
    putt(segscrd)
  }

#' @export
cncadjFun <-
function(nn='cncd') {
  getgd(nn)
  x1 <- cumsum(cncd)
  x2 <-
    sweep(x1,
          STAT = as.numeric(coredata(x1[nrow(x1), ])),
          FUN = '-',
          MAR = 2)
  cncadjd <<-
    setkeyv(setnames(
      melt(data.table(coredata(x2))[, date := index(x2)], id = 'date'),
      old = c('value', 'variable'),
      new = c('adjustby', 'rcode')
    ), c('rcode', 'date'))
  putt(cncadjd)
}

#' @export
pvidFun <-
function(nn=c('cncadjd','prppd')) {
  getgd(nn)
  ilast <- prppd[, .(lastsale = max(deed_date)), id]
  pru <-
    setkey(setkey(prppd,rcode)[cncadjd[,.(unique(rcode))]][ilast, .(price_paid, lastsale, id, rcode), on = c(id = 'id', deed_date =
                                                                     'lastsale')][rcode != 'drop'], rcode, lastsale)
  stopifnot(all(pru[, unique(rcode)] %in% cncadjd[, unique(rcode)])) #this enforced on prev. line
  x4 <-
    setkey(unfactordt(copy(cncadjd))[, .(date = as.character(date), rcode =
                                           rcode, adjustby)], rcode, date)

  pvidd <<-
  unique(setkey(x4[pru, roll = T, rollends = T][, pv := price_paid * exp(-adjustby)][, .(rcode,
                  id,
                  price_paid,
                  lastsale = date,
                  pv,
                  dfinal = cncadjd[, max(date)])]
                , id)
         )

  putt(pvidd)
}

#' @export
pvrcFun <-
function(nn='pvidd') {
  getgd(nn)
  pvrcd <<- setkey(pvidd[, .(PV = sum(pv)), 'rcode,dfinal'], rcode)
  putt(pvrcd)
}

#' @export
hvrcFun <-
function(nn=c('cncadjd','pvrcd')) {
  getgd(nn)
  x1 <- pvrcd[cncadjd, on = c(rcode = 'rcode')][, HV := PV * exp(adjustby)]
  hvrcd <<- x1[, .(rcode, date, HV)]
  putt(hvrcd)
}

#' @export
ppm1Fun <-
function(nn=c('pvidd','ev2d','j1d',ch1d='ch1d'),pcl=grepstring(regpcode(c('WC','EC','W1','SW1','SW3','SW7'))),minppm2=8e3) {
  getgd(nn)
  x1 <- pvidd[,.(rcode=max(rcode),price_paid=max(price_paid),lastsale=max(lastsale),pv=max(pv),dfinal=max(dfinal)),id] #13m unique
  x2 <- ev2d[,.(brn,median,gro)] #7.3m unique brn
  x3 <- j1d[!is.na(brn),.(brn,a0,rc12,id)] #j1d[,!is.na(brn)] #7.45m
  x4 <- x1[x2[x3,on=c(brn='brn')],on=c(id='id')]
  stopifnot(all(x4[,id]%in%ch1d[,id])) #this ignores the fact that only repeats now feature in many tables
  x5 <- ch1d[x4,on=c(id='id')][!is.na(median)] #not sure how na median gets here, but .024 are
  x6 <- x5[!(grepl(pcl,id)&(pv/median)<minppm2&estate=='L')]
  ppm1d <<- x6
  putt(ppm1d)
}

#' @export
ppm2Fun <-
function(nn=c('ppm1d')) {
  getgd(nn)
  x1 <- c('type','estate','newbuild')
  x2 <- lapply(as.list(x1),c,'rcode')
  x3 <- lapply(x2,ppm2a,ppm1d=ppm1d)
  for(i in seq_along(x3)) {
    x3[[i]][,cat:=x1[i]]
    setnames(x3[[i]],old=x1[i],new='catvalue')
    x3[[i]] <- melt(x3[[i]][,n:=as.double(n)],id.var=c('rcode','cat','catvalue'))
  }
  ppm2d <<- rbindlist(x3)
  putt(ppm2d)
}

#' @export
ppmxFun <-
function(nn=c('ppm1d','zoned')) {
  getgd(nn)
  x1 <- ppm1d[!is.na(pv*median),.(price=sum(pv),m2=sum(median,na.rm=T)),rcode][,.(ppm2=price/m2),rcode][,rank:=(rank(ppm2)-1)/(.N-1)]
  miss <- as.list(setdiff(zoned[,unique(rc)],x1[,rcode]))
  for(i in seq_along(miss)) {
    area <- substr(miss[[i]],1,3)
    near <- x1[grepl(area,rcode)]
    miss[[i]] <- data.table(rcode=miss,ppm2=near[,mean(ppm2)],rank=near[,mean(rank)+min(diff(rank))/2])
  }
  ppmxd <<- setkey(rbind(x1,rbindlist(miss))[,rcode:=unlist(rcode)],rank)[,col:=gg_colour_hue(.N*1.1)[1:.N]]
  putt(ppmxd)
}

#' @export
ppm2tFun <-
function(nn=c('ppmxd','cncadjd')) {
  getgd(nn)
  rcx <- intersect(unfactordt(cncadjd)[,sort(unique(rcode))],ppmxd[,sort(unique(rcode))])
  ppm2td <<- ppmxd[cncadjd,on=c(rcode='rcode')][,ppm2t:=exp(adjustby)*ppm2]
  putt(ppm2td)
}

#' @export
rdo1Fun <-
function(
  levels=c(a='Areas',d='Districts',s='Sectors')
  ,
  path = paste0(rdroot(), getpv('rma', 'path'))
){
  x <- setNames(as.list(levels),names(levels))
  for (i in seq_along(levels)) {
    x[[i]] <- readOGR(path, layer = x[[i]], stringsAsFactors = F) #layer is filename without extension
  }
  rdo1d <<- x
  putt(rdo1d)
}

#' @export
rdo2Fun <-
function(
  nn=c('rdo1d')
  ,
  uprojargs = "+proj=longlat +datum=WGS84"
){
  getgd(nn)
  levels  <- names(rdo1d)
  x <- setNames(as.list(levels),levels)
  for (i in seq_along(levels)) {
    x[[i]] <- sp::spTransform(rdo1d[[i]], CRS(uprojargs))#; stopifnot(is(os1,"SpatialPolygonsDataFrame"))
  }
  rdo2d <<- x
  putt(rdo2d)
}

#' @export
rdo3Fun <-
function(
  nn=c('rdo2d','pcrcd')
){
  getgd(nn)
  levels  <- names(rdo2d)
  x <- setNames(as.list(levels),levels)
  for (i in seq_along(levels)) {
    x1 <- data.table(fortify(rdo2d[[i]], region = "name"), key = 'id') #the region argument is the col in @data used to generate columnn 'id'
    pcrc(pcx=x1[,id])
    x3 <- pcrcd[x1,on=c(pcode='id')]
    x[[i]] <- setkey(x3[, rc := substr(rcode, 1, i * 3)], rc)
  }
  rdo3d <<- rbindlist(x)
  putt(rdo3d)
}

#' @export
ldgxFun <-
function(nn=c('celid','pvrcd','seglxd')
                    ) {
  getgd(nn)
  setkey(pvrcd, rcode)
  dace <- names(celid)
  nasset <- length(buice(celid[[1]]))
  years <- as.numeric(format(as.Date((names(
    celid
  ))), '%Y'))
  ll <- vector('list', length(celid))
  for (i in seq_along(celid)) {
    ce <- celid[[i]]
    # beta <- (vcvce(ce)$T%*%rep(1,nasset)/nasset)/mean(vcvce(ce)$T)
    mw <- as.matrix(pvrcd[buice(ce), PV] / pvrcd[, sum(PV)])
    vt <- vcvce(ce)$T
    beta <- (vt %*% mw) / as.numeric(t(mw) %*% vt %*% mw)
    residvol <- setnames(data.table(spvce(ce)), 'residvol')
    meanret <- ce$meanret
    meanrer <- ce$meanrer
    vv <-
      seglxd[year == years[i], list(value = sum(price_paid), num = .N), parent][, mean :=
                                                                               value / num][, list(rcode=parent, round(mean / 1000))]
    ldg <-
      data.frame(
        pcode = buice(ce),
        ldgce(ce),
        sdvce(ce),
        value = setkey(vv, rcode)[buice(ce), V2],
        time = years[i],
        beta = beta,
        residvol = residvol,
        meanret = meanret,
        meanrer = meanrer
      )
    ll[[i]] <- ldg
  }
  ldgxd <<- rbindlist(ll)
  putt(ldgxd)
}

#' @export
decFun <-
function(nn=c('celid','cncd','cnced'),...) {
  getgd(nn)
  decd <<- dec(celid=celid,cncd=cncd,cnced=cnced,...)
  putt(decd)
}

#' @export
ce2Fun <-
function(
  ico=prime1rcd[,sort(unique(ico))]
  ,
  nn=c('cohod','prime1rcd','decd')
  ,
  win=getpv('celi', 'win') #defaults to same window, assumed to be quarters
  ,
  k=2 #factors
  ,
  center = getpv('celi', 'center') #defaults to same centrality
  ,
  parallel=10<length(ico)
) {
  getgd(nn)
  sfInit(par=parallel,cpus=min(ceiling(length(ico)/3),ncpus()))
  ll <- sfLapplyWrap(as.list(ico),ce2,decd=decd,cohod=cohod,win=win,k=k,center=center)
  ce2d <<- rbindlist(ll)
  putt(ce2d)
}

#' @export
dec2Fun <-
function(
  ico=prime1rcd[,sort(unique(ico))]
  ,
  nn=c('cohod','prime1rcd','decd','ce2d')
  ,
  parallel=10<length(ico)
) {
  getgd(nn)
  sfInit(par=parallel,cpus=min(ceiling(length(ico)/3),ncpus()))
  ll <- sfLapplyWrap(as.list(ico),dec2,decd=decd,cohod=cohod,ce2d=ce2d,prime1rcd=prime1rcd)
  dec2d <<- rbindlist(ll)[,comp:=paste0(comp,'r')]
  putt(dec2d)
}

#' @export
dec2xFun <-
function(
  nn=c('dec2d','prime1rcd')
) {
  getgd(nn)
  getprime <- function(x=dec2d,prime1rcd) { #really no need for this function
    setkey(x,rc,ico)[setkey(prime1rcd[,.(rc,ico)],rc,ico)]
  }
  d2 <- getprime(dec2d,prime1rcd)
  compx <- d2[,unique(comp)]
  xx <- setNames(as.list(compx),compx)
  for(i in seq_along(compx)) {
    xx[[i]] <- dttozoo(d2[comp==compx[i],.(bui=rc,date=da,x=value)])
  }
  dec2xd <<- xx
  putt(dec2xd)
}

#' @export
ddFun <-
function(nn='cncd'
                  ,
                  da1 = as.Date('2007-11-30')
                  ,
                  da2 = as.Date('2009-04-30')) {
  getgd(nn)
  i <- index(cncd)
  dd1 <- apply(cncd[i[(da1 < i) & (i <= da2)]], 2, sum)
  dd2 <- apply(cncd[i[(da1 < i)]], 2, sum)
  r1y <- apply(tail(cncd, 4), 2, sum)
  dd12 <- cbind(dd1, dd2, r1y)
  ddd <<-
    setnames(data.table(dd12, keep.rownames = T),
             old = 'rn',
             new = 'zo')[, lev := nchar(zo) / 3][]
  putt(ddd)
}

#' @export
lmscoFun <-
function(nn=c('scoxd','cncd')) {
  getgd(nn)
  rc <- colnames(cncd)
  dt <- data.table(cbind(cncd, scoxd))
  ll <- structure(as.list(rc), names = rc)
  for (i in seq_along(rc)) {
    setnames(dt, old = rc[i], new = 'y')
    ss <- summary(lm(y ~ fmp1 + fmp2 + fmp3, data = dt))
    ll[[i]] <-
      data.table(ss$coefficients[, c(1, 3, 4)], keep.rownames = T)[, rsq := ss$r.squared][, rc :=
                                                                                            rc[i]][, rn := gsub(patt = 'fmp', rep = '', x = rn)]
    dt[, y := NULL]

  }
  lmscod <<-
    setnames(rbindlist(ll),
             c('factor', 'coef', 'tstat', 'pvalue', 'rsquared', 'rc'))
  putt(lmscod)
}

#' @export
vcvdecFun <-
function(nn='celid') {
  getgd(nn)
  ce <- celid[[length(celid)]]
  x <- Reduce(rbind, lapply(vcvce(ce), diag))
  rownames(x) <- c('M', 'S', 'R', 'T')
  vcvdecd <<-
    setnames(data.table(mattotab(x)), c('comp', 'rc', 'v'))[, date := names(celid)[length(celid)]]
  putt(vcvdecd)
}

#' @export
varyxFun <-
function(nn=c('yielddld','cncd','scoxd'),
           da = index(cncd),
           cn = c('term.premium', 'inflation', "earn.yield"),
           norm = getpv('varyx', 'norm')
  ) {
    getgd(nn)
    #daily 3 series, dtlocf(x,weekday=0:6)
    x <- yielddld[index(yielddld)<max(index(scoxd))] #trim yields for the case where running it during first month of quarter
    x <- rbind(x,zoo(x[nrow(x),drop=F],max(index(scoxd)))) #frig a roll
    coredata(x)[is.na(coredata(x))] <- 0
    yields <- rollmeanr(x, fill = 0, k = 90)[da, ]
    y1 <- (yields[, 'GUKG10', drop = F] - yields[, 'BP0003M', drop = F])
    y2 <- (yields[, 'GUKG10', drop = F] - yields[, 'GUKGIN10', drop = F])
    y3 <- (yields[, 'SPX', drop = F])
    varyd <- cbind(y1, y2, y3)
    colnames(varyd) <- cn
    xx <- cbind(scoxd, varyd)
    if (norm != '') {
      xx <- zoonorm(xx, norm)
    }
    varyxd <<- xx[!apply(is.na(coredata(xx)),1,any),]
    putt(varyxd)
  }

#' @export
varfFun <-
function(nn='varyxd',p=getp(pn='p'),...) {
  getgd(nn)
  varfd <<- VAR(varyxd, p=p,...)
  putt(varfd)
}

#' @export
varpFun <-
function(nn='varfd',
           n.ahead = getpv('varp', 'n.ahead'),
           ci=getpv('varp','ci'),
           ...) {
    getgd(nn)
    varpd <<- predict(varfd, n.ahead = n.ahead,ci=ci,...)
    putt(varpd)
  }

#' @export
varcoFun <-
function(nn='varfd',
                     jtype = '*') {
  getgd(nn)
  ss <- summary(varfd)
  jfmp <- grep(jtype, rownames(ss$varresult[[1]]$coefficients))
  xx <-
    lapply(lapply(ss$varresult, '[[', 'coefficients'), round, 2)[jfmp] #coefficient summary table
  varcod <<- xx[!unlist(lapply(xx, is.null))]
  putt(varcod)
}

#' @export
vcfmtFun <-
function(nn=c('varcod','varfd')) {
  getgd(nn)
  co <- data.frame(`rownames<-`(Reduce(rbind, lapply(varcod, '[', , 1, drop = T)),NULL))
  tstat <- data.frame(`rownames<-`(Reduce(rbind, lapply(varcod, '[', , 3, drop = T)),NULL))
  x <-
    Reduce(rbind, lapply(lapply(lapply(
      varcod, '[', , 4, drop = F
    ), '<', .05), t))
  signx <-
    Reduce(rbind, lapply(lapply(lapply(
      varcod, '[', , 1, drop = F
    ), '>', .0), t))
  flagx <- signx
  flagx[] <- sapply(signx, ifelse, '+', '-')
  flagx[!x] <- ''
  sig <- data.frame(`rownames<-`(x,NULL))
  xx <- list(co, tstat, flagx)
  labl <- varlabl()
  for (i in seq_along(xx)) {
    colnames(xx[[i]]) <- gsub('term.premium','growth',colnames(xx[[i]]))#c(labl, 'constant')
    colnames(xx[[i]]) <- gsub('earn.yield','E/P',colnames(xx[[i]]))#c(labl, 'constant')
    colnames(xx[[i]]) <- gsub('fmp','f',colnames(xx[[i]]))#c(labl, 'constant')
    colnames(xx[[i]]) <- gsub('.l1','.lag1',colnames(xx[[i]]))#c(labl, 'constant')
    colnames(xx[[i]]) <- gsub('.l2','.lag2',colnames(xx[[i]]))#c(labl, 'constant')
    rownames(xx[[i]]) <- labl
    xx[[i]] <- xx[[i]][order(rownames(xx[[i]])),order(colnames(xx[[i]]))]
  }
  xx[[2]] <- round(xx[[2]], 1)
  xx[[4]] <-
    data.frame(r.squared = round(as.numeric(lapply(
      summary(varfd)[[2]], '[[', 'r.squared'
    )), 2), row.names = labl)
  vcfmtd <<- xx
  putt(vcfmtd)
}

#' @export
extdaFun <-
  function(nn=c('setdad','varyxd')
           ,
           nper = getpv('extda', 'nper')
  ) {
    getgd(nn)
    setdad <- zoo::as.Date(setdad)
    d1 = max(setdad)
    dd <- mean(diff(as.numeric(setdad)))
    if (29 < dd && dd < 32) {
      period <- 'month'
    } else  if (89 < dd && dd < 93) {
      period <- 'quarter'
    } else if (364 < dd && dd < 366) {
      period <- 'year'
    }
    x1 <- newperiods(
      d1 = d1
      ,
      d2 = d1 + (nper + 2) * dd
      ,
      period = period
      ,
      find = 'last'
      ,
      settolast = F
    )[2:(nper + 1)]
    alldad <- zoo::as.Date(sort(unique(union(setdad,x1))))
    extdad <<- zoo::as.Date(setdiff(alldad,index(varyxd)))
    putt(extdad)
  }

#' @export
scofoFun <-
  function(nn=c('varyxd','extdad','varpd')) {
    getgd(nn)
    n.ahead <- length(varpd$fcst$fmp3[, 'fcst'])
    scofod <<-
      zoo(cbind(varpd$fcst$fmp1[, 'fcst'], varpd$fcst$fmp2[, 'fcst'], varpd$fcst$fmp3[, 'fcst']),
          extdad[1:n.ahead])
    putt(scofod)
  }

#' @export
buifoFun <-
  function(nn=c('scofod','ldgxd','decd')
           ,
           rmom=getpv('buifo','rmom')
  ) {
    getgd(nn)
    buifod <<- buifoX(scofod,ldgxd,decd,rmom)
    putt(buifod)
  }

#' @export
ldgx1Fun <-
  function(j = 'loadings1'
           ,
           nn='ldgxd'
           ,
           year = ldgxd[, max(time)]) {
    getgd(nn)
    ldgx1d <<-
      ldgxd[time == max(time), list(
        loadings1 = mean(loadings1),
        loadings2 = mean(loadings2),
        loadings3 = mean(loadings3),
        sdev = mean(sdev),
        value = mean(value),
        year = mean(as.numeric(time)),
        beta = mean(beta),
        residvol = mean(residvol),
        meanret = mean(meanret),
        meanrer = mean(meanrer),
        meanrems = mean(meanret - meanrer)
      ), pcode]
    putt(ldgx1d)
  }

#' @export
vafoFun <-
  function(nn='buifod',
           nper = 4,
           comp = c('M', 'S')) {
    getgd(nn)
    bf <- Reduce(`+`, buifod[comp])
    x <- t(coredata(cumsum(bf)[nper, ]))
    vafod <<- data.table(rc = rownames(x), vafo = as.numeric(x))
    putt(vafod)
  }

#' @export
pplpFun <-
  function(nn=c('prppd','setdad')) {
    getgd(nn)
    da3m <- as.character(as.Date(prppd[,max(deed_date)])-91)
    x1 <- prppd[da3m <= deed_date] #this catches all with transactions in the last period
    #x2 <- setkey(prppd[setdad[length(setdad) - 4] <= deed_date],rcode,deed_date)
    #x3 <- x2[x2[,.(unique(rcode))],mult='last']#,on=c(rcode='rcode')]
    #x <- unique(rbind(x1,x3))
    x <- x1
    pplpd <<-x[, .(deed_date,
                   pc=pc,#ode, #2016-03-06 mod; was pc=pc #confusion here
                   rcode,
                   price_paid,
                   new_build,
                   street,
                   locality,
                   id)]
    putt(pplpd)
  }

#' @export
ppsumFun <-
  function(nn='pvidd') {
    getgd(nn)
    d0 <- pvidd[,as.character(as.Date(max(lastsale))-(365+30))] #allow 30 day grace period for non-reporting
    xx <-
      pvidd[, .(
        median = round(median(pv) / 1e3),
        p25 = round(quantile(pv, .25) / 1e3),
        p75 = round(quantile(pv, .75) / 1e3),
        trans = sum(d0<lastsale),
        turn = sum(d0<lastsale)/.N
      ), rcode][rcode != 'drop']
    setnames(xx, old = 'rcode', new = 'rc')
    ppsumd <<- xx
    putt(ppsumd)
  }
# ppsumFun <-
# function(nn='pplpd') {
#   getgd(nn)
#   xx <-
#     pplpd[, .(
#       median = round(median(price_paid) / 1e3),
#       p25 = round(quantile(price_paid, .25) / 1e3),
#       p75 = round(quantile(price_paid, .7) / 1e3),
#       trans = .N
#     ), rcode][rcode != 'drop']
#   setnames(xx, old = 'rcode', new = 'rc')
#   ppsumd <<- xx
#   putt(ppsumd)
# }

#' @export
dezocomboFun <-
  function(
    nn=c('ldgx1d','ddd','vafod','ppsumd','pvrcd','ppm2td','cyfod')
  ) {
    getgd(nn)
    setkey(ldgx1d, pcode)
    setkey(ddd, zo)
    setkey(vafod, rc)
    setkey(ppsumd, rc)
    setkey(pvrcd, rcode)
    ppm2 <- setkey(ppm2td[date==max(date),.(rcode,ppm2=round(ppm2t))],rcode)

    dezocombod <<-
      ddd[ppm2][ldgx1d][vafod][ppsumd][pvrcd][, PV := PV / 1e9][, dfinal := NULL][cyfod,on=c(zo='rn')] #added this final join 20171002

    putt(dezocombod)
  }



#' @export
dezojFun <-
  function(nn=c('dezocombod','celid','scoxd','cdfd')
           ,
           vcv = vcvce(celid[[length(celid)]])
           ,
           jrc = 'SW-3--'
           # ,
           # wgt = .1
  ) {
    getgd(nn)
    j <- dezocombod[, match(jrc, zo)]
    vt <- vcv$T[dezocombod[, zo], dezocombod[, zo]]
    te <- sqrt(diag(vt) + diag(vt)[j] - 2 * vt[, j]) #tracking error on spread portfolios
    meanretj <- dezocombod[j, meanret] #scalar, the target
    fpremium <-
      as.matrix(dezocombod[, .(loadings1, loadings2, loadings3)]) %*% as.matrix(apply(scoxd, 2, mean)) *
      4 #entire period factor premium
    vafoj <- dezocombod[j, vafo] #factor-based (VAR) forecast
    dezocombod[, irp := (meanret - meanretj) / te] #irp, info ratio of ex-post premium
    xx <- dezocombod[, irf := (vafo - vafoj) / te][j, irp := 0][j, irf := 0] #irf, info ratio of ex-ante premium
    xx[, rho := vt[, j] / sqrt(diag(vt) * diag(vt)[j])] #correlation
    xx[, fprem := meanret - meanrer]
    x1 <- cdfd[xx,on=c(zo='zo')]
    dezojd <<- x1
    putt(dezojd)
  }

#' #' @export
#' dezoFun <-
#' function(nn='dezointd',fun=mean) {
#'   getgd(nn)
#'   missingl1 <-
#'     dezointd[, setdiff(substr(zo, 1, 3), zo)] #add level 1 mean where no level 1 from pruning
#'   avg <-
#'     copy(dezointd)[, zo := substr(zo, 1, 3)][zo %in% missingl1][, lapply(.SD, fun), by = zo]
#'   if('grp'%in%names(avg)) avg[,grp:=as.numeric(grp)] #this really should go - not sure if grp is used
#'   avg[, lev := 1]
#'   dezod <<- setkey(rbind(dezointd, avg) , zo)
#'   putt(dezod)
#' }

#the following should probably just call dezof(), as is done in gui1 dezodR()
#' @export
dezoFun <-
  function(nn='dezoint2d',fun=mean) {
    getgd(nn)
    missingl1 <-
      dezoint2d[, setdiff(substr(zo, 1, 3), zo)] #add level 1 mean where no level 1 from pruning
    avg <-
      copy(dezoint2d)[, zo := substr(zo, 1, 3)][zo %in% missingl1][, lapply(.SD, fun), by = zo]
    if('grp'%in%names(avg)) avg[,grp:=as.numeric(grp)] #this really should go - not sure if grp is used
    avg[, lev := 1]
    dezod <<- setkey(rbind(dezoint2d, avg) , zo)
    putt(dezod)
  }

#' @export
zomaFun <-
  function(
    #nn=c('rmad','dezod')) { #replaced rmad with rdo3 20170919
    nn=c('rdo3d','dezod')
  ) {
    getgd(nn)
    kk <-
      unique(rdo3d[nchar(rc) == 9, list(k3 = rc,
                                        k2 = substr(rc, 1, 6),
                                        k1 = substr(rc, 1, 3))])
    allkk <- copy(kk)
    k3 <- setkey(dezod[lev == 3, list(zo)], zo)

    i1 <-
      dezod[lev == 1, unique(zo)] #level 1 codes with data, which potentially can populate children also
    setkey(kk, k1)
    kk[i1, maplevel := 1]
    kk[, datalevel := maplevel]

    i2 <- dezod[lev == 2, unique(zo)] #level 2 codes with data
    setkey(kk, k2)[i2, maplevel := 2]
    kk[i2, datalevel := maplevel] #for those with data, set datalevel 2
    setkey(kk, k1)[substr(i2, 1, 3), maplevel := 2] #for all peers of codes with data, set maplevel 2

    i3 <- dezod[lev == 3, zo] #level 3 codes with data
    setkey(kk, k3)[i3, maplevel := 3][i3, datalevel := maplevel] #for those with data, set datalevel 3
    setkey(kk, k2)[substr(i3, 1, 6), maplevel := 3] #for all peers of codes with data, set maplevel 3

    kk <- kk[!is.na(maplevel)]

    kk[, mapkey := '']
    kk[maplevel == 1, mapkey := k1]
    kk[maplevel == 2, mapkey := k2]
    kk[maplevel == 3, mapkey := k3]
    kk[datalevel == 1, datakey := k1]
    kk[datalevel == 2, datakey := k2]
    kk[datalevel == 3, datakey := k3]

    #kk[,k1:=NULL][,k2:=NULL][,k3:=NULL][,maplevel:=NULL][,datalevel:=NULL]
    kk1 <-
      unique(setkey(kk, mapkey, datakey)) #this suddenly stopped working for kk in 2016-12
    kk <- kk1[unique(kk1[, .(mapkey, datakey)]), mult = 'first']


    #every z3 has map and data set to 3?
    hasl3 <- dezod[nchar(zo) == 9, unique(zo)]
    all(hasl3 %in% kk[, datakey])
    all(hasl3 %in% kk[, mapkey])
    kk[hasl3, all(mapkey == datakey)]
    #every peer(z3) has map set to 3, and data set higher?
    i <- k3[, substr(zo, 1, 6)]
    peertol3 <- setdiff(setkey(allkk, k2)[i, k3], hasl3)
    kk[peertol3, all(maplevel == 3)]
    kk[peertol3, all(datalevel < 3)]
    #map level : {peer(z2) + parent(z3)} is closed under child(parent()) ie all their siblings are present
    l23 <- kk[1 < maplevel, unique(substr(unique(mapkey), 1, 6))]
    l23a <- setkey(allkk, k1)[unique(substr(l23, 1, 3)), unique(k2)]
    identical(l23, l23a)
    #map level : z1 is the rc not in parents of z2,z3 (llandrindod, galashiels)
    identical(kk[maplevel == 1, k1], setdiff(dezod[lev == 1, zo], unique(substr(l23, 1, 3))))
    #all kk datakey are in dezod
    length(setdiff(kk[, unique(datakey)], dezod[, unique(zo)])) == 0
    zomad <<- setkey(setkey(rdo3d, rc)[setkey(kk, mapkey)], datakey)[, k1 := NULL][, k2 := NULL][, k3 := NULL]
    putt(zomad)
  }

#' @export
jomaFun <-
  function(nn=c('zomad','dezod')) {
    getgd(nn)
    jomad <<- jomaf(dezod,zomad)
    putt(jomad)
  }

#' @export
jomaxFun <-
  function(j = 'meanret',
           nn=c('zomad','dezod','zoned'), #zoned needed by distance
           rcx = 'SE-22-',
           rad = .0003,
           window = F) {
    getgd(nn)
    dezod <- setkey(setnames(copy(dezod), j, 'de')[, deperc := de * 100], zo)
    jomad <- jomaf(dezod = dezod, zomad = zomad)
    if (window) {
      lmad <- jomad[, list(eex = mean(long),
                           nnx = mean(lat),
                           pc = '-'), rc]
      distanced <-
        setkey(distance(zoned=lmad, save = F, sf = 8), rc)[rcx, allow = T]
      jomad <- setkey(jomad, rc)[setkey(distanced, other)][dist < rad]
    }
    jomaxd <<- jomad[!is.na(datakey)]
    putt(jomaxd)
  }

#' @export
stnFun <-
  function(
    nn='varyxd'
  ) {
    getgd(nn)
    now <- varyxd[nrow(varyxd)]
    #stnd <<- which.min(apply(sweep(head(varyxd,nrow(varyxd)-4*5),STAT=now,MAR=2)^2,1,sum))
    x1 <- t(head(varyxd,nrow(varyxd)-4*5))
    x2 <- cbind(coredata(x1),t(now))
    stnd <<- which.max(cor(x2)[-ncol(x2),ncol(x2)])
    putt(stnd)
  }

#' @export
stnevoFun <-
  function(nn=c('varyxd','stnd')
           ,
           nper = 6) {
    getgd(nn)
    stnevod <<- varyxd[stnd + (0:nper)]
    putt(stnevod)
  }

#' @export
zoseFun <-
  function(nn=c('dezod','pcod')
  ) {
    getgd(nn)
    x <- setnames(copy(dezod),old='zo',new='rc')
    zosed <<- zose(x,pcod)
    putt(zosed)
  }

#' @export
ptcoseFun <-
  function(
    nn=c('ptc1d','pcod')
  ) {
    getgd(nn)
    ptcosed <<- zose(x=copy(ptc1d),pcod=copy(pcod))
    putt(ptcosed)
  }

#' @export
hullFun <-
  function(nn='ldgxd',year=ldgxd[,max(time)]) {#,year=ldgxd[,sort(unique(time))]) {
    getgd(nn)
    ll <- as.list(year)
    for(i in seq_along(year)) {
      ldgd <- ldgxd[time==year[i],grep('loadings',colnames(ldgxd)),with=F]
      xx <- as.matrix(ldgd)
      ii <- suppressMessages(sort(unique(as.numeric(surf.tri(xx,delaunayn(xx)))))) #is it a message? "PLEASE NOTE:  As of version 0.3-5, no degenerate (zero area) blah blah"
      rc <- ldgxd[time==year[i],pcode][ii]
      ll[[i]] <- data.table(year=year[i],rc)
    }
    hulld <<- rbindlist(ll)
    putt(hulld)
  }

#' @export
pclaFun <-
  function(
    nn='pcrcd'
    ,
    fnam=paste0(root.global,'pcname\\pcla.csv')
  ) {
    getgd(nn)
    x <- data.table(read.csv(fnam))
    setnames(x,old='pcode',new='pc')
    pcrc(pc=x[,unique(pc)])
    pclad <<- pcrcd[x,on=c(pcode='pc')]
    putt(pclad)
  }

#' @export
pcla1Fun <-
  function(nn='pclad',level=1:2) {
    getgd(nn)
    ll <- as.list(level)
    for(i in seq_along(level)) {
      x <- copy(pclad)[laname!=''&pcode!='']
      x[,area:=substr(rcode,1,3*level[i])]
      x1 <- x[,.(la=tail(names(sort(table(laname))),1)),area][,lev:=level[i]]
      ll[[i]] <- setkey(x1,area)
    }
    pcla1d <<- rbindlist(ll)
    putt(pcla1d)
  }

#' @export
kmFun <-
  function(nn=c('ldgxd','hulld'),k=4,level=2,yearx=ldgxd[,max(time)],nstart=1000) {
    getgd(nn)
    kmd <<- km(ldgxd=ldgxd,hulld=hulld,k=k,level=level,yearx=yearx,nstart=nstart)
    putt(kmd)
  }

#' @export
lpFun <-
  function(
    rcx=ldgxd[,sort(unique(pcode))]
    ,
    nn=c('ldgxd','celid','hulld')
    ,
    yearx=ldgxd[,max(time)]
    ,
    obj=c('minvar','maxbeta','minwt')
    ,
    check=T
  ) {
    getgd(nn)
    obj <- match.arg(obj)
    ldgd <- ldgxd[time==yearx]
    ce <- celid[[which(substr(names(celid),1,4)==as.character(yearx))]]
    vr <- vcvce(ce)$R
    stopifnot(identical(ldgd[,pcode],colnames(vr)))
    hull <- hulld[year==yearx]
    ii <- hull[,rc]
    ll <- as.list(rcx)
    for(i in seq_along(rcx)) {
      rc <- rcx[i]
      n <- nrow(hull)
      xc <- setkey(ldgd,pcode)[ii]
      if(obj=='maxbeta') {
        budglhs <- matrix(1,nrow(xc),1)
        budgrhs <- 1
        f.obj <- xc[,beta]
        dirn <- 'max'
      } else if(obj=='minwt') {
        budglhs <- NULL
        budgrhs <- NULL
        f.obj <- rep(1,n)
        dirn <- 'min'
      } else if(obj=='minvar') {
        budglhs <- matrix(1,nrow(xc),1)
        budgrhs <- 1
        f.obj <- diag(vr)[ii]
        dirn <- 'min'
      }
      f.con <- t(cbind(budglhs,as.matrix(xc[,.(loadings1,loadings2,loadings3)])))
      f.rhs <- cbind(budgrhs,as.matrix(ldgd[pcode==rc,.(loadings1,loadings2,loadings3)]))
      f.dir <- rep('==',nrow(f.con))
      ss <- lp(dir=dirn,
               objective=f.obj,
               const.mat=f.con,
               const.dir=f.dir,
               const.rhs=as.matrix(f.rhs))
      ll[[i]] <- data.table(year=rep(yearx,n),rc=rep(rc,n),rcc=xc[,pcode],rcx=ss$solution)
      pfx <- t(ss$solution)%*%as.matrix(xc[,.(loadings1,loadings2,loadings3)])
      tgtx <- ldgd[pcode==rc,.(loadings1,loadings2,loadings3)]
      if(check) {stopifnot(sum(abs(pfx-tgtx))<1e-6)}
    }
    lpd <<- setnames(rbindlist(ll)[rcx>1e-6],old='rcx',new='weight')
    putt(lpd)
  }

#' @export
grpFun <-
  function(
    nn=c('kmd','dezod','lpd','decd')
  ) {
    getgd(nn)
    ptcd <- setnames(data.table(melt(coredata(decd$ret-decd$rer))),c('da','rc','pret'))[]
    kmd <- kmd[,.(cardinal=pcode,cluster)]
    lpd <- lpd[weight!=0,.(rc,cardinal=rcc,weight)]
    x0 <- setkey(dezod[,fgroup:=as.numeric(loadings2>0)*2+as.numeric(loadings3<0)],fgroup)[data.table(group=c(0,1,2,3),rname=c('4.other parts-+','3.commuter--','1.central++','2.london+-')),on=c(fgroup='group')][]
    radialg<-x0[,.(zo,rname)]
    x1 <- setkey(lpd[kmd,on=c(cardinal='cardinal')],rc,cluster)[]
    x2 <- x1[,.(w=sum(weight)),'rc,cluster']
    x3 <- setkey(x2,rc)[getkey(x2),.(primecluster=cluster[which.max(w)]),.EACHI]
    cc <- function(x,rcx)(x[grep(paste0(rcx,'$'),rc),primecluster][1])
    cname <- data.table(cluster=sapply(c('YO-','OX-','W--','ME-'),cc,x=x3),region=c('4.other parts','3.southeast','1.central','2.london'))
    x4 <- setkey(cname[x3,on=c(cluster='primecluster')],region)[,.(cname=region,rc)]
    xx <- radialg[x4[dezod,on=c(rc='zo')],on=c(zo='rc')]
    #add in supplementary data
    xx <- xx[unfactordt(ptcd)[da>'2016-06-30'][,.(post=sum(pret)),'rc'],on=c(zo='rc')]
    imissing <- xx[is.na(cname),zo] #this is not nice but does the infill of areas, just like dezojd
    for (i in seq_along(imissing)) setkey(xx,zo)[imissing[i],cname:=xx[!is.na(cname)][grep(imissing[i],zo),cname[1]]]
    xx[is.na(cname),cname:=.SD[match(cname,substr(zo,1,3))][1,cname]]
    grpd <<- xx
    putt(grpd)
  }

#' @export
sse1Fun <-
  function(nn=c('prime1rcd','xvt1d'),ver=getv()$ver,noco=F) {
    getgd(nn)
    sse1d <<- sse1(prime1rcd,xvt1d,ver=ver,noco=noco)
    putt(sse1d)
  }

#' @export
si1Fun <-
  function(nn='ptc1d',k=3,wgt=3) {
    getgd(nn)
    si1d <<- si1f(ptc1d,k,wgt)
    putt(si1d)
  }

#' @export
hed1Fun <-
  function(catx=c('estate','type'),nn=c('ppm1d','ppmxd')) {
    getgd(nn)
    ll <- lapply(catx,hed1,ppmxd=ppmxd,ppm1d=ppm1d)
    hed1d <<- rbindlist(ll)
    putt(hed1d)
  }

#' @export
ca1Fun <-
  function(nn=c('cncd','celid','lpd')) {
    getgd(nn)
    x1 <- msrtce(celid[[length(celid)]],cncd)
    jj <- 1:ncol(cncd)
    x2 <- x1[,jj]+x1[,max(jj)+jj]
    colnames(x2)<- colnames(cncd)
    all(lpd[,unique(rcc)]%in%colnames(x2))
    j1 <- lpd[,unique(rcc)] #cardinals
    x3 <- t(apply(cncd,1,rank)) #ranks of the cardinals
    sort(apply(x3[,j1],1,max)/max(jj)) #expect this to always be 1 - it does not quite make it but close
    x4 <- sort(table(j1[apply(x3[,j1],1,which.max)]))
    #x3a <- t(apply(cncd,1,cutN,n=10))
    x5 <- 4*apply(x2,2,mean)[j1]
    x6a <- data.table(eff=x4/nrow(cncd))
    if(length(setdiff(j1,names(x4)))>0) {
      x6a <- rbind(x6a
                   ,
                   data.table(rc=setdiff(j1,names(x4)),eff=0)
                   ,
                   use.names=F
      )
    }
    x6 <<- setkey(
      setnames(x6a,c('rc','eff'))
      ,eff)[]
    ca1d <<- cbind(x6,data.table(pa=x5[x6[,rc]]))
    putt(ca1d)
  }

#' @export
ca2Fun <-
  function(nn=c('cncd','celid','lpd')) {
    getgd(nn)
    x1 <- msrtce(celid[[length(celid)]],cncd)
    jj <- 1:ncol(cncd)
    x2 <- zm(x1[,jj]+x1[,max(jj)+jj])
    colnames(x2)<- colnames(cncd)
    ca2d <<- setkey(setnames(melt(data.table(coredata(x2[,lpd[,unique(rcc)] ]),keep.rownames=T),id.vars=c('rn')),c('da','rc','rems'))[,da:=as.Date(da)],rc)
    putt(ca2d)
  }

#' @export
sty1Fun <-
  function(nn=c('hulld','lpd','ldgxd'),krange=3:(nrow(hulld)-1)) {
    getgd(nn)
    ll <- setNames(as.list(krange),krange)
    for(k in krange) {
      kmd <- km(ldgxd=ldgxd,hulld=hulld,yearx=ldgxd[,max(time)],nstart=1e4,k=k,level=3)
      #ll[[as.character(k)]] <- kmd[lpd,on=c(pcode='rcc')][,.(rc,hc=pcode,sc=stype,k=nk,cluster,weight)]
      x1 <- kmd[lpd,on=c(pcode='rcc')][,.(rc,hc=pcode,k=nk,cluster,weight)] #now assign stype
      stypes <- setkey(x1[,.(weight=sum(weight)),'cluster,hc'],cluster,weight)[list(cluster=1:k),on=(cluster='cluster'),mult='last'][,.(cluster,stype=hc)]
      x2 <- setkey(stypes[x1,on=c(cluster='cluster')],cluster,weight)
      x3 <- setkey(x2[,.(ws=sum(weight)),'k,stype'][,rclust:=rank(-ws)],ws)
      ll[[as.character(k)]] <- setkey(x3[x2,on=c(stype='stype',k='k')],k,rclust)
    }
    sty1d <<- rbindlist(ll)
    setnames(sty1d,old='stype',new='sc') #added 20170729
    sty1d[,pc:=irregpcode(rc)]
    putt(sty1d)
  }

#' @export
scofostncFun <-
  function(
    nn=c('extdad','scoxd','stnd')
    ,
    io=-icycle,icycle=(nrow(scoxd)-stnd+1)
  ) {
    getgd(nn)
    repeatda <- index(scoxd)[(stnd+1):nrow(scoxd)]
    x1 <- zm(zoo(coredata(scoxd[repeatda]),extdad[1:length(repeatda)]))
    repeatsco <- zm(cbind(rollmean(x1[,1,drop=F],k=7,fill=NA),rollmean(x1[,2:3,drop=F],k=5,fill=NA)))
    coredata(repeatsco)[is.na(repeatsco)] <- coredata(x1)[is.na(repeatsco)]
    scofostncd <<- setnames(rbind(
      data.table(melt(coredata(scoxd)))[,type:='actual']
      ,
      data.table(melt(coredata(repeatsco)))[,type:='forecast']
    ),c('da','factor','value','type'))[,cv:=cumsum(value),factor][,y:=cv]
    putt(scofostncd)
  }

#' @export
scofostnFun <-
  function(nn=c('scofostncd')) {
    getgd(nn)
    j <- c('da','f1','f2','f3','f1-cumul','f2-cumul','f3-cumul')
    scofostnd <<- unfactordt(setnames(dcast(scofostncd,da~factor,value.var='value')[dcast(scofostncd,da~factor,value.var='cv'),on=c(da='da')],j)[])
    putt(scofostnd)
  }

#' @export
buifostnFun <-
  function(nn=c('scofostnd','ldgxd','decd')) {
    getgd(nn)
    scofod <- zoo(as.matrix(scofostnd[,2:4,with=F]),scofostnd[,as.Date(da)])
    x1 <- buifoX(scofod=scofod,ldgxd=ldgxd,decd=decd,rmom=getpv('buifo','rmom'))
    x2 <- x1$M+x1$S
    buifostnd <<- x2 #in this idiom scofo is extended into a distant future
    putt(buifostnd)
  }

#' @export
adszoFun <-
  function(nn=c('pcod','dezod')) {
    getgd(nn)
    ads <- pcod[,.(s=unique(substr(rcode,1,9)))][,d:=substr(s,1,6)][,a:=substr(d,1,3)]
    x1 <- ads[dezod[nchar(zo)==9,.(zo)],.(a,d,s,zo),on=c(s='zo')]
    ads <- ads[!x1[,.(zo)],on=c(s='zo')] #remove sectors which are zones
    x2 <- ads[dezod[nchar(zo)==6,.(zo)],.(a,d,s,zo),on=c(d='zo')]
    ads <- ads[!x2[,.(zo)],on=c(d='zo')] #remove sectors in districts which are zones
    x3 <- ads[dezod[nchar(zo)==3,.(zo)],.(a,d,s,zo),on=c(a='zo')]
    adszod <<- rbind(x1,x2,x3)[!is.na(zo)]
    stopifnot(isTRUE(all.equal(dezod[,sort(unique(zo))],adszod[,sort(unique(zo))]))) #all zones mapped
    putt(adszod)
  }

#' @export
lmrhsFun <-
  function(nn=c('distanced','rdo2d','adszod','segd','vind','ppm1d','prppd','ppm2td','cohod','prime1rcd','ptc1d','refzoned','icod','zoned'),rhsfun=rhscode()) {
    getgd(nn)
    ll <- setNames(as.list(rhsfun),rhsfun)
    for(i in 1:length(rhsfun)) {
      #print(i)
      ll[[i]] <- do.call(paste0('lm',rhsfun[i]),args=list(NULL))
    }
    #browser()
    x1 <- ll[[1]]
    if(1<length(rhsfun)) {
      for(i in 2:length(ll)) {
        #print(i)
        x1 <- x1[ll[[i]],on=c(rc='rc')]
      }
      x1[,N:=NULL]
    }
    rownames(x1) <- x1[,rc]
    lmrhsd <<- x1[,rc:=NULL]
    putt(lmrhsd)
  }

#' @export
lmlhsFun <-
  function() {
    lmlhsd <<- lmlhs()
    putt(lmlhsd)
  }

#' @export
lmrhsmFun <-
  function(nn='lmrhsd') {
    getgd(nn)
    lmrhsmd <<- as.data.table(lapply(lmrhsd,mean))
    putt(lmrhsmd)
  }

#' @export
lmcenFun <-
  function(nn=c('lmlhsd','lmrhsd','lmrhsmd'),pwr=4,scale=F) {
    getgd(nn)
    x1 <- data.table(lmcen(nn=nn,pwr=pwr,scale=scale))[,const:=1]
    rownames(x1) <- rownames(lmlhsd)
    lmcend <<- x1
    putt(lmcend)
  }

#' @export
lmregFun <-
  function(
    nn=c('lmlhsd','lmrhsd','lmcend')
  ){  #},pwr=4) {
    getgd(nn)
    lmregd <<- rbind(lmuni(uni=F),lmuni(uni=T))
    putt(lmregd)
  }

#' @export
lmfitFun <-
  function(nn=c('lmregd','lmcend')) {
    getgd(nn)
    e1 <- lmregd[,unique(eqn)] #equation names
    ll <- setNames(as.list(e1),e1)
    for(ie in seq_along(e1)) {
      #if(e1[ie]=='rer.all') browser()
      c1 <- setkey(lmregd[eqn==e1[ie],.(eqn,rhs,coef)],rhs)
      ll[[ie]] <- as.matrix(lmcend[,c1[,rhs],with=F])%*%c1[,coef]
    }
    x1 <- setnames(as.data.table(ll),e1)[]
    rownames(x1) <- rownames(lmcend)
    lmfitd <<- x1
    putt(lmfitd)
  }

#' @export
lmcontFun <-
  function(nn=c('lmcend','lmregd')) {
    eqnx <- lmregd[grepl('all$',eqn),unique(eqn)]
    ll <- as.list(eqnx)
    for(i in seq_along(eqnx)) {

      x3a <- t(as.matrix(lmcend[,-(1:4),with=F])) #loadings
      dimnames(x3a) <- list(rows=names(lmcend)[-(1:4)],cols=rownames(lmcend))
      x4a <- x3a

      x2 <- lmregd[eqn==eqnx[i]][,.(rhs,coef)]
      x3b <- setkey(x2,rhs)[rownames(x3a)]
      stopifnot(all(x3b[,rhs]==rownames(x3a)))

      x4a[] <- x3b[,coef]*x3a
      x5 <- melt(data.table(x4a,keep.rownames=T),id.vars='rn',value.name='contrib',variable.name='rc')
      ll[[i]] <- x5[,rhs:=ifelse(rn=='const','const',substr(rn,1,3))][,.(contrib=sum(contrib)),'rc,rhs'][,eqn:=eqnx[i]]
    }
    lmcontd <<- setkey(unfactordt(rbindlist(ll)),rc,eqn,rhs)[]
    putt(lmcontd)
  }

#' @export
lmregtFun <-
  function(nn=c('lmcend','lmcontd')) {
    getgd(nn)
    jl <- lhsdt()[,code]
    ll <- as.list(jl)
    for(i in seq_along(jl)) {
      eqnx <- paste0(jl[i],'.all')
      #stopifnot(isTRUE(all.equal(rownames(lmcend),as.character(1:nrow(lmcend))))) #pointless?
      stopifnot(isTRUE(all.equal(rownames(lmcend),sort(rownames(lmcend)))))
      x0 <- dcast(lmcontd[eqn==eqnx],rc~rhs,value.var='contrib')
      stopifnot(isTRUE(all.equal(rownames(lmcend),x0[,rc])))
      x1 <- cbind(lmcend[,jl[i],with=F],x0)
      frml=paste0(paste0(jl[i],'~',paste0(setdiff(colnames(x0),'rc'),collapse='+')),'-1')
      frml2 <- paste0(jl[i],'~',paste0(setdiff(colnames(x0),c('rc','const')),collapse='+'))
      x2 <- summary(lm(dat=x1,frml))
      x2a <- summary(lm(dat=x1,frml2))
      x3 <- setnames(data.table(x2$coefficients,keep.rownames=T),old='t value',new='tstat')[,.(driver=rn,stat='tstat',value=tstat)]
      ll[[i]] <- rbindlist(list(x3,data.table(driver='all',stat='rsquared',value=x2a$r.squared),data.table(driver='all',stat='adj.r.squared',value=x2a$adj.r.squared)))[,eqn:=eqnx]
    }
    lmregtd <<- rbindlist(ll)
    putt(lmregtd)
  }

#' @export
lmnrcFun <-
  function(nn=c('lmcontd','lmlhsd')) {
    getgd(nn)
    ll <- as.list(lmcontd[,unique(rc)])
    x1 <- rbindlist(lapply(ll,lm1rc))
    lmnrcd <<- unfactordt(melt(x1,id.vars=c('rc','rhs')))
    putt(lmnrcd)
  }

#' @export
cyfoFun <- function(nn=c('buifostnd','setdad','extdad','stnd','cncd')) {
  getgd(nn)
  x1 <- setkey(setnames(data.table(as.matrix(apply(buifostnd[extdad[1:4]],2,sum)),keep.rownames=T),old='V1',new='cyfo4q'),rn)
  x2 <- setkey(setnames(data.table(as.matrix(apply(buifostnd[extdad[1:8]],2,sum)),keep.rownames=T),old='V1',new='cyfo8q'),rn)
  x3 <- setkey(setnames(data.table(as.matrix(apply(buifostnd[extdad[1:16]],2,sum)),keep.rownames=T),old='V1',new='cyfo16q'),rn)
  x4 <- setkey(setnames(data.table(as.matrix(apply(coredata(cncd[as.Date(setdad[stnd-(1:4)])]),2,sum)),keep.rownames=T),old='V1',new='stnprior'),rn)
  cyfod <<- x1[x2[x3[x4]]]
  putt(cyfod)
}

#' @export
x8Fun <- function(nn=c('pcod','dezod','rdo2d','ppm1d','distanced','dfinald')) {
  getgd(nn) #this step is messy, just for gui2 map app
  ads <- pcod[,.(s=unique(substr(rcode,1,9)))][,d:=substr(s,1,6)][,a:=substr(d,1,3)] #rcodes tree as 3table
  ads1 <- setkey(ads,a)[dezod[,.(sort(unique(substr(zo,1,3))))]] #just areas modelled
  x1 <- ads1[dezod[nchar(zo)==3],on=c(a='zo')][,src:='a'] #all areas with data -> sectors (9718)
  x2 <- ads1[dezod[nchar(zo)==6],on=c(d='zo')][,src:='d'] #all districts with data -> sectors (9346) there are 180 districts not zones
  x3 <- ads1[dezod[nchar(zo)==9],on=c(s='zo')][,src:='s'] #all sectors with data (81)
  x4 <- rbindlist(list(a1=x3[,level:=1],a2=x2[,level:=2],a3=x1[,level:=3]),use.names=T)
  x5 <-x4[ads1,on=c(s='s'),mult='first'] #first from s, d, a
  x6 <- x5[,.(s,d,a,ppm2,loadings1,loadings2,loadings3,beta,r1y,cyfo4q,cyfo8q,cyfo16q,stnprior)][,pc:=irregpcode(s)]
  x7 <- rdo2d[['s']]
  x7@data <- data.frame(x6[data.table(x7@data),on=c(pc='name')][,de:=ppm2])
  putt(x7)
  x <- c('pcod','dezod','rdo2d','ppm1d','distanced','x7')
  dx <- distanced[rc=='SE-1--'&dist<20,sort(unique(substr(other,1,3)))]
  xx1 <- setkey(ppm1d[,.(pv,median,rc12,rc9=substr(rc12,1,9))][,.(m2=sum(median),pv=sum(pv),N=.N),rc9],N)
  xsv1 <- xx1
  x8<-x7[x7@data$a%in%dx&!is.na(x7@data$a),]
  dd<-data.table(x8@data)
  xx2 <- xsv1[dd,on=c(rc9='s')]
  xx2[!is.na(pv)&N>20,ppm2:=pv/m2]
  setdiff(colnames(x8@data),c('m2','pv','N'))
  jnona <- names(x8@data)[unlist(lapply())]
  x8a <- as.data.frame(xx2[,de:=ppm2])
  x8@data <- x8a[,names(x8a)[!unlist(lapply(lapply(x8a,is.na),any))]] #simply tidying - remove unused m2,N,pv which are not mapped and have NA
  save(list=c('x8','dfinald'),file='x8.RData') #special for gui2 map app
  x8d <<- x8
  putt(x8d)
}

#' @export
x9Fun <- function(nn=c('pcod','dezod','rdo2d','ppm1d','distanced','dfinald')) {
  getgd(nn) #this step is messy, just for gui2 map app
  ads <- pcod[,.(s=unique(substr(rcode,1,9)))][,d:=substr(s,1,6)][,a:=substr(d,1,3)] #rcodes tree as 3table
  ads1 <- setkey(setkey(ads,a)[dezod[,.(sort(unique(substr(zo,1,3))))]],d) #just areas modelled
  x1 <- ads1[dezod[nchar(zo)==3],on=c(a='zo')][,src:='a'][,level:=3] # areas with data
  x2 <- ads1[dezod[nchar(zo)==6],on=c(d='zo')][,src:='d'][,level:=2] # districts with data
  x3 <- ads1[dezod[nchar(zo)==9],on=c(s='zo')][,src:='s'][,level:=1] # sectors with data
  x4 <- rbindlist(list(x3,x2,x1))
  x5 <-x4[ads1,on=c(s='s'),mult='first'] #first from s, d, a
  x5[1==level,pc:=irregpcode(s)]
  x5[1<level,pc:=irregpcode(d)]
  x6 <- setkey(x5[,.(pc,s,d,a,ppm2,loadings1,loadings2,loadings3,beta,r1y,cyfo4q,cyfo8q,cyfo16q,stnprior)],pc)

  d2 <- x5[level==1,unique(substr(s,1,6))]   #d have s (map as s)
  d3 <- setdiff(x5[,unique(substr(s,1,6))],d2)  #d have no s (map as d)

  ms <- setkey(x5,d,s)[d2]
  md <- setkey(x5,d)[d3,mult='first']

  # ms <- ads1[d2,.(s,d,a)][,pc:=irregpcode(s)] #key for 'map as s'
  # md <- unique(ads1[d3,.(d,a)])[,pc:=irregpcode(d)] #key for 'map as d'

  rs <- rdo2d$s
  m1 <- rs[rs@data[,1]%in%ms[,pc],] #can subset spatial polygons with [i,] syntax
  m1pc <- m1@data
  m1@data <- data.frame(x6[m1pc]) #then assign data keyed on pc

  rd <- rdo2d$d
  m2 <- rd[rd@data[,1]%in%md[,pc],]
  m2pc <- rd[rd@data[,1]%in%md[,pc],]@data[,1]
  m2@data <- data.frame(x6[m2pc,mult='first'])

  m12 <- bind(m1,m2)

  ird <- rd@data[,1]%in%md[,pc]
  m2 <- rd[ird,]

  s2 <- setkey(ads1,d)[d2,unique(s)]  #mapsectors (d2)
  stopifnot(all(s1%in%s2))
  s3 <- setdiff(s2,s1)   #sectors to be mapped using d data
  stopifnot(all(s2%in%x5[,s])&all(s3%in%x5[,s]))
  stopifnot(all(d3%in%x5[,d]))
  sm1 <- rdo2d[['s']]
  dm1 <- rdo2d[['d']]
  ss <- sm1[data.table(sm1@data)[,match(irregpcode(s1),sm1@data[,1])],]
  sd <- sm1[data.table(sm1@data)[,match(irregpcode(s3),sm1@data[,1])],]
  dd <- dm1[data.table(dm1@data)[,match(irregpcode(d3),dm1@data[,1])],] #2 indexes are missing so fail
  allmap <- Reduce(bind,list(ss,sd,dd))
  #
  i <- match(irregpcode(d3),dm1@data[,1])
  d3[is.na(i)] #these are districts with only one sector
  x <-d3[is.na(i)]
  i <- match(irregpcode(substr(,1,6)),sm1@data[,1])
  d3[!is.na(i)]

  x7 <- rdo2d[['s']]
  x7@data <- data.frame(x6[data.table(x7@data),on=c(pc='name')][,de:=ppm2])
  putt(x7)
  x <- c('pcod','dezod','rdo2d','ppm1d','distanced','x7')
  dx <- distanced[rc=='SE-1--'&dist<20,sort(unique(substr(other,1,3)))]
  xx1 <- setkey(ppm1d[,.(pv,median,rc12,rc9=substr(rc12,1,9))][,.(m2=sum(median),pv=sum(pv),N=.N),rc9],N)
  xsv1 <- xx1
  x8<-x7[x7@data$a%in%dx&!is.na(x7@data$a),]
  dd<-data.table(x8@data)
  xx2 <- xsv1[dd,on=c(rc9='s')]
  xx2[!is.na(pv)&N>20,ppm2:=pv/m2]
  setdiff(colnames(x8@data),c('m2','pv','N'))
  jnona <- names(x8@data)[unlist(lapply())]
  x8a <- as.data.frame(xx2[,de:=ppm2])
  x8@data <- x8a[,names(x8a)[!unlist(lapply(lapply(x8a,is.na),any))]] #simply tidying - remove unused m2,N,pv which are not mapped and have NA
  save(list=c('x8','dfinald'),file='x8.RData') #special for gui2 map app
  x8d <<- x8
  putt(x8d)
}

#subset map polygons: just areas modelled
#' @export
rdo4Fun <- function(nn=c('dezod','rdo2d')) {
  getgd(nn)
  a1 <- dezod[,substr(zo,1,3)]
  x1 <- rdo2d$d
  f1 <- function(x1,a1){
    x2 <- x1[data.table(x1@data)[,substr(regpcode(name),1,3)%in%a1],]
    x2@data <- data.frame(data.table(x2@data)[,rc:=regpcode(name)][,pc:=name][,de:=1:.N]) #added 20171126
    x2
  }
  rdo4d <<- lapply(rdo2d,f1,a1=a1)
  putt(rdo4d)
}

#all data ready for join to spatial polygons, with some pure d, some pure s, some ds mixed
#currently does not keep a field for 'source zone'
#' @export
dezo1Fun <- function(nn=c('dezod','rdo4d')) {
  getgd(nn)
  x1 <- dezod[nchar(zo)==6]
  i2 <- dezod[nchar(zo)==9,sort(unique(substr(zo,1,6)))]
  x3 <- setkey(x1,zo)[!i2] #dezod d dd
  stopifnot(all.equal(sort(names(x3)),sort(names(dezod))))
  #ds: all the rest
  x4 <- setkey(x1,zo)[i2] #dezod d ds
  stopifnot(all.equal(x1[,zo],sort(union(x3[,zo],x4[,zo]))))
  #d:s relation
  #ds1 <- setnames(data.table(rdo4d$s@data),'pc')[,rcs:=regpcode(pc)][,rcd:=substr(rcs,1,6)]
  ds1 <- data.table(rdo4d$s@data)[,rcs:=regpcode(pc)][,rcd:=substr(rcs,1,6)]
  x5 <- ds1[x4,on=c(rcd='zo')] #ds
  #s having data
  x6 <- dezod[nchar(zo)==9,] #ss
  stopifnot(all(x6[,zo]%in%x5[,rcs]))
  stopifnot(x6[,!any(duplicated(zo))])
  jmeas <- setdiff(names(dezod),c('zo','pc'))
  stopifnot(all(jmeas%in%names(x5)))
  stopifnot(all(jmeas%in%names(x6)))
  #combine 2 sets of s
  x7 <- setnames(setkey(x5,rcs)[!x6[,zo],c(jmeas,'rcs'),with=F],old='rcs',new='zo')
  x8 <- x6[,c(jmeas,'zo'),with=F]
  x9 <- rbind(x7,x8)
  dezo1d <<- rbind(x9,x3[,names(x9),with=F])[,pc:=irregpcode(zo)] #unsure if pc needed 1002
  putt(dezo1d)
}

#' @export
dezo2Fun <- function(nn=c('dezo1d','rdo4d')) {
  getgd(nn)
  x1 <- dezo1d[nchar(zo)==6]
  i1 <- x1[,pc]%in%rdo4d$d@data[,1]
  x2 <- x1[i1]
  y1 <- rdo4d$d
  i2 <- y1@data[,1]%in%x2[,pc]
  y2 <- y1[i2,]
  stopifnot(length(y2)==nrow(x2))
  stopifnot(!any(is.na(y2@data)))
  stopifnot(identical(x2[,pc],y2@data[,1]))
  y2@data <- data.frame(x2)
  #cl1(y2)
  z1 <- y2
  x1 <- dezo1d[nchar(zo)==9]
  i1 <- x1[,pc]%in%rdo4d$s@data[,1]
  x2 <- x1[i1]
  y1 <- rdo4d$s
  i2 <- y1@data[,1]%in%x2[,pc]
  y2 <- y1[i2,]
  stopifnot(length(y2)==nrow(x2))
  stopifnot(!any(is.na(y2@data)))
  setkey(x2,pc)
  stopifnot(identical(x2[,pc],y2@data[,1]))
  y2@data <- data.frame(x2)
  #cl1(y2)
  z2 <- y2
  dezo2d <<- rbind(z1,z2)
  putt(dezo2d)
}


#' @export
dezoint1Fun <- function(nn=c('agpcod','cohod','rdo3d','dezocombod')) {
  getgd(nn)
  x1 <- rdo3d[,sort(unique(rcode))]
  x2 <- sort(unique(substr(x1,1,3))) #all uk areas
  x3 <- dezocombod[,unique(substr(sort(unique(zo)),1,3))] #areas with some model or other
  x3a <- cohod[nchar(rc)==6,unique(rc)] #zones which are districts
  x4 <- setkey(copy(rdo3d)[,rc3:=substr(rcode,1,3)],rc3) #add rc3, area code
  x5 <- x4[x3] #map data for areas with some model or other ie england and wales
  x6 <- x5[,.(x=unique(rcode))]  #districts in areas mapped
  x7 <- x6[3<nchar(x),.(unique(substr(x,1,6)))][] #all districts in areas with some model or other
  x8 <- setdiff(x7[,V1],dezocombod[,unique(zo)]) #180 not mapped
  x8 <-x8
  x9 <- intersect(agpcod[,rc],x8) #just 2 are not : these are the missing districts
  x9a <- cohod[nchar(rc)==6,unique(rc)] #zones which are districts
  x10 <- union(x9,x9a) #because distance is a symmetric function, need both known and unknown
  zone1d <- setkey(agpcod[nchar(rc)==6],rc)[x10]
  d1 <- distance(zone1d,save=F) #distance from the missing districts x8 to the zones; rc in x8, other in x9a
  d2 <- setkey(d1,rc,other)[CJ(x8,x9a)]
  d2[,all(x8%in%rc)&all(x9a%in%other)&nrow(d2)==length(x8)*length(x3a)]
  stopifnot(all(d2[is.na(pc)|is.na(eex),rc%in%c('E--77-','EC-50-')])) #non-geographic codes not needed
  d3 <- d2[!is.na(pc)]
  dezoint1d <<- setkey(d3,rc,dist)[,rr:=0:(.N-1),rc][rr>0&rr<4][,w:=max(rr)-(rr-1),rc][,weight:=w/sum(w),rc][,w:=NULL]
  putt(dezoint1d)
}

#' @export
dezoint2Fun <- function(nn=c('dezoint1d','dezojd')){
  getgd(nn)
  dezoint2d <<- dezoint2(dezoint1d,dezojd)
  putt(dezoint2d)
}

#' @export
dezoint2 <- function(dezoint1d,dezojd){
  x6 <- dezojd[dezoint1d[,.(rc,other,weight)],on=c(zo='other')]
  x7 <- x6[,zo:=NULL][,as.data.table(lapply(.SD,weighted.mean,weight=weight)),rc][,weight:=NULL]
  x8 <- setnames(x7,old='rc',new='zo')
  dezoint2d <- rbind(dezojd,x8)
  dezoint2d
}

#' @export
dezospdfFun <- function(nn=c('dezoint2d','rdo2d')) { #for leaflet maps, finally get 1:1 between zones and map polygons
  getgd(nn)
  x1 <- rdo2d$d
  x2 <- setkey(data.table(x1@data)[,rc:=regpcode(name)],rc)
  dws <- dezoint2d[nchar(zo)==9,substr(zo,1,6)]
  dns <- setdiff(dezoint2d[nchar(zo)==6,zo],dws)
  ss <- dezoint2d[nchar(zo)==9,zo] #rc of zones which are sectors
  x4 <- data.table(rdo2d$s@data)[,rc:=regpcode(name)]#for districts with sectors, merge unzoned sectors and replace (in loop)
  dnsspdf <- x1[match(irregpcode(dns),x1@data[,'name']),] #these need no mods districts-no-sector-spdf
  lldws <- as.list(NULL)
  for(i in seq_along(dws)) {
    s0 <- x4[grep(grepstring(dws[i],car=T),rc),rc] #all sectors in this district
    sm <- setdiff(s0,ss) #sectors to merge
    if(0<length(sm)) {
      x5 <- rdo2d$s[x4[,grep(grepstring(sm),rc)],]
      x6 <- rgeos::gUnaryUnion(x5)
      x7 <- SpatialPolygonsDataFrame(x6,setkey(dezoint2d,zo)[dws[i]])
      x8 <- rdo2d$s[x4[,grep(grepstring(s0),rc)],]
      x7@data <- data.frame(name=irregpcode(dws[i]))
      lldws[[length(lldws)+1]] <- x7
    }
  }
  x9 <- Reduce(raster::bind,lldws)
  x10 <- rdo2d$s[grep(grepstring(irregpcode(ss),car=T,dol=T),rdo2d$s@data[,'name']),]
  x11 <- bind(bind(dnsspdf,x9),x10) #combine 3: sectors,dns,lldws
  x11@data <- data.frame(setnames(data.table(x11@data),'pc')[,rc:=regpcode(pc)][,de:=rank(pc)])
  x12 <- x11[order(x11@data[,'rc']),]
  x13 <- x12[which(!duplicated(x12@data[,'rc'])),]
  dezospdfd <<- x13
  putt(dezospdfd)
}



#' @export
fadeFun <- function(nn=c('yxpr1d','cncd','segd')) {
  getgd(nn)
  isegd <- setkey(segd[,.(idseg=paste0(id,'.',deed_date),startnew,years=round(as.numeric(deed_date-startdate)/365.25),id,startdate,deed_date)],idseg) #quite slow
  ll <- as.list(seq_along(yxpr1d))
  for(i in seq_along(yxpr1d)) {
    x1 <- yxpr1d[i] #unary list of dt
    yx <- as.matrix(x1[[1]])
    dimnames(yx) <- list(rownames(x1[[1]]),colnames=names(x1[[1]]))
    dax <- colnames(yx)[-(1:2)]
    y <- yx[,1,drop=F]
    fit <- yx[,dax]%*%cncd[as.Date(dax),names(x1),drop=F]/(365.25/4)
    x2 <- setkey(data.table(idx=rownames(yx),r=as.numeric(y),fit=as.numeric(fit),err=as.numeric(y)-as.numeric(fit)),idx)
    ll[[i]] <- isegd[x2]
  }
  faded <<- rbindlist(ll)[!duplicated(idseg)]
  putt(faded)
}


#codepoint data - slow!
#' @export
cpFun <- function(fnams=dir(dd)) {
  dd <- paste0(rdroot(), 'codepo')
  ll <- as.list(fnams)
  for(i in seq_along(fnams)) {
    x8 <- rbindlist(lapply(as.list(fnams[i]),function(x,dd) {data.table(read.csv(paste0(dd,'\\',x),header=F))},dd=dd))
    cpd <- x8[,.(pc=V1,EE=V3,NN=V4)][,pc1:=pc]
    cpd[!grepl(' ',pc1),pc1:=paste0(substr(pc1,1,4),' ',substr(pc1,5,7))][,rc:=regpcode(pc1)]
    stopifnot(all.equal(as.integer(cpd[,unique(nchar(rc))]),12))
    ll[[i]] <- cpd[,.(pc=pc1,rc,EE,NN)]
  }
  cpd <<- rbindlist(ll)
  putt(cpd)
}
cpchk <- function() {
  getgd('cpd')
  res <- T
  res&all(is.data.table(cpd)&names(cpd)==c('pc','rc','EE','NN'))
}


#identify degenerate codes and their mapping to unique {rc12,EE,NN,EENN,rcunique}
#' @export
rcuniFun <- function(nn=c('cpd','ppm1d')) {
  x1 <- setkey(cpd,rc)
  x2 <- ppm1d[,.(lastsale=max(lastsale)),rc12]
  x3 <- x1[x2][,EENN:=paste0(EE,'|',NN)]
  x4 <- setkey(x3,EENN,lastsale)[x3[,max(lastsale),EENN],mult='last']
  x5 <- x4[x3[,.(EENN,rc)],on=c(EENN='EENN')][!is.na(EE),.(rcunique=rc,rc12=i.rc,EENN,EE,NN)]
  rcunid <<- x5
  putt(rcunid)
}
rcunichk <- function() {
  getgd(paste0((gsub('chk','',match.call()[[1]])),'d'))
  getgd('cpd')
  res <- nrow(rcunid[rcunique==rc12,])==nrow(unique(rcunid[,.(EE,NN)]))
  res & all(c('rcunique','rc12','EENN','EE','NN')%in%names(rcunid))
}




#' @export
cicpFun <- function(nn=c('j1d','cpd','prppd'),pfs='+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0') {
  # - [ ] degenerate rc replace with one with geodata most recent
  getgd(nn)
  x1 <- prppd[,.(lastsale=max(deed_date),zo=max(rcode)),rc12]
  x2 <- setkey(x1[,.(rc12,lastsale,zo)],rc12,lastsale)[cpd[,.(EE,NN,EENN=paste0(EE,'|',NN),rc)],on=c(rc12='rc'),mult='first',nomatch=0]
  x3 <- setnames(setkey(x2,EENN,lastsale)[x2[,.(unique(EENN))],mult='last'],old='rc12',new='rcunique')
  x4 <- setkey(x1[,.(rc12)],rc12)[cpd[,.(EE,NN,EENN=paste0(EE,'|',NN),rc)],on=c(rc12='rc'),mult='first',nomatch=0]
  x5 <- x3[x4[,.(rc=rc12,pc=irregpcode(rc12),EENN)],on=c(EENN='EENN'),mult='all']
  #do next 3 lines way upstream - have a place where each assumption about the data consistency gets enforced and amount of non-compliance recorded
  if(F) { #the idea here was to trim 'stray/oddball' units, but need 'collar' so no point, trim the polygons at civo
    x6 <- sp::spTransform(SpatialPointsDataFrame(coords=x5[,.(EE,NN)], data=data.frame(x5), coords.nrs = 3:4,proj4string = CRS("+init=epsg:27700")),pfs)
    x7 <- rdo1d$a[match(ciard,regpcode(rdo1d$a@data[,1])),]
    x5 <- data.table(raster::intersect(x6,x7)@data)
  }
  cicpd <<- x5 #believed the right class!
  putt(cicpd)
}


#' @export
ciarFun <- function(x='W') {
  # - [ ] define area
  ciard <<- regpcode(x)
  putt(ciard)
}

#' @export
cisu2Fun <- function(nn='ciard') {
  # - [ ] stub for the old collar
  getgd(nn)
  cisu2d <<- ciard
  putt(cisu2d)
}


#' @export
cicpxFun <- function(nn=c('cicpd','ciard')) {
  # - [ ] subset cpd to area
  getgd(nn)
  cicpxd <<- cicpd[grep(grepstring(ciard),rcunique),]
  putt(cicpxd)
}


#' @export
cirdo4Fun <- function(nn=c('rdo4d','cisu2d')) {
  # - [ ] cirdo4 from rdo4 (os) #not so big anyway
  getgd(nn)
  cirdo4 <- function(rdo4d,cisu2d) {
    x1 <- as.list(seq_along(rdo4d))
    for(i in seq_along(rdo4d)) {
      x1[[i]] <- rdo4d[[i]][grep(
        grepstring(unique(substr(cisu2d,1,3*i))),
        rdo4d[[i]]@data[,'rc']
      ),]
    }
    x1
  }
  cirdo4d <<- cirdo4(rdo4d=rdo4d,cisu2d=cisu2d)
  putt(cirdo4d)
}


#' @export
cifadeFun <- function(nn=c('cisu2d','faded')) {
  # - [ ] cifade (err)
  getgd(nn)
  cifade <- function(cisu2d,faded) {
    x1 <- faded[grep(grepstring(cisu2d),idseg)]
    x1[,rc12:=substr(id,1,12)]
  }
  cifaded <<- cifade(cisu2d=cisu2d,faded=faded)
  putt(cifaded)
}

#' @export
cippm1Fun <- function(nn=c('ppm1d','cicpxd','cisu2d')) {
  # - [ ] cippm1 (ppm)
  getgd(nn)
  cippm1 <- function(ppm1d,cicpxd,cisu2d) {
    cicpxd[ppm1d,on=c(rc='rc12'),nomatch=0][grep(grepstring(cisu2d),rc)]
  }
  cippm1d <<- cippm1(ppm1d=ppm1d,cicpxd=cicpxd,cisu2d=cisu2d)
  putt(cippm1d)
}


#' @export
cierrFun <- function(nn=c('cifaded','cisu2d','cicpxd'),yearsmax=22,daysmin=90,errmax=1,errmin=-1) {
  # - [ ] screen err
  getgd(nn)
  cierr <- function(cifaded,cisu2d,cicpxd,yearsmax,daysmin,errmax,errmin) {
    #browser()
    d1 <- paste0(as.character(2017-yearsmax),'-12-31')
    cifaded[,days:=as.integer(deed_date-startdate)]
    x1 <- setkey(cifaded[(d1<startdate)][daysmin<=days][err<errmax][errmin<err],id,deed_date)
    x1a <- x1[,.(err=sum(err)*365/days,r=sum(r)*365/days,fit=fit*365/days,rc12=max(rc12),days=max(days)),'id,startdate'] #added startdate into by
    x1b <- x1a[grep(grepstring(cisu2d),id)][,inlier:=T][cicpxd[,.(rc,rcunique,zo)],on=c(rc12='rc'),nomatch=0]
    x1b
  }
  cierrd <<- cierr(cifaded=cifaded,cisu2d=cisu2d,cicpxd=cicpxd,yearsmax=yearsmax,daysmin=daysmin,errmax=errmax,errmin=errmin)
  putt(cierrd)
}

#' @export
cippmFun <- function(nn=c('cippm1d'),pvmax=50e6,pvmin=1e4,m2max=500,m2min=30,ppm2max=2e4,ppm2min =2e2) {
  # - [ ]  screen ppm
  getgd(nn)
  cippm <- function(cippm1d,pvmax=50e6,pvmin=1e4,m2max=500,m2min=30,ppm2max=2e4,ppm2min =2e2) {
    x2 <- copy(cippm1d)[,ppm2:=pv/median]
    x3 <- setkey(x2[pv<pvmax][pvmin<pv][median<m2max][m2min<median][ppm2<ppm2max][ppm2min<ppm2],id)
    x3
  }
  cippmd <<- cippm(cippm1d=cippm1d,pvmax=pvmax,pvmin=pvmin,m2max=m2max,m2min=m2min,ppm2max=ppm2max,ppm2min=ppm2min)
  putt(cippmd)
}

#' @export
cicercFun <- function(nn=c('cippmd','cierrd'),k1=1) {
  # - [ ] cicerc: join and screen to minimum k1 records on both inputs
  getgd(nn)
  cicerc <- function(cippmd,cierrd,k1=1) {
    x1 <- cippmd[,.(rcode,rcunique,id,m2=median,pv,ppm2=pv/median,estate,newbuild,type,lastsale)][cierrd[,.(id,r,err)],on=c(id='id'),nomatch=0]
    x2 <- x1[,.N,rcunique][k1<=N,.(rcunique,N)]
    x2
  }
  cicercd <<- cicerc(cippmd=cippmd,cierrd=cierrd,k1=k1)
  putt(cicercd)
}


#' @export
cicenFun <- function(nn=c('cicercd','cippmd','cierrd')) {
  # - [ ] cicen: number of records by type in each cell
  getgd(nn)
  cicen <- function(cicercd,cippmd,cierrd) {
    x1 <- cippmd[cicercd,on=c(rcunique='rcunique')][,.(nppm=.N),rcunique]
    x2 <- cierrd[cicercd,on=c(rcunique='rcunique')][,.(nerr=.N),rcunique]
    x1[x2,on=c(rcunique='rcunique')]
  }
  cicend <<- cicen(cicercd=cicercd,cippmd=cippmd,cierrd=cierrd)
  putt(cicend)
}

#' @export
cinbrFun <- function(ty=c('cippmd','cierrd'),nn='cicend',k1=getpv('cinbr','k1br'),...) {
  # - [ ] neighbour: for each (cell,type) the first m units
  getgd(c(ty,nn))
  cinbr <- function(ty,nn) {
    x1 <- rbindlist(lapply(ty,cinbr1,k1=k1))
    setkey(x1,rcunique)[cicend[,.(rcunique)],mult='all',nomatch=0]
  }
  cinbrd <<- cinbr(ty=ty,nn=nn)
  putt(cinbrd)
}

#' @export
cinoupFun <- function(nn=c('cippmd','cinbrd'),k1=getpv('cinoup','k1p'),p1=getpv('cinoup','p1p'),...) {
  # - [ ] the sublime function: rnk, p, weight, inlier
  getgd(nn)
  cinoup <- function(k=100,p=.05,...) {
    cinoupd <<- cinou(x='cippmd',nn='cinbrd',jrank='ppm2',k=k,p=p,...)
  }
  cinoupd <<- cinoup(k=k1,p=p1)
  putt(cinoupd)
}

#' @export
cinoueFun <- function(nn=c('cippmd','cierrd','cinbrd'),k1=getpv('cinoue','k1e'),p1=getpv('cinoue','p1e'),...) {
  # - [ ] rnk, p, weight, inlier
  getgd(nn)
  cinoue <- function(...) {
    cinou(x='cierrd',nn='cinbrd',jrank='err',...)
  }
  cinoued <<- cinoue(k=k1,p=p1)
  putt(cinoued)
}
cinouechk <- function(nn=c('cinoued','cinoupd','cicend','cinbrd')) {
  x1 <- cinoued[,.(err=weighted.mean(err,weight),Ne=as.numeric(.N)),rcunique]
  x2 <- cinoupd[,.(m2=weighted.mean(median,weight),pv=weighted.mean(pv,weight),Np=as.numeric(.N)),rcunique][,ppm2:=pv/m2][]
  x3 <- melt(x1,measure.vars=c('err','Ne'))
  x4 <- melt(x2,measure.vars=c('Np','m2','pv','ppm2'))
  all.equal(x3[,sort(unique(rcunique))],x4[,sort(unique(rcunique))]) &
    all.equal(x3[,sort(unique(rcunique))],cinbrd[,sort(unique(rcunique))])&
    all.equal(cicend[,sort(unique(rcunique))],x3[,sort(unique(rcunique))])&
    cicend[x3[variable=='Ne'],on=c(rcunique='rcunique')][,all(nerr<=value)]
}

#' @export
cinouaFun <- function(yrs=2^(0:4),nn=c('cinoued','setdad')) {
  # - [ ] aggregate err on key of rcunique, yrs
  x0 <- cinoued[weight>0,.(startdate=startdate,err,zo,rcunique,days)] #days is segment length, weight is 0 for outliers
  da1 <- as.Date(setdad[length(setdad)-yrs*4])
  ll <- list(NULL)
  for(i in seq_along(yrs)) ll[[i]] <- x0[da1[i]-365<startdate][,yrs:=yrs[i]][,.(err=mean(err)),'rcunique,zo,yrs']
  x1 <- rbindlist(ll)
  cinouad <<- x1
  putt(cinouad)
}

#' @export
civoFun <- function(nn=c('cicpxd','cicend','ciard','rdo1d'),
                    uproj1 = '+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 +y_0=-100000 +ellps=airy +datum=OSGB36 +units=m +no_defs',
                    uproj2 = '+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0',
                    #uproj2 = "+proj=longlat +datum=WGS84",
                    eps=getpv('civo','epsvo')
) {
  # - [ ] voronoi
  getgd(nn)
  x3 <- civo(cicpxd=cicpxd,cicend=cicend,uproj1=uproj1,uproj2=uproj2,eps=eps)
  x4 <- raster::intersect(x3,rdo1d$a[match(ciard,regpcode(rdo1d$a@data[,1])),])
  x4@data <- x4@data[,setdiff(names(x4@data),c('EE','NN')),with=F]
  civod <<- x4
  putt(civod)
}

#' @export
ciagFun <- function(nn=c('cinoupd','cinoued','cicntnd'),startyear=1995) {
  # - [ ] aggregate abnormal return and ppm2
  getgd(nn)
  x1 <- ciag(cinoupd=cinoupd,cinoued=cinoued,startyear=startyear)
  ciagd <<- x1[cicntnd,on=c(rcunique='rcunique')]
  putt(ciagd)
}

#' @export
cijoFun <- function(nn=c('ciagd','civod')) {
  # - [ ] join aggregate to spdf
  getgd(nn)
  cijod <<- cijo(ciagd=ciagd,civod=civod)
  putt(cijod)
}

#' @export
jomneini <- function() {
  #run to initialise after newrd()
  x1 <-
    structure(list(step = "pxoscp", garbage = NA_character_, fun = "pxoscpFun", 
                   comment = " <comment>", output = "pxoscpd", depends = "", 
                   satisfied = TRUE, level = 0, pre = "os", pp = "os", nn = "01", 
                   seq = 1L, iseq = 1L, class = "data.table", size = "211.9 Mb", 
                   run = "2019-06-14 21:40:50", stage = 0, runseq = NA_integer_), 
              row.names = c(NA,-1L), class = "data.frame")
  jomned <<- data.table(x1)
  putt(jomned)
}


#' @export
jomneFun <- function(
  nn=c('dep5d','jomned')
  ,
  #              prex=dep1d[,unique(pre)]
  ppx=dep5d[,unique(pp)]
  ,
  cleangarbage=T

) {
  getgd(nn)
  # - [ ] runs all steps in dep5d and records in jomned
  #x1 <- copy(dep1d)[pre%in%prex]
  x1 <- copy(dep5d)[pp%in%ppx]
  x2 <- copy(jomned)#[!is.na(run)][,secs:=NULL]
  rr <- getreturn();setreturn(F)
  for(i in 1:nrow(x1)) { #was dep1d???
    x1[i,print(step)]
    x1[i,do.call(fun,args=as.list(formals(fun)))]
    x1[i,class:=class(get(output,envir=globalenv()))[1]]%>%
      .[i,size:=format(object.size(get(output,envir=globalenv())),units='auto')]%>%
      .[i,run:=format(Sys.time())]
    #delete section
    if(cleangarbage && 0<nrow(x1) && x1[i,!is.na(garbage)]&&x1[i,0<nchar(garbage)]) {
      x5 <- x1[i,unlist(strsplit(garbage,','))]
      x6 <- intersect(ls(envir=globalenv()),x5)
      rm(list=x6,envir=globalenv())
    }
    #end delete
    jomned <<- rbind(x2,x1,fill=T)[!is.na(run)]
    putt(jomned)
  }
  setreturn(rr)
  #jomned[,secs:=as.numeric(as.POSIXct(data.table::shift(run,1,type="lead"))-as.POSIXct(run))] #should be just for this run
  jomned <<- jomned
  putt(jomned)
}

#join multi-area into one
#' @export
majoFun <- function(ax) {
  x1 <- majo(ax)
  x1@data[,'de'] <- x1@data[,'ppm2']
  majod <<- x1
  putt(majod)
}

#factor return summary, with interpolated zones added as weighted mean
#' @export
frsuFun <- function(yrs=2^(0:4),nn=c('decd','dezoint1d')) {
  getgd(nn)
  da1 <- rev(as.character(index(decd[[1]])))[4*yrs]
  ll1 <- ll <- list(NULL)
  for(i in seq_along(decd)) ll[[i]] <- setkey(setnames(unfactordt(data.table(melt(coredata(decd[[i]])))),c('da','zo',names(decd)[i])),zo,da)
  x1 <- ll[[1]][ll[[2]]][ll[[3]]][ll[[4]]][ll[[5]]]
  for(i in seq_along(da1))  ll1[[i]] <- unfactordt(melt(x1[da>=da1[i]][,da:=NULL][,lapply(.SD,sum),zo],id.vars=c('zo'))[,yrs:=yrs[i]])
  x2 <- rbindlist(ll1)
  x3 <- dezoint1d[,.(rc,other,weight)][x2,on=c(other='zo'),nomatch=0][,.(sum(weight*value)),'rc,variable,yrs'][,.(zo=rc,variable,value=V1,yrs)]
  x4 <- rbind(x2,x3)
  frsud <<- x4
  putt(frsud)
}

#fit lognormal to price quantilse for later interpolate/extrapolate
#' @export
cdfFun <- function(nn=c('dezocombod','dezoint1d'),tol=.01) {
  getgd(nn)
  x1 <- dezocombod[,data.table(t(suppressMessages(suppressWarnings(get.lnorm.par(p=c(0.25,0.5,0.75),q=c(p25,median,p75),plot=F,show=F,tol=tol))))),zo]
  cdfd <<- dezoint2(dezoint1d,x1)
  putt(cdfd)
}

#fraction of property in category, interpolated
#' @export
typeFun <-  function(nn=c('ppm2d','dezoint1d')) {
  getgd(nn)
  x1 <- ppm2d[variable=='n']
  x6 <- x1[dezoint1d[,.(rc,other,weight)],on=c(rcode='other')]
  x7 <- x6[,as.data.table(lapply(.SD[,.(value)],weighted.mean,weight=weight)),'rc,cat,catvalue,variable']
  x8 <- setnames(x7,old='rc',new='rcode')
  typed <<- rbind(x1,x8)
  putt(typed)
}

#encode step
#' @export
cicntnFun <- function(nn='cinoupd',brkx=brk(),countthresh=c(0,3),typex=c('F','D','S','T')) {
  # - [ ] encodes count(brk,typex)
  getgd(nn)
  cicntnd <<- cicntn(cinoupd,brkx,countthresh,typex)
  putt(cicntnd)
}
#this is not exported to avoid autorun
cicntnchk <- function(nn=c('cinoupd','cicntnd','cidecnd','ciagd'),jx=NULL) { #incomplete, just check on random sample that something is in the bin when flagged
  getgd(nn)
  stopifnot(identical(cicntnd[,sort(unique(rcunique))],ciagd[,sort(unique(rcunique))]))
  if(is.null(jx)) {
    jx <- cinoupd[sample(1:nrow(cinoupd),10),rcunique]
  }
  jxx <- cinoupd[,match(jx,rcunique)]
  for(j in jxx) {
    x1 <- cinoupd[j,rcunique]
    x2 <- setkeyv(copy(cinoupd)[type!='O'],c('rcunique','rcneigh'))[list(x1,x1)]
    x3 <- setkey(cidecnd,rcunique)[x1] #note it is checking the complete round trip code, decode
    x4 <- x3[,-1,with=F]
    i <- which(as.numeric(unlist(x4))>0)
    brkx <- brk()*1000
    x5<-NULL
    for(ii in seq_along(i)) {
      x5[ii] <- sum(brkx[i[ii]]<x2[,pv]&x2[,pv]<brkx[i[ii]+1])
    }
    stopifnot(all(x5>=x4[,i,with=F])) #x4 is a lower bound
    x6 <- cbind(c('actual','LB.stored'),rbind(as.data.table(as.list(x5)),x4[,i,with=F],use.names=F)) #LB meaning 'lower bound' because on store bits for '>1' and '>3'
    stopifnot(all(x6[1,-1,with=F]>=x6[2,-1,with=F]))
    print(x6)
    print(paste0(x1,' passed'))
  }
  TRUE
}
# cicntnchk <- function(nn=c('cinoupd','cicntnd','cidecnd','ciagd'),jx=NULL) { #incomplete, just check on random sample that something is in the bin when flagged
#   getgd(nn)
#   stopifnot(identical(cicntnd[,sort(unique(rcunique))],ciagd[,sort(unique(rcunique))]))
#   if(is.null(jx)) {
#     jx <- sample(1:nrow(cinoupd),10)
#   }
#   for(j in jx) {
#     x1 <- cinoupd[j,rcunique]
#     x2 <- setkeyv(copy(cinoupd)[type!='O'],c('rcunique','rcneigh'))[list(x1,x1)]
#     x3 <- setkey(cidecnd,rcunique)[x1] #note it is checking the complete round trip code, decode
#     x4 <- x3[,-1,with=F]
#     i <- which(as.numeric(unlist(x4))>0)
#     brkx <- brk()*1000
#     x5<-NULL
#     for(ii in seq_along(i)) {
#       x5[ii] <- any(brkx[i[ii]]<x2[,pv]&x2[,pv]<brkx[i[ii]+1])
#     }
#     stopifnot(all(x5))
#     print(paste0(x1,' passed'))
#   }
#   TRUE
# }

#decode step
#' @export
cidecnFun <- function(typex=c('D','S','T','F'),nn='cicntnd') {
  # - [ ] decodes count(brk,typex) - should not be a step
  getgd(nn)
  x1 <- cidecn(typex=typex,cicntnd=cicntnd)
  cidecnd <<- cbind(cicntnd[,.(rcunique=rcunique)],x1)
  #browser()
  putt(cidecnd)
}

#steps for prezzo3ipf
#' @export
xx020Fun <- function(nn='ppm2td') {
  getgd(nn)
  x1 <- ppm2td[,.('qp1'=quantile(ppm2t,probs=c(.01)),'q1'=quantile(ppm2t,probs=c(.25)),'q3'=quantile(ppm2t,probs=c(.75)),'qp99'=quantile(ppm2t,probs=c(.99)),q2=median(ppm2t)),date][,.(date,qp1,q1,q3,qp99,iqr=q3/q1,ipr=qp99/qp1)]
  setnames(x1,c('date','      1','   25','   75','   99','75/25','99/ 1'))
  xx020 <<- melt(x1,id.var='date')
  putt(xx020)
}

#030
#' @export
xx030aFun <- function(nn=c('ppm2td')) {
  getgd(nn)
  npc <- ppm2td[,length(unique(rcode))]
  x1a <- setkey(ppm2td,date,ppm2t)[date==as.Date('2001-12-31'),][round(npc*.25)]
  x1b <- setkey(ppm2td,date,ppm2t)[date==as.Date('2001-12-31'),][round(npc*.75)]
  rv1 <- x1b[,ppm2t]/x1a[,ppm2t]
  x2a <- setkey(ppm2td,date,ppm2t)[date==as.Date('2017-09-30'),][round(npc*.25)]
  x2b <- setkey(ppm2td,date,ppm2t)[date==as.Date('2017-09-30'),][round(npc*.75)]
  rv2 <- x2b[,ppm2t]/x2a[,ppm2t]
  xx030a <<- data.table(x1a1=x1a[,irregpcode(rcode)],x1a2=x1a[,signif(ppm2t,3)],x1b1=x1b[,irregpcode(rcode)],x1b2=x1b[,signif(ppm2t,3)],
                        x2a1=x2a[,irregpcode(rcode)],x2a2=x2a[,signif(ppm2t,3)],x2b1=x2b[,irregpcode(rcode)],x2b2=x2b[,signif(ppm2t,3)])
  putt(xx030a)
}

#030b
#' @export
xx030bFun <- function(nn=c('ppm2td')) {
  getgd(nn)
  x1 <- ppm2td[,.('qp1'=quantile(ppm2t,probs=c(.01)),'q1'=quantile(ppm2t,probs=c(.25)),'q3'=quantile(ppm2t,probs=c(.75)),'qp99'=quantile(ppm2t,probs=c(.99)),q2=median(ppm2t)),date][,.(date,qp1,q1,q3,qp99,iqr=q3/q1,ipr=qp99/qp1)]
  setnames(x1,c('date','      1','   25','   75','   99','75/25','99/ 1'))
  x2 <- unfactordt(melt(x1,id.var='date'))
  xx030b <<- x2
  putt(xx030b)
}

#040
#' @export
xx040dFun <- function(nn=c('dezod','ppm2td')) {
  getgd(nn)
  x1 <- dezod[,.(zo,r1y)]
  x2 <- ppm2td[date==as.Date('2016-09-30'),.(rcode,ppm2t)]
  xx040d <<- x1[x2,on=c(zo='rcode')]
  putt(xx040d)
}

#041
#' @export
xx041Fun <- function(nn=c('dezod','ppm2td')) {
  getgd(nn)
  x1 <- dezod[,.(zo,r1y)]
  x2 <- ppm2td[date==as.Date('2016-09-30'),.(rcode,ppm2t)]
  x3 <- x1[x2,on=c(zo='rcode')][,area:=irregpcode(substr(zo,1,3))]
  xx041d <<- x3
  putt(xx041d)
}

#' @export
aggbyareaFun <- function(nn=c('dezoint2d','dezospdfd')) {
  getgd(nn)
  aggbyaread <<- aggbyarea(dezoint2d,dezospdfd)
  putt(aggbyaread)
}

#module table stores functions converted to script
#' @export
motabFun <- function(nn='dep1d',nnxx=nnxxd()) {
  getgd(nn)
  #stopifnot(exists('motabd'))
  motabd <<- rbindlist(lapply(paste0('px',nnxx),motab,depd=dep1d))
  putt(motabd)
}

#' @export
pxxxtdFun <- function(nn='dep1d') {
  getgd(nn)
  x1 <- dep1d[,.(subject=step)][,pxxxtd(subject)[,.(depends=step)],subject]
  pxxxtdd <<- x1
  putt(pxxxtdd)
}

#dependency mapping
#' @export
dep0Fun <- function(x=c("os", "lr", "ep", "jo", "zo", "xv", "so", "ce", "pv", "g1",
                        "fo", "hu", "g2", "fu", "ci", "cj", "pr", "de", "mo")) {
  x1 <- rbindlist(lapply(x,dep0a))
  x2 <- x1[,.(step=gsub('d$','',step),dep=gsub('d$','',dep))]
  dep0d <<- x2
  putt(dep0d)
}

#dependency mapping
#' @export
dep1Fun <- function(
  nnpp=c('01os','02lr','03ep','04jo','05zo','06xv','07so','08ce','09pv','10g1','11fo','12hu','13g2','14fu','15ci','16cj','18pr','17de','20mo') #include everything ../px|nnpp|/pxpp.R
  ,
  collapsex=', '
  ,
  libpath='../aappdF/px'
) {
  # - [ ] parses functions to record dependencies for [pre]...Fun, sorted by auto-run-sequence - replaces depFun
  nn <- lapply(nnpp,substr,1,2)
  #stopifnot(identical(lapply(lapply(nn,as.numeric),zeroprepend,ntot=2),nn)&&all.equal(unlist(nn),sort(unlist(nn))))
  stopifnot(identical(lapply(lapply(nn,as.numeric),zeroprepend,ntot=2),nn))
  ll <- list(nnpp)
  for(i in seq_along(nnpp)) {
    ll[[i]] <- dep1(pre=substr(nnpp[i],3,4),filename=paste0(libpath,nnpp[i],'/R/px',substr(nnpp[i],3,4),'.R'),collapsex=collapsex)
    ll[[i]][,nn:=nn[i]][,seq:=i] #nn is first part of pkg name; seq is sequence number within pkg
  }
  x0 <- rbindlist(ll)[,iseq:=1:.N] #iseq is sequence within all
  dep1d <<- x0 #lacks stage,step
  putt(dep1d)
}

#pxmobkFun <- function(nn = c('pxmobjd', 'dep0d'),verbose=F) {
#' @export
dep2Fun <- function(nn = c('pxmobjd', 'dep0d'),verbose=F) {
getgd(nn)
  #from final steps trace back dependencies
  #dep0d has a short list of external dependencies 'bj'
  x1 <- pxmobjd[, .(step = unique(step))]  #final steps used in output
  stopifnot(all(x1[, step] %in% dep0d[, unique(step)])) #required are listed
  #1 trace outputs to raw inputs
  x2 <- setkey(copy(dep0d), step)
  levx <- 1
  x4 <-    x2[x1][, lev := levx] #level 1 are the final outputs used in hugo x4
  x3a <-  x2[!x1] #initialise 'done', thus partitioning x1 between x4 and x3
  x3 <-   x4[, .(todo = unique(setdiff(dep, c('', step))))] #initialise this level's dep
  while (0 < nrow(x3) & levx < 20) {
    #dep becomes step
    levx <- levx + 1
    if (verbose)
      print(paste0('transferring ', paste0(x3[, todo], collapse = ',')))
    x4 <-  rbind(x4, x3a[x3, .(step, dep, lev = levx)]) #add this round of steps
    x3a <- x3a[!x3] #update 'done'
    x3 <-  x4[, .(todo = unique(setdiff(dep, c('', step))))] #next round = dep not yet in step
  }
  #2 trace raw inputs to final outputs
  x5 <- setkey(x4[, .(lev = max(lev)), by = .(step, dep)][order(lev, step)], step) #initialise 'remaining'
  x0 <- x5[dep == '', .(step = unique(step))] #initial steps with no dep
  x6 <- x5[x0, .(step, dep, lev = 0)] #assign 'done'
  x5 <- x5[!x0] #assign 'remaining'
  levx <- 0
  while (0 < nrow(x5) & levx < 40) {
    levx <- levx + 1
    x0 <-  x5[, .(all(dep %in% x6[, step])), step][V1 == T, .(step = unique(step))] #steps with all dep satisfied
    if (verbose)
      print(paste0('transfer steps: ', x0[, paste0(step, collapse = ',')]))
    x6 <- rbind(x6, x5[x0, .(step, dep, lev = levx)])
    x5 <- x5[!x0] #update 'remaining'
  }
  stopifnot(all(pxmobjd[, step] %in% x6[, unique(step)])) #
  x7 <-  x6[, .(lev = min(lev)), step][order(lev), .(step, stage = as.numeric(as.factor(lev)))]
  dep2d <<- x7 #ordered steps: each stage depends only on prior stages; required steps are in step
  putt(dep2d)
}


#' @export
#pxmoblFun <- function(nn=c('pxmobjd','pxmobkd','dep0d')) {
dep3Fun <- function(nn=c('pxmobjd','dep2d','dep0d')) {
getgd(nn)
  stopifnot(all(pxmobjd[,step]%in%dep2d[,step]))
  x1 <- dep0d[dep2d,on=c(step='step')]
  x2 <- x1[x1,on=c(dep='step'),allow=T][!is.na(step)][i.dep!='']
  stopifnot(x2[,all(i.stage<stage)])  #2 each step is satisfied by previous stages
  dep3d <<- x1
  putt(dep3d)
}

#' @export
#pxmobmFun <- function(nn=c('pxmobld','dep1d')) {
dep4Fun <- function(nn=c('dep3d','dep1d')) {
getgd(nn)
  dep4d <<- dep1d[unique(dep3d[,.(step,stage)]),on=c(step='step')][,runseq:=1:.N]
  putt(dep4d)
}

#' @export
#pxmobnFun <- function(nn=c('pxmobmd')) {
dep5Fun <- function(nn=c('dep4d'),ncachestep=20) { #low ncachestep uses less mem
getgd(nn)
  y1 <- copy(dep4d)
  y2 <- setkey(y1[,strsplit(depends,', '),runseq],runseq) #dependencies of this runseq
  y3 <- setkey(y1[,y1[((.I+1):min(nrow(y1),.I+ncachestep)),sort(unique(unlist(strsplit(depends,', '))))],runseq],runseq) #dependencies of following runstep
  y4 <- y1[,.(runseq)][,setdiff(y2[.(.I),V1],y3[.(.I),V1]),runseq] #current dependencies not in following (=garbage)
  y5 <- y4[,.(garbage=paste0(V1,collapse=',')),runseq]
  dep5d <<- y5[y1,on=c(runseq='runseq')]
  putt(dep5d)
}


#' @export
pars1Fun <- function() {
x1 <- structure(list(ii = c(42L, 12L, 13L, 14L, 15L, 16L, 17L, 20L, 
21L, 23L, 24L, 26L, 28L, 29L, 31L, 32L, 34L, 35L, 36L, 37L, 43L, 
49L, 50L, 52L, 53L, 54L, 33L, 48L, 41L, 38L, 19L, 56L, 55L, 58L, 
27L, 27L, 64L, 38L, 1L, 1L, 1L, 1L, 39L, 40L, 3L, 3L, 3L, 3L, 
3L, 3L, 3L, 3L, 3L, 3L, 3L, 3L, 3L, 3L, 3L, 3L, 3L, 3L, 3L, 3L, 
30L, 30L, 33L, 63L, 59L, 61L, 25L, 19L, 10L, 48L, 1L, 25L, 4L, 
5L, 46L, 9L, 1L, 18L, 1L, 47L, 60L, 62L, 39L, 1L, 6L, 38L, 7L, 
33L, 40L, 38L, 33L, 33L, 27L, 40L, 51L, 57L, 33L, 11L, 50L, 41L, 
22L, 50L, 22L, 39L, 33L, 30L, 30L, 7L), driver = c(".", ".", 
".", ".", ".", ".", ".", ".", ".", ".", ".", ".", ".", ".", ".", 
".", ".", ".", ".", ".", ".", ".", ".", ".", ".", ".", ".", ".", 
".", ".", ".", ".", ".", ".", ".", ".", ".", ".", ".", ".", ".", 
".", ".", ".", ".", ".", ".", ".", ".", ".", ".", ".", ".", ".", 
".", ".", ".", ".", ".", ".", ".", ".", ".", ".", ".", ".", ".", 
".", ".", ".", ".", ".", ".", ".", ".", ".", ".", ".", ".", ".", 
".", ".", ".", ".", ".", ".", ".", ".", ".", ".", ".", ".", ".", 
".", ".", ".", ".", ".", ".", ".", ".", ".", ".", ".", ".", ".", 
".", ".", ".", ".", ".", "."), iseq = c(1L, 1L, 1L, 1L, 1L, 1L, 
1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 
1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 
1L, 1L, 1L, 1L, 1L, 1L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 
11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L, 20L, 1L, 1L, 1L, 
1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 
1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 
1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 2L, 1L), sname = c("dezo", 
"prpp", "setda", "extda", "yxpr3", "zone", "distance", "spriore", 
"tpriore", "yxfo3", "yxfi3", "xv", "xvij", "xv9", "yxta", "yxtap", 
"ldgx", "scox", "dec", "segex", "yielddl", "varco", "scofo", 
"segsum", "fosum", "fisum", "celi", "varp", "corodec", "xtran", 
"linke", "ptc", "ptc", "ptc", "xvm", "xvm", "civo", "xtran", 
"pp", "pp", "pp", "pp", "rma", "wima", "seg", "seg", "seg", "seg", 
"seg", "seg", "seg", "seg", "seg", "seg", "seg", "seg", "seg", 
"seg", "seg", "seg", "seg", "seg", "seg", "seg", "ijse", "ijse", 
"celi", "cinbr", "cinoue", "cinoup", "yxxv", "neare", "rpp", 
"varp", "coho", "yxxv", "seg", "seg", "varyx", "extda", "pp", 
"neare", "pp", "varf", "cinoue", "cinoup", "rma", "pp", "pco", 
"xtran", "yxpr1", "celi", "wima", "xtran", "celi", "celi", "xvm", 
"wima", "buifo", "ptc", "celi", "expp", "scofo", "corodec", "priore", 
"scofo", "priore", "rma", "celi", "ijse", "ijse", "yxpr1"), pname = c("", 
"-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", 
"-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "center", 
"ci", "comp", "da", "delim", "deltasp", "deltatp", "deltatpce", 
"domean", "dorank", "epsvo", "exponentiate", "fnam", "fnam2", 
"frac", "fromraw", "i", "i", "ipc", "ipc", "ipc", "ipc", "ipc", 
"ipc", "ipc", "ipc", "ipc", "ipc", "ipc", "ipc", "ipc", "ipc", 
"ipc", "ipc", "ipc", "ipc", "ipc", "ipc", "is", "it", "k", "k1br", 
"k1e", "k1p", "maxp", "maxsep", "minprice", "n.ahead", "nco", 
"nfold", "nmin", "nminhard", "norm", "nper", "nrows", "num", 
"overlap", "p", "p1e", "p1p", "path", "pcgrep", "pcodir", "percent", 
"period", "radius", "rcoded", "rcx", "rcx1", "ref", "rescale", 
"rmad", "rmom", "rollptc", "rotate", "seg.ipc", "shiftstn", "skip", 
"sstrength", "stnd", "tstrength", "uprojargs", "win", "xvijd", 
"xvijd", "zone"), pvalue = c("", "", "", "", "", "", "", "", 
"", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
"", "", "FALSE", "0.68", "ret", "", "-", "0", "0", "-3", "TRUE", 
"TRUE", ".000001", "", "lr\\\\pp-complete.csv", "lr\\\\ppd_data_withcols.csv", 
"1", "TRUE", "", "", "DE1", "NG1", "NG2", "NG3", "NG4", "NG5", 
"NG6", "NG7", "NG8", "NG9", "NG10", "SE21", "SE11", "SE17", "SE16", 
"TW10", "W7", "W1", "W10", "WC", "", "", "3", "20", "100", "50", 
"8", "100", "10000", "6", "20", "5", "5000", "4000", "", "100", 
"-1", "6", "TRUE", "1", ".05", ".05", "os", ".+", "codepo", "", 
"quarter", "30", "", "", "SW-6--", "SW-6--", "TRUE", "", "5", 
"FALSE", "mean", "1", "4", "1", "5.00E+05", "2002-06-30", "5.00E+04", 
"#NAME?", "18", "7", "6", "1"), pmode = c("", "", "", "", "", 
"", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
"", "", "", "", "", "logical", "numeric", "character", "", "character", 
"numeric", "numeric", "numeric", "logical", "logical", "numeric", 
"", "character", "character", "numeric", "logical", "", "", "character", 
"character", "character", "character", "character", "character", 
"character", "character", "character", "character", "character", 
"character", "character", "character", "character", "character", 
"character", "character", "character", "character", "", "", "numeric", 
"numeric", "numeric", "numeric", "numeric", "numeric", "numeric", 
"numeric", "numeric", "numeric", "numeric", "numeric", "character", 
"numeric", "numeric", "numeric", "logical", "numeric", "numeric", 
"numeric", "character", "character", "character", "", "character", 
"numeric", "", "", "character", "character", "logical", "", "numeric", 
"logical", "character", "numeric", "numeric", "numeric", "numeric", 
"character", "numeric", "character", "numeric", "numeric", "numeric", 
"numeric"), desc = c("", "uses seg.nmin,seg.nminhard", "", "", 
"", "", "", "", "", "", "", "", "uses yxxv.maxp", "uses yxxv.maxp", 
"", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
"", "", "", "", "", "", "", "", "pp csv file in rdroot()", "pp csv file with headers path append to rdroot()", 
"fraction remaining after decimation", "read static data from text file", 
"to be determined", "to be determined", "postcodes included", 
"postcodes included", "postcodes included", "postcodes included", 
"postcodes included", "postcodes included", "postcodes included", 
"postcodes included", "postcodes included", "postcodes included", 
"postcodes included", "postcodes included", "postcodes included", 
"postcodes included", "postcodes included", "postcodes included", 
"postcodes included", "postcodes included", "postcodes included", 
"postcodes included", "", "", "", "", "", "", "", "", "", "", 
"min in cohort", "", "minimum per zone", "minimum for root node", 
"", "forecast periods", "rows to read", "", "leave this true - based on a misunderstanding of what inczod::level does", 
"VAR order", "", "", "map subdir of rdroot()", "postcode filter for initial reads", 
"postcode coordinates subdir of rdroot()", "", "sample interval", 
"", "to be determined", "", "", "", "", "to be determined", "years residual momentum", 
"roll index estimation", "", "", "periods shift for stn date", 
"", "", "manual stn date", "", "", "", "", "", "postcode zone to view"
), values = c("", "", "", "", "", "", "", "", "", "", "", "", 
"", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
"", "", "", "", "", "deltat", "", "", "voronoi colinearity epsilon", 
"", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
"", "", "", "", "", "", "", "", "", "", "", "", "", "", "neighbours", 
"abnret # records", "ppm2 # records", "", "", "", "", "", "", 
"", "", "", "", "", "", "", "", "abnret tail trim", "ppm2 tail trim", 
"", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
"", "", "", "", "", "", "", "", "", "")), row.names = c(NA, -112L
), class = "data.frame")
pars <<- as.data.table(x1)
putt(pars)
}


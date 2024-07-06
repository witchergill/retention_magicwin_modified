combo.data.bizz<-data.frame()
net.agent<-raw.agnt.data[raw.agnt.data$site=='BIZZ',1:3]

for (i in 1:nrow(net.agent)) {
  z<-range_read(retention_data_bizz,sheet = net.agent$Agent[i],range = "A:C",col_names = T)
  #z$ClientName<-as.character(unlist(z$ClientName))
  z$UserID<-as.character(unlist(z$`User ID`))
  z$agent<-net.agent$Agent[i]
  z<-z[!is.na(z[,3]),]
  combo.data.bizz<-rbind(combo.data.bizz,z)
}

combo.data.bizz$`ClientName`<-unlist(combo.data.bizz$`ClientName`)

combo.data.bizz<-combo.data.bizz[!duplicated(combo.data.bizz$`User ID`),]

for(i in 1:nrow(net.agent)){
range_write(retention_data_club,data = combo.data.club[combo.data.club$agent==net.agent$Agent[i],1:2],sheet = net.agent$Agent[i],range = "A:B",col_names = T)
}




#summaraising 

combo.data.games%>%
  group_by(`agent`)%>%
  dplyr::summarise(count=n())%>%
  View()


#finding duplicates
  View(combo.data.games[duplicated(combo.data.games$`User ID`),])





dumm.id
dum.table<-data.frame('User ID'=dumm.id)


View(left_join(dum.table,data.net,by = "User ID"))












################ date code



for(i in 1:nrow(net.agent)){
  Sys.sleep(2) #wait for 2 sec this is a cooldown period
  success=T
  while (success) {
    looping=T
    tryCatch(assign('calls.pre',as.numeric(str_extract_all(range_read_cells(net.agent$link[i],sheet = 'data 1',range ='M:M',discard_empty = T, )$loc[-1],pattern = "\\d+")),envir = .GlobalEnv),error=function(e){
      Sys.sleep(10)
      
      print('error')
      assign('looping',FALSE,envir = .GlobalEnv)
    })
    if(looping){
      success=F
    }
  }
  
  
  
  if(length(calls.pre)==0){
    next              #skipping to next sheet if there is no calls done
  }
  
  success=T
  while (success) {
    looping=T
    tryCatch(assign('calls.pre.date',as.numeric(str_extract_all(range_read_cells(net.agent$link[i],sheet = 'data 1',range ='R:R',discard_empty = T, )$loc[-1],pattern = "\\d+")),envir = .GlobalEnv),error=function(e){
      Sys.sleep(10)
      print('error')
      assign('looping',FALSE,envir = .GlobalEnv)
    })
    if(looping){
      success=F
    }
  }
  
  
  #filtering the data where date is not pasted
  pre.blnk.dates<-calls.pre[!(calls.pre%in%calls.pre.date)]
  
  if(length(pre.blnk.dates)==0 | length(pre.blnk.dates)>=150){
    next
  }
  
  #pasting the today's date into the sheets
  for(j in 1:length(pre.blnk.dates)){
    success=T
    while (success) {
      looping=T
      tryCatch(range_write(net.agent$link[i],data = as.data.frame(Sys.Date()),sheet = 'data 1',range = str_c('R',pre.blnk.dates[j]),col_names = F,reformat = F),error=function(e){
        Sys.sleep(10)
        print('error')
        assign('looping',FALSE,envir = .GlobalEnv)
      })
      if(looping){
        success=F
      }
    }
    
    Sys.sleep(1)
  }
  
  
  
}

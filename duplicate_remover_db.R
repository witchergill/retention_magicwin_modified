combo.data.biz<-data.frame()
net.agent<-raw.agnt.data[raw.agnt.data$site=='BIZZ',1:3]

for (i in 1:nrow(net.agent)) {
  z<-range_read(retention_data_bizz,sheet = net.agent$Agent[i],range = "A:C",col_names = T)
  z$ClientName<-as.character(unlist(z$ClientName))
  z$UserID<-as.character(unlist(z$UserID))
  z$agent<-net.agent$Agent[i]
  z<-z[!is.na(z[,3]),]
  combo.data.biz<-rbind(combo.data.biz,z)
}

combo.data.biz$`ClientName`<-unlist(combo.data.biz$`ClientName`)

combo.data.biz<-combo.data.biz[!duplicated(combo.data.biz$`User ID`),]

for(i in 1:nrow(net.agent)){
range_write(retention_data_club,data = combo.data.club[combo.data.club$agent==net.agent$Agent[i],1:2],sheet = net.agent$Agent[i],range = "A:B",col_names = T)
}

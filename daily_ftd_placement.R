#getting the data from the drive for .games
#retrieving the data from the local storage 
data.games<-read_xlsx("C:/Users/dell/Documents/bizz_games_khlr_club_net/data.games.xlsx",header = T)

#reading the data of the agents whom data is delivered
agents<-c('ikra','kiran')
old.ftd.data<-data.frame()
for(i in 1:length(agents)){
  
  old.ftd.data<-rbind(old.ftd.data,range_read(retention_data_games,sheet = agents[i],col_names = T))
  
}

old.ftd.data<-na.omit(old.ftd.data[,'User ID'])

data.games.userid<-na.omit(data.games[data.games$`First DepositOn (Date)`>=Sys.Date()-5,c('Master',"User ID")])

new.ftd.data<-data.games.userid[!(data.games.userid$`User ID`%in%old.ftd.data$`User ID`),]

#reordering the data into random order
new.ftd.data<-new.ftd.data[sample(1:nrow(new.ftd.data),nrow(new.ftd.data)),]
#writing the new ftd data to the agents
x.allt.data<-data.frame()
for(i in 1:length(agents)){
  x.allt.data<-new.ftd.data[((nrow(x.allt.data)+ifelse(i>1,1,0)):(floor(nrow(new.ftd.data)/length(agents))+nrow(x.allt.data))),]
  sheet_append(retention_data_games,data = x.allt.data,sheet = agents[i])
}

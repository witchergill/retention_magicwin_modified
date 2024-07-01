# placing daily ftd from the sheets

#daily ftd for games
#getting the games.dataset
data.games<- read_xlsx(str_c("C:/Users/dell/Documents/Backup_codes/Daily_Data/clients_"
                       ,str_sub(Sys.Date(),-2,-1),"_",tolower(str_sub(months(Sys.Date()),1,3)),".xlsx"))

#replacing + and whitespace with ""
data.games$`Phone No`<-str_replace_all(data.games$`Phone No`,"\\+","")
data.games$`Phone No`<-str_replace_all(data.games$`Phone No`," ","")
#data.games$`Phone No`<-str_extract(data.games$`Phone No`,"\\d+")


#selecting the last number alloted to the ftd sheet
last.number<-range_read(retention_data_games,sheet = 'FTD',range = "B:B",col_names = T)
last.number<-last.number$`User ID`[nrow(last.number)]

#now extracting all the ftds which are not alloted to the ftd sheet
new.games.ftd.data<-data.games[1:which(data.games$`User ID`==last.number),]
#selecting only ftds
new.games.ftd.data<-new.games.ftd.data[new.games.ftd.data$`First Deposit`!=0 & new.games.ftd.data$`Last Deposit`==0,]

#writing the new ftd data
range_write(retention_data_games,data = arrange(new.games.ftd.data[-nrow(new.games.ftd.data),c(2,4,10,11,12,13,14,18)],`First DepositOn (Date)`),sheet = 'FTD',range = 'A:H',col_names = T,reformat = F)

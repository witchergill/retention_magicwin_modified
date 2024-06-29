retention_data<-"https://docs.google.com/spreadsheets/d/1aFVHPMelya8IJeKwCs_VILRLmIJawPE53Snk5hvRzzY/edit#gid=2048906413"
retention_data_club<-"https://docs.google.com/spreadsheets/d/1eq2BRxXG6DIIaFRkLngeQufJ1oj6cMOEJj3_lEuDD0k/edit#gid=1020407375"
retention_data_bizz<-"https://docs.google.com/spreadsheets/d/10iWJ-rqEc5Gy6aWoxJ5VW_OQmB3fssFUsWpBc2KWEks/edit#gid=220908430"
retention_data_games<-"https://docs.google.com/spreadsheets/d/1Nmlk4iX1XtbTgGrfTEl2s6IgPCA9HaWA28xjV3zsaq0/edit#gid=1082323624"
agents_data<-"https://docs.google.com/spreadsheets/d/1j0rlgY3gNQudLqh3fbgPuseB3ZtdzZbMNfrWL2cg0Hk/edit#gid=1443504558"



library(tidyverse)
library(googlesheets4)
library(googledrive)
library(openxlsx2)
library(quantmod)

googledrive::drive_auth('uc.kheloyaar@gmail.com',cache = ".secrets")
googlesheets4::gs4_auth(token = drive_token())



while(T){
  
  #writing the data of web scrapped data into a google sheet
  a<-Sys.time()
  
  if(as.numeric(seconds(Sys.time())-seconds(a))>=180){
    
    success=T
    while(success){
      info<-as.numeric(read.csv('c:/users/dell/documents/info.csv',header = T)[1,2])
      looping=T
      
      tryCatch(range_write(ss = "1EDJbezLibqPImch_5l8Bltl0kiwk_MgJB3-GpD83pdw",data = as.data.frame(info),range = "A1",col_names = F,reformat = F),error=function(e){
        
        Sys.sleep(6)
        print('error')
        assign('looping',FALSE,envir = .GlobalEnv)
        
      })
      
      if(looping){
        success=F
      }
      
    }
    
  }
  
  
  
  
  success=T
  while(success){
    looping=T
    
    tryCatch(assign('meth',as.character(range_read(agents_data,sheet = 'retention_modi',range = 'Z1',col_names = F)),envir = .GlobalEnv),error=function(e){
      
      Sys.sleep(6)
      print('error')
      assign('looping',FALSE,envir = .GlobalEnv)
      
    })
    
    if(looping){
      success=F
    }
    
  }
  
  #retrieving the agents data from the agents data sheet
  if((meth=='SHOW'|meth=="UPDATE")){
    
    success=T
    while(success){
      looping=T
      
      tryCatch(assign('agnt.data',range_read(agents_data,sheet = 'retention_modi',range = 'A:H',col_names = T),envir = .GlobalEnv),error=function(e){
        
        Sys.sleep(6)
        print('error')
        assign('looping',FALSE,envir = .GlobalEnv)
        
      })
      
      if(looping){
        success=F
      }
      
    }
    
    raw.agnt.data<-agnt.data
    agnt.data<-agnt.data[which(agnt.data$allotment==TRUE),]
    
    if(nrow(agnt.data)>0){               #further conditions if their is data in the agents.data data.frame
      
      for(k in 1:nrow(agnt.data)){
        
        #importing the data for the agent
        success=T
        while(success){
          looping=T
          
          tryCatch(assign('data',range_read(ifelse(agnt.data$site[k]=="NET",retention_data,ifelse(agnt.data$site[k]=="CLUB",yes = retention_data_club,ifelse(agnt.data$site[k]=='BIZZ',retention_data_bizz,retention_data_games))),sheet = agnt.data$Agent[k],range = "A:H",col_names = T),envir = .GlobalEnv),error=function(e){
            
            Sys.sleep(6)
            print('error')
            assign('looping',FALSE,envir = .GlobalEnv)
            
          })
          
          if(looping){
            success=F
          }
          
        }
        
        
        
        #for retention data allotment
        
        if(agnt.data$Data.Type[k]=='RET'){            #this is the condition for retention data where from and to date are used
          
          if(agnt.data$site[k]=="BIZZ"|agnt.data$site[k]=="CLUB"){
            
            #working process if the data is of site club and bizz
            
            data.ret<-data[data$LastDeposit!=0,]
            #data.ret<-data.ret[,-2] 
            #data.ret<-data.ret[(data.ret$LastDepositOn>=agnt.data$From.Date[i] & data.ret$LastDepositOn<=agnt.data$To.Date),]          #shifting to old concept
            
            require(plyr)
            data.ret<-arrange(data.ret,desc(data.ret$LastDepositOn))
            
            #selecting 7 days before dataset   data.s is the dataframe filtered accord to dates
            data.s<-data.frame()
            for(i in 1:nrow(data.ret)){
              if((is.na(data.ret$LastDepositOn[i]) & is.na(data.ret$FirstDepositOn[i]))){
                next
              }else{
                
                
                if(is.na(data.ret$LastDepositOn[i])){
                  if(data.ret$FirstDepositOn[i]<=Sys.Date()-10){
                    data.s<-rbind(data.s,data.ret[i,])
                  }else{
                    next
                  }
                  
                }else{
                  if(data.ret$LastDepositOn[i]<=Sys.Date()-10){
                    data.s<-rbind(data.s,data.ret[i,])
                  }else{
                    next
                  }
                }
              }
            }
            #ton ton das
            
            #now filtering the duplicates based on the user specific number of data to not be repeated
            #retrieving all the userids on which user have worked
            data.user<-range_read(ss = agnt.data$link[k],sheet = agnt.data$data.source[k],col_names = T,range = "C:C")
            
            if(nrow(data.user)<1200){
              data.user<-data.user[1:nrow(data.user),]
            }else{
              data.user<-data.user[(nrow(data.user)-1200):nrow(data.user),]
            }
            
            
            colnames(data.user)<-"id"
            #now removing all the userids from the agents data.ret database which he already worked upon
            data.s<-data.s[!(data.s$`UserID`%in% data.user$id),]
            
            from.date<-min(data.s$LastDepositOn[1:200])
            to.date<-max(data.s$LastDepositOn[1:200])
            
            if(meth=="SHOW"){         #checking whether client want to check the number of records for the particular dates or wants to write directly
              success=T
              while(success){
                looping=T
                
                tryCatch(range_write(agents_data,data = as.data.frame(nrow(data.s)),sheet = 'retention_modi',range = str_c('I',1+which(raw.agnt.data$Agent==agnt.data$Agent[k])),col_names = F,reformat = F),error=function(e){
                  
                  Sys.sleep(6)
                  print('error')
                  assign('looping',FALSE,envir = .GlobalEnv)
                  
                })
                
                if(looping){
                  success=F
                }
                
              }
              
              
              
            }else{
              
              success=T
              while(success){
                looping=T
                
                tryCatch(sheet_append(agnt.data$link[k],data = data.s[1:200,],sheet = agnt.data$data.source[k]),error=function(e){
                  
                  Sys.sleep(6)
                  print('error')
                  assign('looping',FALSE,envir = .GlobalEnv)
                  
                })
                
                if(looping){
                  success=F
                }
                
              }
              
              
            }
            
            
            
          }else{
            
            #working process if the data is of site net and games
            
            data.ret<-data[data$`Last Deposit`!=0,]
            #filtering data from the user specif dates concept is removed
            #data.ret<-data.ret[(data.ret$`Last DepositOn (Date)`>=agnt.data$From.Date[i] & data.ret$`Last DepositOn (Date)`<=agnt.data$To.Date),]
            
            require(plyr)
            data.ret<-arrange(data.ret,desc(data.ret$`Last DepositOn (Date)`))
            
            #selecting 7 days before dataset   data.s is the dataframe filtered accord to dates
            data.s<-data.frame()
            for(i in 1:nrow(data.ret)){
              if((is.na(data.ret$`Last DepositOn (Date)`[i]) & is.na(data.ret$`First DepositOn (Date)`[i]))){
                next
              }
              if(is.na(data.ret$`Last DepositOn (Date)`[i])){
                if(data.ret$`First DepositOn (Date)`[i]<=Sys.Date()-10){
                  data.s<-rbind(data.s,data.ret[i,])
                }else{
                  next
                }
                
              }else{
                if(data.ret$`Last DepositOn (Date)`[i]<=Sys.Date()-10){
                  data.s<-rbind(data.s,data.ret[i,])
                }else{
                  next
                }
              }
            }
            
            #now filtering the duplicates based on the user specific number of data to not be repeated
            #retrieving all the userids on which user have worked
            data.user<-range_read(ss = agnt.data$link[k],sheet = "1-2 Mnths",col_names = T,range = "B:B")
            
            if(nrow(data.user)<1200){
              data.user<-data.user[1:nrow(data.user),]
            }else{
              data.user<-data.user[(nrow(data.user)-1200):nrow(data.user),]
            }
            
            #data.user<-data.user[(nrow(data.user)-indx.user.data):nrow(data.user),]
            colnames(data.user)<-"id"
            #now removing all the userids from the agents data.ret database which he already worked upon
            data.s<-data.s[!(data.s$`User ID`%in% data.user$id),]
            
            #reading data from the agents sheet
            
            from.date<-min(data.s$`Last DepositOn (Date)`[1:200])
            to.date<-max(data.s$`Last DepositOn (Date)`[1:200])
            
            if(meth=="SHOW"){         #checking whether client want to check the number of records for the particular dates or wants to write directly
              success=T
              while(success){
                looping=T
                
                tryCatch(range_write(agents_data,data = as.data.frame(nrow(data.s)),sheet = 'retention_modi',range = str_c('I',1+which(raw.agnt.data$Agent==agnt.data$Agent[k])),col_names = F,reformat = F),error=function(e){
                  
                  Sys.sleep(6)
                  print('error')
                  assign('looping',FALSE,envir = .GlobalEnv)
                  
                })
                
                if(looping){
                  success=F
                }
                
              }
              
              
              
            }else{
              
              success=T
              while(success){
                looping=T
                
                tryCatch(sheet_append(agnt.data$link[k],data = data.s[1:200,-8],sheet = agnt.data$data.source[k]),error=function(e){          #pasting only 200 data on agent's dataset
                  
                  Sys.sleep(6)
                  print('error')
                  assign('looping',FALSE,envir = .GlobalEnv)
                  
                })
                
                if(looping){
                  success=F
                }
                
              }
              
              
            }
            
            
          }
          
          
        }
        
        #for the combined allotment
        
        if(agnt.data$Data.Type[k]=='COMB'){
          
          if(agnt.data$site[k]=="BIZZ"|agnt.data$site[k]=="CLUB"){
            
            #working process if the data is of site club and bizz
            
            data.ftd<-data[data$LastDeposit==0,]
            #data.ftd<-data.ftd[,-2]
            data.ftd<-data.ftd[(data.ftd$FirstDepositOn>=agnt.data$From.Date[k] & data.ftd$FirstDepositOn<=agnt.data$To.Date[k]),]
            
            if(meth=="SHOW"){         #checking whether client want to check the number of records for the particular dates or wants to write directly
              success=T
              while(success){
                looping=T
                
                tryCatch(range_write(agents_data,data = as.data.frame(nrow(data.ftd)),sheet = 'retention_modi',range = str_c('I',1+which(raw.agnt.data$Agent==agnt.data$Agent[k])),col_names = F,reformat = F),error=function(e){
                  
                  Sys.sleep(6)
                  print('error')
                  assign('looping',FALSE,envir = .GlobalEnv)
                  
                })
                
                if(looping){
                  success=F
                }
                
              }
              
              
              
            }else{
              
              success=T
              while(success){
                looping=T
                
                tryCatch(sheet_append(agnt.data$link[k],data = data.ftd,sheet = "FTD"),error=function(e){
                  
                  Sys.sleep(6)
                  print('error')
                  assign('looping',FALSE,envir = .GlobalEnv)
                  
                })
                
                if(looping){
                  success=F
                }
                
              }
              
              
            }
            
            
            
          }else{
            
            #working process if the data is of site net and games
            data.ftd<-data
            
            
            data.s<-data.frame()
            for(i in 1:nrow(data.ftd)){
              if((is.na(data.ftd$`Last DepositOn (Date)`[i]) & is.na(data.ftd$`First DepositOn (Date)`[i]))){
                next
              }
              if(is.na(data.ftd$`Last DepositOn (Date)`[i])){
                if(data.ftd$`First DepositOn (Date)`[i]<=Sys.Date()){          #only 5 days for combo.case
                  data.s<-rbind(data.s,data.ftd[i,])
                }else{
                  next
                }
                
              }else{
                if(data.ftd$`Last DepositOn (Date)`[i]<=Sys.Date()){
                  data.s<-rbind(data.s,data.ftd[i,])
                }else{
                  next
                }
              }
            }
            #coalesce function 
            #data.s<-arrange(data,desc(coalesce(data$`Last DepositOn (Date)`,data$`First DepositOn (Date)`)))
            data.s$ndate<-coalesce(data.s$`Last DepositOn (Date)`,data.s$`First DepositOn (Date)`)
            #data.s<-data[data$`Last Deposit`==0,]
            data.s<-arrange(data.s,desc(data.s$ndate))
            #data.s<-data.s[(data.s$ndate>=agnt.data$From.Date[k] & data.s$ndate<=agnt.data$To.Date[k]),]
            #now filtering the duplicates based on the user specific number of data to not be repeated
            #retrieving all the userids on which user have worked
            data.user<-range_read(ss = agnt.data$link[k],sheet = "FTD",col_names = T,range = "B:B")
            
            if(nrow(data.user)<1200){
              data.user<-data.user[1:nrow(data.user),]
            }else{
              data.user<-data.user[(nrow(data.user)-1200):nrow(data.user),]
            }
            
            colnames(data.user)<-"id"
            #now removing all the userids from the agents data.ret database which he already worked upon
            data.s<-data.s[!(data.s$`User ID`%in% data.user$id),]
            
            #reading data from the agents sheet
            
            from.date<-min(data.s$`Last DepositOn (Date)`[1:200])
            to.date<-max(data.s$`Last DepositOn (Date)`[1:200])
            
            if(meth=="SHOW"){         #checking whether client want to check the number of records for the particular dates or wants to write directly
              success=T
              while(success){
                looping=T
                
                tryCatch(range_write(agents_data,data = as.data.frame(nrow(data.s)),sheet = 'retention_modi',range = str_c('I',1+which(raw.agnt.data$Agent==agnt.data$Agent[k])),col_names = F,reformat = F),error=function(e){
                  
                  Sys.sleep(6)
                  print('error')
                  assign('looping',FALSE,envir = .GlobalEnv)
                  
                })
                
                if(looping){
                  success=F
                }
                
              }
              
              
              
            }else{
              
              success=T
              while(success){
                looping=T
                
                tryCatch(sheet_append(agnt.data$link[k],data = data.s[1:200,-8:-9],sheet = "FTD"),error=function(e){
                  
                  Sys.sleep(6)
                  print('error')
                  assign('looping',FALSE,envir = .GlobalEnv)
                  
                })
                
                if(looping){
                  success=F
                }
                
              }
              
              
            }
            
            
            
          }
        }
        
        #for FTD only data updation
        if(agnt.data$Data.Type[k]=='FTD'){
          
          if(agnt.data$site[k]=="BIZZ"|agnt.data$site[k]=="CLUB"){
            
            #working process if the data is of site club and bizz
            
            data.ftd<-data[data$LastDeposit==0,]
            #data.ftd<-data.ftd[,-2]
            data.ftd<-data.ftd[(data.ftd$FirstDepositOn>=agnt.data$From.Date[k] & data.ftd$FirstDepositOn<=agnt.data$To.Date[k]),]
            
            if(meth=="SHOW"){         #checking whether client want to check the number of records for the particular dates or wants to write directly
              success=T
              while(success){
                looping=T
                
                tryCatch(range_write(agents_data,data = as.data.frame(nrow(data.ftd)),sheet = 'retention_modi',range = str_c('I',1+which(raw.agnt.data$Agent==agnt.data$Agent[k])),col_names = F,reformat = F),error=function(e){
                  
                  Sys.sleep(6)
                  print('error')
                  assign('looping',FALSE,envir = .GlobalEnv)
                  
                })
                
                if(looping){
                  success=F
                }
                
              }
              
              
              
            }else{
              
              success=T
              while(success){
                looping=T
                
                tryCatch(sheet_append(agnt.data$link[k],data = data.ftd,sheet = "FTD"),error=function(e){
                  
                  Sys.sleep(6)
                  print('error')
                  assign('looping',FALSE,envir = .GlobalEnv)
                  
                })
                
                if(looping){
                  success=F
                }
                
              }
              
              
            }
            
            
            
          }else{
            
            #working process if the data is of site net and games
            data.ftd<-arrange(data,desc(coalesce(data$`Last DepositOn (Date)`,data$`First DepositOn (Date)`)))
            data.ftd$ndate<-coalesce(data$`Last DepositOn (Date)`,data$`First DepositOn (Date)`)
            #data.ftd<-data[data$`Last Deposit`==0,]
            data.ftd<-data.ftd[(data.ftd$ndate>=agnt.data$From.Date[k] & data.ftd$ndate<=agnt.data$To.Date[k]),]
            
            if(meth=="SHOW"){         #checking whether client want to check the number of records for the particular dates or wants to write directly
              success=T
              while(success){
                looping=T
                
                tryCatch(range_write(agents_data,data = as.data.frame(nrow(data.ftd)),sheet = 'retention_modi',range = str_c('I',1+which(raw.agnt.data$Agent==agnt.data$Agent[k])),col_names = F,reformat = F),error=function(e){
                  
                  Sys.sleep(6)
                  print('error')
                  assign('looping',FALSE,envir = .GlobalEnv)
                  
                })
                
                if(looping){
                  success=F
                }
                
              }
              
              
              
            }else{
              
              success=T
              while(success){
                looping=T
                
                tryCatch(sheet_append(agnt.data$link[k],data = data.ftd[,-8],sheet = ""),error=function(e){
                  
                  Sys.sleep(6)
                  print('error')
                  assign('looping',FALSE,envir = .GlobalEnv)
                  
                })
                
                if(looping){
                  success=F
                }
                
              }
              
              
            }
            
            
            
          }
        }
        
        
        #pasting the to and from dates
        to.fro.date<-data.frame('a'=from.date,'b'=to.date)
        
        success=T
        while(success){
          looping=T
          
          tryCatch(range_write(agents_data,data = to.fro.date,sheet = 'retention_modi',range = str_c('F',1+which(raw.agnt.data$Agent==agnt.data$Agent[k]),":","G",1+which(raw.agnt.data$Agent==agnt.data$Agent[k])),col_names = F,reformat = F),error=function(e){
            
            Sys.sleep(6)
            print('error')
            assign('looping',FALSE,envir = .GlobalEnv)
            
          })
          
          if(looping){
            success=F
          }
          
        }
        
      }
      
      success=T
      while(success){
        looping=T
        
        tryCatch(range_clear(agents_data,sheet = 'retention_modi',range = "D2:D200"),error=function(e){
          
          Sys.sleep(6)
          print('error')
          assign('looping',FALSE,envir = .GlobalEnv)
          
        })
        
        if(looping){
          success=F
        }
        
      }
      
      
    }
    #removing the alloted text from meth and from the sheet
    success=T
    while(success){
      looping=T
      
      tryCatch(range_write(agents_data,data = as.data.frame(FALSE),sheet = 'retention_modi',range = 'Z1',col_names = F),error=function(e){
        
        Sys.sleep(6)
        print('error')
        assign('looping',FALSE,envir = .GlobalEnv)
        
      })
      
      if(looping){
        success=F
      }
      
    }
    
    
    success=T
    while(success){
      looping=T
      
      tryCatch(range_clear(agents_data,sheet = 'retention_modi',range = 'K2'),error=function(e){
        
        Sys.sleep(6)
        print('error')
        assign('looping',FALSE,envir = .GlobalEnv)
        
      })
      
      if(looping){
        success=F
      }
      
    }
    
  }
  
  
  Sys.sleep(3)
}

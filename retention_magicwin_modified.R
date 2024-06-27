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

  
  
  
  
  success=T
  while(success){
    looping=T
    
    tryCatch(assign('meth',as.character(range_read(agents_data,sheet = 'retention_modi',range = 'Z1',col_names = F)),envir = .GlobalEnv),error=function(e){
      
      Sys.sleep(6)
      print('error')
      looping=F
      
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
        looping=F
        
      })
      
      if(looping){
        success=F
      }
      
    }
    
    raw.agnt.data<-agnt.data
    agnt.data<-agnt.data[which(agnt.data$allotment==TRUE),]
    
    if(nrow(agnt.data)>0){               #further conditions if their is data in the agents.data data.frame
      
      for(i in 1:nrow(agnt.data)){
        
        #importing the data for the agent
        success=T
        while(success){
          looping=T
          
          tryCatch(assign('data',range_read(ifelse(agnt.data$site[i]=="NET",retention_data,ifelse(agnt.data$site[i]=="CLUB",yes = retention_data_club,ifelse(agnt.data$site[i]=='BIZZ',retention_data_bizz,retention_data_games))),sheet = agnt.data$Agent[i],range = "A:H",col_names = T),envir = .GlobalEnv),error=function(e){
            
            Sys.sleep(6)
            print('error')
            looping=F
            
          })
          
          if(looping){
            success=F
          }
          
        }
        
        
        #for retention data allotment
        
        if(agnt.data$Data.Type[i]=='RET'){            #this is the condition for retention data where from and to date are used
          
          if(agnt.data$site[i]=="BIZZ"|agnt.data$site[i]=="CLUB"){
            
            #working process if the data is of site club and bizz
            
            data.ret<-data[data$LastDeposit!=0,]
            #data.ret<-data.ret[,-2]
            data.ret<-data.ret[(data.ret$LastDepositOn>=agnt.data$From.Date[i] & data.ret$LastDepositOn<=agnt.data$To.Date),]
            
            if(meth=="SHOW"){         #checking whether client want to check the number of records for the particular dates or wants to write directly
              success=T
              while(success){
                looping=T
                
                tryCatch(range_write(agents_data,data = as.data.frame(nrow(data.ret)),sheet = 'retention_modi',range = str_c('I',1+which(raw.agnt.data$Agent==agnt.data$Agent[i])),col_names = F,reformat = F),error=function(e){
                  
                  Sys.sleep(6)
                  print('error')
                  looping=F
                  
                })
                
                if(looping){
                  success=F
                }
                
              }
              
              
              
            }else{
              
              success=T
              while(success){
                looping=T
                
                tryCatch(sheet_append(agnt.data$link[i],data = data.ret,sheet = agnt.data$data.source[i]),error=function(e){
                  
                  Sys.sleep(6)
                  print('error')
                  looping=F
                  
                })
                
                if(looping){
                  success=F
                }
                
              }
              
              
            }
            
            
            
          }else{
            
            #working process if the data is of site net and games
            
            data.ret<-data[data$`Last Deposit`!=0,]
            data.ret<-data.ret[(data.ret$`Last DepositOn (Date)`>=agnt.data$From.Date[i] & data.ret$`Last DepositOn (Date)`<=agnt.data$To.Date),]
            
            if(meth=="SHOW"){         #checking whether client want to check the number of records for the particular dates or wants to write directly
              success=T
              while(success){
                looping=T
                
                tryCatch(range_write(agents_data,data = as.data.frame(nrow(data.ret)),sheet = 'retention_modi',range = str_c('I',1+which(raw.agnt.data$Agent==agnt.data$Agent[i])),col_names = F,reformat = F),error=function(e){
                  
                  Sys.sleep(6)
                  print('error')
                  looping=F
                  
                })
                
                if(looping){
                  success=F
                }
                
              }
              
              
              
            }else{
              
              success=T
              while(success){
                looping=T
                
                tryCatch(sheet_append(agnt.data$link[i],data = data.ret[,-8],sheet = agnt.data$data.source[i]),error=function(e){
                  
                  Sys.sleep(6)
                  print('error')
                  looping=F
                  
                })
                
                if(looping){
                  success=F
                }
                
              }
              
              
            }
            
            
          }
          
          
        }
        
        #for the ftd allotment
        
        if(agnt.data$Data.Type[i]=='FTD'){
          
          if(agnt.data$site[i]=="BIZZ"|agnt.data$site[i]=="CLUB"){
            
            #working process if the data is of site club and bizz
            
            data.ftd<-data[data$LastDeposit==0,]
            #data.ftd<-data.ftd[,-2]
            data.ftd<-data.ftd[(data.ftd$FirstDepositOn>=agnt.data$From.Date[i] & data.ftd$FirstDepositOn<=agnt.data$To.Date),]
            
            if(meth=="SHOW"){         #checking whether client want to check the number of records for the particular dates or wants to write directly
              success=T
              while(success){
                looping=T
                
                tryCatch(range_write(agents_data,data = as.data.frame(nrow(data.ftd)),sheet = 'retention_modi',range = str_c('I',1+which(raw.agnt.data$Agent==agnt.data$Agent[i])),col_names = F,reformat = F),error=function(e){
                  
                  Sys.sleep(6)
                  print('error')
                  looping=F
                  
                })
                
                if(looping){
                  success=F
                }
                
              }
              
              
              
            }else{
              
              success=T
              while(success){
                looping=T
                
                tryCatch(sheet_append(agnt.data$link[i],data = data.ftd,sheet = "FTD"),error=function(e){
                  
                  Sys.sleep(6)
                  print('error')
                  looping=F
                  
                })
                
                if(looping){
                  success=F
                }
                
              }
              
              
            }
            
            
            
          }else{
            
            #working process if the data is of site net and games
            
            data.ftd<-data[data$`Last Deposit`==0,]
            data.ftd<-data.ftd[(data.ftd$`First DepositOn (Date)`>=agnt.data$From.Date[i] & data.ftd$`First DepositOn (Date)`<=agnt.data$To.Date),]
            
            if(meth=="SHOW"){         #checking whether client want to check the number of records for the particular dates or wants to write directly
              success=T
              while(success){
                looping=T
                
                tryCatch(range_write(agents_data,data = as.data.frame(nrow(data.ftd)),sheet = 'retention_modi',range = str_c('I',1+which(raw.agnt.data$Agent==agnt.data$Agent[i])),col_names = F,reformat = F),error=function(e){
                  
                  Sys.sleep(6)
                  print('error')
                  looping=F
                  
                })
                
                if(looping){
                  success=F
                }
                
              }
              
              
              
            }else{
              
              success=T
              while(success){
                looping=T
                
                tryCatch(sheet_append(agnt.data$link[i],data = data.ftd[,-8],sheet = agnt.data$data.source[i]),error=function(e){
                  
                  Sys.sleep(6)
                  print('error')
                  looping=F
                  
                })
                
                if(looping){
                  success=F
                }
                
              }
              
              
            }
            
            
            
          }
        }
        
        
      }
      
      success=T
      while(success){
        looping=T
        
        tryCatch(range_clear(agents_data,sheet = 'retention_modi',range = "D2:D200"),error=function(e){
          
          Sys.sleep(6)
          print('error')
          looping=F
          
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
        looping=F
        
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
        looping=F
        
      })
      
      if(looping){
        success=F
      }
      
    }
    
  }
  
  
  Sys.sleep(3)
}







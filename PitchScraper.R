require(readr)
require(data.table)
require(RMySQL)

import_db = dbConnect(MySQL(), user='############', password='############', dbname='############', host='############')
stats_db = dbConnect(MySQL(), user='############', password='############', dbname='############', host='############')

day_scrape <- function(import_db, stats_db, backdays) {
  
  Firstdate <- Sys.Date()-backdays
  
  ### Get List of pitchers who pitched since test date.
  query <- paste0("SELECT pitcher FROM ############ where gamedate >='",Firstdate,"' group by pitcher")
  rs = dbSendQuery(import_db, query)
  pitchers_to_get = fetch(rs, n=-1)
  ##
  ### ###
  
  ### setup progress bar
  pb <- txtProgressBar(min = 0, max = nrow(pitchers_to_get), style = 3)
  
  ### iterates through pitcher id list to download csv and import data to database and saves to file
  for(i in 1:nrow(pitchers_to_get))
  {
    ### make sure pitcher id is a number and make search URL
    pitcher <- as.numeric(pitchers_to_get[i,1])
    url <- paste0("https://baseballsavant.mlb.com/app/pitch-data/",pitcher)
    try(
      {
        ### Import CSV from webpage
        payload <- utils::read.csv(url, na.strings = c("null"))
        
        ### ### Filters by date:
        ##
        payload <- payload[ which(payload$game_date>=Firstdate ), ]
        ##
        ### ###
        
        ### Renames some columns
        setnames(payload, 
                old=c("name_display_first_last", "pitch_hand", "hitter_name_display_first_last", "bat_side", 
                      "polynomial_x_1", "polynomial_x_2", "polynomial_x_3", 
                      "polynomial_y_1", "polynomial_y_2", "polynomial_y_3", 
                      "polynomial_z_1", "polynomial_z_2", "polynomial_z_3" ), 
  
                new=c("pitcher_name", "throws", "batter_name", "stand", "x0", "x1", "x2", "y0", "y1", "y2", "z0", "z1", "z2"))
        
        ### Write date to DB and file
        dbWriteTable(stats_db, "############", payload, row.names = NA, append = TRUE)
        fwrite(payload, file = "############", na = "NULL", append = TRUE)
        
        ### update progress bar
        setTxtProgressBar(pb, i)
      }
    , silent = T)
  }
  
  ### completes progress bar
  close(pb)
}

build <- function(import_db, stats_db) {

  ### Get List of pitchers already downloaded.
  rs1 = dbSendQuery(stats_db, "SELECT pitcher FROM ############ group by pitcher")
  pitchers_had = fetch(rs1, n=-1)
  
  ### Get List of all pitchers.
  rs = dbSendQuery(import_db, "SELECT pitcher FROM ############ group by pitcher")
  pitchers_all = fetch(rs, n=-1)
  
  ### Make list of pitchers excluding those already downloaded
  pitchers_to_get <- as.data.frame(pitchers_all$pitcher[!pitchers_all$pitcher %in% pitchers_had$pitcher])
  colnames(pitchers_to_get) <- c("pitcher")
  
  ### setup progress bar
  pb <- txtProgressBar(min = 0, max = nrow(pitchers_to_get), style = 3)
  
  ### iterates through pitcher id list to download csv and import data to database and saves to file
  for(i in 1:nrow(pitchers_to_get))
  {
    ### make sure pitcher id is a number and make search URL
    pitcher <- as.numeric(pitchers_to_get[i,1])
    url <- paste0("https://baseballsavant.mlb.com/app/pitch-data/",pitcher)
    try(
      {
        ### Import CSV from webpage
        payload <- utils::read.csv(url, na.strings = c("null"))
        
        ### Renames some columns
        setnames(payload, 
                 old=c("name_display_first_last", "pitch_hand", "hitter_name_display_first_last", "bat_side", 
                       "polynomial_x_1", "polynomial_x_2", "polynomial_x_3", 
                       "polynomial_y_1", "polynomial_y_2", "polynomial_y_3", 
                       "polynomial_z_1", "polynomial_z_2", "polynomial_z_3" ), 
                 
                 new=c("pitcher_name", "throws", "batter_name", "stand", "x0", "x1", "x2", "y0", "y1", "y2", "z0", "z1", "z2"))
        
        ### Write date to DB and file
        dbWriteTable(stats_db, "############", payload, row.names = NA, append = TRUE)
        fwrite(payload, file = "############", na = "NULL", append = TRUE)
        
        ### update progress bar
        setTxtProgressBar(pb, i)
      }
      , silent = T)
  }
  
  ### completes progress bar
  close(pb)
}

day_scrape(import_db, stats_db, 1)
# build(import_db, stats_db)

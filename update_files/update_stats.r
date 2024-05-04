# ^ UPDATING THE STATS
# ! RUN THIS ONE LAST!!!!!!!!!!!!!!!!!!!!!!!!

# ^ WHEN TO RUN: probably everyday, but always AFTER updating the scheduling and players
# ^ SCRIPT GOAL: collect all player ids from recent games and gather all their statcast stats to be inserting into our 'pitches' table
# ^ INPUTS: no direct inputs from the user, we get all records from my database and the most updated table from the package
# ^ OUTPUT: write a csv file containing all of the stats from all players in between the specified dates
# ^ PACKAGE: baseballr is doing most of the heavy lifting in terms of collecting the data (https://rdrr.io/github/BillPetti/baseballr/man/baseballr-package.html)
# ^ TO RUN: press [CTRL + A] and then [CTRL + Enter]

library(baseballr)
library(dplyr)
library(tidyr)
library(ggplot2)
library(DBI)
library(RMySQL)

# I want to know how long this will take, so get a start time
start_time <- Sys.time()

# all print statements will be used for debugging, if something goes wrong I will know where it went wrong
print("Starting...")

get_max_date <- function(table_name = c("game_schedule", "pitches")){

    ############################
    # this function gets the max date of the specified table
    # as of now, it doesnt really matter what table I take it from but i am able to switch easily if i need to
    # INPUT: one of the two table names
    # OUTPUT: a single date that is the max of that table
    ############################


    # connect to the database
    con <- dbConnect(MySQL(),
                    user = "******",
                    password = "*******",
                    host = "*****",
                    dbname = "******")

    # get the day after the maximum date that is found in the table
    # this date is used as the start date for getting our updated data
    # if the max date is 09/03/2023, this query returns 09/04/2023
    query <- paste("select max(game_date) from", table_name, ";")

    result <- dbSendQuery(con, query)
    max_date <- fetch(result, n = -1)
    dbClearResult(result)
    rm(result)
    rm(query)
    dbDisconnect(con)
    rm(con)

    return(max_date)
}

# ^ make sure to update the schedule table first!!
get_game_ids <- function(){

  ####################################
  # this function gets all of the game ids from games that were not final and todays date
  # this gives me flexability to not have to run this script everyday, as it will capture all finished games between my last update and the current date
  # INPUT: None
  # OUTPUT: list of game ids that fell between the two dates
  ####################################

  con <- dbConnect(MySQL(),
                user = "******",
                password = "*******",
                host = "*****",
                dbname = "******")

  query <- paste("select game_id from game_schedule where game_status = 'Final' and game_date between '", max_date + 1, "' and '", Sys.Date(), "';", sep = "")

  result <- dbSendQuery(con, query)
  all_game_ids <- fetch(result, n = -1)
  dbClearResult(result)
  rm(result)
  rm(query)
  dbDisconnect(con)
  rm(con)

  return(all_game_ids)
}

get_lineup_ids <- function(id) {

  ####################################
  # there is a built in function that returns all players who appeared in the lineup for a given game
  # INPUT: game id (found from the function above)
  # OUTPUT: a list of the player ids that appeared in that game
  ####################################

  player_ids <- mlb_batting_orders(id, "all") %>%
    select(id)

  return(player_ids)
}

get_latest_stats <- function(id){
  
  ####################################
  # this is the main function that does most of the work
  # this function takes our starting date (from the 'get_max_date' function) <----  THESE DATES ARE THE SAME
  # and the current date (Sys.Date()) <-------------------------------------------
  # we use 'lapply' with this to loop through our ids
  # if a player did not play, their output is just NA and will be removed later
  # INPUT: a player ID
  # OUTPUT: statcast df for that player with all data between the specified dates
  ####################################
  
  
  
  
  # this is the function that collects our data for the listed dates
  # based on our functions used, the date range is the same date because we only want data from the previous day
  raw_data <- statcast_search(start_date = max_date + 1, 
                               end_date = Sys.Date(),
                               playerid = id,
                               player_type = 'batter')
  
  # we only want to keep the data where a player saw at least one pitch in the game
  # if a player did not play, then we change their value in the list to be NA so we can remove it easier later
  if(nrow(raw_data) > 0){
    # change the date from chr datatype to a date datatype
    raw_data$game_date <- as.Date(raw_data$game_date)
    raw_data
  }
  else{
    NA
  }
}

clean_data <- function(list_of_tibbles){

    ####################################
    # this function will clean our data and make sure it is ready to be inserted into our table
    # 1. remove any NA values in the list (this would be if someone didnt play, but since we are getting the lineups, this shouldnt be an issue)
    # 2. combine all of the dfs into one large df
    # 3. remove the unneccessary columns
    # 4. set missing values or empty fields to NA
    # INPUT: a list of tibbles generated from our statcast search function
    # OUTPUT: one large df containing all stats from all players
    ####################################

    # remove the NA values
    temp_df <- list_of_tibbles[!is.na(list_of_tibbles)]

    # combine into one
    all_stats <- do.call("rbind", temp_df)
    
    # remove all unneeded columns
    cols_to_remove <- c("spin_dir", "player_name", "spin_rate_deprecated", "break_angle_deprecated", "break_length_deprecated", "des", "tfs_deprecated",
            "tfs_zulu_deprecated", "fielder_2", "umpire", "vx0", "vy0", "vz0", "ax", "ay", "az", "sz_top", "sz_bot", "fielder_2", "fielder_3", "fielder_4",
            "fielder_5", "fielder_6", "fielder_7", "fielder_8", "fielder_9", "woba_value", "woba_denom", "iso_value", "home_score", "away_score", "post_fld_score",
            "post_home_score", "post_away_score", "if_fielding_alignment", "of_fielding_alignment", "delta_run_exp", "sv_id", "pitcher_1", "fielder_2_1")

    all_stats_df <- as.data.frame(all_stats)
    all_stats_clean <- all_stats_df[, -which(colnames(all_stats_df) %in% cols_to_remove)]
    
    # any NA values or empty fields get replaced with NA
    all_stats_clean[is.na(all_stats_clean)] <- NA
    all_stats_clean[all_stats_clean == ""] <- NA
    
    return(all_stats_clean)
}

# ! --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# ! --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# ! --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# ! --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# ! END OF FUNCTIONS
# ! SCRIPTING STARTS HERE
# ! --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# ! --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# ! --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# ! --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
print("Getting max date")
max_date <- as.Date(unlist(get_max_date("pitches")))


print("Getting relevent game ids")
all_game_ids <- get_game_ids()


print("Getting Lineups and Preparing IDs")
player_ids <- lapply(FUN = get_lineup_ids, all_game_ids$game_id)
ids <- unlist(player_ids)
lookups <- unique(ids)
lookups <- gsub("L", "", lookups)
lookups <- as.numeric(lookups)


print("Getting data, this could take a while...")
# the tryCatch will catch any error and return NA if we find an error
updated_data <- lapply(FUN = tryCatch(get_latest_stats, error = function(e) NA), lookups)

print("Data Gathered, cleaning data now")
data_to_write <- clean_data(updated_data)

print("Data is cleaned, writing to a table")
write.table(data_to_write, "/Users/drewc/OneDrive/Documents/2024/MLB Project 2024/update_files/mlb2024_updated_stats.csv", row.names = FALSE, col.names = FALSE, sep = ",",quote = TRUE)


end_time <- Sys.time()
print(paste("Time to complete: ", end_time - start_time))


# ^ UPDATING THE PLAYERS TABLE

# ^ WHEN TO RUN: probably everyday. guys are being sent down and called up every day. trades, free agent signings, injuries, etc
# ^ SCRIPT GOAL: find all of the mismatched records in my database and the most updated version from the baseballr package
# ^ INPUTS: no direct inputs from the user, we get all records from my database and the most updated table from the package
# ^ OUTPUT: write a csv file containing all of the records that did not match
# ^ PACKAGE: baseballr is doing most of the heavy lifting in terms of collecting the data (https://rdrr.io/github/BillPetti/baseballr/man/baseballr-package.html)
# ^ TO RUN: press [CTRL + A] and then [CTRL + Enter]


library(DBI)
library(RMySQL)
library(baseballr)
library(dplyr)
library(tidyr)

# ? #####################################################################################
# ? GET THE CURRENT VERSION OF MY PLAYERS TABLE
# ? #####################################################################################

# connect to the database
# send my query and fetch the results into the 'my_players' variable
# remove the variables used to get the data and then disconnect from the database
con <- dbConnect(MySQL(),
                user = "******",
                password = "*******",
                host = "*****",
                dbname = "******")


query <- "select * from players;"

result <- dbSendQuery(con, query)

# n = -1 means to return all rows, by default there is a limit to how many rows are returned
my_players <- fetch(result, n = -1)
dbClearResult(result)
rm(result)
rm(query)
dbDisconnect(con)
rm(con)


# ? #####################################################################################
# ? GET THE UPDATED VERSION OF THE PLAYERS TABLE
# ? #####################################################################################

# in order to get the rosters for each team, we need to get each of the team IDs
# doing it this way means that if any team ID changes for some reason, our code wont break
teams <- mlb_teams(season = 2024, sport_ids = c(1)) %>%
  select(team_id)

# create our own function that takes the team ID and returns the 40 man roster for that team in the 2024 season
# we also trim down the columns of this and only choose relevant columns
get_mlb_rosters <- function(x){
  
  trimmed_player_info_df <- mlb_rosters(team_id = x, season = 2024, roster_type = "fullRoster") %>%
    select(person_id, 
      person_full_name, 
      position_abbreviation, 
      position_type, 
      status_code, 
      status_description, 
      team_id)
  
  return(trimmed_player_info_df)
}

# we then use the function that was created above to collect player data
# use lapply to go through each team ID from the teams df we got above 
# this returns a list of data frames, then we can use rbind to combine them all into one dataframe
players_list <- lapply(X = teams$team_id, 
                       FUN = function(x){get_mlb_rosters(x)})

curr_players <- do.call("rbind", players_list)

# add this column that will allows us to properly look up a players stats based on what position they play
curr_players$statcast_pos_lookup <- ifelse(curr_players$position_type == "Pitcher", "pitcher", "batter")

# remove any duplicated players
# still unsure why this was happening...
curr_players <- curr_players[!duplicated(curr_players$person_id), ]

# remove "." or other special characters from the names, keep the "-" (dashes)
# iconv removes accent marks in our player names, keeps it consistent
curr_players$person_full_name <- iconv(curr_players$person_full_name, to = "ASCII//TRANSLIT")
curr_players$person_full_name <- gsub("\\.", "", curr_players$person_full_name)

# change the columns to match the ones found in our database
colnames(curr_players) <- c("player_id", "player_name", "position_abbrv", "position_type", "status_code", "status_description", "team_id", "statcast_pos_lookup")


# ? #####################################################################################
# ? FIND THE MISMATCHED ROWS AND WRITE THEM TO A FILE
# ? #####################################################################################

# this returns all of the players in the most updated list from mlb that do not have a match in my list
# this means that these players had some sort of change to them and I need to update my table
# I chose these three columns to look for because we can add a new player, change teams, or change status (injury, called up, etc)
mismatched_players <- anti_join(curr_players, my_players, by = c("player_id", "team_id", "status_code"))

# write this data to a csv file to be read and processed through Python
write.table(mismatched_players, "/Users/drewc/OneDrive/Documents/2024/MLB Project 2024/update_files/mlb2024_updated_players.csv", row.names = FALSE, col.names = FALSE, sep = ",",quote = FALSE)

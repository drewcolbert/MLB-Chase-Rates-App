# ^ UPDATING THE MLB SCHEDULE

# ^ WHEN TO RUN: probably everyday. games are played all the time, it will be easier to keep up with this everyday
# ^ SCRIPT GOAL: find all of the mismatched records in my database and the most updated version from the baseballr package
# ^ INPUTS: no direct inputs from the user, we get all records from my database and the most updated table from the package
# ^ OUTPUT: write a csv file containing all of the records that did not match
# ^ PACKAGE: baseballr is doing most of the heavy lifting in terms of collecting the data (https://rdrr.io/github/BillPetti/baseballr/man/baseballr-package.html)
# ^ TO RUN: press [CTRL + A] and then [CTRL + Enter]


# ! TODO: make it so I only have to query for a certain day (or range of days)
# ! TODO: there is no reason to be querying the entire schedule each time I need to make an update to it


library(baseballr)
library(dplyr)
library(tidyr)
library(ggplot2)
library(DBI)
library(RMySQL)
library(rvest)

# ? #####################################################################################
# ? GET THE CURRENT VERSION OF MY SCHEDULE
# ? #####################################################################################

# get all of the games currently in my database
con <- dbConnect(MySQL(),
                user = "******",
                password = "*******",
                host = "*****",
                dbname = "******")


query <- "select * from game_schedule;"

result <- dbSendQuery(con, query)
my_games <- fetch(result, n = -1)
dbClearResult(result)
rm(result)
rm(query)
dbDisconnect(con)
rm(con)

# ? #####################################################################################
# ? GET THE UPDATED VERSION OF THE SCHEDULE
# ? #####################################################################################

# ^ the following explanation is for the if-else code block below
# when we get the schedule data, we want to get the rescheduled data as well because it is nice to keep track of this
# i did this manually last year for the Red Sox and it sucks and makes things confusing
# however, the 'mlb_schedule' function does not return these columns by default
# it will only return these columns after a game has already been postponed or delayed or rescheduled
# what the code below does is check if those columns are present before selecting columns
# if ANY of those columnsare present, then we can add them to our select statement and move on to remaning the columns to match
# if NOT, then we exclude them from our select statement and add them manually after the fact, filling them with NA values
# it looks like a lot, but it isnt
# * TEST: is it possible for the rescheduled columns to appear without the resume ones and vice versa?

# get the reference table first
updated_games <- mlb_schedule(season = 2024) %>% 
  filter(series_description == "Regular Season",
        game_date <= Sys.Date()) 

# these are the columns to check for
columns_to_check <- c("reschedule_game_date", "rescheduled_from_date", "resume_game_date", "resumed_from_date")

# run our check, and return the proper data frame
if (any(columns_to_check %in% colnames(updated_games))) {
  updated_games <- updated_games %>%
      select(game_pk, 
      season_display, 
      date,
      official_date, 
      double_header, 
      scheduled_innings, 
      day_night,
      games_in_series, 
      series_game_number, 
      series_description, 
      if_necessary_description,
      status_detailed_state,
      teams_away_score, 
      teams_away_series_number, 
      teams_away_league_record_wins, 
      teams_away_league_record_losses, 
      teams_away_team_id, 
      teams_home_score,
      teams_home_series_number,
      teams_home_league_record_wins, 
      teams_home_league_record_losses,
      teams_home_team_id,
      venue_name,
      reschedule_game_date,
      rescheduled_from_date)

    colnames(updated_games) <- c("game_id", "season_year", "game_date", "official_game_date", "double_header", "scheduled_innings", "day_night", "games_in_series",
                             "series_game_number", "series_description", "if_necessary_description", "game_status", "away_team_score",
                            "away_team_series_num", "away_team_wins", "away_team_losses", "away_team_id", "home_team_score","home_team_series_num", "home_team_wins",
                            "home_team_losses", "home_team_id", "venue", "reschedule_game_date", "rescheduled_from_date") 
} else {
  updated_games <- updated_games %>%
      select(game_pk, 
      season_display, 
      date,
      official_date, 
      double_header, 
      scheduled_innings, 
      day_night,
      games_in_series, 
      series_game_number, 
      series_description, 
      if_necessary_description,
      status_detailed_state,
      teams_away_score, 
      teams_away_series_number, 
      teams_away_league_record_wins, 
      teams_away_league_record_losses, 
      teams_away_team_id, 
      teams_home_score,
      teams_home_series_number,
      teams_home_league_record_wins, 
      teams_home_league_record_losses,
      teams_home_team_id,
      venue_name)

    colnames(updated_games) <- c("game_id", "season_year", "game_date", "official_date", "double_header", "scheduled_innings", "day_night", "games_in_series",
                             "series_game_number", "series_description", "if_necessary_description", "game_status", "away_team_score",
                            "away_team_series_num", "away_team_wins", "away_team_losses", "away_team_id", "home_team_score","home_team_series_num", "home_team_wins",
                            "home_team_losses", "home_team_id", "venue")
    
    updated_games$reschedule_game_date <- rep(NA, nrow(updated_games))
    updated_games$rescheduled_from_date <- rep(NA, nrow(updated_games))
    updated_games$resume_game_date <- rep(NA, nrow(updated_games))
    updated_games$resumed_from_date <- rep(NA, nrow(updated_games))
}

# ? #####################################################################################
# ? FIND THE MISMATCHED ROWS AND WRITE THEM TO A FILE
# ? #####################################################################################

# find the games that are different and need to be updated
mismatched_games <- anti_join(updated_games, my_games, by = c("game_id", "game_status", "away_team_score", "home_team_score"))

# when games get postponed, they will show up as duplicated entries in our mismatched games (both the postponed game info and the final game info are included, both having the same game id)
# when we enter this data into our DB, we will get a duplicate key
# since the row that says postponed has less info than the final game, we only the one where the game went final
# it still tells us when the game was supposed to be played and the date it actually was plyaed
mismatched_games <- mismatched_games %>%
  filter(game_status != "Postponed")

# write those games to a file
write.table(mismatched_games, "/Users/drewc/OneDrive/Documents/2024/MLB Project 2024/update_files/mlb2024_updated_games.csv", row.names = FALSE, col.names = FALSE, sep = ",",quote = TRUE)

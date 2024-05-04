library(baseballr)
library(dplyr)
library(tidyr)
library(ggplot2)
library(DBI)
library(RMySQL)
library(rvest)


start_time <- Sys.time()

# Setting for the 2024 MLB Season. I want to go bigger and better this year


## WHAT IS THE GOAL??? this needs to be figured out and defined
# gambling?
# predictions?
# tracking certain stats?


# I already have a database set up, but right now it only includes data from the Red Sox # nolint # nolint: line_length_linter.
# if this can be set up for all teams that would be amazing  # nolint
# figure out the DB schema.. set it all up beforehand, don't just wing it


# i also want this to somehow dynamically change a teams roster if someone gets traded, cut, injured, etc # nolint
# my idea rn is to pull the data from my db and pull data from baseballr and if something has changed then update the db # nolint
# run anti-join.. update db with those values where player_id = player_id
# feels very inefficient and does not tell me when these things happened...


# figuring out a way to get projected starters and lineups would be great as well.... # nolint


# when i am preprocessing the statcast data, i need to insert the team id into where the team abbreviation is in the data # nolint
# replace "BOS" with the team_id

# * -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# * -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

##### GATHERING DATA #####

# first things first, gather all teams with team IDs
# the sport ID is 1 to get all mlb teams
# we are trimming down the excess columns to include only (or potentially) relevant columns # nolint
teams <- mlb_teams(season = 2024, sport_ids = c(1)) %>%
  select(team_id, 
    team_full_name, 
    team_abbreviation, 
    club_name, 
    league_name, 
    division_name)

# ? write.table(teams, "/ProgramData/MySQL/MySQL Server 8.0/Uploads/mlb2024_teams.csv", row.names = FALSE, col.names = FALSE, sep = ",", quote = FALSE)


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

players <- do.call("rbind", players_list)

players$statcast_pos_lookup <- ifelse(players$position_type == "Pitcher", "pitcher", "batter")

players <- players[!duplicated(players$person_id), ]

# remove "." or other special characters from the names, keep the "-" (dashes)
# iconv removes accent marks in our player names, keeps it consistent
players$person_full_name <- iconv(players$person_full_name, to = "ASCII//TRANSLIT")
players$person_full_name <- gsub("\\.", "", players$person_full_name)


# ? write.table(players, "/ProgramData/MySQL/MySQL Server 8.0/Uploads/mlb2024_players.csv", row.names = FALSE, col.names = FALSE, sep = ",", quote = FALSE)

# * -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# * -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# this gives us the full schedule for all teams
# includes the game ids and info about the series and the teams records
# regular season only

full_schedule <- mlb_schedule(season = 2024) %>% 
  filter(series_description == "Regular Season")

columns_to_check <- c("reschedule_game_date", "rescheduled_from_date", "resume_game_date", "resumed_from_date")

if (any(columns_to_check %in% colnames(full_schedule))) {
  full_schedule <- full_schedule %>%
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
      rescheduled_from_date,
      resume_game_date,
      resumed_from_date) %>%
    # the '~' character indicates that this is a function to be applied to all of the columns
    mutate_all(~ifelse(is.na(.), "\\N", .)) # a lot of columns will be NA, we want them to be '\N' in our table so they get uploaded as NULL in MySQL

    colnames(full_schedule) <- c("game_id", "season_year", "game_date", "double_header", "scheduled_innings", "day_night", "games_in_series",
                             "series_game_number", "series_description", "if_necessary_description", "game_status", "away_team_score",
                            "away_team_series_num", "away_team_wins", "away_team_losses", "away_team_id", "home_team_score", "home_team_series_num", "home_team_wins",
                            "home_team_losses", "home_team_id", "venue", "reschedule_game_date", "rescheduled_from_date", "resume_game_date", "resumed_from_date") 
} else {
  full_schedule <- full_schedule %>%
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
      venue_name) %>%
    # the '~' character indicates that this is a function to be applied to all of the columns
    mutate_all(~ifelse(is.na(.), "\\N", .)) # a lot of columns will be NA, we want them to be '\N' in our table so they get uploaded as NULL in MySQL

    colnames(full_schedule) <- c("game_id", "season_year", "game_date", "official_date", "double_header", "scheduled_innings", "day_night", "games_in_series",
                             "series_game_number", "series_description", "if_necessary_description", "game_status", "away_team_score",
                            "away_team_series_num", "away_team_wins", "away_team_losses", "away_team_id", "home_team_score", "home_team_series_num", "home_team_wins",
                            "home_team_losses", "home_team_id", "venue")

    full_schedule$reschedule_game_date <- rep("\\N", nrow(full_schedule))
    full_schedule$rescheduled_from_date <- rep("\\N", nrow(full_schedule))
    full_schedule$resume_game_date <- rep("\\N", nrow(full_schedule))
    full_schedule$resumed_from_date <- rep("\\N", nrow(full_schedule))
}

write.table(full_schedule, "/ProgramData/MySQL/MySQL Server 8.0/Uploads/mlb2024_schedule.csv", row.names = FALSE, col.names = FALSE, sep = ",",quote = FALSE)



# this does not give me a team id
# so i can get a team id by:
  # using the game_id to get the home and away teams
  # use the player id to reference the player table and get the team id that way
    # the problem with this is that i need to keep track of if the player is a pitcher or hitter because this does not give me their id, only hitter and pitcher ids
test <- statcast_search("2023-05-01", "2023-09-01", 647351, player_type = "batter")




end_time <- Sys.time()
end_time - start_time



head(full_schedule)
View(head(full_schedule))

# * -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# * -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# for set-up i need to get data from the beginning of the year to now
# 1. get game ids for all games that have happened this year
# 2. get all lineups from those games using the game ids
# 3. get pitch data for each unqiue id that appears
# 4. write that to a csv
# 5. insert it into the pitches table
# 6. set up the automation for next time

get_max_date <- function(table_name = c("game_schedule", "pitches")){

    ############################
    # this function returns the day after the max date located in our table
    # this is used to ensure that we only get data from the previous game and do not get duplicate data from past games
    # the output is a single date value
    ############################


    # connect to the database
    con <- dbConnect(MySQL(),
                    user = "root",
                    password = "FOODS_test1",
                    host = "localhost",
                    dbname = "mlb_statcast_data")

    # get the day after the maximum date that is found in the table
    # this date is used as the start date for getting our updated data
    # if the max date is 09/03/2023, this query returns 09/04/2023
    query <- paste("select date_sub(max(game_date), interval -1 day) from", table_name, ";")

    result <- dbSendQuery(con, query)
    max_date <- fetch(result, n = -1)
    dbClearResult(result)
    rm(result)
    rm(query)
    dbDisconnect(con)
    rm(con)

    return(max_date)
}


max_date_game <- as.Date(unlist(get_max_date("game_schedule"))) - 1
max_pitch_date <- get_max_date("pitches")



con <- dbConnect(MySQL(),
                user = "root",
                password = "FOODS_test1",
                host = "localhost",
                dbname = "mlb_statcast_data")

query <- paste("select game_id from game_schedule where game_status = 'Final';")

result <- dbSendQuery(con, query)
all_game_ids <- fetch(result, n = -1)
dbClearResult(result)
rm(result)
rm(query)
dbDisconnect(con)
rm(con)


get_ids <- function(id) {

  player_ids <- mlb_batting_orders(id, "all") %>%
    select(id)

  return(player_ids)
}

y = rep('all', length(all_game_ids))

all_lineups <- lapply(FUN = get_ids, all_game_ids$game_id)

ids <- do.call("c", all_lineups)
lookups <- unique(ids)
lookups <- gsub("L", "", lookups)
lookups <- as.numeric(lookups)

get_latest_stats <- function(id){
  
  ####################################
  # this is the main function that does most of the work
  # this function takes our starting date (from the 'get_max_date' function) <----  THESE DATES ARE THE SAME
  # and the day before the current date (Sys.Date() - 1) <------------------------
  # we use 'mapply' with this to loop through our ids and lookup values (from the 'get_player_info' function)
  # the output is a list of dataframes containing each players statcast data
  # if a player did not play, their output is just NA and will be removed later
  ####################################
  
  
  
  
  # this is the function that collects our data for the listed dates
  # based on our functions used, the date range is the same date because we only want data from the previous day
  raw_data <- statcast_search(start_date = '2024-03-20', 
                               end_date = '2024-04-05',
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

latest_stats <- lapply(FUN = get_latest_stats, lookups)
latest_stats <- latest_stats[-which(latest_stats == "NA")]


all_stats <- do.call("rbind", latest_stats)
all_stats_df <- data.frame(all_stats)
typeof(all_stats_df)
View(head(all_stats_df))
nrow(all_stats_df)
length(names(all_stats_df))
cols_to_remove <- c("spin_dir", "player_name", "spin_rate_deprecated", "break_angle_deprecated", "break_length_deprecated", "des", "tfs_deprecated",
            "tfs_zulu_deprecated", "fielder_2", "umpire", "vx0", "vy0", "vz0", "ax", "ay", "az", "sz_top", "sz_bot", "fielder_2", "fielder_3", "fielder_4",
            "fielder_5", "fielder_6", "fielder_7", "fielder_8", "fielder_9", "woba_value", "woba_denom", "iso_value", "home_score", "away_score", "post_fld_score",
            "post_home_score", "post_away_score", "if_fielding_alignment", "of_fielding_alignment", "delta_run_exp", "sv_id", "pitcher_1", "fielder_2_1")

all_stats_new <- all_stats_df[, -which(names(all_stats_df) %in% cols_to_remove)]
length(names(all_stats_new))
nrow(all_stats_new)

all_stats_new[is.na(all_stats_new)] <- "\\N"
all_stats_new[all_stats_new == ""] <- "\\N"

write.table(all_stats_new, "/ProgramData/MySQL/MySQL Server 8.0/Uploads/mlb2024_stats.csv", row.names = FALSE, col.names = FALSE, sep = ",",quote = FALSE)













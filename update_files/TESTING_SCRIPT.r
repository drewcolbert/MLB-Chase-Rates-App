library(baseballr)
library(dplyr)
library(tidyr)
library(ggplot2)
library(DBI)
library(RMySQL)
library(rvest)

con <- dbConnect(MySQL(),
                user = "root",
                password = "FOODS_test1",
                host = "localhost",
                dbname = "mlb_statcast_data")


query <- "select * from game_schedule;"

result <- dbSendQuery(con, query)
my_games <- fetch(result, n = -1)
dbClearResult(result)
rm(result)
rm(query)
dbDisconnect(con)
rm(con)



old_games <- mlb_schedule(season = 2023) %>% 
  filter(series_description == "Regular Season") %>%
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
    teams_home_league_record_wins, 
    teams_home_league_record_losses,
    teams_home_team_id,
    venue_name,
    reschedule_game_date,
    rescheduled_from_date,
    resume_game_date,
    resumed_from_date) %>%
  mutate_all(~ifelse(is.na(.), "\\N", .))

View(old_games)



# get the reference table first
all_games <- mlb_schedule(season = 2024) %>%
  filter(series_description == "Spring Training")

# these are the columns to check for
columns_to_check <- c("reschedule_game_date", "rescheduled_from_date", "resume_game_date", "resumed_from_date")

# run our check, and return the proper data frame
if (any(columns_to_check %in% colnames(all_games))) {
  all_games <- all_games %>%
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
      resumed_from_date)

    colnames(all_games) <- c("game_id", "season_year", "game_date", "double_header", "scheduled_innings", "day_night", "games_in_series",
                             "series_game_number", "series_description", "if_necessary_description", "game_status", "away_team_score",
                            "away_team_series_num", "away_team_wins", "away_team_losses", "away_team_id", "home_team_score", "home_team_series_num", "home_team_wins",
                            "home_team_losses", "home_team_id", "venue", "reschedule_game_date", "rescheduled_from_date", "resume_game_date", "resumed_from_date") 
} else {
  all_games <- all_games %>%
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

    colnames(all_games) <- c("game_id", "season_year", "game_date", "official_date", "double_header", "scheduled_innings", "day_night", "games_in_series",
                             "series_game_number", "series_description", "if_necessary_description", "game_status", "away_team_score",
                            "away_team_series_num", "away_team_wins", "away_team_losses", "away_team_id", "home_team_score", "home_team_series_num", "home_team_wins",
                            "home_team_losses", "home_team_id", "venue")
    
    all_games$reschedule_game_date <- rep(NA, nrow(all_games))
    all_games$rescheduled_from_date <- rep(NA, nrow(all_games))
    all_games$resume_game_date <- rep(NA, nrow(all_games))
    all_games$resumed_from_date <- rep(NA, nrow(all_games))
}

View(all_games)



con <- dbConnect(MySQL(),
                user = "root",
                password = "FOODS_test1",
                host = "localhost",
                dbname = "TEST_mlb")


query <- "select * from game_schedule;"

result <- dbSendQuery(con, query)
my_games <- fetch(result, n = -1)
dbClearResult(result)
rm(result)
rm(query)
dbDisconnect(con)
rm(con)


mismatched_games <- anti_join(all_games, my_games, by = c("game_id", "game_status", "away_team_score", "home_team_score"))

# since a lot of these values will be missing, we need to change them to \N so they can be entered as NULL in the database
mismatched_games <- mismatched_games %>%
  mutate_all(~ifelse(is.na(.), "\\N", .))

write.table(mismatched_games, "/Users/drewc/OneDrive/Documents/2024/MLB Project 2024/TEST_mlb2024_updated_games.csv", row.names = FALSE, col.names = FALSE, sep = ",",quote = FALSE)

View(mismatched_games)




'''
constraint batter_id_fk foreign key (batter_id) references players(player_id),
constraint pitcher_id_fk foreign key (pitcher_id) references players(player_id),
constraint game_id_fk foreign key (game_id) references game_schedule(game_id)
'''
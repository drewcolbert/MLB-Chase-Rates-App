create database TEST_mlb;

use TEST_mlb;

-- teams table
create table teams (
team_id int NOT NULL, -- 111
team_full_name varchar(100), -- Boston Red Sox
team_abbrv varchar(3), -- BOS
team_short_name varchar(30), -- Red Sox
league_name varchar(40), -- American League
division_name varchar(50), -- American League East
constraint teams_pk primary key (team_id)
);


-- players table
create table players (
player_id int NOT NULL, -- 647351
player_name varchar(100), -- Abraham Toro
position_abbrv varchar(5), -- 1B
position_type varchar(15), -- Infielder, Pitcher, Catcher, Outfielder, Hitter, Two-Way Player
status_code varchar(5), -- A, NYR, etc
status_description varchar(50), -- Active, Not Yet Reported, etc
team_id int NOT NULL, -- 133
statcast_pos_lookup varchar(10), -- batter or pitcher
constraint players_pk primary key (player_id),
constraint team_player_fk foreign key (team_id) references teams(team_id)
);

drop table game_schedule;
-- schedule table
create table game_schedule (
game_id int NOT NULL, -- 745444
season_year int,
game_date datetime,
official_date datetime,
double_header char(1),
scheduled_innings tinyint,
day_night varchar(5),
games_in_series tinyint,
series_game_number tinyint,
series_description varchar(25),
if_necessary_description varchar(30),
game_status varchar(30),
away_team_score int default NULL,
away_team_series_num int,
away_team_wins int,
away_team_losses int,
away_team_id int,
home_team_score int default NULL,
home_team_series_num int,
home_team_wins int,
home_team_losses int,
home_team_id int,
venue varchar(50),
reschedule_game_date varchar(10) default NULL,
rescheduled_from_date varchar(10) default NULL,
resume_game_date varchar(10) default NULL,
resumed_from_date varchar(10) default NULL,
constraint games_pk primary key (game_id),
constraint away_team_fk foreign key (away_team_id) references teams(team_id),
constraint home_team_fk foreign key (home_team_id) references teams(team_id)
);


load data infile "\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\mlb2024_teams.csv" into table teams fields terminated by ",";
load data infile "\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\mlb2024_players.csv" into table players fields terminated by ",";
load data infile "\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\mlb2024_schedule.csv" into table game_schedule fields terminated by ",";


select * from game_schedule;

drop table current_games;
create temporary table current_games like game_schedule;
insert into current_games (game_id, 
							season_year, 
							game_date,
							official_date,
							double_header, 
							scheduled_innings , 
							day_night, 
							games_in_series,
							series_game_number, 
							series_description, 
							if_necessary_description, 
							game_status, 
							away_team_score,
							away_team_series_num, 
							away_team_wins, 
							away_team_losses, 
							away_team_id, 
							home_team_score,
							home_team_series_num,
							home_team_wins,
							home_team_losses, 
							home_team_id, 
							venue, 
							reschedule_game_date, 
							rescheduled_from_date)
values(745444,2024,'2024-03-20','2024-03-20','N',9,'night',2,1,'Regular Season','Normal Game','Final',100,1,1,0,119,2,1,0,1,135,"Gocheok Sky Dome",NULL,NULL);

update game_schedule
join current_games on game_schedule.game_id = current_games.game_id
set game_schedule.game_id = current_games.game_id,
	game_schedule.season_year = current_games.season_year,
	game_schedule.game_date = current_games.game_date,
	game_schedule.official_date = current_games.official_date,
	game_schedule.double_header = current_games.double_header,
	game_schedule.scheduled_innings = current_games.scheduled_innings,
	game_schedule.day_night = current_games.day_night,
	game_schedule.games_in_series = current_games.games_in_series,
	game_schedule.series_game_number = current_games.series_game_number,
	game_schedule.series_description = current_games.series_description,
	game_schedule.if_necessary_description = current_games.if_necessary_description,
	game_schedule.game_status = current_games.game_status,
	game_schedule.away_team_score = current_games.away_team_score,
	game_schedule.away_team_series_num = current_games.away_team_series_num,
	game_schedule.away_team_wins = current_games.away_team_wins,
	game_schedule.away_team_losses = current_games.away_team_losses,
	game_schedule.away_team_id = current_games.away_team_id,
	game_schedule.home_team_score = current_games.home_team_score,
	game_schedule.home_team_series_num = current_games.home_team_series_num,
	game_schedule.home_team_wins = current_games.home_team_wins,
	game_schedule.home_team_losses = current_games.home_team_losses,
	game_schedule.home_team_id = current_games.home_team_id,
	game_schedule.venue = current_games.venue,
	game_schedule.reschedule_game_date = current_games.reschedule_game_date,
	game_schedule.rescheduled_from_date = current_games.rescheduled_from_date;
    
select * from game_schedule where away_team_score is not null;
    
    
    
    
update game_schedule
join current_games on game_schedule.game_id = current_games.game_id
set game_schedule.venue_name = current_games.venue_name;
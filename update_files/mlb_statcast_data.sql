-- create the database
-- create database mlb_statcast_data;

-- must intialize the database to actually use it
use mlb_statcast_data;

grant all privileges on *.* to 'root'@'localhost';


------------------------------------------------------------------------------------------------------------------------------------------
-- create the tables
-- examples will be on the side


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

drop table transactions;
create table transactions (
player_id int NOT NULL, -- 647351
player_name varchar(100), -- Abraham Toro
position_abbrv varchar(5), -- 1B
position_type varchar(15), -- Infielder, Pitcher, Catcher, Outfielder, Hitter, Two-Way Player
status_code varchar(5), -- A, NYR, etc
status_description varchar(50), -- Active, Not Yet Reported, etc
team_id int NOT NULL, -- 133
statcast_pos_lookup varchar(10),
transaction_date datetime
);

drop table game_schedule;
-- schedule table
create table game_schedule (
game_id int NOT NULL, -- 745444
season_year int,
game_date datetime,
official_date datetime,
double_header char(1),
scheduled_innings int,
day_night varchar(5),
games_in_series int,
series_game_number int,
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


drop table pitches;
-- statcast stats table
create table pitches (
pitch_id int auto_increment NOT NULL,
pitch_type_abbrv char(2),
game_date datetime,
release_speed float,
release_pos_x float,
release_pos_z float,
batter_id int,
pitcher_id int,
at_bat_event varchar(40),
pitch_result_full varchar(30),
pitch_zone varchar(2),
game_type char(1),
batter_side char(1),
pitcher_throwing_hand char(1),
home_team char(3),
away_team char(3),
pitch_result char(3),
hit_location char(1),
batted_ball_type varchar(15),
balls_before_pitch smallint,
strikes_before_pitch smallint,
game_year smallint,
horizontal_movement float,
vertical_movement float,
plate_x float,
plate_z float,
on_3B char(6),
on_2B char(6),
on_1B char(6),
outs_when_up smallint,
inning_of_AB smallint,
inning_top_or_bottom char(3),
hit_location_x float,
hit_location_y float,
hit_distance smallint,
launch_speed float,
launch_angle float,
pitch_speed float,
pitch_spin_rate_at_release float,
release_extension float,
game_id int,
release_position_y float,
est_ba_using_launch_speedangle float,
est_woba_using_launch_speedangle float,
babip_value smallint,
launch_speedangle smallint,
at_bat_number smallint,
pitch_number_of_at_bat smallint,
pitch_type_full varchar(20),
pre_batter_team_score smallint,
field_team_score smallint,
post_batter_team_score smallint,
spin_axis smallint,
change_in_win_exp float,
constraint statcast_pk primary key (pitch_id)
);

load data infile "\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\mlb2024_teams.csv" into table teams fields terminated by ",";
load data infile "\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\mlb2024_players.csv" into table players fields terminated by ",";
load data infile "\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\mlb2024_schedule.csv" into table game_schedule fields terminated by ",";
load data infile "\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\mlb2024_stats.csv" 
into table pitches 
fields terminated by "," 
lines terminated by "\n"
(pitch_type_abbrv,
game_date,
release_speed,
release_pos_x,
release_pos_z,
batter_id,
pitcher_id,
at_bat_event,
pitch_result_full,
pitch_zone,
game_type,
batter_side,
pitcher_throwing_hand,
home_team,
away_team,
pitch_result,
hit_location,
batted_ball_type,
balls_before_pitch,
strikes_before_pitch,
game_year,
horizontal_movement,
vertical_movement,
plate_x,
plate_z,
on_3B,
on_2B,
on_1B,
outs_when_up,
inning_of_AB,
inning_top_or_bottom,
hit_location_x,
hit_location_y,
hit_distance,
launch_speed,
launch_angle,
pitch_speed,
pitch_spin_rate_at_release,
release_extension,
game_id,
release_position_y,
est_ba_using_launch_speedangle,
est_woba_using_launch_speedangle,
babip_value,
launch_speedangle,
at_bat_number,
pitch_number_of_at_bat,
pitch_type_full,
pre_batter_team_score,
field_team_score,
post_batter_team_score,
spin_axis,
change_in_win_exp);



-- CHASE RATES
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- GROUPED BY EACH PLAYER

-- GOAL: get the chases, swings, chase_rate for each team and include the team_abbrv for each player
-- using CTEs and multiple joins to accomplish this
-- CTEs are temporary table expressions that I can reference later on in my query
-- i need batter id in both of CTEs so i can join them together later on this value
with 
	-- create a CTE that contains the batter_id, player_name, and team_id
    -- the important part here is that this is derived from the pitches table so we are getting all players who are in that table already
    -- the CTE has batter_id, player_name, and team_id all grouped by the player, so only row for player
	team_ids as 
			(select batter_id, player_name, team_id 
			from pitches
			join players on players.player_id = pitches.batter_id
			group by batter_id
            ),
	
    -- create a CTE that has the batter_id, chases, and swings found from data in the pitches table
	chase_rates as
		(select batter_id,
        
				-- when the pitch location is outside of the typical strikezone AND the batter swung the bat, then we count it as a chase
				count(case when (plate_x < -0.705 or plate_x > 0.705 or plate_z < 1.5 or plate_z > 3.6) and 
						pitch_result_full in ('swinging_strike', 'hit_into_play', 'foul') then plate_x end) as chases,
				
				-- when the batter swings the bat it counts as a swing
				count(case when pitch_result_full in ('swinging_strike', 'hit_into_play', 'foul') then pitch_result_full end) as swings,
				
				-- get the total hits and at bats for each player, this will be used to calculate batting average
				count(case when at_bat_event in ('single', 'double', 'triple', 'home_run') then at_bat_event end) as hits,
				count(case when at_bat_event not in ('walk', 'hit_by_pitch', 'sac_fly', 'sac_bunt', 'sac_fly_double_play', 'catcher_interf') then at_bat_event end) as at_bats
		from pitches
		group by batter_id
		)
	
	-- this is our final select statement where we bring it all together
    -- each column in our select statement must have the table name we are referencing
   select team_ids.player_name, teams.team_abbrv, chase_rates.chases, chase_rates.swings, round(chase_rates.chases/chase_rates.swings, 3) as chase_pct, round(chase_rates.hits/chase_rates.at_bats, 3) as BA
   from team_ids
   
   -- multiple joins allow us to bring together all tables used
   join teams on team_ids.team_id = teams.team_id
   join chase_rates on chase_rates.batter_id = team_ids.batter_id
                
   -- group by the player name so we get one row per player and bring together all of their stats
	group by player_name
   
   -- we only want players who have played a decent amount this year, this number will change... unless I can do this dynamically
	having swings > 100
    
    order by chase_pct
    limit 10;
-----------------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- GROUPED BY TEAM

-- this first CTE gets all chases, swings, and chase % for each team
-- does so using a subquery
with chase_table as (
				
                -- using a subquery
				-- we have a union of the queries where a team is hitting at home vs the road
				-- we do this because we only have data for hitters currently
				-- if we just grouped by team, we would get chase rates for their pitchers too, and the data would not be accurate
				select 
					team,
					sum(chases) as chases,
					sum(swings) as swings,
					round(chases/swings, 3) as chase_pct,
                    round(sum(hits)/sum(at_bats), 3) as team_ba

				-- subquery starts here
				-- its the union of the query where we get the home team during the bottom of the inning, and the away team for the top of the inning
				from (
						select
							home_team as team,

							-- when the pitch location is outside of the typical strikezone AND the batter swung the bat, then we count it as a chase
							count(case when (plate_x < -0.705 or plate_x > 0.705 or plate_z < 1.5 or plate_z > 3.6) and 
									pitch_result_full in ('swinging_strike', 'hit_into_play', 'foul') then plate_x end) as chases,

							-- when the batter swings the bat it counts as a swing
							count(case when pitch_result_full in ('swinging_strike', 'hit_into_play', 'foul') then pitch_result_full end) as swings,
                            
                            -- get team hits and at bats for batting average
							count(case when at_bat_event in ('single', 'double', 'triple', 'home_run') then at_bat_event end) as hits,
							count(case when at_bat_event not in ('walk', 'hit_by_pitch', 'sac_fly', 'sac_bunt', 'sac_fly_double_play', 'catcher_interf') then at_bat_event end) as at_bats
						from 
							pitches

						where 
							inning_top_or_bottom = 'Bot'
						group by 
							home_team
						union
						select
							away_team as team,
							-- when the pitch location is outside of the typical strikezone AND the batter swung the bat, then we count it as a chase
							count(case when (plate_x < -0.705 or plate_x > 0.705 or plate_z < 1.5 or plate_z > 3.6) and 
									pitch_result_full in ('swinging_strike', 'hit_into_play', 'foul') then plate_x end) as chases,
							-- when the batter swings the bat it counts as a swing
							count(case when pitch_result_full in ('swinging_strike', 'hit_into_play', 'foul') then pitch_result_full end) as swings,
                            
                            -- get team hits and at bats for batting average
							count(case when at_bat_event in ('single', 'double', 'triple', 'home_run') then at_bat_event end) as hits,
							count(case when at_bat_event not in ('walk', 'hit_by_pitch', 'sac_fly', 'sac_bunt', 'sac_fly_double_play', 'catcher_interf') then at_bat_event end) as at_bats
						from 
								pitches
						where 
							inning_top_or_bottom = 'Top'
						group by 
								away_team
					) as subquery
				group by team),
	
    -- this CTE gets the total number of wins for each team
    -- uses a union to get the counts of home wins and away wins, and then selects the sum of those two outputs to get the total
    -- this is actually similar to what we did above, we only want when each team is home or away
	team_wins as (select team_abbrv as team, sum(wins) as wins
					from (
							select away_team_id as team, count(game_id) as wins
							from game_schedule
							join teams on teams.team_id = game_schedule.home_team_id
							where away_team_score > home_team_score and game_status = 'Final'
							group by away_team_id
							union all
							select home_team_id as team, count(game_id) as wins
							from game_schedule
							join teams on teams.team_id = game_schedule.home_team_id
							where home_team_score > away_team_score and game_status = 'Final'
							group by home_team_id
						) as subquery
					join teams on teams.team_id = subquery.team
					group by team
				)
-- select all of the columns I need from the CTES
-- join by the team abbrvs to combine the results
select chase_table.team, chase_table.chases, chase_table.swings, chase_table.chase_pct, chase_table.team_ba, team_wins.wins
from chase_table
join team_wins on team_wins.team = chase_table.team;
-----------------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- TOTAL IN MLB

-- this is the exact same thing as above, just not grouped by each team
select 
    sum(chases) as chases,
    sum(swings) as swings,
    round(chases/swings, 3) as chase_pct

-- subquery starts here
-- its the union of the query where we get the home team during the bottom of the inning, and the away team for the top of the inning
from (
		select
			home_team as team,

			-- when the pitch location is outside of the typical strikezone AND the batter swung the bat, then we count it as a chase
			count(case when (plate_x < -0.705 or plate_x > 0.705 or plate_z < 1.5 or plate_z > 3.6) and 
					pitch_result_full in ('swinging_strike', 'hit_into_play', 'foul') then plate_x end) as chases,

			-- when the batter swings the bat it counts as a swing
			count(case when pitch_result_full in ('swinging_strike', 'hit_into_play', 'foul') then pitch_result_full end) as swings
		from 
			pitches

		where 
			inning_top_or_bottom = 'Bot'
		group by 
			home_team
		union
		select
			away_team as team,
			-- when the pitch location is outside of the typical strikezone AND the batter swung the bat, then we count it as a chase
			count(case when (plate_x < -0.705 or plate_x > 0.705 or plate_z < 1.5 or plate_z > 3.6) and 
					pitch_result_full in ('swinging_strike', 'hit_into_play', 'foul') then plate_x end) as chases,
			-- when the batter swings the bat it counts as a swing
			count(case when pitch_result_full in ('swinging_strike', 'hit_into_play', 'foul') then pitch_result_full end) as swings
		from 
				pitches
		where 
			inning_top_or_bottom = 'Top'
		group by 
				away_team
	) as subquery;
-----------------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-- SPECIFIC PITCH LOCATIONS
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- BY PLAYER AND TEAM
-- when we create the chart, we can filter by player or by team and display only those values

-- all pitches
select player_name, team_abbrv, plate_x, plate_z
from pitches
join players on players.player_id = pitches.batter_id
join teams on teams.team_id = players.team_id;

-- all swing locations
select player_name, team_abbrv, plate_x, plate_z
from pitches
join players on players.player_id = pitches.batter_id
join teams on teams.team_id = players.team_id
where pitch_result_full in ('swinging_strike', 'hit_into_play', 'foul');
-----------------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------



select home_team as team, plate_x, plate_z
from pitches
join teams on teams.team_abbrv = pitches.home_team
where pitches.inning_top_or_bottom = 'Bot' and team_full_name = 'Boston Red Sox' and plate_x is not null and plate_z is not null
union
select away_team as team, plate_x, plate_z
from pitches
join teams on teams.team_abbrv = pitches.away_team
where pitches.inning_top_or_bottom = 'Top' and team_full_name = 'Boston Red Sox' and plate_x is not null and plate_z is not null;


select plate_x, plate_z
from pitches
join players on players.player_id = pitches.batter_id
where players.player_name = 'Trevor Story' and plate_x is not null and plate_z is not null;


select team_full_name from teams;








-- GROUPED BY TEAM

-- this first CTE gets all chases, swings, and chase % for each team
-- does so using a subquery
with chase_table as (
				
                -- using a subquery
				-- we have a union of the queries where a team is hitting at home vs the road
				-- we do this because we only have data for hitters currently
				-- if we just grouped by team, we would get chase rates for their pitchers too, and the data would not be accurate
				select 
					team,
					sum(chases) as chases,
					sum(swings) as swings,
					round(chases/swings, 3) as chase_pct,
                    round(sum(hits)/sum(at_bats), 3) as team_ba

				-- subquery starts here
				-- its the union of the query where we get the home team during the bottom of the inning, and the away team for the top of the inning
				from (
						select
							home_team as team,

							-- when the pitch location is outside of the typical strikezone AND the batter swung the bat, then we count it as a chase
							count(case when (plate_x < -0.705 or plate_x > 0.705 or plate_z < 1.5 or plate_z > 3.6) and 
									pitch_result_full in ('swinging_strike', 'hit_into_play', 'foul') then plate_x end) as chases,

							-- when the batter swings the bat it counts as a swing
							count(case when pitch_result_full in ('swinging_strike', 'hit_into_play', 'foul') then pitch_result_full end) as swings,
                            
                            -- get team hits and at bats for batting average
							count(case when at_bat_event in ('single', 'double', 'triple', 'home_run') then at_bat_event end) as hits,
							count(case when at_bat_event not in ('walk', 'hit_by_pitch', 'sac_fly', 'sac_bunt', 'sac_fly_double_play', 'catcher_interf') then at_bat_event end) as at_bats
						from 
							pitches

						where 
							inning_top_or_bottom = 'Bot'
						group by 
							home_team
						union
						select
							away_team as team,
							-- when the pitch location is outside of the typical strikezone AND the batter swung the bat, then we count it as a chase
							count(case when (plate_x < -0.705 or plate_x > 0.705 or plate_z < 1.5 or plate_z > 3.6) and 
									pitch_result_full in ('swinging_strike', 'hit_into_play', 'foul') then plate_x end) as chases,
							-- when the batter swings the bat it counts as a swing
							count(case when pitch_result_full in ('swinging_strike', 'hit_into_play', 'foul') then pitch_result_full end) as swings,
                            
                            -- get team hits and at bats for batting average
							count(case when at_bat_event in ('single', 'double', 'triple', 'home_run') then at_bat_event end) as hits,
							count(case when at_bat_event not in ('walk', 'hit_by_pitch', 'sac_fly', 'sac_bunt', 'sac_fly_double_play', 'catcher_interf') then at_bat_event end) as at_bats
						from 
								pitches
						where 
							inning_top_or_bottom = 'Top'
						group by 
								away_team
					) as subquery
				group by team),
	
    -- this CTE gets the total number of wins for each team
    -- uses a union to get the counts of home wins and away wins, and then selects the sum of those two outputs to get the total
    -- this is actually similar to what we did above, we only want when each team is home or away
	team_wins as (select team_abbrv as team, sum(wins) as wins
					from (
							select away_team_id as team, count(game_id) as wins
							from game_schedule
							join teams on teams.team_id = game_schedule.home_team_id
							where away_team_score > home_team_score and game_status = 'Final'
							group by away_team_id
							union all
							select home_team_id as team, count(game_id) as wins
							from game_schedule
							join teams on teams.team_id = game_schedule.home_team_id
							where home_team_score > away_team_score and game_status = 'Final'
							group by home_team_id
						) as subquery
					join teams on teams.team_id = subquery.team
					group by team
				)
-- select all of the columns I need from the CTES
-- join by the team abbrvs to combine the results
select chase_table.team, chase_table.chases, chase_table.swings, chase_table.chase_pct, chase_table.team_ba, team_wins.wins
from chase_table
join team_wins on team_wins.team = chase_table.team
join teams on teams.team_abbrv = chase_table.team
where team_full_name = 'Boston Red Sox';


select player_name, status_description, transaction_date 
from transactions
join teams on teams.team_id = transactions.team_id
where team_abbrv = 'BOS' and status_description like 'Injured%';











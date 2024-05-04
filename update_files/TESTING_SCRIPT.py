import pymysql
import csv

def update_players_info(file_path):
    # WHAT THE $%@! IS A CURSOR????
    # a cursor is an object that allows me to retrieve and manipulate data from a result set
    # the cursor is what allows me to run the SQL query to gather all of the data
    # cursors usually return the results as tuples, but different cursor return the data in different ways


    # connects to my MySQL DB and allows me to query from it
    # this is a very similar set up to the R version of doing this
    connection = pymysql.connect(host='localhost',
                                user='root',
                                password='FOODS_test1',
                                database='mlb_statcast_data',
                                # this code tells pymysql to choose the cursor that returns the reuslts as dictionary
                                # this gives me column names as the keys and the column values as the values
                                cursorclass=pymysql.cursors.DictCursor
                                )

    # create the cursor object
    cursor = connection.cursor()

    create_table_query = """
    create temporary table current_players (
		player_id int NOT NULL,
		player_name varchar(100),
		position_abbrv varchar(5),
		position_type varchar(15),
		status_code varchar(5),
		status_description varchar(50),
		team_id int NOT NULL,
		statcast_pos_lookup varchar(10)
	);
    """
    cursor.execute(create_table_query)
    print("Temp Table Created...")

    print("Inserting Data...")
    with open(file_path, "r") as current_player_info:
        csv_reader = csv.reader(current_player_info)
        next(csv_reader)
        for row in csv_reader:
            insert_query = """
            INSERT INTO current_players (player_id, player_name, position_abbrv, position_type, status_code, status_description, team_id, statcast_pos_lookup)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, row)
    
    print("Data Successfully Inserted!")

    print("Updating Table...")
    update_query = """
    update players
    join current_players on players.player_id = current_players.player_id
    set players.player_id = current_players.player_id,
		players.player_name = current_players.player_name,
        players.position_abbrv = current_players.position_abbrv,
        players.position_type = current_players.position_type,
        players.status_code = current_players.status_code,
        players.status_description = current_players.status_description,
        players.team_id = current_players.team_id,
        players.statcast_pos_lookup = current_players.statcast_pos_lookup;
    """

    cursor.execute(update_query)

    insert_new_players_query = """
    insert into players (player_id, player_name, position_abbrv, position_type, status_code, status_description, team_id, statcast_pos_lookup)
    select * 
    from current_players
    where player_id not in (select player_id from players);
    """

    cursor.execute(insert_new_players_query)
    
    drop_table_query = """
    drop temporary table if exists current_players;
    """

    cursor.execute(drop_table_query)

    print("Table Updated and Temp Table Removed")


    # close the cursor and the connection when finished
    cursor.close() 
    connection.commit() 
    connection.close()

    print("Update Completed!")



update_players_info("/ProgramData/MySQL/MySQL Server 8.0/Uploads/mlb2024_updated_players.csv")





# TEST the updating schedule
# this should insert 472 new rows to the game_schedule table

def update_schedule_info(file_path):
    # WHAT THE $%@! IS A CURSOR????
    # a cursor is an object that allows me to retrieve and manipulate data from a result set
    # the cursor is what allows me to run the SQL query to gather all of the data
    # cursors usually return the results as tuples, but different cursor return the data in different ways


    # connects to my MySQL DB and allows me to query from it
    # this is a very similar set up to the R version of doing this
    connection = pymysql.connect(host='localhost',
                                user='root',
                                password='FOODS_test1',
                                database='TEST_mlb',
                                # this code tells pymysql to choose the cursor that returns the reuslts as dictionary
                                # this gives me column names as the keys and the column values as the values
                                cursorclass=pymysql.cursors.DictCursor
                                )

    # create the cursor object
    cursor = connection.cursor()

    create_table_query = """
    create temporary table current_games like game_schedule;
    """
    cursor.execute(create_table_query)
    print("Temp Table Created...")

    print("Inserting Data...")
    with open(file_path, "r") as current_games_info:
        csv_reader = csv.reader(current_games_info)
        #next(csv_reader)
        for row in csv_reader:
            insert_query = """
            INSERT INTO current_games (game_id, 
                                        season_year, 
                                        game_date,
                                        official_date,
                                        double_header, 
                                        scheduled_innings, 
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
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, row)
    
    print("Data Successfully Inserted!")

    print("Updating Table...")
    update_query = """
    update game_schedule
    join current_games on game_schedule.game_id = current_games.game_id
    set game_schedule.game_id = current_games.game_id
        game_schedule.season_year = current_games.season_year
        game_schedule.game_date = current_games.game_date
        game_schedule.official_date = current_games.official_date
        game_schedule.double_header = current_games.double_header
        game_schedule.scheduled_innings = current_games.scheduled_innings
        game_schedule.day_night = current_games.day_night
        game_schedule.games_in_series = current_games.games_in_series
        game_schedule.series_game_number = current_games.series_game_number
        game_schedule.series_description = current_games.series_description
        game_schedule.if_necessary_description = current_games.if_necessary_description
        game_schedule.game_status = current_games.game_staus
        game_schedule.away_team_score = current_games.away_team_score
        game_schedule.away_team_series_num = current_games.away_team_series_num
        game_schedule.away_team_wins = current_games.away_team_wins
        game_schedule.away_team_losses = current_games.away_team_losses
        game_schedule.away_team_id = current_games.away_team_id
        game_schedule.home_team_score = current_games.home_team_score
        game_schedule.home_team_series_num = current_games.home_team_series_num
        game_schedule.home_team_wins = current_games.home_team_wins
        game_schedule.home_team_losses = current_games.home_team_losses
        game_schedule.home_team_id = current_games.home_team_id
        game_schedule.venue_name = current_games.venue_name
        game_schedule.reschedule_game_date = current_games.reschedule_game_date
        game_schedule.rescheduled_from_date = current_games.rescheduled_from_date
    """

    cursor.execute(update_query)

    insert_new_games_query = """
    insert into game_schedule (game_id, 
                                season_year, 
                                game_date,
                                official_date, 
                                double_header, 
                                scheduled_innings, 
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
    select * 
    from current_games
    where player_id not in (select player_id from players);
    """

    cursor.execute(insert_new_games_query)
    
    drop_table_query = """
    drop temporary table if exists current_games;
    """

    cursor.execute(drop_table_query)

    print("Table Updated and Temp Table Removed")


    # close the cursor and the connection when finished
    cursor.close() 
    connection.commit() 
    connection.close()

    print("Update Completed!")



update_schedule_info("/Users/drewc/OneDrive/Documents/2024/MLB Project 2024/mlb2024_updated_games.csv")


import datetime
p = ['o', 'y']
curr_date = datetime.date.today().strftime('%Y-%m-%d')
p.append(curr_date)
print(p)

print(curr_date)
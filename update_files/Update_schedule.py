import pymysql
import csv


def update_schedule_info(file_path):
    try:
        # a cursor is an object that allows me to retrieve and manipulate data from a result set
        # the cursor is what allows me to run the SQL query to gather all of the data
        # cursors usually return the results as tuples, but different cursor return the data in different ways


        # connects to my MySQL DB and allows me to query from it
        # this is a very similar set up to the R version of doing this
        connection = pymysql.connect(host='****',
                                    user='*********',
                                    password='*******',
                                    database='******',
                                    # this code tells pymysql to choose the cursor that returns the reuslts as dictionary
                                    # this gives me column names as the keys and the column values as the values
                                    cursorclass=pymysql.cursors.DictCursor
                                    )

        # create the cursor object
        cursor = connection.cursor()

        create_table_query = """
        create temporary table current_games (game_id int NOT NULL,
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
                                                resumed_from_date varchar(10) default NULL);
        """
        cursor.execute(create_table_query)
        print("Temp Table Created...")

        print("Inserting Data...")
        with open(file_path, "r") as current_games_info:
            csv_reader = csv.reader(current_games_info)
            for row in csv_reader:
                row = [None if value == "NA" else value for value in row]
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
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """
                cursor.execute(insert_query, row)
        
        print("Data Successfully Inserted!")
        print("Updating Table...")
        update_query = """
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
        """

        cursor.execute(update_query)
        print("Adding new data...")
        insert_new_games_query = """
        insert into game_schedule
        select * 
        from current_games
        where game_id not in (select game_id from game_schedule);
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
    except:
        cursor.close()
        connection.close()
        raise



update_schedule_info("/Users/drewc/OneDrive/Documents/2024/MLB Project 2024/update_files/mlb2024_updated_games.csv")



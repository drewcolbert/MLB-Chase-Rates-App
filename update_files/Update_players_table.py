import pymysql
import csv
import datetime

def update_players_info(file_path):
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

        # create a temp table to hold our reference data
        # this has the same layout as my players table
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

        # execute the query and print an update statement
        cursor.execute(create_table_query)
        print("Temp Table Created...")

        # the csv file is generated from the 'update_players_table.r' file
        # the way the table is generated in R, it has no header so we dont need to skip a line
        print("Inserting Data...")
        with open(file_path, "r") as current_player_info:
            csv_reader = csv.reader(current_player_info)
            for row in csv_reader:
                # this is the query we want to execute in SQL to add data to the table
                # the %s acts a placeholder, when we execute the query, Python inserts the actual values of the row
                insert_query = """
                INSERT INTO current_players (player_id, player_name, position_abbrv, position_type, status_code, status_description, team_id, statcast_pos_lookup)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(insert_query, row)
        
        print("Data Successfully Inserted!")

        # this where we actually update the players table 
        # its safer to update everything since there could be multiple things that change at once
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

        # we could run into a situation where a new player joins the league
        # if that is the case, we want to insert them into our table
        insert_new_players_query = """
        insert into players (player_id, player_name, position_abbrv, position_type, status_code, status_description, team_id, statcast_pos_lookup)
        select * 
        from current_players
        where player_id not in (select player_id from players);
        """

        cursor.execute(insert_new_players_query)
        
        # when we are finished, we want to drop the temp table from our database
        drop_table_query = """
        drop temporary table if exists current_players;
        """

        cursor.execute(drop_table_query)

        print("Table Updated and Temp Table Removed")

        print("Updating Transactions table...")
        with open(file_path, "r") as current_player_info:
            csv_reader = csv.reader(current_player_info)
            for row in csv_reader:
                row.append(datetime.date.today().strftime("%Y-%m-%d"))
                # this is the query we want to execute in SQL to add data to the table
                # the %s acts a placeholder, when we execute the query, Python inserts the actual values of the row
                insert_query = """
                INSERT INTO transactions (player_id, player_name, position_abbrv, position_type, status_code, status_description, team_id, statcast_pos_lookup, transaction_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(insert_query, row)

        print("Transactions table updated!")

        # close the cursor and the connection when finished
        cursor.close() 
        connection.commit() 
        connection.close()

        print("Update Completed!")
    except:
        cursor.close()
        connection.close()
        raise



update_players_info("/Users/drewc/OneDrive/Documents/2024/MLB Project 2024/update_files/mlb2024_updated_players.csv")


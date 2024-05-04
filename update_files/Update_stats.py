import pymysql
import csv


def update_stats(file_path):
    try:
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

        print("Inserting Data...")
        with open(file_path, "r") as current_games_info:
            csv_reader = csv.reader(current_games_info)
            for row in csv_reader:
                row = [None if value == "NA" else value for value in row]
                insert_query = """
                INSERT INTO pitches (pitch_type_abbrv,
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
                                        change_in_win_exp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """
                cursor.execute(insert_query, row)

        # close the cursor and the connection when finished
        cursor.close() 
        connection.commit() 
        connection.close()

        print("Update Completed!")
    except:
        cursor.close()
        connection.close()
        raise



update_stats("/Users/drewc/OneDrive/Documents/2024/MLB Project 2024/update_files/mlb2024_updated_stats.csv")
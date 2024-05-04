import pymysql
import pandas as pd
from matplotlib import pyplot as plt
import seaborn as sns
from io import BytesIO


# create a function to get my connection to the database
# this saves me from writing this many times, I only have to do it once and then call the function
def get_db():
    try:
        connection = pymysql.connect(host='*****',
                                    user='*******',
                                    password='********',
                                    database='****',
                                    cursorclass=pymysql.cursors.DictCursor
                                    )
        return connection
    except:
        raise ConnectionError("Could not connect to MySQL Database. Try closing the connection or restarting VS Code.")


def get_all_teams():
    try:
        # need to establish the team names for my filter
        # note how i use the function above to get a connection, makes it so i can close it later
        connection = get_db()
        cursor = connection.cursor()

        teams_query = '''
        select distinct(team_full_name) from teams;
        '''
        cursor.execute(teams_query)
        teams_query_result = cursor.fetchall()
        teams = [team['team_full_name'] for team in teams_query_result]

        return teams
    except:
        raise Exception("Could not get all teams. Try seeing if you are connected to the database.")
    finally:
        cursor.close()
        connection.close()


def get_leaderboards(sort_type):
    try:
        connection = get_db()
        cursor = connection.cursor()

        if sort_type == "top":
            leaderboard_query = '''
            with 
                team_ids as 
                    (select batter_id, player_name, team_id 
                    from pitches
                    join players on players.player_id = pitches.batter_id
                    group by batter_id
                ),

                chase_rates as
                    (select batter_id,
                            count(case when (plate_x < -0.705 or plate_x > 0.705 or plate_z < 1.5 or plate_z > 3.6) and pitch_result_full in ('swinging_strike', 'hit_into_play', 'foul') then plate_x end) as chases,
                            count(case when pitch_result_full in ('swinging_strike', 'hit_into_play', 'foul') then pitch_result_full end) as swings,
                            count(case when at_bat_event in ('single', 'double', 'triple', 'home_run') then at_bat_event end) as hits,
                            count(case when at_bat_event not in ('walk', 'hit_by_pitch', 'sac_fly', 'sac_bunt', 'sac_fly_double_play', 'catcher_interf') then at_bat_event end) as at_bats
                    from pitches
                    group by batter_id
                    )
                select team_ids.player_name, teams.team_abbrv, chase_rates.chases, chase_rates.swings, round(chase_rates.chases/chase_rates.swings, 3) as chase_pct, round(chase_rates.hits/chase_rates.at_bats, 3) as BA
                from team_ids
                join teams on team_ids.team_id = teams.team_id
                join chase_rates on chase_rates.batter_id = team_ids.batter_id
                group by player_name
                having swings > 100
                order by chase_pct
                limit 10;
            '''
        elif sort_type == "bottom":
            leaderboard_query = '''
            with 
                team_ids as 
                    (select batter_id, player_name, team_id 
                    from pitches
                    join players on players.player_id = pitches.batter_id
                    group by batter_id
                ),

                chase_rates as
                    (select batter_id,
                            count(case when (plate_x < -0.705 or plate_x > 0.705 or plate_z < 1.5 or plate_z > 3.6) and pitch_result_full in ('swinging_strike', 'hit_into_play', 'foul') then plate_x end) as chases,
                            count(case when pitch_result_full in ('swinging_strike', 'hit_into_play', 'foul') then pitch_result_full end) as swings,
                            count(case when at_bat_event in ('single', 'double', 'triple', 'home_run') then at_bat_event end) as hits,
                            count(case when at_bat_event not in ('walk', 'hit_by_pitch', 'sac_fly', 'sac_bunt', 'sac_fly_double_play', 'catcher_interf') then at_bat_event end) as at_bats
                    from pitches
                    group by batter_id
                    )
                select team_ids.player_name, teams.team_abbrv, chase_rates.chases, chase_rates.swings, round(chase_rates.chases/chase_rates.swings, 3) as chase_pct, round(chase_rates.hits/chase_rates.at_bats, 3) as BA
                from team_ids
                join teams on team_ids.team_id = teams.team_id
                join chase_rates on chase_rates.batter_id = team_ids.batter_id
                group by player_name
                having swings > 100
                order by chase_pct desc
                limit 10;
            '''

        cursor.execute(leaderboard_query)
        result = cursor.fetchall()

        return result
    
    except:
        raise Exception("Could not get leaderboards. Check connection to database.")
    finally:
        cursor.close()
        connection.close()



def create_chase_rate_chart(team1, player1, team2, player2):
    # -----------------------------------------------------------------------------------------------------------------------------------------------------------
    # -----------------------------------------------------------------------------------------------------------------------------------------------------------
    # START DEFINING QUERIES AND GATHERING DATA
    # -----------------------------------------------------------------------------------------------------------------------------------------------------------
    # -----------------------------------------------------------------------------------------------------------------------------------------------------------

    try:
        # ^ define all of the queries
        # ^ this includes when we get a specific player and when we get the whole team
        team_only_all_query = '''
        select home_team as team, plate_x, plate_z
        from pitches
        join teams on teams.team_abbrv = pitches.home_team
        where pitches.inning_top_or_bottom = 'Bot' and team_full_name = %s and plate_x is not null and plate_z is not null
        union
        select away_team as team, plate_x, plate_z
        from pitches
        join teams on teams.team_abbrv = pitches.away_team
        where pitches.inning_top_or_bottom = 'Top' and team_full_name = %s and plate_x is not null and plate_z is not null;
        '''

        team_only_swings_query = '''
        select home_team as team, plate_x, plate_z
        from pitches
        join teams on teams.team_abbrv = pitches.home_team
        where pitches.inning_top_or_bottom = 'Bot' and team_full_name = %s and plate_x is not null and plate_z is not null and pitch_result_full in ('swinging_strike', 'hit_into_play', 'foul')
        union
        select away_team as team, plate_x, plate_z
        from pitches
        join teams on teams.team_abbrv = pitches.away_team
        where pitches.inning_top_or_bottom = 'Top' and team_full_name = %s and plate_x is not null and plate_z is not null and pitch_result_full in ('swinging_strike', 'hit_into_play', 'foul');
        '''

        all_query = '''
        select plate_x, plate_z, batter_side
        from pitches
        where batter_id = %s and plate_x is not null and plate_z is not null;
        '''

        swing_query = '''
        select plate_x, plate_z, batter_side
        from pitches
        where batter_id = %s and plate_x is not null and plate_z is not null and pitch_result_full in ('swinging_strike', 'hit_into_play', 'foul');
        '''

        connection = get_db()
        cursor = connection.cursor()

        # ^ this is the logic that handles which query to use based on the users input
        if player1 == 999998 and player2 != 999998:
            cursor.execute(team_only_all_query, [team1, team1]) # * filter one is team query
            all_result1 = cursor.fetchall()
            chart1_all = pd.DataFrame(all_result1)

            cursor.execute(team_only_swings_query, [team1, team1])
            swing_result1 = cursor.fetchall()
            chart1_swings = pd.DataFrame(swing_result1)

            cursor.execute(all_query, (player2,)) # * filter two is player
            all_result2 = cursor.fetchall()
            chart2_all = pd.DataFrame(all_result2)

            cursor.execute(swing_query, (player2,))
            swing_result2 = cursor.fetchall()
            chart2_swings = pd.DataFrame(swing_result2)

        elif player1 != 999998 and player2 == 999998:
            cursor.execute(all_query, (player1,)) # * filter one is player
            all_result1 = cursor.fetchall()
            chart1_all = pd.DataFrame(all_result1)

            cursor.execute(swing_query, (player1,))
            swing_result1 = cursor.fetchall()
            chart1_swings = pd.DataFrame(swing_result1)

            cursor.execute(team_only_all_query, [team2, team2]) # * filter two is team
            all_result2 = cursor.fetchall()
            chart2_all = pd.DataFrame(all_result2)

            cursor.execute(team_only_swings_query, [team2, team2])
            swing_result2 = cursor.fetchall()
            chart2_swings = pd.DataFrame(swing_result2)

        elif player1 == 999998 and player2 == 999998:
            cursor.execute(team_only_all_query, [team1, team1]) # * filter one is team
            all_result1 = cursor.fetchall()
            chart1_all = pd.DataFrame(all_result1)

            cursor.execute(team_only_swings_query, [team1, team1])
            swing_result1 = cursor.fetchall()
            chart1_swings = pd.DataFrame(swing_result1)

            cursor.execute(team_only_all_query, [team2, team2]) # * filter two is team
            all_result2 = cursor.fetchall()
            chart2_all = pd.DataFrame(all_result2)

            cursor.execute(team_only_swings_query, [team2, team2])
            swing_result2 = cursor.fetchall()
            chart2_swings = pd.DataFrame(swing_result2)
        else:
            cursor.execute(all_query, (player1,)) # * filter one is player
            all_result1 = cursor.fetchall()
            chart1_all = pd.DataFrame(all_result1)

            cursor.execute(swing_query, (player1,))
            swing_result1 = cursor.fetchall()
            chart1_swings = pd.DataFrame(swing_result1)

            cursor.execute(all_query, (player2,)) # * filter two is player
            all_result2 = cursor.fetchall()
            chart2_all = pd.DataFrame(all_result2)

            cursor.execute(swing_query, (player2,))
            swing_result2 = cursor.fetchall()
            chart2_swings = pd.DataFrame(swing_result2)

    # -----------------------------------------------------------------------------------------------------------------------------------------------------------
    # -----------------------------------------------------------------------------------------------------------------------------------------------------------
    # END DEFINING QUERIES AND GATHERING DATA
    # -----------------------------------------------------------------------------------------------------------------------------------------------------------
    # -----------------------------------------------------------------------------------------------------------------------------------------------------------


        # ^ define the strikezone
        strike_zone = {'x1': -0.705, 'x2': 0.705, 'y1': 1.5, 'y2': 3.6}


    # -----------------------------------------------------------------------------------------------------------------------------------------------------------
    # -----------------------------------------------------------------------------------------------------------------------------------------------------------
    # START CREATING THE PLOTS
    # -----------------------------------------------------------------------------------------------------------------------------------------------------------
    # -----------------------------------------------------------------------------------------------------------------------------------------------------------

        if 'batter_side' not in chart1_all.columns and 'batter_side' in chart2_all.columns:

            # ^ ###############################################################################################################################
            # ^ creating chart 1
            # ^ ###############################################################################################################################
            sns.kdeplot(data = chart1_swings, x = 'plate_x', y = 'plate_z', fill = True, levels = 10, cmap = "coolwarm", alpha = 0.8)
            sns.kdeplot(data = chart1_all, x = 'plate_x', y = 'plate_z', fill = False, levels = 8, color = "black", alpha = 0.5)

            plt.gca().add_patch(plt.Rectangle((strike_zone['x1'], strike_zone['y1']), 
                                            strike_zone['x2'] - strike_zone['x1'], 
                                            strike_zone['y2'] - strike_zone['y1'], 
                                            fill=False, edgecolor='black', linewidth=1.5))

            # Plot the strike zone boundary lines
            plt.plot([-0.705, -0.705], [0.25, 0.33], color='black')
            plt.plot([-0.705, 0.705], [0.33, 0.33], color='black')
            plt.plot([0.705, 0.705], [0.33, 0.25], color='black')
            plt.plot([0.705, 0], [0.25, 0], color='black')
            plt.plot([0, -0.705], [0, 0.25], color='black')
            plt.axis("off")
            plt.tick_params(axis = "both", which = "both", bottom = False, top = False, left = False, right = False)
            buffer1 = BytesIO()
            plt.savefig(buffer1, format = 'png')
            buffer1.seek(0)
            plt.close()

            # ^ ###############################################################################################################################
            # ^ creating chart 2
            # ^ ###############################################################################################################################

            if len(chart2_all['batter_side'].unique()) > 1:
                image2 = plt.imread('static/righty.png')
                image_extent2 = [-3, -0.75 , 0, 8]
            elif chart2_all["batter_side"].unique()[0] == 'R':
                image2 = plt.imread("static/righty.png")
                image_extent2 = [-3, -0.75 , 0, 8]
            else:
                image2 = plt.imread("static/lefty.png")
                image_extent2 = [0.75, 3 , 0, 8]
            
            sns.kdeplot(data = chart2_swings, x = 'plate_x', y = 'plate_z', fill = True, levels = 10, cmap = 'coolwarm', alpha = 0.8)
            sns.kdeplot(data = chart2_all, x = 'plate_x', y = 'plate_z', fill = False, levels = 8, color = "black", alpha = 0.5)

            plt.imshow(image2, extent = image_extent2, aspect = 'auto', alpha = 0.3)

            plt.gca().add_patch(plt.Rectangle((strike_zone['x1'], strike_zone['y1']), 
                                            strike_zone['x2'] - strike_zone['x1'], 
                                            strike_zone['y2'] - strike_zone['y1'], 
                                            fill=False, edgecolor='black', linewidth=1.5))

            # Plot the strike zone boundary lines
            plt.plot([-0.705, -0.705], [0.25, 0.33], color='black')
            plt.plot([-0.705, 0.705], [0.33, 0.33], color='black')
            plt.plot([0.705, 0.705], [0.33, 0.25], color='black')
            plt.plot([0.705, 0], [0.25, 0], color='black')
            plt.plot([0, -0.705], [0, 0.25], color='black')
            plt.axis("off")
            plt.tick_params(axis = "both", which = "both", bottom = False, top = False, left = False, right = False)
            buffer2 = BytesIO()
            plt.savefig(buffer2, format = 'png')
            buffer2.seek(0)
            plt.close()
        
        elif 'batter_side' in chart1_all.columns and 'batter_side' not in chart2_all.columns:

            if len(chart1_all['batter_side'].unique()) > 1:
                image1 = plt.imread("static/righty.png")
                image_extent1 = [-3, -0.75 , 0, 8]
            elif chart1_all["batter_side"].unique()[0] == 'R':
                image1 = plt.imread("static/righty.png")
                image_extent1 = [-3, -0.75 , 0, 8]
            else:
                image1 = plt.imread("static/lefty.png")
                image_extent1 = [0.75, 3 , 0, 8]

            # ^ ###############################################################################################################################
            # ^ creating chart 1
            # ^ ###############################################################################################################################
            sns.kdeplot(data = chart1_swings, x = 'plate_x', y = 'plate_z', fill = True, levels = 10, cmap = "coolwarm", alpha = 0.8)
            sns.kdeplot(data = chart1_all, x = 'plate_x', y = 'plate_z', fill = False, levels = 8, color = "black", alpha = 0.5)

            plt.imshow(image1, extent = image_extent1, aspect = 'auto', alpha = 0.3)

            plt.gca().add_patch(plt.Rectangle((strike_zone['x1'], strike_zone['y1']), 
                                            strike_zone['x2'] - strike_zone['x1'], 
                                            strike_zone['y2'] - strike_zone['y1'], 
                                            fill=False, edgecolor='black', linewidth=1.5))

            # Plot the strike zone boundary lines
            plt.plot([-0.705, -0.705], [0.25, 0.33], color='black')
            plt.plot([-0.705, 0.705], [0.33, 0.33], color='black')
            plt.plot([0.705, 0.705], [0.33, 0.25], color='black')
            plt.plot([0.705, 0], [0.25, 0], color='black')
            plt.plot([0, -0.705], [0, 0.25], color='black')
            plt.axis("off")
            plt.tick_params(axis = "both", which = "both", bottom = False, top = False, left = False, right = False)
            buffer1 = BytesIO()
            plt.savefig(buffer1, format = 'png')
            buffer1.seek(0)
            plt.close()

            # ^ ###############################################################################################################################
            # ^ creating chart 2
            # ^ ###############################################################################################################################
            
            sns.kdeplot(data = chart2_swings, x = 'plate_x', y = 'plate_z', fill = True, levels = 10, cmap = 'coolwarm', alpha = 0.8)
            sns.kdeplot(data = chart2_all, x = 'plate_x', y = 'plate_z', fill = False, levels = 8, color = "black", alpha = 0.5)

            plt.gca().add_patch(plt.Rectangle((strike_zone['x1'], strike_zone['y1']), 
                                            strike_zone['x2'] - strike_zone['x1'], 
                                            strike_zone['y2'] - strike_zone['y1'], 
                                            fill=False, edgecolor='black', linewidth=1.5))

            # Plot the strike zone boundary lines
            plt.plot([-0.705, -0.705], [0.25, 0.33], color='black')
            plt.plot([-0.705, 0.705], [0.33, 0.33], color='black')
            plt.plot([0.705, 0.705], [0.33, 0.25], color='black')
            plt.plot([0.705, 0], [0.25, 0], color='black')
            plt.plot([0, -0.705], [0, 0.25], color='black')
            plt.axis("off")
            plt.tick_params(axis = "both", which = "both", bottom = False, top = False, left = False, right = False)
            buffer2 = BytesIO()
            plt.savefig(buffer2, format = 'png')
            buffer2.seek(0)
            plt.close()

        
        elif 'batter_side' in chart1_all.columns and 'batter_side' in chart2_all.columns:

            if len(chart1_all['batter_side'].unique()) > 1:
                image1 = plt.imread("static/righty.png")
                image_extent1 = [-3, -0.75 , 0, 8]
            elif chart1_all["batter_side"].unique()[0] == 'R':
                image1 = plt.imread("static/righty.png")
                image_extent1 = [-3, -0.75 , 0, 8]
            else:
                image1 = plt.imread("static/lefty.png")
                image_extent1 = [0.75, 3 , 0, 8]

            # ^ ###############################################################################################################################
            # ^ creating chart 1
            # ^ ###############################################################################################################################
            sns.kdeplot(data = chart1_swings, x = 'plate_x', y = 'plate_z', fill = True, levels = 10, cmap = "coolwarm", alpha = 0.8)
            sns.kdeplot(data = chart1_all, x = 'plate_x', y = 'plate_z', fill = False, levels = 8, color = "black", alpha = 0.5)

            plt.imshow(image1, extent = image_extent1, aspect = 'auto', alpha = 0.3)

            plt.gca().add_patch(plt.Rectangle((strike_zone['x1'], strike_zone['y1']), 
                                            strike_zone['x2'] - strike_zone['x1'], 
                                            strike_zone['y2'] - strike_zone['y1'], 
                                            fill=False, edgecolor='black', linewidth=1.5))

            # Plot the strike zone boundary lines
            plt.plot([-0.705, -0.705], [0.25, 0.33], color='black')
            plt.plot([-0.705, 0.705], [0.33, 0.33], color='black')
            plt.plot([0.705, 0.705], [0.33, 0.25], color='black')
            plt.plot([0.705, 0], [0.25, 0], color='black')
            plt.plot([0, -0.705], [0, 0.25], color='black')
            plt.axis("off")
            plt.tick_params(axis = "both", which = "both", bottom = False, top = False, left = False, right = False)
            buffer1 = BytesIO()
            plt.savefig(buffer1, format = 'png')
            buffer1.seek(0)
            plt.close()

            # ^ ###############################################################################################################################
            # ^ creating chart 2
            # ^ ###############################################################################################################################
            
            if len(chart2_all['batter_side'].unique()) > 1:
                image2 = plt.imread("static/righty.png")
                image_extent2 = [-3, -0.75 , 0, 8]
            elif chart2_all["batter_side"].unique()[0] == 'R':
                image2 = plt.imread("static/righty.png")
                image_extent2 = [-3, -0.75 , 0, 8]
            else:
                image2 = plt.imread("static/lefty.png")
                image_extent2 = [0.75, 3 , 0, 8]

            sns.kdeplot(data = chart2_swings, x = 'plate_x', y = 'plate_z', fill = True, levels = 10, cmap = 'coolwarm', alpha = 0.8)
            sns.kdeplot(data = chart2_all, x = 'plate_x', y = 'plate_z', fill = False, levels = 8, color = "black", alpha = 0.5)

            plt.imshow(image2, extent = image_extent2, aspect = 'auto', alpha = 0.3)

            plt.gca().add_patch(plt.Rectangle((strike_zone['x1'], strike_zone['y1']), 
                                            strike_zone['x2'] - strike_zone['x1'], 
                                            strike_zone['y2'] - strike_zone['y1'], 
                                            fill=False, edgecolor='black', linewidth=1.5))

            # Plot the strike zone boundary lines
            plt.plot([-0.705, -0.705], [0.25, 0.33], color='black')
            plt.plot([-0.705, 0.705], [0.33, 0.33], color='black')
            plt.plot([0.705, 0.705], [0.33, 0.25], color='black')
            plt.plot([0.705, 0], [0.25, 0], color='black')
            plt.plot([0, -0.705], [0, 0.25], color='black')
            plt.axis("off")
            plt.tick_params(axis = "both", which = "both", bottom = False, top = False, left = False, right = False)
            buffer2 = BytesIO()
            plt.savefig(buffer2, format = 'png')
            buffer2.seek(0)
            plt.close()
        
        else:
            # ^ ###############################################################################################################################
            # ^ creating chart 1
            # ^ ###############################################################################################################################
            sns.kdeplot(data = chart1_swings, x = 'plate_x', y = 'plate_z', fill = True, levels = 10, cmap = "coolwarm", alpha = 0.8)
            sns.kdeplot(data = chart1_all, x = 'plate_x', y = 'plate_z', fill = False, levels = 8, color = "black", alpha = 0.5)

            plt.gca().add_patch(plt.Rectangle((strike_zone['x1'], strike_zone['y1']), 
                                            strike_zone['x2'] - strike_zone['x1'], 
                                            strike_zone['y2'] - strike_zone['y1'], 
                                            fill=False, edgecolor='black', linewidth=1.5))

            # Plot the strike zone boundary lines
            plt.plot([-0.705, -0.705], [0.25, 0.33], color='black')
            plt.plot([-0.705, 0.705], [0.33, 0.33], color='black')
            plt.plot([0.705, 0.705], [0.33, 0.25], color='black')
            plt.plot([0.705, 0], [0.25, 0], color='black')
            plt.plot([0, -0.705], [0, 0.25], color='black')
            plt.axis("off")
            plt.tick_params(axis = "both", which = "both", bottom = False, top = False, left = False, right = False)
            buffer1 = BytesIO()
            plt.savefig(buffer1, format = 'png')
            buffer1.seek(0)
            plt.close()

            # ^ ###############################################################################################################################
            # ^ creating chart 2
            # ^ ###############################################################################################################################
            sns.kdeplot(data = chart2_swings, x = 'plate_x', y = 'plate_z', fill = True, levels = 10, cmap = 'coolwarm', alpha = 0.8)
            sns.kdeplot(data = chart2_all, x = 'plate_x', y = 'plate_z', fill = False, levels = 8, color = "black", alpha = 0.5)

            plt.gca().add_patch(plt.Rectangle((strike_zone['x1'], strike_zone['y1']), 
                                            strike_zone['x2'] - strike_zone['x1'], 
                                            strike_zone['y2'] - strike_zone['y1'], 
                                            fill=False, edgecolor='black', linewidth=1.5))

            # Plot the strike zone boundary lines
            plt.plot([-0.705, -0.705], [0.25, 0.33], color='black')
            plt.plot([-0.705, 0.705], [0.33, 0.33], color='black')
            plt.plot([0.705, 0.705], [0.33, 0.25], color='black')
            plt.plot([0.705, 0], [0.25, 0], color='black')
            plt.plot([0, -0.705], [0, 0.25], color='black')
            plt.axis("off")
            plt.tick_params(axis = "both", which = "both", bottom = False, top = False, left = False, right = False)
            buffer2 = BytesIO()
            plt.savefig(buffer2, format = 'png')
            buffer2.seek(0)
            plt.close()

    # -----------------------------------------------------------------------------------------------------------------------------------------------------------
    # -----------------------------------------------------------------------------------------------------------------------------------------------------------
    # END CREATING THE PLOTS
    # -----------------------------------------------------------------------------------------------------------------------------------------------------------
    # -----------------------------------------------------------------------------------------------------------------------------------------------------------

        
        return buffer1, buffer2
    

    except:
        raise Exception("Could not generate the charts. Trying checking chart generation code or check connection to the database.")
    finally:
        cursor.close()
        connection.close()



def create_chase_rate_table(team1, player1, team2, player2): 
    try:
        player_query = '''
                    with 
                        team_ids as 
                                (select batter_id, player_name, team_id 
                                from pitches
                                join players on players.player_id = pitches.batter_id
                                group by batter_id
                                ),
                        chase_rates as
                            (select batter_id,
                                    count(case when (plate_x < -0.705 or plate_x > 0.705 or plate_z < 1.5 or plate_z > 3.6) and pitch_result_full in ('swinging_strike', 'hit_into_play', 'foul') then plate_x end) as chases,
                                    count(case when pitch_result_full in ('swinging_strike', 'hit_into_play', 'foul') then pitch_result_full end) as swings,
                                    count(case when at_bat_event in ('single', 'double', 'triple', 'home_run') then at_bat_event end) as hits,
                                    count(case when at_bat_event not in ('walk', 'hit_by_pitch', 'sac_fly', 'sac_bunt', 'sac_fly_double_play', 'catcher_interf') then at_bat_event end) as at_bats
                            from pitches
                            group by batter_id
                            )
                    select team_ids.player_name, teams.team_abbrv, chase_rates.chases, chase_rates.swings, round(chase_rates.chases/chase_rates.swings, 3) as chase_pct, round(chase_rates.hits/chase_rates.at_bats, 3) as BA
                    from team_ids
                    join teams on team_ids.team_id = teams.team_id
                    join chase_rates on chase_rates.batter_id = team_ids.batter_id   
                    where chase_rates.batter_id = %s
                    group by player_name;
                    '''

        team_query = '''
                    with chase_table as (
                                    select 
                                        team,
                                        sum(chases) as chases,
                                        sum(swings) as swings,
                                        round(chases/swings, 3) as chase_pct,
                                        round(sum(hits)/sum(at_bats), 3) as team_ba
                                    from (
                                            select
                                                home_team as team,
                                                count(case when (plate_x < -0.705 or plate_x > 0.705 or plate_z < 1.5 or plate_z > 3.6) and pitch_result_full in ('swinging_strike', 'hit_into_play', 'foul') then plate_x end) as chases,
                                                count(case when pitch_result_full in ('swinging_strike', 'hit_into_play', 'foul') then pitch_result_full end) as swings,
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
                                                count(case when (plate_x < -0.705 or plate_x > 0.705 or plate_z < 1.5 or plate_z > 3.6) and pitch_result_full in ('swinging_strike', 'hit_into_play', 'foul') then plate_x end) as chases,
                                                count(case when pitch_result_full in ('swinging_strike', 'hit_into_play', 'foul') then pitch_result_full end) as swings,
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
                        team_wins as (select 
                                        team_abbrv as team, 
                                        sum(wins) as wins
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
                    select chase_table.team, chase_table.chases, chase_table.swings, chase_table.chase_pct, chase_table.team_ba, team_wins.wins
                    from chase_table
                    join team_wins on team_wins.team = chase_table.team
                    join teams on teams.team_abbrv = chase_table.team
                    where team_full_name = %s;
                    '''
        
        connection = get_db()
        cursor = connection.cursor()

        if player1 == 'View All' and player2 != 'View All':
            cursor.execute(team_query, (team1,))
            table1_result = cursor.fetchall()
            table1_df = pd.DataFrame(table1_result)
            table1_df.rename(columns = {"team":"Team", "chases":"Chases", "swings":"Swings", "chase_pct":"Chase %", "team_ba":"Team BA", "wins":"Wins"}, inplace = True)

            cursor.execute(player_query, (player2,))
            table2_result = cursor.fetchall()
            table2_df = pd.DataFrame(table2_result)
            table2_df.rename(columns = {"player_name":"Player", "team_abbrv":"Team", "chases":"Chases", "swings":"Swings", "chase_pct":"Chase %", "BA":"Batting Average"}, inplace = True)

        elif player1 != 'View All' and player2 == 'View All':
            cursor.execute(player_query, (player1,))
            table1_result = cursor.fetchall()
            table1_df = pd.DataFrame(table1_result)
            table1_df.rename(columns = {"player_name":"Player", "team_abbrv":"Team", "chases":"Chases", "swings":"Swings", "chase_pct":"Chase %", "BA":"Batting Average"}, inplace = True)

            cursor.execute(team_query, (team2,))
            table2_result = cursor.fetchall()
            table2_df = pd.DataFrame(table2_result)
            table2_df.rename(columns = {"team":"Team", "chases":"Chases", "swings":"Swings", "chase_pct":"Chase %", "team_ba":"Team BA", "wins":"Wins"}, inplace = True)

        elif player1 == 'View All' and player2 == 'View All':
            cursor.execute(team_query, (team1,))
            table1_result = cursor.fetchall()
            table1_df = pd.DataFrame(table1_result)
            table1_df.rename(columns = {"team":"Team", "chases":"Chases", "swings":"Swings", "chase_pct":"Chase %", "team_ba":"Team BA", "wins":"Wins"}, inplace = True)

            cursor.execute(team_query, (team2,))
            table2_result = cursor.fetchall()
            table2_df = pd.DataFrame(table2_result)
            table2_df.rename(columns = {"team":"Team", "chases":"Chases", "swings":"Swings", "chase_pct":"Chase %", "team_ba":"Team BA", "wins":"Wins"}, inplace = True)
        else:
            cursor.execute(player_query, (player1,))
            table1_result = cursor.fetchall()
            table1_df = pd.DataFrame(table1_result)
            table1_df.rename(columns = {"player_name":"Player", "team_abbrv":"Team", "chases":"Chases", "swings":"Swings", "chase_pct":"Chase %", "BA":"Batting Average"}, inplace = True)

            cursor.execute(player_query, (player2,))
            table2_result = cursor.fetchall()
            table2_df = pd.DataFrame(table2_result)
            table2_df.rename(columns = {"player_name":"Player", "team_abbrv":"Team", "chases":"Chases", "swings":"Swings", "chase_pct":"Chase %", "BA":"Batting Average"}, inplace = True)

        return table1_df, table2_df
    
    except:
        raise Exception("Could not gather data. Try checking connection to the database.")
    finally:
        cursor.close()
        connection.close()

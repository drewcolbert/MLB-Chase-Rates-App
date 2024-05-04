from flask import Flask, jsonify, render_template, request
import pymysql
import pandas as pd
from matplotlib import pyplot as plt
import seaborn as sns
from io import BytesIO
from setup_functions import get_db, get_all_teams, get_leaderboards, create_chase_rate_chart, create_chase_rate_table
import codecs

# start the instance of the application
# template folder tells the app where to find index.html (or other templates)
# static folder tells the app where to find styles.css
# ^ these are both the default folder names that Flask will look for, but I can change them to whatever i want
app = Flask(__name__, template_folder = 'templates', static_folder = 'static')


plt.switch_backend('agg')

# this is the base of the app, i pass in the file to read and the teams that go into the filter
@app.route('/')
def index():
    return render_template('index.html', teams = get_all_teams(), top_ten = get_leaderboards("top"), bottom_ten = get_leaderboards("bottom"))


# as a user chooses a team, this function will dynamically update the players in the second filter dropdown
@app.route('/team_members', methods = ['POST'])
def team_members():

    # open db connection
    connection = get_db()

    # get the selected team from our javascript in 'index.html' (used chatgpt)
    selected_team = request.json.get('team')

    # query the data based on the currently selected team
    cursor = connection.cursor()

    # ! there are two placeholders in this query but one of them is hardcoded
    # ! this is because the wildcard in SQL (%) was causing issues and acting a placeholder in pythons eyes
    # ! so i just made it an actual placeholder and then hardcoded it since it will never change
    select_query = '''
    select player_name, player_id 
    from players 
    join pitches on pitches.batter_id = players.player_id
    join teams on teams.team_id = players.team_id
    where team_full_name = %s and statcast_pos_lookup like %s
    group by player_name, player_id;
    '''
    
    # here we executing the query and entering the placeholders
    # the order they are in the tuple is the order they will be placed in the query
    cursor.execute(select_query, ((selected_team, 'batter%')))
    result = cursor.fetchall()

    # if we get a result, we create a list of dictionaries with each players name and id
    # I also inserted a view all option so i can see the whole teams data
    if result is not None:
        players = [{'name': player['player_name'], 'id': player['player_id']} for player in result]
        players.insert(0, {'name': 'View All', 'id':999998})
        cursor.close()
    else:
        players[{'name':"No results", 'id':999999}]
        cursor.close()
        raise ValueError(f"No results were found for {selected_team}. Try with another team or check database content.")
    
    connection.close()
    

    return jsonify(players)

@app.route('/generate_charts', methods = ['POST'])
def create_charts():
    data = request.json
    team1 = data['chart1']['team']
    player1 = data['chart1']['player']
    team2 = data['chart2']['team']
    player2 = data['chart2']['player']

    buffer1, buffer2 = create_chase_rate_chart(str(team1), int(player1), str(team2), int(player2))

    encoded_buffer1 = codecs.encode(buffer1.getvalue(), 'base64').decode('utf-8')
    encoded_buffer2 = codecs.encode(buffer2.getvalue(), 'base64').decode('utf-8')

    return jsonify({
        'chart1': 'data:image/png;base64,' + encoded_buffer1,
        'chart2': 'data:image/png;base64,' + encoded_buffer2
    })


@app.route('/generate_tables', methods = ['POST'])
def create_tables():
    data = request.json
    team1 = data['table1']['team']
    player1 = data['table1']['player']
    team2 = data['table2']['team']
    player2 = data['table2']['player']

    table1, table2 = create_chase_rate_table(team1, player1, team2, player2)

    
    # Convert DataFrames to HTML tables
    table1_json = table1.to_json(orient = 'records')
    table2_json = table2.to_json(orient = 'records')
    

    return jsonify({
        'table1': table1_json,
        'table2': table2_json
    })

if __name__ == "__main__":
    app.run()





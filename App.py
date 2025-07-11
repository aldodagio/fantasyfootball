from flask import Flask, jsonify
from flask_bootstrap import Bootstrap
from flask import render_template
from flask import request
import pdfkit
import os
import pandas as pd
from postgres_db.Connection import Connection

app = Flask(__name__)
static_folder = os.path.join(app.root_path, 'static', 'excel_files')
bootstrap = Bootstrap(app)

def getYear(season_id):
    if season_id == 1:
        year = '2010'
    elif season_id == 2:
        year = '2011'
    elif season_id == 3:
        year = '2012'
    elif season_id == 4:
        year = '2013'
    elif season_id == 5:
        year = '2014'
    elif season_id == 6:
        year = '2015'
    elif season_id == 7:
        year = '2016'
    elif season_id == 8:
        year = '2017'
    elif season_id == 9:
        year = '2018'
    elif season_id == 10:
        year = '2019'
    elif season_id == 11:
        year = '2020'
    elif season_id == 12:
        year = '2021'
    elif season_id == 13:
        year = '2022'
    elif season_id == 14:
        year = '2023'
    elif season_id == 15:
        year = '2024'
    elif season_id == 16:
        year = '2025'
    return year
def getSeasonID(year):
    if year == '2010':
        id = 1
    elif year == '2011':
        id = 2
    elif year == '2012':
        id = 3
    elif year == '2013':
        id = 4
    elif year == '2014':
        id = 5
    elif year == '2015':
        id = 6
    elif year == '2016':
        id = 7
    elif year == '2017':
        id = 8
    elif year == '2018':
        id = 9
    elif year == '2019':
        id = 10
    elif year == '2020':
        id = 11
    elif year == '2021':
        id = 12
    elif year == '2022':
        id = 13
    elif year == '2023':
        id = 14
    elif year == '2024':
        id = 15
    return id

@app.route("/")
def home():
    connection = Connection()
    seasons = connection.select_seasons()
    return render_template('index.html', seasons=seasons)


@app.route('/download-pdf')
def download_pdf():
    pdfkit.from_file('prediction_view.html', 'out.pdf')


@app.route('/season/<season_id>')
def season_view(season_id):
    # Logic to retrieve season data and render the season view template
    season_end = int(season_id) + 1
    id = getSeasonID(season_id)
    connection = Connection()
    players = connection.player_dropdown(id)
    players_with_points = connection.select_players_with_total_points(id)
    return render_template('season_view.html', season=season_id, season_end=season_end, players=players,
                           players_with_points=players_with_points)


@app.route('/season/<season_id>/QB')
def season_QB_view(season_id):
    # Logic to retrieve season data and render the season view template
    season_end = int(season_id) + 1
    id = getSeasonID(season_id)
    connection = Connection()
    players = connection.qb_dropdown(id)
    players_with_points = connection.select_qbs_with_total_points(id)
    return render_template('season_view.html', position='Quarterback', season_id=id, season=season_id,
                           season_end=season_end, players=players, players_with_points=players_with_points)


@app.route('/QB/lr_prediction/<season_id>')
def lr_prediction_QB_view(season_id):
    connection = Connection()
    actuals = connection.select_qbs_with_total_points(season_id)
    predictions = connection.get_linear_regression_predictions_qb(getYear(int(season_id)+1))
    rank_changes = calculate_rank_change(predictions, actuals)
    return render_template('prediction_view.html', predictions=predictions, actuals=actuals, rank_changes=rank_changes)

@app.route('/K/lr_prediction/<season_id>')
def lr_prediction_K_view(season_id):
    connection = Connection()
    actuals = connection.select_k_with_total_points(season_id)
    predictions = connection.get_linear_regression_predictions_k(getYear(int(season_id)+1))
    rank_changes = calculate_rank_change(predictions, actuals)
    return render_template('prediction_view.html', predictions=predictions, actuals=actuals, rank_changes=rank_changes)

@app.route('/DST/lr_prediction/<season_id>')
def lr_prediction_DST_view(season_id):
    connection = Connection()
    actuals = connection.select_dst_with_total_points(season_id)
    predictions = connection.get_linear_regression_predictions_dst(getYear(int(season_id)+1))
    rank_changes = calculate_rank_change(predictions, actuals)
    return render_template('prediction_view.html', predictions=predictions, actuals=actuals, rank_changes=rank_changes)


@app.route('/non-qb/lr_prediction/<season_id>')
def lr_prediction_nonqb_view(season_id):
    connection = Connection()
    actuals = connection.select_nonqb_with_total_points(season_id)
    predictions = connection.get_linear_regression_predictions_nonqb(getYear(int(season_id)+1))
    rank_changes = calculate_rank_change(predictions, actuals)
    return render_template('prediction_view.html', predictions=predictions, actuals=actuals, rank_changes=rank_changes)


@app.route('/all/lr_prediction/<season_id>')
def lr_prediction_all_view(season_id):
    connection = Connection()
    actuals = connection.select_all_with_total_points(season_id)
    predictions = connection.get_linear_regression_predictions_all(getYear(int(season_id)+1))
    rank_changes = calculate_rank_change(predictions, actuals)
    return render_template('prediction_view.html', predictions=predictions, actuals=actuals, rank_changes=rank_changes)


def calculate_rank_change(predictions, actuals):
    rank_changes = {}
    i = 1
    j = 1
    for prediction in predictions:
        for actual in actuals:
            if prediction.name == actual.first_name + ' ' + actual.last_name:
                rank_changes[prediction.name] = j - i
                j = 1
                break
            j = j + 1
        else:
            rank_changes[prediction.name] = None
        i = i + 1
    return rank_changes


@app.route('/RB/lr_prediction/<season_id>')
def lr_prediction_RB_view(season_id):
    connection = Connection()
    actuals = connection.select_rbs_with_total_points(season_id)
    predictions = connection.get_linear_regression_predictions_rb(getYear(int(season_id)+1))
    rank_changes = calculate_rank_change(predictions, actuals)
    return render_template('prediction_view.html', predictions=predictions, actuals=actuals, rank_changes=rank_changes)


@app.route('/WR/lr_prediction/<season_id>')
def lr_prediction_WR_view(season_id):
    connection = Connection()
    actuals = connection.select_wrs_with_total_points(season_id)
    predictions = connection.get_linear_regression_predictions_wr(getYear(int(season_id)+1))
    rank_changes = calculate_rank_change(predictions, actuals)
    return render_template('prediction_view.html', predictions=predictions, actuals=actuals, rank_changes=rank_changes)


@app.route('/TE/lr_prediction/<season_id>')
def lr_prediction_TE_view(season_id):
    connection = Connection()
    actuals = connection.select_tes_with_total_points(season_id)
    predictions = connection.get_linear_regression_predictions_te(getYear(int(season_id)+1))
    rank_changes = calculate_rank_change(predictions, actuals)
    return render_template('prediction_view.html', predictions=predictions, actuals=actuals, rank_changes=rank_changes)


# Custom filter to check if a value is a digit
def is_digit(value):
    if value is None:
        return False
    try:
        int(value)
        return True
    except (ValueError, TypeError):
        return False


app.jinja_env.filters['is_digit'] = is_digit


@app.route('/season/<season_id>/RB')
def season_RB_view(season_id):
    # Logic to retrieve season data and render the season view template
    season_end = int(season_id) + 1
    id = getSeasonID(season_id)
    connection = Connection()
    players = connection.rb_dropdown(id)
    players_with_points = connection.select_rbs_with_total_points(id)
    return render_template('season_view.html', position='Running Back', season_id=id, season=season_id,
                           season_end=season_end, players=players, players_with_points=players_with_points)


@app.route('/season/<season_id>/WR')
def season_WR_view(season_id):
    # Logic to retrieve season data and render the season view template
    season_end = int(season_id) + 1
    id = getSeasonID(season_id)
    connection = Connection()
    players = connection.wr_dropdown(id)
    players_with_points = connection.select_wrs_with_total_points(id)
    return render_template('season_view.html', position='Wide Receiver', season_id=id, season=season_id,
                           season_end=season_end, players=players, players_with_points=players_with_points)


@app.route('/season/<season_id>/TE')
def season_TE_view(season_id):
    # Logic to retrieve season data and render the season view template
    season_end = int(season_id) + 1
    id = getSeasonID(season_id)
    connection = Connection()
    players = connection.te_dropdown(id)
    players_with_points = connection.select_tes_with_total_points(id)
    return render_template('season_view.html', position='Tight End', season_id=id, season=season_id,
                           season_end=season_end, players=players, players_with_points=players_with_points)

@app.route('/season/<season_id>/K')
def season_K_view(season_id):
    # Logic to retrieve season data and render the season view template
    season_end = int(season_id) + 1
    id = getSeasonID(season_id)
    connection = Connection()
    players = connection.k_dropdown(id)
    players_with_points = connection.select_k_with_total_points(id)
    return render_template('season_view.html', position='Kicker', season_id=id, season=season_id,
                           season_end=season_end, players=players, players_with_points=players_with_points)

@app.route('/season/<season_id>/DST')
def season_DST_view(season_id):
    # Logic to retrieve season data and render the season view template
    season_end = int(season_id) + 1
    id = getSeasonID(season_id)
    connection = Connection()
    players = connection.dst_dropdown(id)
    players_with_points = connection.select_dst_with_total_points(id)
    return render_template('season_view.html', position='Defense/Special Teams', season_id=id, season=season_id,
                           season_end=season_end, players=players, players_with_points=players_with_points)

@app.route('/linear_regression')
def linear_regression():
    connection = Connection()
    seasons = connection.select_seasons()
    return render_template('linear_regression.html', seasons=seasons)


@app.route('/about_us')
def about_us():
    return render_template('about_us.html')


@app.route('/database_manager')
def database_manager():
    return render_template('database_manager.html')


@app.route('/qb_manager')
def qb_manager():
    connection = Connection()
    players = connection.select_current_qbs()
    return render_template('qb_manager.html', players=players)


@app.route('/rb_manager')
def rb_manager():
    connection = Connection()
    players = connection.select_currrent_rbs()
    return render_template('rb_manager.html', players=players)


@app.route('/wr_manager')
def wr_manager():
    connection = Connection()
    players = connection.select_current_wrs()
    return render_template('wr_manager.html', players=players)


@app.route('/te_manager')
def te_manager():
    connection = Connection()
    players = connection.select_current_tes()
    return render_template('te_manager.html', players=players)


@app.route('/player_manager')
def player_manager():
    return render_template('player_manager.html')


@app.route('/team_manager')
def team_manager():
    connection = Connection()
    teams = connection.select_teams()
    return render_template('team_manager.html', teams=teams)


@app.route('/update_team', methods=['POST'])
def update_team():
    data = request.get_json()
    current_name = data.get('current_name')
    new_name = data.get('new_name')

    if not current_name or not new_name:
        return jsonify({"success": False, "error": "Missing data"})

    try:
        connection = Connection()
        connection.update_team(current_name, new_name)
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route('/update_player_last_name', methods=['POST'])
def update_player_last_name():
    data = request.get_json()
    last_name = data.get('last_name')
    player_id = data.get('player_id')

    if not id or not last_name:
        return jsonify({"success": False, "error": "Missing data"})

    try:
        connection = Connection()
        connection.update_player_last_name(player_id, last_name)
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route('/update_player_first_name', methods=['POST'])
def update_player_first_name():
    data = request.get_json()
    first_name = data.get('first_name')
    player_id = data.get('player_id')

    if not id or not first_name:
        return jsonify({"success": False, "error": "Missing data"})

    try:
        connection = Connection()
        connection.update_player_first_name(player_id, first_name)
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route('/update_player_team', methods=['POST'])
def update_player_team():
    data = request.get_json()
    team_name = data.get('team_name')
    player_id = data.get('player_id')

    if not id or not team_name:
        return jsonify({"success": False, "error": "Missing data"})

    try:
        connection = Connection()
        connection.update_player_team(player_id, team_name)
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route('/draft_day_tool')
def draft_day_tool():
    connection = Connection()
    qb_predictions = connection.get_linear_regression_predictions_qb()
    rb_predictions = connection.get_linear_regression_predictions_rb()
    wr_predictions = connection.get_linear_regression_predictions_wr()
    te_predictions = connection.get_linear_regression_predictions_te()
    return render_template('draft_day_tool.html',
                           qb_predictions=qb_predictions,
                           rb_predictions=rb_predictions,
                           wr_predictions=wr_predictions,
                           te_predictions=te_predictions)


@app.route('/search', methods=['GET'])
def player_search():
    # Logic to handle player search and render the player search template
    player_id = request.args.get('player_select')
    season_id = request.args.get('season_id')
    pos = request.args.get('position')
    connection = Connection()
    if pos == 'Quarterback':
        player_stats = connection.get_qb_stats(player_id, season_id)
    elif pos == 'Running Back':
        player_stats = connection.get_rb_stats(player_id, season_id)
    elif pos == 'Wide Receiver':
        player_stats = connection.get_wrs_stats(player_id, season_id)
    elif pos == 'Tight End':
        player_stats = connection.get_te_stats(player_id, season_id)
    elif pos == 'Kicker':
        player_stats = connection.get_k_stats(player_id, season_id)
    elif pos == 'Defense/Special Teams':
        player_stats = connection.get_dst_stats(player_id, season_id)
    return render_template('player_search.html', player_stats=player_stats)


if __name__ == '__main__':
    app.run(host='0.0.0.0')

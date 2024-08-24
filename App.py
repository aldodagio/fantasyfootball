from flask import Flask
from flask_bootstrap import Bootstrap
from flask import render_template
from flask import request, redirect, url_for, send_file
import pdfkit
import io
import os
import pandas as pd
from nfl.Season import Season
from postgres_db.Connection import Connection

app = Flask(__name__)
static_folder = os.path.join(app.root_path, 'static', 'excel_files')
bootstrap = Bootstrap(app)


@app.route("/")
def home():
    connection = Connection()
    seasons = connection.select_seasons()
    return render_template('index.html', seasons=seasons)


@app.route('/download-pdf')
def download_pdf():
    connection = Connection()
    actuals = connection.select_all_with_total_points(14)
    predictions = connection.get_linear_regression_predictions_all()
    rank_changes = calculate_rank_change(predictions, actuals)
    rendered = render_template('prediction_view.html', predictions=predictions, actuals=actuals,
                               rank_changes=rank_changes)
    pdf = pdfkit.from_string(rendered, False)
    response = io.BytesIO(pdf)
    return send_file(response, attachment_filename='report.pdf', as_attachment=True, mimetype='application/pdf')


@app.route('/season/<season_id>')
def season_view(season_id):
    # Logic to retrieve season data and render the season view template
    season_end = int(season_id) + 1
    if season_id == '2010':
        id = 1
    elif season_id == '2011':
        id = 2
    elif season_id == '2012':
        id = 3
    elif season_id == '2013':
        id = 4
    elif season_id == '2014':
        id = 5
    elif season_id == '2015':
        id = 6
    elif season_id == '2016':
        id = 7
    elif season_id == '2017':
        id = 8
    elif season_id == '2018':
        id = 9
    elif season_id == '2019':
        id = 10
    elif season_id == '2020':
        id = 11
    elif season_id == '2021':
        id = 12
    elif season_id == '2022':
        id = 13
    elif season_id == '2023':
        id = 14
    connection = Connection()
    players = connection.player_dropdown(id)
    players_with_points = connection.select_players_with_total_points(id)
    return render_template('season_view.html', season=season_id, season_end=season_end, players=players,
                           players_with_points=players_with_points)


@app.route('/season/<season_id>/QB')
def season_QB_view(season_id):
    # Logic to retrieve season data and render the season view template
    season_end = int(season_id) + 1
    if season_id == '2010':
        id = 1
    elif season_id == '2011':
        id = 2
    elif season_id == '2012':
        id = 3
    elif season_id == '2013':
        id = 4
    elif season_id == '2014':
        id = 5
    elif season_id == '2015':
        id = 6
    elif season_id == '2016':
        id = 7
    elif season_id == '2017':
        id = 8
    elif season_id == '2018':
        id = 9
    elif season_id == '2019':
        id = 10
    elif season_id == '2020':
        id = 11
    elif season_id == '2021':
        id = 12
    elif season_id == '2022':
        id = 13
    elif season_id == '2023':
        id = 14
    connection = Connection()
    players = connection.qb_dropdown(id)
    players_with_points = connection.select_qbs_with_total_points(id)
    return render_template('season_view.html', position='Quarterback', season_id=id, season=season_id,
                           season_end=season_end, players=players, players_with_points=players_with_points)


@app.route('/QB/lr_prediction')
def lr_prediction_QB_view():
    connection = Connection()
    actuals = connection.select_qbs_with_total_points(14)
    predictions = connection.get_linear_regression_predictions_qb()
    rank_changes = calculate_rank_change(predictions, actuals)
    return render_template('prediction_view.html', predictions=predictions, actuals=actuals, rank_changes=rank_changes)


@app.route('/non-qb/lr_prediction')
def lr_prediction_nonqb_view():
    connection = Connection()
    actuals = connection.select_nonqb_with_total_points(14)
    predictions = connection.get_linear_regression_predictions_nonqb()
    rank_changes = calculate_rank_change(predictions, actuals)
    return render_template('prediction_view.html', predictions=predictions, actuals=actuals, rank_changes=rank_changes)

@app.route('/all/lr_prediction')
def lr_prediction_all_view():
    connection = Connection()
    actuals = connection.select_all_with_total_points(14)
    predictions = connection.get_linear_regression_predictions_all()
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


@app.route('/RB/lr_prediction')
def lr_prediction_RB_view():
    connection = Connection()
    actuals = connection.select_rbs_with_total_points(14)
    predictions = connection.get_linear_regression_predictions_rb()
    rank_changes = calculate_rank_change(predictions, actuals)
    return render_template('prediction_view.html', predictions=predictions, actuals=actuals, rank_changes=rank_changes)


@app.route('/WR/lr_prediction')
def lr_prediction_WR_view():
    connection = Connection()
    actuals = connection.select_wrs_with_total_points(14)
    predictions = connection.get_linear_regression_predictions_wr()
    rank_changes = calculate_rank_change(predictions, actuals)
    return render_template('prediction_view.html', predictions=predictions, actuals=actuals, rank_changes=rank_changes)


@app.route('/TE/lr_prediction')
def lr_prediction_TE_view():
    connection = Connection()
    actuals = connection.select_tes_with_total_points(14)
    predictions = connection.get_linear_regression_predictions_te()
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
@app.route('/QB/mb_prediction')
def mb_prediction_QB_view():
    filename = 'MatthewBerryQBs.xlsx'
    file_path = os.path.join(static_folder, filename)
    if os.path.exists(file_path):
        df = pd.read_excel(file_path)
        # Convert third and fourth columns to integers if they are floats
        for col in df.columns[2:4]:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

        # Convert dataframe to dictionary
        data = df.to_dict(orient='records')
        columns = df.columns.tolist()
        return render_template('mb_predictions.html', data=data, columns=columns, filename=filename)
    else:
        return "File not found", 404
@app.route('/RB/mb_prediction')
def mb_prediction_RB_view():
    filename = 'MatthewBerryRBs.xlsx'
    file_path = os.path.join(static_folder, filename)
    if os.path.exists(file_path):
        df = pd.read_excel(file_path)
        # Convert third and fourth columns to integers if they are floats
        for col in df.columns[2:4]:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

        # Convert dataframe to dictionary
        data = df.to_dict(orient='records')
        columns = df.columns.tolist()
        return render_template('mb_predictions.html', data=data, columns=columns, filename=filename)
    else:
        return "File not found", 404


@app.route('/WR/mb_prediction')
def mb_prediction_WR_view():
    filename = 'MatthewBerryWRs.xlsx'
    file_path = os.path.join(static_folder, filename)
    if os.path.exists(file_path):
        df = pd.read_excel(file_path)
        # Convert third and fourth columns to integers if they are floats
        for col in df.columns[2:4]:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

        # Convert dataframe to dictionary
        data = df.to_dict(orient='records')
        columns = df.columns.tolist()
        return render_template('mb_predictions.html', data=data, columns=columns, filename=filename)
    else:
        return "File not found", 404


@app.route('/TE/mb_prediction')
def mb_prediction_TE_view():
    filename = 'MatthewBerryTEs.xlsx'
    file_path = os.path.join(static_folder, filename)
    if os.path.exists(file_path):
        df = pd.read_excel(file_path)
        columns_to_keep = ['Matthew Berry Ranks', 'Wide Receiver', 'Difference', 'Artificial Intelligence Ranks', 'Average Difference']
        df = df[columns_to_keep]
        # Convert third and fourth columns to integers if they are floats
        for col in df.columns[2:4]:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

        # Convert dataframe to dictionary
        data = df.to_dict(orient='records')
        columns = df.columns.tolist()
        return render_template('mb_predictions.html', data=data, columns=columns, filename=filename)
    else:
        return "File not found", 404

@app.route('/season/<season_id>/RB')
def season_RB_view(season_id):
    # Logic to retrieve season data and render the season view template
    season_end = int(season_id) + 1
    if season_id == '2010':
        id = 1
    elif season_id == '2011':
        id = 2
    elif season_id == '2012':
        id = 3
    elif season_id == '2013':
        id = 4
    elif season_id == '2014':
        id = 5
    elif season_id == '2015':
        id = 6
    elif season_id == '2016':
        id = 7
    elif season_id == '2017':
        id = 8
    elif season_id == '2018':
        id = 9
    elif season_id == '2019':
        id = 10
    elif season_id == '2020':
        id = 11
    elif season_id == '2021':
        id = 12
    elif season_id == '2022':
        id = 13
    elif season_id == '2023':
        id = 14
    connection = Connection()
    players = connection.rb_dropdown(id)
    players_with_points = connection.select_rbs_with_total_points(id)
    return render_template('season_view.html', position='Running Back', season_id=id, season=season_id,
                           season_end=season_end, players=players, players_with_points=players_with_points)


@app.route('/season/<season_id>/WR')
def season_WR_view(season_id):
    # Logic to retrieve season data and render the season view template
    season_end = int(season_id) + 1
    if season_id == '2010':
        id = 1
    elif season_id == '2011':
        id = 2
    elif season_id == '2012':
        id = 3
    elif season_id == '2013':
        id = 4
    elif season_id == '2014':
        id = 5
    elif season_id == '2015':
        id = 6
    elif season_id == '2016':
        id = 7
    elif season_id == '2017':
        id = 8
    elif season_id == '2018':
        id = 9
    elif season_id == '2019':
        id = 10
    elif season_id == '2020':
        id = 11
    elif season_id == '2021':
        id = 12
    elif season_id == '2022':
        id = 13
    elif season_id == '2023':
        id = 14
    connection = Connection()
    players = connection.wr_dropdown(id)
    players_with_points = connection.select_wrs_with_total_points(id)
    return render_template('season_view.html', position='Wide Receiver', season_id=id, season=season_id,
                           season_end=season_end, players=players, players_with_points=players_with_points)


@app.route('/season/<season_id>/TE')
def season_TE_view(season_id):
    # Logic to retrieve season data and render the season view template
    season_end = int(season_id) + 1
    if season_id == '2010':
        id = 1
    elif season_id == '2011':
        id = 2
    elif season_id == '2012':
        id = 3
    elif season_id == '2013':
        id = 4
    elif season_id == '2014':
        id = 5
    elif season_id == '2015':
        id = 6
    elif season_id == '2016':
        id = 7
    elif season_id == '2017':
        id = 8
    elif season_id == '2018':
        id = 9
    elif season_id == '2019':
        id = 10
    elif season_id == '2020':
        id = 11
    elif season_id == '2021':
        id = 12
    elif season_id == '2022':
        id = 13
    elif season_id == '2023':
        id = 14
    connection = Connection()
    players = connection.te_dropdown(id)
    players_with_points = connection.select_tes_with_total_points(id)
    return render_template('season_view.html', position='Tight End', season_id=id, season=season_id,
                           season_end=season_end, players=players, players_with_points=players_with_points)


@app.route('/linear_regression')
def linear_regression():
    connection = Connection()
    seasons = connection.select_seasons()
    return render_template('linear_regression.html', seasons=seasons)


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
    return render_template('player_search.html', player_stats=player_stats)

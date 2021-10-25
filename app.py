import logging
import os
import signal
import sys

from flask import Flask, jsonify
from flask_cors import CORS
from gevent.pywsgi import WSGIServer

from module.controller import ShooterOnline

# crating instance of shooter online object.
shooter_online = ShooterOnline()

# instance flask application
app = Flask(__name__)


def close_app():

    print("Application exited nicely.")
    sys.exit(0)


def exit_signal_handler():
    print('You pressed Ctrl+C! Application will be terminated')

    # close application
    close_app()


@app.route('/', methods=['GET', 'POST'])
def info():
    """
    Application hello world!
    :return: application name and version.
    """
    return "Shooter Server - v.01-beta"


@app.route('/memory', methods=['GET'])
def get_memory():
    """
    Return shooter online memory
    :return:
    """
    return jsonify(shooter_online.memory)


@app.route('/list/<string:tickers>', methods=['GET'])
def get_data(tickers):
    """
    Return current prices of a list of tickers
    :return:
    """
    return jsonify(shooter_online.get_data(tickers.split(";")))


@app.route('/data/<string:ticker>', methods=['GET'])
def get_ticker(ticker):
    return jsonify(shooter_online.memory[ticker])


@app.route('/risk_mid/<string:ticker>', methods=['GET'])
def get_risk_mid(ticker):
    return jsonify(shooter_online.get_risk_mid(ticker))


@app.route('/multiplier/<string:ticker>', methods=['GET'])
def get_multiplier(ticker):
    return jsonify(shooter_online.get_multiplier(ticker))


def run_app(current_dir="."):

    print("Shooter Server is starting...")
    print("Current dir: " + current_dir)

    """
    Flask logging configuration
    """
    flask_logger = logging.getLogger('werkzeug')
    flask_logger.setLevel(logging.ERROR)
    flask_logger.disable = True
    os.environ['WERKZEUG_RUN_MAIN'] = 'true'

    """
    Application name
    """
    logging.info("Starting Shooter Online session.")
    shooter_online.start()

    # start flask_app
    app.config['SECRET_KEY'] = 'secret!'
    CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=True)
    app.config['CORS_HEADERS'] = 'Content-Type'

    # start flask engine and application main loop
    logging.info("Starting flask application.")

    # dev - debug version - ATTENTION! It must not run in that way in production environment.
    # Python GIL will serialize communication threads the behavior will be compromised.
    # ---
    # app.run()

    # prod - deploy version
    http_server = WSGIServer(('0.0.0.0', 7000), app, log=None)
    http_server.serve_forever()


if __name__ == '__main__':
    """
    Application entry-point.
    """
    # registering exit signal handler
    signal.signal(signal.SIGINT, exit_signal_handler)

    run_app()

    sys.exit(0)

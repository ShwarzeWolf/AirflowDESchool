from flask import Flask, request
from werkzeug.exceptions import NotFound


app = Flask(__name__)

# Question: why do we use POST (not get) method to launch processing?

@app.route('/')
def healthcheck():
    return 'Ok'

@app.route('/datasets', methods=['POST'])
def launch_processing():
    dataset_id = request.json.get('dataset_id')

    if dataset_id:
        print(f'dataset with id {dataset_id} has started the processing')
        return 'Ok'
    else:
        raise NotFound('Session was not defined')


app.run()
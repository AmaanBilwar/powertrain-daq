from flask import Flask, request, jsonify
from flask_cors import CORS
import os 
from dotenv import load_dotenv
import uuid
load_dotenv()

# load envs




app = Flask(__name__)
CORS(app)



if __name__ == '__main__':
    app.run(debug=True, port=5000)
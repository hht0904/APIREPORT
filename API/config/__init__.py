from flask import Flask
from flask_cors import CORS
from config.database import Database


db = Database(host='192.168.1.160', port=10000)


def create_app():
    app = Flask(__name__)
    from sum.sum import bp as sum_bp
    app.register_blueprint(sum_bp)
    from diagnose.diagnose import bp as diagnose_bp
    app.register_blueprint(diagnose_bp)
    from service.service import bp as service_bp
    app.register_blueprint(service_bp)
    from room.room import bp as room_bp
    app.register_blueprint(room_bp)
    from gender.gender import bp as gender_bp
    app.register_blueprint(gender_bp)
    from faculty.faculty import bp as faculty_bp
    app.register_blueprint(faculty_bp)
    CORS(app)
    return app

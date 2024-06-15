from flask import Blueprint, jsonify, request
from config import db
from datetime import datetime

bp = Blueprint('diagnose', __name__)

def validate_date(date_str):
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False

def calculate_age(birth_date):
    if birth_date is None:
        return None
    today = datetime.now()
    age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
    if age < 200:
        return age
    return None

def parse_birth_date(birth_date_str):
    try:
        birth_year = int(birth_date_str)
        if birth_year < 200:
            today = datetime.now()
            birth_date = datetime(today.year - birth_year, today.month, today.day)
        else:
            birth_date = datetime(birth_year, 1, 1)
    except ValueError:
        formats = ['%d-%m-%Y', '%Y']
        for fmt in formats:
            try:
                return datetime.strptime(birth_date_str, fmt)
            except ValueError:
                continue
        return None
    return birth_date

@bp.route('/api/diagnose/average_age/', methods=['GET'])
def api_average_age():
    try:
        start_date = request.headers.get('startdate')
        end_date = request.headers.get('enddate')

        if not start_date or not validate_date(start_date):
            error_data = {
                "code": "400",
                "status": "error",
                "type": "message",
                "value": "Invalid or missing start_date. Format should be YYYY-MM-DD"
            }
            return jsonify(error_data), 400
        if end_date and not validate_date(end_date):
            error_data = {
                "code": "400",
                "status": "error",
                "type": "message",
                "value": "Invalid end_date. Format should be YYYY-MM-DD"
            }
            return jsonify(error_data), 400

        start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')

        if end_date_obj < start_date_obj:
            error_data = {
                "code": "400",
                "status": "error",
                "type": "message",
                "value": "End date cannot be earlier than start date"
            }
            return jsonify(error_data), 400

        query = f"""
            SELECT BirthDate, Diagnose FROM report.report
            WHERE 
            (year > {start_date_obj.year} OR (year = {start_date_obj.year} AND 
            month > {start_date_obj.month}) OR (year = {start_date_obj.year} AND month = {start_date_obj.month} AND
            day >= {start_date_obj.day}))
            AND (year < {end_date_obj.year} OR (year = {end_date_obj.year} AND month < {end_date_obj.month}) OR 
            (year = {end_date_obj.year} AND month = {end_date_obj.month} AND day <= {end_date_obj.day}))
        """

        data = db.execute_query(query)

        if data.empty:
            error_data = {
                "code": "404",
                "status": "error",
                "type": "message",
                "value": "No data found for the given date range"
            }
            return jsonify(error_data), 404

        average_age_by_diagnose = {}
        for idx, row in data.iterrows():
            birth_date_str = row["BirthDate"]
            birth_date = parse_birth_date(birth_date_str)
            age = calculate_age(birth_date)
            diagnose = row["Diagnose"]

            if age is not None and diagnose:
                if diagnose not in average_age_by_diagnose:
                    average_age_by_diagnose[diagnose] = {"total_age": 0, "count": 0}

                average_age_by_diagnose[diagnose]["total_age"] += age
                average_age_by_diagnose[diagnose]["count"] += 1

        json_data = []
        for diagnose, info in average_age_by_diagnose.items():
            if info["count"] > 0:
                average_age = int(info["total_age"] / info["count"])
            else:
                average_age = None

            json_data.append({
                "diagnose": diagnose,
                "average_age": average_age
            })
            response_data = {
                "code": "200",
                "hints": "",
                "status": "success",
                "type": "message",
                "value": json_data
            }
        return jsonify(response_data), 200
    except FileNotFoundError as e:
        error_data = {
            "code": "404",
            "hints": "File Not Found Error",
            "status": "error",
            "type": "message",
            "value": str(e)
        }
        return jsonify(error_data), 404
    except Exception as e:
        error_data = {
            "code": "500",
            "hints": "Internal Server Error",
            "status": "error",
            "type": "message",
            "value": str(e)
        }
        return jsonify(error_data), 500

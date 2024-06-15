import pandas as pd
from flask import Blueprint, jsonify, request
from config import db
from datetime import datetime

bp = Blueprint('faculty', __name__)

def validate_date(date_str):
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False
#ti le benh trong khoa
@bp.route('/api/faculty/rate_of_patients_disease', methods=['GET'])
def rate_of_patients_disease():
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
            SELECT Faculty, Diagnose, COUNT(*) as PatientCount FROM report.report
            WHERE 
            (year > {start_date_obj.year} OR (year = {start_date_obj.year} AND 
            month > {start_date_obj.month}) OR (year = {start_date_obj.year} AND month = {start_date_obj.month} AND
            day >= {start_date_obj.day}))
            AND (year < {end_date_obj.year} OR (year = {end_date_obj.year} AND month < {end_date_obj.month}) OR 
            (year = {end_date_obj.year} AND month = {end_date_obj.month} AND day <= {end_date_obj.day}))
            GROUP BY Faculty, Diagnose
        """

        data = db.execute_query(query)

        if data.empty:
            error_data ={
                "code": "404",
                "status": "error",
                "type": "message",
                "value": "No data found for the given date range"
            }
            return jsonify(error_data), 404

        total_patients_per_faculty = data.groupby('Faculty')['PatientCount'].sum().reset_index()
        total_patients_per_faculty.columns = ['Faculty', 'TotalPatients']

        data = pd.merge(data, total_patients_per_faculty, on='Faculty')
        data['DiseaseRate'] = data['PatientCount'] / data['TotalPatients']

        json_data = data.to_dict(orient='records')

        response_data = {
            "code": "200",
            "status": "success",
            "type": "message",
            "value": json_data
        }

        return jsonify(response_data)

    except Exception as e:
        error_data = {
            "code": "500",
            "status": "error",
            "type": "message",
            "value": str(e)
        }
        return jsonify(error_data), 500



#tong ben nhan trong khoa
@bp.route('/api/faculty/sum_patient_faculty', methods=['GET'])
def sum_patient_faculty():
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
            SELECT faculty FROM report.report
            WHERE 
            (year > {start_date_obj.year} OR 
             (year = {start_date_obj.year} AND month > {start_date_obj.month}) OR 
             (year = {start_date_obj.year} AND month = {start_date_obj.month} AND day >= {start_date_obj.day}))
            AND 
            (year < {end_date_obj.year} OR 
             (year = {end_date_obj.year} AND month < {end_date_obj.month}) OR 
             (year = {end_date_obj.year} AND month = {end_date_obj.month} AND day <= {end_date_obj.day}))
        """

        data = db.execute_query(query)

        if data.empty:
            error_data = {
                "code": "400",
                "status": "error",
                "type": "message",
                "value": "No data found for the given date range"
            }
            return jsonify(error_data), 404

        grouped = data.groupby('faculty').size().reset_index(name='PatientCount')

        json_data = grouped.to_dict(orient='records')

        response_data = {
            "code": "200",
            "status": "success",
            "type": "message",
            "value": json_data
        }

        return jsonify(response_data)

    except Exception as e:
        error_data = {
            "code": "500",
            "status": "error",
            "type": "message",
            "value": str(e)
        }
        return jsonify(error_data), 500



#benh pho bien nhat
@bp.route('/api/faculty/most_common_disease', methods=['GET'])
def most_common_disease():
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
            SELECT Faculty, Diagnose, COUNT(*) as PatientCount 
            FROM report.report
            WHERE 
            (year > {start_date_obj.year} OR (year = {start_date_obj.year} AND 
            month > {start_date_obj.month}) OR (year = {start_date_obj.year} AND month = {start_date_obj.month} AND
            day >= {start_date_obj.day}))
            AND (year < {end_date_obj.year} OR (year = {end_date_obj.year} AND month < {end_date_obj.month}) OR 
            (year = {end_date_obj.year} AND month = {end_date_obj.month} AND day <= {end_date_obj.day}))
            GROUP BY Faculty, Diagnose
        """

        data = db.execute_query(query)

        if data.empty:
            error_data = {
                "code": "404",
                "hints": "",
                "status": "error",
                "type": "message",
                "value": "No data found for the given date range"
            }
            return jsonify(error_data), 404

        most_common_disease_per_faculty = data.loc[data.groupby('Faculty')['PatientCount'].idxmax()].reset_index(drop=True)

        response_data = most_common_disease_per_faculty.to_dict(orient='records')

        return jsonify({
            "code": "200",
            "status": "success",
            "type": "message",
            "value": response_data
        })

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
    # Handle other errors
        error_data = {
            "code": "500",
            "hints": "Internal Server Error",
            "status": "error",
            "type": "message",
            "value": str(e)
        }
        return jsonify(error_data), 500

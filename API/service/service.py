import json
from flask import Blueprint, jsonify, request
import pandas as pd
from config import db
from datetime import datetime

bp = Blueprint('service', __name__)

def validate_date(date_str):
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False

@bp.route('/api/service/sumservice/', methods=['GET'])
def sum_service():
    try:
        # Get JSON data from request
        start_date = request.headers.get('startdate')
        end_date = request.headers.get('enddate')

        # Validate dates
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

        # Convert dates to datetime objects
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
        # Extract day, month, year from start_date and end_date

        # Adjust query to include date filtering
        query = f"""
                       SELECT Service, Gender FROM report.report
                       WHERE 
                       (year > {start_date_obj.year} OR 
                        (year = {start_date_obj.year} AND month > {start_date_obj.month}) OR 
                        (year = {start_date_obj.year} AND month = {start_date_obj.month} AND day >= {start_date_obj.day}))
                       AND 
                       (year < {end_date_obj.year} OR 
                        (year = {end_date_obj.year} AND month < {end_date_obj.month}) OR 
                        (year = {end_date_obj.year} AND month = {end_date_obj.month} AND day <= {end_date_obj.day}))
                   """

        # Execute query and get data
        data = db.execute_query(query)

        # Check if data is empty and return appropriate response
        if data.empty:
            error_data = {
                "code": "404",
                "status": "error",
                "type": "message",
                "value": "No data found for the given date range"
            }
            return jsonify(error_data), 404

        # Initialize counters
        data['Service'] = data['Service'].str.strip('[]').str.replace('\\"', '').str.split('\",\"')
        data = data.explode('Service')
        data['Service'] = data['Service'].str.replace('"', '')  # Remove double quotes

        # Create pivot table to count occurrences of each service
        pivot_table = pd.pivot_table(data, index='Service', aggfunc='size')

        # Convert to dictionary format
        json_data = pivot_table.reset_index(name='total').to_dict(orient='records')

        response_data = {
            "code": "200",
            "hints": "",
            "status": "success",
            "type": "message",
            "value": json_data
        }

        return jsonify(response_data)

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
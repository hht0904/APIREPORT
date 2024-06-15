from flask import Blueprint, jsonify, request
from config import db
from datetime import datetime

bp = Blueprint('sum', __name__)


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


@bp.route('/api/sum/old/', methods=['GET'])
def api_old():
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
        start_day, start_month, start_year = start_date_obj.day, start_date_obj.month, start_date_obj.year
        end_day, end_month, end_year = end_date_obj.day, end_date_obj.month, end_date_obj.year

        # Adjust query to include date filtering
        query = f"""
            SELECT ID Patient,BirthDate,Diagnose  FROM report.report
            WHERE 
            (year > {start_year} OR (year = {start_year} AND 
            month > {start_month}) OR (year = {start_year} AND month = {start_month} AND
            day >= {start_day}))
            AND (year < {end_year} OR (year = {end_year} AND month < {end_month}) OR 
            (year = {end_year} AND month = {end_month} AND day <= {end_day}))
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
        children_count = 0
        adults_count = 0
        elders_count = 0

        # Process and return data with age calculation
        for idx, row in data.iterrows():
            birth_date_str = row["BirthDate"]
            birth_date = parse_birth_date(birth_date_str)
            age = calculate_age(birth_date)

            if age is not None:
                if age < 18:
                    children_count += 1
                elif age >= 18 and age < 60:
                    adults_count += 1
                elif age >= 60:
                    elders_count += 1

            data.at[idx, "Age"] = age

        # Create response dictionary
        data_count = {
            "children_count": children_count,
            "adults_count": adults_count,
            "elders_count": elders_count
        }
        response_data = {
            "code": "200",
            "hints": "",
            "status": "success",
            "type": "message",
            "value": [data_count]
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
        # Handle other errors
        error_data = {
            "code": "500",
            "hints": "Internal Server Error",
            "status": "error",
            "type": "message",
            "value": str(e)
        }
        return jsonify(error_data), 500


@bp.route('/api/sum/sum_patient_doctor', methods=['GET'])
def sum_patient_doctor():
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
            SELECT DoctorName FROM report.report
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
                "code": "404",
                "hints": "",
                "status": "error",
                "type": "message",
                "value": "No data found for the given date range"
            }
            return jsonify(error_data), 404
        grouped = data.groupby('DoctorName').size().reset_index(name='PatientCount')
        # Clean up and process data
        json_data=grouped.to_dict(orient='records')

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
        error_data = {
            "code": "500",
            "hints": "Internal Server Error",
            "status": "error",
            "type": "message",
            "value": str(e)
        }
        return jsonify(error_data), 500



#tong benh nhan
@bp.route('/api/sum/sumpatient', methods=['GET'])
def api_sum_patient():
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
            SELECT COUNT(*) as total_patients FROM report.report
            WHERE 
            (year > {start_date_obj.year} OR (year = {start_date_obj.year} AND 
            month > {start_date_obj.month}) OR (year = {start_date_obj.year} AND month = {start_date_obj.month} AND
            day >= {start_date_obj.day}))
            AND (year < {end_date_obj.year} OR (year = {end_date_obj.year} AND month < {end_date_obj.month}) OR 
            (year = {end_date_obj.year} AND month = {end_date_obj.month} AND day <= {end_date_obj.day}))
        """

        result = db.execute_query(query)
        if result.empty:
            error_data = {
                "code": "404",
                "status": "error",
                "type": "message",
                "value": "No data found for the given date range"
            }
            return jsonify(error_data), 404
        # Lấy tổng số bệnh nhân
        total_patients = int(result.iloc[0]['total_patients'])

        # Chuẩn bị dữ liệu phản hồi
        response_data = {
            "code": "200",
            "hints": "",
            "status": "success",
            "type": "message",
            "value": {"total_patients": total_patients}
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
        # Xử lý các lỗi khác
        error_data = {
            "code": "500",
            "hints": "Internal Server Error",
            "status": "error",
            "type": "message",
            "value": str(e)
        }
        return jsonify(error_data), 500

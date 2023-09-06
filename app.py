from flask import Flask, jsonify, request
import psycopg2

app = Flask(__name__)
db_params = {
    "host": "localhost",
    "database": "PropReturns",
    "user": "postgres",
    "password": "Dhyey@16",
    "port": 5435,
}

@app.route('/get_data_by_document_no', methods=['GET'])
def get_data_by_document_no():
    document_no = request.args.get('document_number')
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM "mbprop" WHERE "document_number" = %s', (document_no,))
    data = cursor.fetchall()
    conn.close()
    return jsonify(data)

@app.route('/get_data_by_year_of_registration', methods=['GET'])
def get_data_by_year_of_registration():
    year_of_registration = request.args.get('year_year')
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM "mbprop" WHERE "year_year" = %s', (year_of_registration,))
    data = cursor.fetchall()
    conn.close()
    return jsonify(data)

if __name__ == "__main__":
    app.run(debug=True)

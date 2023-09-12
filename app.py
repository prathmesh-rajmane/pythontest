from flask import Flask, request, jsonify
import pandas as pd
import Carbon_Source_dev
import io

app = Flask(__name__)

@app.route('/tmp', methods=['POST'])
def add_message():

    file = request.files['UserInput.xlsx']
    dataExcel = pd.read_excel(file, sheet_name="UserInput")
    
    PythonResponse = Carbon_Source_dev.Dashboard_Readin(dataExcel)

    headers = {'Content-Type': 'application/json'}
    return jsonify(PythonResponse), 200, headers

@app.route('/getData', methods=['POST'])
def getData():
    
    PythonResponse = None

    excel_content = request.data
    excel_file = io.BytesIO(excel_content)
    UserInputExcel = pd.read_excel(excel_file)
    print("Received user input file....")

    if UserInputExcel is None:
        print("Excel excel file is not present")
        PythonResponse =  {
            "message" : "Excel excel file is not present"
        }
    elif UserInputExcel.empty:
        PythonResponse = {
            "message" : "No data present in dataFrame"
        }
    else:
        print("Processing excel file and getting output")
        PythonResponse = Carbon_Source_dev.Dashboard_Readin(UserInputExcel)

    headers = {'Content-Type': 'application/json'}
    return jsonify(PythonResponse), 200, headers

if __name__ == '__main__':
    app.run(host= '0.0.0.0', port=5000)
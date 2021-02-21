from flask import Flask, request, render_template, redirect, url_for, abort, \
    send_from_directory
from werkzeug.utils import secure_filename
from werkzeug.datastructures import FileStorage
import os
import utils
import pandas as pd
import numpy as np

app = Flask(__name__)
app.config['UPLOAD_EXTENSIONS'] = ['.csv']

#docker compose connection
engine=utils.connection_bdd(host="db", user="test", password="supersecret", database="opencellid")

# local connection
#engine=utils.connection_bdd(host="127.0.0.1", user="root", password="my-secret-pw", database="cell_database")


@app.route('/')
def index():
    return render_template('index.html')

@app.route('/', methods=['POST'])
def upload_file():
    i=0 
    file_list=request.files.getlist('file')
    print("The length of list is: ", len(file_list))
    print("in_upload")
    for uploaded_file in file_list:
        print("i=")
        print(i)
        f = FileStorage(uploaded_file)
        print("uploaded_file:")
        print(type(upload_file))
        print("fileStorage?:")
        print(type(f))
        print("file_get:")
        #print(type(request.files.get('file')))
        
        #uploaded_file=request.files['file']
        filename = secure_filename(uploaded_file.filename)
        
        if filename != '':
            file_ext = os.path.splitext(filename)[1]
            print(file_ext)
            
            if file_ext not in app.config['UPLOAD_EXTENSIONS']:
                abort(400)
            if file_ext =='.csv':
                print("in_csv")
                #df = pd.read_csv(request.files.get('file'), header = 0, error_bad_lines=False)
                df = pd.read_csv(f, header = 0, error_bad_lines=False)
                number_of_split=(int)(len(df.index)/200000)
                print(len(df.index))
                print("numbers of split:")
                print(number_of_split)
                if (number_of_split<1):
                    number_of_split=1
                for df_part in np.array_split(df, number_of_split):
                    utils.importCSV_API(engine, df_part)
            else:
                print("no file")
        i=i+1
    return render_template('data.html')

@app.route('/CID_Search', methods=['POST', 'GET' ])
def CellID_Search():
    if request.method == "POST":
        mcc = request.form["MCC"]
        net = request.form["NET"]
        lac = request.form["LAC"]
        return utils.Search_cellID(engine=engine, mcc=mcc, net=net, lac=lac)
    else:
        return render_template("search.html")



if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
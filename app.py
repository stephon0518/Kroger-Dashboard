from flask import Flask, render_template, redirect, url_for, g, request, send_file,  jsonify, make_response
from flask_caching import Cache
from dotenv import load_dotenv
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, generate_blob_sas, BlobSasPermissions


from datetime import datetime, timedelta
import os
import sqlite3
import psycopg2
import polars as pl
import pandas as pd
import json
import plotly
import plotly.express as px


app = Flask(__name__)
cache = Cache(app, config={'CACHE_TYPE':'simple'})
load_dotenv()
accountName = os.getenv('ACCOUNT_NAME')
accountKey = os.getenv('ACCOUNT_KEY')
conatinerName = os.getenv('CONTAINER_NAME')
connStr = os.getenv('AZURE_BLOB')
detailedDF = pl.read_csv("data/detailed-data.csv")
#sampleDF = pl.read_csv("data/sample-dataset.csv")
basketDF = (
    detailedDF
        .group_by(['HSHD_NUM', 'BASKET_NUM'])
        .agg([
            pl.col("SPEND").sum().alias("TOTAL_SPENT_PER_BASKET"),
            pl.col("SPEND").count().alias("ITEM_COUNT")
        ])
)
hshdDF = (
    basketDF
        .group_by('HSHD_NUM')
        .agg([
            pl.col("TOTAL_SPENT_PER_BASKET").mean().alias("AVG_SPEND_PER_BASKET"),
            pl.col("ITEM_COUNT").mean().alias("AVG_ITEMS_PER_BASKET"),
            pl.col("TOTAL_SPENT_PER_BASKET").sum().alias("TOTAL_SPENT"),
            pl.col("TOTAL_SPENT_PER_BASKET").count().alias("TOTAL_TRANSACTIONS"),
        ])
)
demosDF = (
    detailedDF
        .group_by(['HSHD_NUM', 'AGE_RANGE', 'MARITAL', 'INCOME_RANGE','HOMEOWNER', 'CHILDREN']).agg()
)
joinedDF = demosDF.join(hshdDF,on='HSHD_NUM',how='inner')
tableDF = detailedDF.select(["HSHD_NUM","BASKET_NUM","DATE", "PRODUCT_NUM","DEPARTMENT", "COMMODITY","SPEND",'UNITS',"STORE_R", "WEEK_NUM", "YEAR"])


# # SQL Lite DB for login information.
# def connect_db():
#     conn = sqlite3.connect('dashboard.db')
#     cursor = conn.cursor()

#     cursor.executescript('''
#         create table if not exists user (
#             id integer primary key autoincrement,
#             username TEXT NOT NULL,
#             password TEXT NOT NULL,
#             email TEXT NOT NULL
#         );
#      ''')

#     conn.commit()
#     return conn

def connect_db():
    dbname = os.getenv('PG_DATABASE')
    user = os.getenv('PG_USER')
    password = os.getenv('PG_PASSWORD')
    host = os.getenv('PG_HOST')
    port = os.getenv('PG_PORT') # Default PostgreSQL port is 5432
    
    # Form the connection string
    conn_str = f"dbname={dbname} user={user} password={password} host={host} port={port}"
    
    conn = psycopg2.connect(conn_str)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS user (
            id SERIAL PRIMARY KEY,
            username TEXT NOT NULL,
            password TEXT NOT NULL,
            email TEXT NOT NULL
        );
    ''')

    conn.commit()
    
    return conn


@cache.memoize()
def loadData():
    #blob_client = container_client.get_blob_client("")
    return 0


# Gets blob from storage account and 
def getDetailedDataFrame():
    frames = []
    blobList = []

    blob_service_client  = BlobServiceClient.from_connection_string(connStr)
    container_client = blob_service_client.get_container_client(conatinerName)

    blob_list = container_client.list_blobs()
    for blob_i in blob_list:
        blobList.append(blob_i.name)
    
    for blob_i in blobList:
    #generate a shared access signature for each blob file
        
        sas_i = generate_blob_sas(account_name = accountName,
                                container_name = conatinerName,
                                blob_name = blob_i,
                                account_key=accountKey,
                                permission=BlobSasPermissions(read=True),
                                expiry=datetime.utcnow() + timedelta(hours=1))

def take2():
    frames = []
    namePart = os.getenv("DETAILED_BLOB_NAME_PART")
    for i in range(8):
        file = f"{conatinerName}/detailed_data.csv/part-0000{i}-{namePart}-{255+i}-c0000.csv"
        data = pd.read_csv(
            f"abfs://{file}",
            storage_options={
                "connection_string": connStr
        })

        frames.append(data)

    return pd.concat(frames)


# ROUTES

#------------------------------------------------

# Initial Route
@app.route('/', methods=['GET'])
def loadRegister():
    return render_template('register.html')

@app.route('/dashboard', methods=['GET'])
def loadDashboard():
    totalSpentAgeJSON, spendAgeJSON = getCharts("AGE_RANGE", "Age")
    totalSpentMarJSON, spendMarJSON = getCharts("MARITAL", "Marital Status")
    totalSpentIncJSON, spendIncJSON = getCharts("INCOME_RANGE", "Income Range")
    totalSpentChiJSON, spendChiJSON = getCharts("CHILDREN", "Number of Children")


    # Use render_template to pass graphJSON to html
    return render_template('dashboard.html',
                             totalSpentAgeBar=totalSpentAgeJSON, averageSpendAgePie=spendAgeJSON,
                             totalSpentMarBar=totalSpentMarJSON, averageSpendMarPie=spendMarJSON,
                             totalSpentIncBar=totalSpentIncJSON, averageSpendIncPie=spendIncJSON,
                             totalSpentChiBar=totalSpentChiJSON, averageSpendChiPie=spendChiJSON
                             )


def getCharts(selected_value, title):
    rangeDetail = (
        joinedDF
            .group_by([selected_value])
            .agg([
                pl.col("AVG_SPEND_PER_BASKET").mean(),
                pl.col('AVG_ITEMS_PER_BASKET').mean(),
                pl.col("TOTAL_TRANSACTIONS").sum(),
                pl.col("TOTAL_SPENT").sum(),
            ])
            .sort(selected_value)
    )
    rangeDetail = rangeDetail.with_columns(pl.when(pl.col(selected_value) == "null").then(pl.lit("N/A")).otherwise(pl.col(selected_value)).alias(selected_value))

    # Create charts
    totalSpent = px.bar(rangeDetail, x=selected_value, y='TOTAL_SPENT', barmode='group', title=f'Total Spent By {title}')
    spent = px.pie(rangeDetail, values='AVG_SPEND_PER_BASKET', names=selected_value, title=f'Average Spent Per Basket By {title}')
     
    # Create graphJSON
    totalSpentJSON = json.dumps(totalSpent, cls=plotly.utils.PlotlyJSONEncoder)
    spendJSON = json.dumps(spent, cls=plotly.utils.PlotlyJSONEncoder)

    return totalSpentJSON, spendJSON

@app.route('/dasboard/table/data-og', methods=['GET'])
def dashboardTableDataOG():
    dataDF = tableDF.to_dicts()
    return jsonify(dataDF)


@app.route('/dashboard/table/data')
def dashboardTableData():
    dataDF = tableDF
    
    # search
    search = request.args.get('search')
    if search:
        dataDF = dataDF.filter((pl.col('HSHD_NUM') == int(search)))
    totalFiltered = dataDF.height

    # sorting
    sort = request.args.get('sort')
    if sort:
        s = sort.split(',')

        desc = s[0] == '-'

        dataDF = dataDF.sort(s[1], descending=desc)

    # page
    start = request.args.get('start', type=int, default=-1)
    length = request.args.get('length', type=int, default=-1)
    if start != -1 and length != -1:
        dataDF = dataDF.slice(start, start+length)

    data = dataDF.to_pandas().values.tolist()
    dataJSON = jsonify(data)

    return {
        'data': data,
        'total': totalFiltered 
    }

@app.route('/login', methods=['GET'])
def loadLogin():
    return render_template('login.html')

@app.route('/login', methods=['POST'])
def login():
    username = request.form['username']
    password = request.form['password']
    
    g.db = connect_db()
    cursor = g.db.execute('SELECT * FROM user where username = %s and password = %s', (username, password))
    user = cursor.fetchone()
    if (user == None):
        g.db.close()
        return None
    g.db.close()
    return redirect(url_for('loadDashboard'))

@app.route('/register', methods=['POST'])
def register():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        email = request.form['email']
        
        g.db = connect_db()
        cursor = g.db.execute('SELECT * FROM user where username = %s and email = %s', (username, email))
        users = cursor.fetchall()

        if len(users) != 0:
            g.db.close()
            return render_template('register.html', error='username and email is already in use.')
        
        g.db.execute('INSERT INTO user (username, password, email) VALUES (%s, %s, %s)', (username, password, email))

        cursor = g.db.execute('SELECT * FROM user where username = %s', (username,))
        newUser = cursor.fetchone()

        g.db.commit()
        g.db.close()
        return redirect(url_for('loadDashboard'))

    

    
if __name__ == '__main__':
    PORT = os.environ.get('PORT', 5000)
    app.run(debug=True, host='0.0.0.0', port=PORT)
    #app.run(debug=True)
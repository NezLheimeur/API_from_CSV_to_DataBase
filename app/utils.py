import os
import dateutil.parser
import pandas as pd
from sqlalchemy import create_engine


#connect to the database
def connection_bdd(host, user, password, database):
    try:
        engine = create_engine('mysql://'+user+':'+password+'@'+host+':3306/'+database)
        return engine
    except Exception as error:
        print(error)

def clean_df_db_dups(df, tablename, engine, dup_cols=[],
                         filter_continuous_col=None, filter_categorical_col=None):
    """
    Remove rows from a dataframe that already exist in a database
    Required:
        df : dataframe to remove duplicate rows from
        engine: SQLAlchemy engine object
        tablename: tablename to check duplicates in
        dup_cols: list or tuple of column names to check for duplicate row values
    Optional:
        filter_continuous_col: the name of the continuous data column for BETWEEEN min/max filter
                               can be either a datetime, int, or float data type
                               useful for restricting the database table size to check
        filter_categorical_col : the name of the categorical data column for Where = value check
                                 Creates an "IN ()" check on the unique values in this column
    Returns
        Unique list of values from dataframe compared to database table
    """
    args = 'SELECT %s FROM %s' %(', '.join(['`{0}`'.format(col) for col in dup_cols]), tablename)
    args_contin_filter, args_cat_filter = None, None
    if filter_continuous_col is not None:
        if df[filter_continuous_col].dtype == 'datetime64[ns]':
            args_contin_filter = """ "%s" BETWEEN Convert(datetime, '%s')
                                          AND Convert(datetime, '%s')""" %(filter_continuous_col,
                              df[filter_continuous_col].min(), df[filter_continuous_col].max())


    if filter_categorical_col is not None:
        args_cat_filter = ' "%s" in(%s)' %(filter_categorical_col,
                          ', '.join(["'{0}'".format(value) for value in df[filter_categorical_col].unique()]))

    if args_contin_filter and args_cat_filter:
        args += ' Where ' + args_contin_filter + ' AND' + args_cat_filter
    elif args_contin_filter:
        args += ' Where ' + args_contin_filter
    elif args_cat_filter:
        args += ' Where ' + args_cat_filter

    df.drop_duplicates(dup_cols, keep='last', inplace=True)
    #print("args:")
    #print(args)
    #print("read sql")
    #print(pd.read_sql(args, engine))
    df = pd.merge(df, pd.read_sql(args, engine), how='left', on=dup_cols, indicator=True)
    df = df[df['_merge'] == 'left_only']
    df.drop(['_merge'], axis=1, inplace=True)
    return df

#import CSV file into database table from a file inside the csv_files folder
def importCSV(engine, csv_path):

    file = os.path.abspath('app') +"\\csv\\" + csv_path
    print(file)
    df = pd.read_csv(file, header = 0, error_bad_lines=False)
    #print(df)

    #engine = create_engine('mysql://'+user+':'+password+'@'+host+'/'+db)
    try:
        with engine.connect() as conn, conn.begin():
            df.to_sql('myTempTable', conn, if_exists='replace', index=False)
    except Exception as error:
        print(error)

     
    try:
        connection = engine.connect()
        sqlQuery="CREATE TABLE IF NOT EXISTS cellID LIKE myTempTable;"
        connection.execute(sqlQuery)
        sqlQuery="INSERT IGNORE INTO cellID SELECT * FROM myTempTable;"
        result = connection.execute(sqlQuery)
        connection.close()
    except Exception as error:
        print(error)
    
#import CSV data (in dataframe format) into database table. this function is called when a csv file is uploaded by user (see app.py file)
def importCSV_API(engine, dataframe):
    
    print("in_csv_api")
    df= None
    try:
        with engine.connect() as conn, conn.begin():
            dataframe.iloc[0:0].to_sql('myTempTable', conn, if_exists='replace', index=False)
    except Exception as error:
        print(error)

    try:
        print("in_data")
        connection = engine.connect()
        sqlQuery="CREATE TABLE IF NOT EXISTS cellID LIKE myTempTable;"
        connection.execute(sqlQuery)
        #print("before clean")
        #print(dataframe)
        dataframe = clean_df_db_dups(dataframe, "cellID", engine, dup_cols=list(dataframe.columns))
        #print("after clean")
        #print(dataframe)
        dataframe.to_sql("cellID", engine, if_exists='append', index=False)
        
        #sqlQuery="INSERT IGNORE INTO cellID SELECT * FROM myTempTable"
        #result = connection.execute(sqlQuery)
        connection.close()
    except Exception as error:
        print(error)
    
#return the amount of money processed during a range of time by currency. the acquirer shoud be specified and the table we should look in 
def Search_cellID(engine, mcc, net, lac):
        
    if (mcc=="0" or mcc=="" or mcc==0):
        mcc="mcc"
    if (net=="0" or net=="" or net==0):
        net="net"
    if (lac=="0" or lac=="" or lac==0):
        lac="area"
    

    try:
        connection = engine.connect()
        sqlQuery="SELECT cell FROM cellID  where  mcc="+mcc+" and net="+net+" and area="+lac+" ;"
        print(sqlQuery)
        
        result_data_frame=pd.read_sql(sqlQuery, connection)
        result = result_data_frame.to_html(classes='data')
        return result
        connection.close()
    except Exception as error:
        print(error)
    



#engine=connection_bdd(host="127.0.0.1", user="root", password="my-secret-pw", database="my_database")


#importCSV(engine, csv_path="270.csv")
#Amount_processed(engine=engine, table_name="table_csv", acquirer="CSV", time_begin="2020-08-18T23:00:33.859Z", time_end="2020-08-18T23:03:32.531Z")


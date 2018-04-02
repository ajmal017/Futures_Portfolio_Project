

import pandas as pd
import quandl
import sqlite3
from sqlite3 import Error
import datetime as dt

quandl.ApiConfig.api_key = "_srX4KQ6xix5aBeij-72"


# In[2]:
def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
 
    return None

# In[3]:
def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

# In[4]:
def create_db(filename):
    """ 
        create database and 5 tables
    """
    
    database = filename
 
    create_fut_contracts_table = """ CREATE TABLE IF NOT EXISTS fut_contracts (
                                        symbol TEXT PRIMARY KEY,
                                        sector TEXT,
                                        currency TEXT,
                                        point_value REAL,
                                        price_scale REAL,
                                        last_trade TEXT,
                                        last_update TEXT
                                    ); """
 
    create_fut_prices_table = """CREATE TABLE IF NOT EXISTS fut_prices (
                                    id INTEGER PRIMARY KEY,
                                    symbol TEXT,
                                    trade_date TEXT,
                                    exp_date TEXT,
                                    op REAL,
                                    h REAL,
                                    l REAL,
                                    settle REAL,
                                    vol REAL,
                                    oi REAL    
                                );"""
    
    create_fx_table = """ CREATE TABLE IF NOT EXISTS fx(
                            id INTEGER PRIMARY KEY,
                            fx_symbol TEXT,
                            trade_date TEXT,
                            fx_quote REAL,
                            H REAL,
                            L REAL);"""
    
    create_rates_table = """ CREATE TABLE IF NOT EXISTS int_rates(
                            id INTEGER PRIMARY KEY,
                            ir_currency TEXT,
                            ir_symbol TEXT,
                            trade_date TEXT,
                            int_rate REAL);"""
    
    create_cot_table = """ CREATE TABLE IF NOT EXISTS cot(
                            );"""
    
    # create a database connection
    conn = create_connection(database)
    if conn is not None:
        # create fut_cont table
        create_table(conn, create_fut_contracts_table)
        # create fut_prices table
        create_table(conn, create_fut_prices_table)
        # create fx table
        create_table(conn, create_fx_table)
        # create int_rates table
        create_table(conn, create_rates_table)
    else:
        print("Error! cannot create the database connection.")
        
# In[5]:
def construct_contract_list(exchange,symbol,start_year,end_year,months):
    """ 
        concatenate exchange, symbol, year, month strings into 1 unique
        symbol string
    """
    contract_list = []
    months = months
    #rev_yrs = reversed(range(start_year,end_year+1))
    for y in range(start_year, end_year+1):
        for m in months:
            contract_list.append("%s%s%s%s" % (exchange, symbol, m, y))
            
    return contract_list

# In[6]:
def contracts_not_in_db(contract_list, db_file):
    """
        check if symbol already exists in database
        write to database only if it does not
    """
    conn = create_connection(db_file)
    cur = conn.cursor()
    cur.execute("""SELECT * FROM fut_contracts """)
    rows = cur.fetchall()
    
    contract_exist = []
    new_contracts = []
    
    for row in rows:
        contract_exist.append(row[0])
        
    for contract in contract_list:
        if contract not in contract_exist:
            new_contracts.append(contract)
    
    return new_contracts
    
# In[7]:
def fetch_quandl_data(new_contracts, sector, currency, point_value, price_scale, database):
    """
        call Quandl API for each symbol
        pass contract data to write_contracts_to_db func
        pass price data to write_prices_to_db func
    """
    for contract in new_contracts:
        try:
            front_cont = quandl.get([contract])
            last_trade = front_cont.tail(1).index.date[0]
        except:
            print(contract, "Contract Does Not Exist")
        
        else:
            last_update = dt.date.today()
            write_contracts_to_db(contract, sector, currency, point_value, price_scale, last_trade, last_update, database)
            write_prices_to_db(front_cont,database,contract,last_trade)
    #all_prices = pd.concat(prices) 
    #return prices

# In[8]:
def write_contracts_to_db(symbol,sector,currency,point_value,price_scale,last_trade,last_update, database):
    try:
        conn = sqlite3.connect(database)
        cur = conn.cursor()
        query =  '''INSERT INTO fut_contracts (symbol,sector,currency,point_value,price_scale,last_trade,last_update)
                                VALUES (?, ?, ?, ?, ?, ?, ?)''' 
        data = (str(symbol), str(sector), str(currency), point_value, price_scale, str(last_trade), str(last_update))
        cur = conn.cursor()
        cur.execute(query,data)
        conn.commit()
    except Error as e:
        print(e)
            
# In[9]:
def write_prices_to_db(df,database,symbol,last_trade):
    """
        Identify 6 columns that are common to all exhcanges and symbols
        Itertuples and executemany methods allow for fast iteration and
        write performance
        API calls are throttled and slow things down
    """
    op_col = [col for col in df.columns if 'Open' in col]
    hi_col = [col for col in df.columns if 'High' in col]
    lo_col = [col for col in df.columns if 'Low' in col]
    cl_col = [col for col in df.columns if 'Settle' in col]
    vol_col = [col for col in df.columns if 'Volume' in col]
    oi_col = [col for col in df.columns if 'Open Interest' in col]
    
    cols = [op_col[0],hi_col[0],lo_col[0],cl_col[0],vol_col[0],oi_col[0]]
    df = df[cols]

    prices = []
    for row in df.itertuples():   
        trade_date = row[0]
        op = row[1]
        h = row[2]
        l = row[3]
        settle = row[4]
        vol = row[5]
        oi = row[6]

        data = (str(symbol),str(trade_date), str(last_trade), op, h, l, settle, vol, oi)
        prices.append(data)

    try:
        query =  '''INSERT INTO fut_prices (symbol,trade_date,exp_date,op,h,l,settle,vol,oi)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'''


        #data = (str(symbol),str(trade_date), str(days_till_exp), op, h, l, cl, settle, vol, oi)
        conn = sqlite3.connect(database)
        cur = conn.cursor()
        cur.executemany(query, prices)
        conn.commit()
        #cur.close()
    except Error as e:
        print(e)
    
    cur.close()   
    
    if conn is not None:
        conn.close()    
        
# In[10]:
def update_db(num_days_since_update,database):
    """
        1.Goes through the list of contracts in fut_contracts table
        2.Only calls Quandl API on those that are current(not expired)-
        -num_days_since_update <= 5 days(exchanges are not allowed to stay
        closed for more than 4 days even on holidays)
        3.Filters all data before last update
        4.Writes only new price data and updates fut_contracts table
        
    """
    conn = create_connection(database)
    cur = conn.cursor()
    cur.execute("""SELECT * FROM fut_contracts """)
    rows = cur.fetchall()
    #conn.close()  
        
    cont = []
    for row in rows:
        last_trade = pd.to_datetime(row[5])
        last_update = pd.to_datetime(row[6])
        date_diff = last_update - last_trade #pd.to_datetime(row[6]) - pd.to_datetime(row[5])
        symbol = row[0]
        if date_diff.days <= num_days_since_update:
            try:
                cont = quandl.get([row[0]])
                new = cont[cont.index > row[5]]
                last_trade = new.tail(1).index.date[0]
                last_updated = dt.date.today()
                
                write_prices_to_db(new,database,symbol,last_trade)
            
                try:
                    query = '''UPDATE fut_contracts
                            SET last_trade = ?, last_update = ?
                            WHERE symbol = ?'''
                    
                    data = (last_trade,last_updated,row[0])
                    
                    cur = conn.cursor()
                    cur.execute(query,data)
                    conn.commit()
                except Error as e:
                        print(e) 
            except:
                print("Contract Expired")
                    
        cur.close()   
    
    if conn is not None:
        conn.close()    

# In[23]:
def populate_db(df_markets, database):
        
    for row in df_markets.itertuples():
        exch = row[1]
        sym = row[2]
        
        print("Processing {} ".format(sym))
        
        months = row[3]
        start = row[4]
        end = row[5]
        bpv = row[6]
        price_scale = row[7]
        currency = row[8]
        sector = row[9]

        symbol_list = construct_contract_list(exch,sym,start,end,months)
        new_symbols = contracts_not_in_db(symbol_list,database)
        fetch_quandl_data(new_symbols,sector,currency,bpv,price_scale,database)
        
  



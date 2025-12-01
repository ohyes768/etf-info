# database.py
import sqlite3
import pandas as pd
from datetime import datetime, timedelta

# 数据库文件名
DB_FILENAME = "stock_data.db"

# 计算5年前的日期
def get_five_years_ago():
    return datetime.now() - timedelta(days=5*365)

def init_database():
    """
    初始化数据库表结构
    """
    conn = sqlite3.connect(DB_FILENAME)
    cursor = conn.cursor()
    
    # 创建股票价格数据表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_price (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_code TEXT NOT NULL,
            date DATE NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            UNIQUE(stock_code, date)
        )
    ''')
    
    # 创建净资产数据表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS net_assets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_code TEXT NOT NULL,
            report_date DATE NOT NULL,
            equity TEXT,
            net_assets REAL,
            UNIQUE(stock_code, report_date)
        )
    ''')
    
    # 创建每日PB数据表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS daily_pb (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_code TEXT NOT NULL,
            date DATE NOT NULL,
            close_price REAL,
            total_shares REAL,
            market_cap REAL,
            net_assets REAL,
            report_date DATE,
            pb REAL,
            UNIQUE(stock_code, date)
        )
    ''')
    
    # 创建总股本变动数据表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS total_shares_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_code TEXT NOT NULL,
            change_date DATE NOT NULL,
            total_shares REAL,
            restricted_shares REAL,
            circulating_shares REAL,
            change_reason TEXT,
            UNIQUE(stock_code, change_date)
        )
    ''')
    
    conn.commit()
    conn.close()

def save_price_data_to_db(stock_code, price_data):
    """
    将股价数据保存到数据库 (只保存近5年数据)
    """
    conn = sqlite3.connect(DB_FILENAME)
    
    # 过滤近5年数据
    five_years_ago = get_five_years_ago()
    price_data_filtered = price_data[price_data['date'] >= five_years_ago]
    
    # 添加股票代码列
    price_data_with_code = price_data_filtered.copy()
    price_data_with_code['stock_code'] = stock_code
    
    # 重命名列以匹配数据库结构
    price_data_with_code = price_data_with_code.rename(columns={
        'date': 'date',
        'open': 'open',
        'high': 'high',
        'low': 'low',
        'close': 'close',
        'volume': 'volume'
    })
    
    # 保存到数据库，冲突时忽略（保留已有数据）
    for index, row in price_data_with_code.iterrows():
        try:
            cursor = conn.cursor()
            # 将日期转换为字符串格式
            date_str = row['date'].strftime('%Y-%m-%d') if pd.notnull(row['date']) else None
            cursor.execute('''
                INSERT OR IGNORE INTO stock_price 
                (stock_code, date, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (row['stock_code'], date_str, row['open'], row['high'], 
                  row['low'], row['close'], row['volume']))
        except Exception as e:
            print(f"保存股价数据时出错: {e}")
    
    conn.commit()
    conn.close()
    print(f"股价数据已保存到数据库，共 {len(price_data_with_code)} 条记录 (近5年)")

def get_price_data_from_db(stock_code):
    """
    从数据库获取股价数据
    """
    conn = sqlite3.connect(DB_FILENAME)
    # 只查询近5年的数据
    five_years_ago = get_five_years_ago()
    query = '''
        SELECT date, open, high, low, close, volume 
        FROM stock_price 
        WHERE stock_code = ? AND date >= ?
        ORDER BY date
    '''
    try:
        price_data = pd.read_sql_query(query, conn, params=(stock_code, five_years_ago))
        if not price_data.empty:
            price_data['date'] = pd.to_datetime(price_data['date'])
            print(f"从数据库读取到 {len(price_data)} 条股价记录 (近5年)")
        return price_data
    except Exception as e:
        print(f"从数据库读取股价数据失败: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

def save_net_assets_to_db(stock_code, net_assets_data):
    """
    将净资产数据保存到数据库 (只保存近5年数据)
    """
    conn = sqlite3.connect(DB_FILENAME)
    
    # 过滤近5年数据
    five_years_ago = get_five_years_ago()
    net_assets_filtered = net_assets_data[net_assets_data['报告期'] >= five_years_ago]
    
    # 添加股票代码列
    net_assets_with_code = net_assets_filtered.copy()
    net_assets_with_code['stock_code'] = stock_code
    
    # 重命名列以匹配数据库结构
    net_assets_with_code = net_assets_with_code.rename(columns={
        '报告期': 'report_date',
        '所有者权益（或股东权益）合计': 'equity',
        '净资产(元)': 'net_assets'
    })
    
    # 保存到数据库，冲突时忽略（保留已有数据）
    for index, row in net_assets_with_code.iterrows():
        try:
            cursor = conn.cursor()
            # 将日期转换为字符串格式
            report_date_str = row['report_date'].strftime('%Y-%m-%d') if pd.notnull(row['report_date']) else None
            cursor.execute('''
                INSERT OR IGNORE INTO net_assets 
                (stock_code, report_date, equity, net_assets)
                VALUES (?, ?, ?, ?)
            ''', (row['stock_code'], report_date_str, row['equity'], row['net_assets']))
        except Exception as e:
            print(f"保存净资产数据时出错: {e}")
    
    conn.commit()
    conn.close()
    print(f"净资产数据已保存到数据库，共 {len(net_assets_with_code)} 条记录 (近5年)")

def get_net_assets_from_db(stock_code):
    """
    从数据库获取净资产数据 (近5年)
    """
    conn = sqlite3.connect(DB_FILENAME)
    # 只查询近5年的数据
    five_years_ago = get_five_years_ago()
    query = '''
        SELECT report_date, equity, net_assets 
        FROM net_assets 
        WHERE stock_code = ? AND report_date >= ?
        ORDER BY report_date DESC
    '''
    try:
        net_assets_data = pd.read_sql_query(query, conn, params=(stock_code, five_years_ago))
        if not net_assets_data.empty:
            net_assets_data['report_date'] = pd.to_datetime(net_assets_data['report_date'])
        return net_assets_data
    except Exception as e:
        print(f"从数据库读取净资产数据失败: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

def save_pb_to_db(stock_code, pb_data):
    """
    将每日PB数据保存到数据库 (只保存近5年数据)
    """
    conn = sqlite3.connect(DB_FILENAME)
    
    # 过滤近5年数据
    five_years_ago = get_five_years_ago()
    pb_data_filtered = pb_data[pb_data['date'] >= five_years_ago]
    
    # 添加股票代码列
    pb_data_with_code = pb_data_filtered.copy()
    pb_data_with_code['stock_code'] = stock_code
    
    # 重命名列以匹配数据库结构
    pb_data_with_code = pb_data_with_code.rename(columns={
        'date': 'date',
        'close_price': 'close_price',
        'total_shares': 'total_shares',
        'market_cap': 'market_cap',
        'net_assets': 'net_assets',
        'report_date': 'report_date',
        'pb': 'pb'
    })
    
    # 保存到数据库，冲突时忽略（保留已有数据）
    for index, row in pb_data_with_code.iterrows():
        try:
            cursor = conn.cursor()
            # 将日期转换为字符串格式
            date_str = row['date'].strftime('%Y-%m-%d') if pd.notnull(row['date']) else None
            report_date_str = row['report_date'].strftime('%Y-%m-%d') if pd.notnull(row['report_date']) else None
            cursor.execute('''
                INSERT OR IGNORE INTO daily_pb 
                (stock_code, date, close_price, total_shares, market_cap, net_assets, report_date, pb)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (row['stock_code'], date_str, row['close_price'], row['total_shares'],
                  row['market_cap'], row['net_assets'], report_date_str, row['pb']))
        except Exception as e:
            print(f"保存PB数据时出错: {e}")
    
    conn.commit()
    conn.close()
    print(f"PB数据已保存到数据库，共 {len(pb_data_with_code)} 条记录 (近5年)")

def get_pb_from_db(stock_code):
    """
    从数据库获取PB数据 (近5年)
    """
    conn = sqlite3.connect(DB_FILENAME)
    # 只查询近5年的数据，并按日期升序排列
    five_years_ago = get_five_years_ago()
    query = '''
        SELECT date, close_price, total_shares, market_cap, net_assets, report_date, pb 
        FROM daily_pb 
        WHERE stock_code = ? AND date >= ?
        ORDER BY date ASC
    '''
    try:
        pb_data = pd.read_sql_query(query, conn, params=(stock_code, five_years_ago))
        if not pb_data.empty:
            pb_data['date'] = pd.to_datetime(pb_data['date'])
            if 'report_date' in pb_data.columns and pb_data['report_date'].dtype == 'object':
                pb_data['report_date'] = pd.to_datetime(pb_data['report_date'])
        return pb_data
    except Exception as e:
        print(f"从数据库读取PB数据失败: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

def save_total_shares_history_to_db(stock_code, shares_history_data):
    """
    将总股本变动历史数据保存到数据库
    """
    conn = sqlite3.connect(DB_FILENAME)
    
    # 添加股票代码列
    shares_history_with_code = shares_history_data.copy()
    shares_history_with_code['stock_code'] = stock_code
    
    # 重命名列以匹配数据库结构
    shares_history_with_code = shares_history_with_code.rename(columns={
        '变更日期': 'change_date',
        '总股本': 'total_shares',
        '流通受限股份': 'restricted_shares',
        '已流通股份': 'circulating_shares',
        '变动原因': 'change_reason'
    })
    
    # 保存到数据库，冲突时替换
    for index, row in shares_history_with_code.iterrows():
        try:
            cursor = conn.cursor()
            # 将日期转换为字符串格式
            change_date_str = row['change_date'].strftime('%Y-%m-%d') if pd.notnull(row['change_date']) else None
            cursor.execute('''
                INSERT OR REPLACE INTO total_shares_history 
                (stock_code, change_date, total_shares, restricted_shares, circulating_shares, change_reason)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (row['stock_code'], change_date_str, row['total_shares'], 
                  row['restricted_shares'], row['circulating_shares'], row['change_reason']))
        except Exception as e:
            print(f"保存总股本变动历史数据时出错: {e}")
    
    conn.commit()
    conn.close()
    print(f"总股本变动历史数据已保存到数据库，共 {len(shares_history_with_code)} 条记录")

def get_total_shares_history_from_db(stock_code):
    """
    从数据库获取总股本变动历史数据
    """
    conn = sqlite3.connect(DB_FILENAME)
    query = '''
        SELECT change_date, total_shares, restricted_shares, circulating_shares, change_reason
        FROM total_shares_history 
        WHERE stock_code = ?
        ORDER BY change_date DESC
    '''
    try:
        shares_history_data = pd.read_sql_query(query, conn, params=(stock_code,))
        if not shares_history_data.empty:
            shares_history_data['change_date'] = pd.to_datetime(shares_history_data['change_date'])
        return shares_history_data
    except Exception as e:
        print(f"从数据库读取总股本变动历史数据失败: {e}")
        return pd.DataFrame()
    finally:
        conn.close()
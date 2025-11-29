# etf_pb_cal.py
import pandas as pd
import sqlite3
from datetime import datetime
from database import init_database
from pbcal import calculate_daily_pb

def process_all_stocks_from_holdings():
    """
    从hold.csv文件中读取所有股票代码，并计算每个股票的历史PB数据
    """
    try:
        # 读取hold.csv文件
        holdings_df = pd.read_csv('hold.csv', encoding='utf-8')
        print(f"成功读取到 {len(holdings_df)} 只股票")
        
        # 初始化数据库
        init_database()
        
        # 遍历所有股票代码
        for index, row in holdings_df.iterrows():
            try:
                # 直接按位置获取正确的列数据，避免列名混淆问题
                stock_code_raw = str(row.iloc[1])  # 第二列是股票代码
                stock_name = str(row.iloc[2])      # 第三列是股票名称
                
                # 清理股票代码，移除可能的*号前缀
                stock_code = stock_code_raw.strip()
                if '*' in stock_code:
                    # 分割并取最后一个部分作为股票代码
                    parts = stock_code.split('*')
                    stock_code = parts[-1]
                
                # 确保股票代码不是空字符串
                if not stock_code:
                    print(f"[{index+1}/{len(holdings_df)}] 跳过无效股票代码")
                    continue
                    
                print(f"\n[{index}/{len(holdings_df)}] 开始处理 {stock_name} ({stock_code})...")
                
                # 计算该股票的历史PB数据
                pb_history = calculate_daily_pb(stock_code)
                
                if pb_history is not None and not pb_history.empty:
                    print(f"{stock_name} ({stock_code}) 的PB数据计算完成，共 {len(pb_history)} 条记录")
                    
                    # 显示最新的PB值
                    latest_row = pb_history.iloc[-1]
                    try:
                        print(f"  最新日期: {latest_row['date'].strftime('%Y-%m-%d')}")
                        print(f"  收盘价: {latest_row['close_price']:.2f}")
                        print(f"  PB: {latest_row['pb']:.2f}")
                    except Exception as e:
                        print(f"  无法显示最新数据: {e}")
                else:
                    print(f"{stock_name} ({stock_code}) 的PB数据计算失败或无数据")
                    
            except Exception as e:
                print(f"处理第 {index+1} 行数据时发生错误: {e}")
                continue
        
        print("\n所有股票处理完成！")
        
        # 输出统计信息
        try:
            conn = sqlite3.connect('stock_data.db')
            cursor = conn.cursor()
            
            # 查询各个表的数据量
            cursor.execute("SELECT COUNT(DISTINCT stock_code) FROM stock_price")
            price_stocks = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(DISTINCT stock_code) FROM net_assets")
            assets_stocks = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(DISTINCT stock_code) FROM daily_pb")
            pb_stocks = cursor.fetchone()[0]
            
            print(f"\n数据库统计信息:")
            print(f"  已处理股价数据的股票数量: {price_stocks}")
            print(f"  已处理净资产数据的股票数量: {assets_stocks}")
            print(f"  已计算PB数据的股票数量: {pb_stocks}")
            
            conn.close()
        except Exception as e:
            print(f"获取数据库统计信息时出错: {e}")
        
    except FileNotFoundError:
        print("错误: 找不到 hold.csv 文件，请确保文件存在于当前目录")
    except Exception as e:
        print(f"处理过程中发生错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    start_time = datetime.now()
    print(f"开始执行ETF持仓股票PB计算任务: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    process_all_stocks_from_holdings()
    
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"\n任务完成时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"总耗时: {duration}")
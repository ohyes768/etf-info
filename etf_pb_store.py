# etf_pb_store.py
import pandas as pd
import os
import time
from stock_pbcal import init_database, calculate_daily_pb

def process_etf_stocks(etf_code):
    """
    处理ETF中的所有股票，计算并保存PB值到数据库
    
    Args:
        etf_code (str): ETF代码
    """
    # 构建文件路径（从etf-codes目录中读取）
    filename = f"etf-codes/hold_{etf_code}.csv"
    
    # 检查文件是否存在
    if not os.path.exists(filename):
        print(f"文件 {filename} 不存在")
        return
    
    try:
        # 直接使用pandas读取CSV文件
        df = pd.read_csv(filename, encoding='utf-8-sig')
        
        # 初始化数据库
        init_database()
        
        # 提取所有股票代码（去重）
        stock_codes = df['股票代码'].unique().tolist()
        
        print(f"ETF {etf_code} 包含 {len(stock_codes)} 只不同的股票")
        print("开始计算各股票的PB值...")
        
        # 遍历所有股票代码并计算PB
        for i, stock_code in enumerate(stock_codes, 1):
            print(f"\n[{i}/{len(stock_codes)}] 正在处理股票: {stock_code}")
            try:
                pb_result = calculate_daily_pb(str(stock_code))
                # 增加等待耗时5s，避免频繁调用
                time.sleep(5)
                if pb_result is not None:
                    latest_pb = pb_result.iloc[-1]['pb']
                    print(f"股票 {stock_code} 的最新PB值: {latest_pb:.2f}")
                else:
                    print(f"股票 {stock_code} 的PB计算失败")
            except Exception as e:
                print(f"处理股票 {stock_code} 时出现错误: {e}")
                
        print(f"\n已完成ETF {etf_code} 中所有股票的PB计算和保存")
        
    except Exception as e:
        print(f"处理文件时出错: {e}")

# 直接指定ETF代码
if __name__ == "__main__":
    # 直接指定ETF代码，无需交互式输入
    etf_code = "588170"  # 指定要处理的ETF代码
    process_etf_stocks(etf_code)
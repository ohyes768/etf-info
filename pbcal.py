# pbcal.py
import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
from database import (
    init_database, 
    get_price_data_from_db, 
    save_price_data_to_db,
    get_net_assets_from_db,
    save_net_assets_to_db,
    get_pb_from_db,
    save_pb_to_db
)

def get_net_assets_per_quarter(stock_code):
    """
    获取股票每季度的净资产数据 (近5年)
    """
    # 首先尝试从数据库获取
    net_assets_data = get_net_assets_from_db(stock_code)
    if not net_assets_data.empty:
        print("从数据库获取净资产数据 (近5年)...")
        return net_assets_data
    
    print("从网络获取净资产数据...")
    # 获取数据
    debt_info = ak.stock_financial_debt_ths(stock_code)
    
    # 筛选报告期和净资产数据
    net_assets_data = debt_info[['报告期', '所有者权益（或股东权益）合计']].copy()
    
    # 转换净资产为数值
    def convert_to_numeric(value):
        """将带'亿'单位的字符串转换为数值"""
        try:
            if isinstance(value, str) and '亿' in value:
                return float(value.replace('亿', '')) * 100000000  # 转换为具体数值
            elif isinstance(value, str) and '-' in value and '亿' in value:
                return float(value.replace('亿', '')) * 100000000  # 负数情况
            return float(value)
        except:
            return np.nan
    
    # 添加数值列
    net_assets_data['净资产(元)'] = net_assets_data['所有者权益（或股东权益）合计'].apply(convert_to_numeric)
    
    # 转换报告期为日期格式
    net_assets_data['报告期'] = pd.to_datetime(net_assets_data['报告期'])
    
    # 按报告期排序
    net_assets_data = net_assets_data.sort_values('报告期', ascending=False)
    
    # 保存到数据库
    save_net_assets_to_db(stock_code, net_assets_data)
    
    # 返回近5年数据
    five_years_ago = datetime.now() - timedelta(days=5*365)
    return net_assets_data[net_assets_data['报告期'] >= five_years_ago]

def get_stock_price_data(stock_code):
    """
    获取股票历史价格数据，优先从数据库读取，否则从网络获取 (近5年)
    """
    # 首先尝试从数据库获取
    price_data = get_price_data_from_db(stock_code)
    if not price_data.empty:
        return price_data
    
    # 从网络获取数据
    print("从网络获取价格数据...")
    try:
        # 根据股票代码确定市场前缀
        if stock_code.startswith(('6', '688')):
            symbol = f"sh{stock_code}"
        else:
            symbol = f"sz{stock_code}"
        
        price_data = ak.stock_zh_a_daily(symbol=symbol)
        
        # 确保 'date' 列是 datetime 类型
        price_data['date'] = pd.to_datetime(price_data['date'])
        
        # 过滤近5年数据
        five_years_ago = datetime.now() - timedelta(days=5*365)
        price_data = price_data[price_data['date'] >= five_years_ago]
        
        # 保存到数据库
        save_price_data_to_db(stock_code, price_data)
        
        return price_data
    except Exception as e:
        print(f"获取股价数据失败: {e}")
        return None

def calculate_daily_pb(stock_code):
    """
    计算股票每日PB值 (近5年)
    """
    # 首先尝试从数据库获取已计算的PB数据
    pb_df = get_pb_from_db(stock_code)
    if not pb_df.empty:
        print("从数据库获取PB数据 (近5年)...")
        return pb_df
    
    # 获取净资产数据
    print("获取净资产数据 (近5年)...")
    net_assets_df = get_net_assets_per_quarter(stock_code)
    if net_assets_df.empty:
        print("无法获取净资产数据")
        return None
    
    print("净资产数据:")
    print(net_assets_df[['报告期', '所有者权益（或股东权益）合计', '净资产(元)']].head(10))
    
    # 获取历史股价数据
    print("\n获取股价数据 (近5年)...")
    price_data = get_stock_price_data(stock_code)
    if price_data is None or price_data.empty:
        print("无法获取股价数据")
        return None
    
    # 确保日期列为datetime格式
    price_data['date'] = pd.to_datetime(price_data['date'])
    
    # 获取总股本信息
    try:
        stock_info = ak.stock_individual_info_em(symbol=stock_code)
        total_shares_row = stock_info[stock_info['item'] == '总股本']
        if not total_shares_row.empty:
            total_shares_text = total_shares_row.iloc[0]['value']
            if isinstance(total_shares_text, str) and '亿' in total_shares_text:
                total_shares = float(total_shares_text.replace('亿', '')) * 100000000
            else:
                total_shares = float(total_shares_text)
        else:
            # 如果无法获取总股本，使用估算值
            print("无法获取总股本信息，使用估算值")
            # 通过最新市值和股价反推总股本
            latest_price = price_data.iloc[-1]['close']
            # 假设最新市值约为最新净资产的某个倍数
            latest_net_assets = net_assets_df.iloc[0]['净资产(元)']
            total_shares = latest_net_assets / 10 / latest_price  # 假设合理PB为10左右
            print(f"估算总股本为: {total_shares:,.0f} 股")
    except Exception as e:
        print(f"获取总股本信息失败: {e}")
        return None
    
    print(f"总股本: {total_shares:,.0f} 股")
    
    # 按报告期排序净资产数据
    net_assets_df = net_assets_df.sort_values('报告期')
    
    # 计算每日PB (只计算近5年数据)
    pb_results = []
    for idx, row in price_data.iterrows():
        current_date = row['date']
        close_price = row['close']
        
        # 找到当前日期之前最近的财报日期
        applicable_report = net_assets_df[net_assets_df['报告期'] <= current_date]
        
        if not applicable_report.empty:
            # 使用最近的财报数据
            latest_net_assets = applicable_report.iloc[-1]['净资产(元)']
            report_date = applicable_report.iloc[-1]['报告期']
            
            # 计算PB: PB = 总市值 / 净资产
            # 总市值 = 股价 * 总股本
            market_cap = close_price * total_shares
            pb = market_cap / latest_net_assets if latest_net_assets != 0 else np.nan
            
            pb_results.append({
                'date': current_date,
                'close_price': close_price,
                'total_shares': total_shares,
                'market_cap': market_cap,
                'net_assets': latest_net_assets,
                'report_date': report_date,
                'pb': pb
            })
        else:
            # 如果没有找到适用的财报数据
            pb_results.append({
                'date': current_date,
                'close_price': close_price,
                'total_shares': total_shares,
                'market_cap': np.nan,
                'net_assets': np.nan,
                'report_date': None,
                'pb': np.nan
            })
    
    # 创建结果DataFrame
    pb_df = pd.DataFrame(pb_results)
    
    # 保存到数据库
    save_pb_to_db(stock_code, pb_df)
    
    return pb_df

# 主程序
if __name__ == "__main__":
    # 初始化数据库
    init_database()
    
    stock_code = "688072"  # 中微公司
    
    print(f"开始计算 {stock_code} 的每日PB (近5年)...")
    pb_history = calculate_daily_pb(stock_code)
    
    if pb_history is not None:
        # 显示最近的PB数据
        print("\n最近10个交易日的PB数据:")
        print(pb_history[['date', 'close_price', 'market_cap', 'net_assets', 'pb']].tail(10).to_string(index=False))
        
        # 显示最新的PB值
        latest_row = pb_history.iloc[-1]
        print(f"\n最新数据:")
        print(f"日期: {latest_row['date'].strftime('%Y-%m-%d')}")
        print(f"收盘价: {latest_row['close_price']:.2f}")
        print(f"总市值: {latest_row['market_cap']:,.0f}")
        print(f"净资产: {latest_row['net_assets']:,.0f}")
        print(f"PB: {latest_row['pb']:.2f}")
    else:
        print("计算失败")
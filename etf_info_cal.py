import requests
import os
import pandas as pd
from datetime import datetime, timedelta
import time
import json
import sys
from dotenv import load_dotenv

# 在脚本开头添加这一行
load_dotenv()

def analyze_etf(etf_code):
    """
    分析指定ETF的历史数据
    
    Args:
        etf_code (str): ETF基金代码
    """
    # 定义CSV文件路径 - 与etf_select.py中相同的目录结构
    csv_directory = "etf-codes"
    csv_file = os.path.join(csv_directory, f"his_{etf_code}.csv")
        
    # 确保etf-codes目录存在
    os.makedirs(csv_directory, exist_ok=True)

    # 如果CSV文件存在，则从CSV读取数据；否则调用API获取数据并保存到CSV
    if os.path.exists(csv_file):
        print("从本地CSV文件读取数据...")
        # 读取CSV文件
        df = pd.read_csv(csv_file)
        print(f"从CSV文件加载了 {len(df)} 条数据")
    else:
        print("从网络获取数据...")
        appcode = os.getenv('ALIYUN_APPCODE')
        if not appcode:
            print("错误: 请在.env文件中设置ALIYUN_APPCODE")
            return
        headers = {"Authorization": f"APPCODE {appcode}"}
        all_data = []
        pidx = 1

        while True:
            fund_url = f"https://alirmcom2.market.alicloudapi.com/query/comkm?symbol=SH{etf_code}&period=D&pidx={pidx}&psize=500&withlast=0"
            fund_his_response = requests.get(fund_url, headers=headers)
            fund_his_result = fund_his_response.json()['Obj']
            
            print(f"第{pidx}页获取到 {len(fund_his_result)} 条数据")
            
            # 将当前页数据添加到总数据中
            all_data.extend(fund_his_result)
            
            # 如果当前页数据少于500条，说明已经到最后一页，停止循环
            if len(fund_his_result) < 500:
                print("数据获取完成")
                break
            
            # 如果获取到500条数据，继续获取下一页
            if len(fund_his_result) == 500:
                pidx += 1
                # 添加短暂延时避免请求过于频繁
                time.sleep(0.5)
                
        print(f"总共获取到 {len(all_data)} 条数据")
        
        # 将数据保存到CSV文件
        if all_data:
            # 将JSON数据转换为DataFrame
            df = pd.json_normalize(all_data)
            df.to_csv(csv_file, index=False)
            print(f"数据已保存到 {csv_file}")

    # 转换日期列
    df['D'] = pd.to_datetime(df['D'])

    # 计算近一年的数据
    today = df['D'].max()
    one_year_ago = today - timedelta(days=365)
    recent_data = df[df['D'] >= one_year_ago].copy()  # 使用.copy()创建副本

    # 计算每日振幅百分比：(最高价-最低价)/开盘价 * 100%
    recent_data.loc[:, '振幅'] = (recent_data['H'] - recent_data['L']) / recent_data['O'] * 100

    # 找出近一年内日最大振幅及对应日期
    max_volatility_idx = recent_data['振幅'].idxmax()
    max_volatility_day = recent_data.loc[max_volatility_idx]
    max_volatility_date = max_volatility_day['D']
    max_volatility_value = max_volatility_day['振幅']

    # 计算近一年日均成交额
    avg_turnover = recent_data['A'].mean()

    # 输出结果
    print("=== 近一年数据分析 ===")
    print(f"数据时间范围: {recent_data['D'].min().strftime('%Y-%m-%d')} 至 {recent_data['D'].max().strftime('%Y-%m-%d')}")
    print(f"\n近一年日最大振幅:")
    print(f"日期: {max_volatility_date.strftime('%Y-%m-%d')}")
    print(f"振幅: {max_volatility_value:.2f}%")
    print(f"开盘价: {max_volatility_day['O']}")
    print(f"最高价: {max_volatility_day['H']}")
    print(f"最低价: {max_volatility_day['L']}")
    print(f"收盘价: {max_volatility_day['C']}")
    print(f"涨跌幅度: {max_volatility_day['H'] - max_volatility_day['L']:.3f}")

    # 计算涨跌情况
    开盘价 = max_volatility_day['O']
    收盘价 = max_volatility_day['C']
    最高价 = max_volatility_day['H']
    最低价 = max_volatility_day['L']

    if 收盘价 > 开盘价:
        涨跌情况 = "上涨"
        涨跌幅 = (收盘价 - 开盘价) / 开盘价 * 100
    elif 收盘价 < 开盘价:
        涨跌情况 = "下跌"
        涨跌幅 = (开盘价 - 收盘价) / 开盘价 * 100
    else:
        涨跌情况 = "持平"
        涨跌幅 = 0

    print(f"当日{涨跌情况}: {涨跌幅:.2f}% (开盘价{开盘价} -> 收盘价{收盘价})")
    print(f"日内最大涨幅: {(最高价 - 开盘价) / 开盘价 * 100:.2f}%")
    print(f"日内最大跌幅: {(开盘价 - 最低价) / 开盘价 * 100:.2f}%")

    print(f"\n近一年日均成交额: {avg_turnover/100000000:.2f}亿元")
    
    # 计算近60日的数据
    sixty_days_ago = today - timedelta(days=60)
    recent_60_days_data = recent_data[recent_data['D'] >= sixty_days_ago].copy()
    
    # 找出近60日内最大振幅及移除后的平均振幅
    max_volatility_60_idx = recent_60_days_data['振幅'].idxmax()
    max_volatility_60_day = recent_60_days_data.loc[max_volatility_60_idx]
    recent_60_days_data_without_max = recent_60_days_data.drop(max_volatility_60_idx)
    avg_volatility_60_days_without_max = recent_60_days_data_without_max['振幅'].mean()
    
    print(f"\n=== 近60日数据分析 ===")
    print(f"数据时间范围: {recent_60_days_data['D'].min().strftime('%Y-%m-%d')} 至 {recent_60_days_data['D'].max().strftime('%Y-%m-%d')}")
    print(f"近60日最大振幅: {max_volatility_60_day['振幅']:.2f}% (日期: {max_volatility_60_day['D'].strftime('%Y-%m-%d')})")
    print(f"去除最大振幅日后的近60日平均日振幅: {avg_volatility_60_days_without_max:.2f}%")

def main():
    """
    主函数
    """
    # 交互式输入ETF基金代码
    etf_code = input("请输入ETF基金代码（直接回车使用默认代码588200）: ").strip()
    
    # 如果用户没有输入任何内容，使用默认代码
    if not etf_code:
        etf_code = "588200"
    
    print(f"开始分析ETF基金: {etf_code}")
    analyze_etf(etf_code)

if __name__ == "__main__":
    main()
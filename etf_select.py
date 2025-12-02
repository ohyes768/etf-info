import akshare as ak
import pandas as pd
import os
from datetime import datetime, timedelta
import sys

def analyze_etf(etf_code):
    """
    分析指定ETF的历史数据
    
    Args:
        etf_code (str or int): ETF基金代码
    """
    # 定义CSV文件路径 - 修改为etf-codes目录
    csv_directory = "etf-codes"
    csv_file = os.path.join(csv_directory, f"etf_history_{etf_code}.csv")

    # 确保etf-codes目录存在
    os.makedirs(csv_directory, exist_ok=True)

    # 如果CSV文件存在，则从CSV读取数据；否则调用akshare接口获取数据并保存到CSV
    if os.path.exists(csv_file):
        print("从本地CSV文件读取数据...")
        etf_his = pd.read_csv(csv_file)
    else:
        print("从网络获取数据...")
        # 获取ETF历史数据
        etf_his = ak.fund_etf_hist_em(etf_code)
        # 保存到CSV文件
        etf_his.to_csv(csv_file, index=False)
        print(f"数据已保存到 {csv_file}")

    # 获取ETF基金规模信息
    try:
        # 方法1: 尝试通过基金概况接口获取
        etf_info = ak.fund_etf_spot_em(str(etf_code))
        print("ETF基金基本信息:")
        print(etf_info)
        
        # 如果上面的方法不返回规模信息，尝试其他方法
        # 方法2: 通过ETF基金排行榜获取规模信息
        etf_rank = ak.fund_etf_rank_em()
        etf_data = etf_rank[etf_rank['基金代码'] == str(etf_code)]
        if not etf_data.empty:
            scale = etf_data.iloc[0]['规模'] if '规模' in etf_data.columns else "未知"
            print(f"\nETF基金({etf_code})规模: {scale}")
            
    except Exception as e:
        print(f"获取基金规模信息时出错: {e}")

    # 转换日期列为datetime类型
    etf_his['日期'] = pd.to_datetime(etf_his['日期'])

    # 计算近1年和近3年的数据
    today = datetime.now()
    one_year_ago = today - timedelta(days=365)
    three_years_ago = today - timedelta(days=3*365)

    # 近1年数据
    recent_1_year = etf_his[etf_his['日期'] >= one_year_ago]
    # 近3年数据
    recent_3_years = etf_his[etf_his['日期'] >= three_years_ago]

    # 计算最大日振幅和日均成交额
    max_volatility_1_year = recent_1_year['振幅'].max() if not recent_1_year.empty else 0
    avg_turnover_1_year = recent_1_year['成交额'].mean() if not recent_1_year.empty else 0

    # 近3年最大日振幅和日均成交额
    max_volatility_3_years = recent_3_years['振幅'].max() if not recent_3_years.empty else 0
    avg_turnover_3_years = recent_3_years['成交额'].mean() if not recent_3_years.empty else 0

    print("\n近1年数据:")
    print(f"最大日振幅: {max_volatility_1_year:.2f}%")
    print(f"日均成交额: {avg_turnover_1_year/100000000:,.2f}亿元")

    print("\n近3年数据:")
    print(f"最大日振幅: {max_volatility_3_years:.2f}%")
    print(f"日均成交额: {avg_turnover_3_years/100000000:,.2f}亿元")

    # 查找历史最大日振幅的详细信息
    max_volatility_row = etf_his.loc[etf_his['振幅'].idxmax()]

    print("\n=== 历史最大日振幅详情 ===")
    print(f"日期: {max_volatility_row['日期'].strftime('%Y-%m-%d')}")
    print(f"最大日振幅: {max_volatility_row['振幅']:.2f}%")
    print(f"开盘价: {max_volatility_row['开盘']}")
    print(f"最高价: {max_volatility_row['最高']}")
    print(f"最低价: {max_volatility_row['最低']}")
    print(f"收盘价: {max_volatility_row['收盘']}")
    print(f"涨跌幅: {max_volatility_row['涨跌幅']:.2f}%")
    print(f"涨跌额: {max_volatility_row['涨跌额']}")

    # 计算价格波动情况
    price_range = max_volatility_row['最高'] - max_volatility_row['最低']
    growth_from_low = max_volatility_row['收盘'] - max_volatility_row['最低']
    decline_from_high = max_volatility_row['最高'] - max_volatility_row['收盘']

    print(f"\n=== 价格波动分析 ===")
    print(f"当日价格波动范围: {price_range:.3f} (最高价{max_volatility_row['最高']} - 最低价{max_volatility_row['最低']})")

    if max_volatility_row['收盘'] >= max_volatility_row['开盘']:
        print(f"当日收涨: 收盘价比开盘价高 {max_volatility_row['收盘'] - max_volatility_row['开盘']:.3f}")
    else:
        print(f"当日收跌: 收盘价比开盘价低 {max_volatility_row['开盘'] - max_volatility_row['收盘']:.3f}")
        
    if max_volatility_row['收盘'] > max_volatility_row['最低']:
        print(f"从最低价上涨: 收盘价比最低价高 {growth_from_low:.3f}")
    elif max_volatility_row['收盘'] == max_volatility_row['最低']:
        print("收盘价等于最低价")
    else:
        print(f"收盘价比最低价低 {abs(growth_from_low):.3f}")

    if max_volatility_row['最高'] > max_volatility_row['收盘']:
        print(f"从最高价回落: 收盘价比最高价低 {decline_from_high:.3f}")
    elif max_volatility_row['最高'] == max_volatility_row['收盘']:
        print("收盘价等于最高价")
    else:
        print(f"收盘价比最高价高 {abs(decline_from_high):.3f}")

def main():
    """
    主函数
    """
    # 检查命令行参数
    if len(sys.argv) > 1:
        etf_code = sys.argv[1]
    else:
        # 默认ETF代码
        etf_code = "588200"
    
    print(f"开始分析ETF基金: {etf_code}")
    analyze_etf(etf_code)

if __name__ == "__main__":
    main()
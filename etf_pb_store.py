# etf_season_cal.py
import pandas as pd
import sqlite3
import numpy as np
import os
from database import get_pb_from_db
from datetime import datetime
import re

def read_etf_holdings(etf_code):
    """
    读取ETF持仓CSV文件，并补充缺失季度的数据
    
    Args:
        etf_code (str): ETF代码
        
    Returns:
        pandas.DataFrame: ETF持仓数据（包含补充的季度数据）
    """
    # 从etf-codes目录读取文件
    filename = f"etf-codes/hold_{etf_code}.csv"
    
    try:
        # 直接使用pandas读取CSV文件，确保股票代码作为字符串处理
        df = pd.read_csv(filename, encoding='utf-8-sig', dtype={'股票代码': str})
        
        # 确保股票代码格式正确（6位数字，不足的前面补0）
        df['股票代码'] = df['股票代码'].apply(lambda x: x.zfill(6) if pd.notna(x) and str(x).isdigit() else x)
        
        # 清理数据
        df['股票代码'] = df['股票代码'].astype(str).str.strip()
        # 处理占净值比例列，去掉百分号并转换为小数
        df['占净值比例'] = df['占净值比例'].astype(str).str.rstrip('%').astype(float) / 100
        
        # 过滤掉权重为0的持仓数据
        original_count = len(df)
        df = df[df['占净值比例'] > 0.0]
        filtered_count = original_count - len(df)
        if filtered_count > 0:
            print(f"已过滤掉 {filtered_count} 条权重为0的持仓数据")
        
        print(f"成功读取ETF {etf_code} 的持仓数据，共{len(df)}条记录")
        
        # 获取所有唯一季度
        unique_quarters = df['季度'].unique().tolist()
        print(f"原始数据包含季度: {unique_quarters}")
        
        # 提取年份和季度编号信息
        quarter_details = []
        for quarter in unique_quarters:
            year = extract_year_from_quarter(quarter)
            quarter_num = extract_quarter_number(quarter)
            if year is not None and quarter_num is not None:
                quarter_details.append((year, quarter_num, quarter))
        
        if not quarter_details:
            print("无法解析季度信息")
            return df
            
        # 按年份和季度排序
        quarter_details.sort()
        
        # 确定时间范围
        min_year = min([qd[0] for qd in quarter_details])
        max_year = max([qd[0] for qd in quarter_details])
        
        # 构建完整的季度序列
        complete_quarter_data = []
        last_known_data = None
        
        for year in range(min_year, max_year + 1):
            for quarter_num in range(1, 5):  # 1-4季度
                # 检查该季度是否存在
                quarter_exists = any(qd[0] == year and qd[1] == quarter_num for qd in quarter_details)
                
                if quarter_exists:
                    # 季度存在，保存当前数据作为后续缺失季度的参考
                    quarter_name = next(qd[2] for qd in quarter_details if qd[0] == year and qd[1] == quarter_num)
                    quarter_data = df[df['季度'] == quarter_name].copy()
                    # 再次过滤权重为0的数据
                    quarter_data = quarter_data[quarter_data['占净值比例'] > 0.0]
                    complete_quarter_data.append(quarter_data)
                    last_known_data = quarter_data.copy()
                else:
                    # 季度不存在，使用上一季度的数据
                    if last_known_data is not None:
                        # 修改季度名称为当前缺失的季度
                        missing_quarter_name = f"{year}年{quarter_num}季度股票投资明细"
                        filled_data = last_known_data.copy()
                        filled_data['季度'] = missing_quarter_name
                        complete_quarter_data.append(filled_data)
                        print(f"补充缺失季度: {missing_quarter_name} (使用上一季度数据)")
        
        # 合并所有数据
        if complete_quarter_data:
            df = pd.concat(complete_quarter_data, ignore_index=True)
            print(f"补充缺失季度后，总记录数: {len(df)}")
        
        return df
        
    except FileNotFoundError:
        print(f"文件 {filename} 不存在")
        return None
    except Exception as e:
        print(f"读取CSV文件时出错: {e}")
        return None

def extract_year_from_quarter(quarter_str):
    """
    从季度字符串中提取年份
    
    Args:
        quarter_str (str): 季度字符串，如"2025年1季度股票投资明细"
        
    Returns:
        int: 年份，如果无法提取则返回None
    """
    # 使用正则表达式提取年份
    match = re.search(r'(\d{4})年', quarter_str)
    if match:
        return int(match.group(1))
    return None

def extract_quarter_number(quarter_str):
    """
    从季度字符串中提取季度编号
    
    Args:
        quarter_str (str): 季度字符串，如"2025年1季度股票投资明细"
        
    Returns:
        int: 季度编号(1, 2, 3, 4)，如果无法提取则返回None
    """
    # 使用正则表达式提取季度编号
    match = re.search(r'(\d)季度', quarter_str)
    if match:
        return int(match.group(1))
    return None

def calculate_single_quarter_etf_pb_time_series(quarter_data, quarter_name):
    """
    计算单个季度的ETF PB时间序列
    
    Args:
        quarter_data (pandas.DataFrame): 单个季度的持仓数据
        quarter_name (str): 季度名称
        
    Returns:
        pandas.DataFrame: ETF的PB时间序列
    """
    print(f"\n开始计算 {quarter_name} 的ETF PB时间序列...")
    print(f"该季度包含 {len(quarter_data)} 只股票")
    
    # 提取年份和季度
    year = extract_year_from_quarter(quarter_name)
    quarter_num = extract_quarter_number(quarter_name)
    
    if year is None or quarter_num is None:
        print(f"无法从季度名称 '{quarter_name}' 中提取年份或季度信息")
        return None
    
    # 根据季度确定日期范围
    if quarter_num == 1:
        start_date = pd.Timestamp(year=year, month=1, day=1)
        end_date = pd.Timestamp(year=year, month=3, day=31)
    elif quarter_num == 2:
        start_date = pd.Timestamp(year=year, month=4, day=1)
        end_date = pd.Timestamp(year=year, month=6, day=30)
    elif quarter_num == 3:
        start_date = pd.Timestamp(year=year, month=7, day=1)
        end_date = pd.Timestamp(year=year, month=9, day=30)
    elif quarter_num == 4:
        start_date = pd.Timestamp(year=year, month=10, day=1)
        end_date = pd.Timestamp(year=year, month=12, day=31)
    else:
        print(f"无效的季度编号: {quarter_num}")
        return None
    
    print(f"季度 {quarter_name} 对应日期范围: {start_date.strftime('%Y-%m-%d')} 到 {end_date.strftime('%Y-%m-%d')}")
    
    # 获取所有股票的历史PB数据
    all_stock_pb_data = {}
    available_stocks = []
    
    for index, row in quarter_data.iterrows():
        stock_code = row['股票代码'].strip()
        weight = row['占净值比例']
        
        # print(f"正在获取股票 {stock_code} ({row['股票名称']}) 的历史PB数据...")
        
        # 获取股票历史PB数据
        try:
            pb_data = get_pb_from_db(stock_code)
            
            if not pb_data.empty and len(pb_data) > 0:
                # 过滤日期范围内的数据
                pb_data_filtered = pb_data[(pb_data['date'] >= start_date) & (pb_data['date'] <= end_date)]
                
                if not pb_data_filtered.empty:
                    all_stock_pb_data[stock_code] = {
                        'data': pb_data_filtered,
                        'weight': weight,
                        'name': row['股票名称']
                    }
                    available_stocks.append(stock_code)
                    # print(f"  -> 成功获取 {len(pb_data_filtered)} 条PB记录 (限定在季度日期范围内)")
                else:
                    print(f"  -> 股票 {stock_code} 在指定日期范围内未找到有效PB数据")
            else:
                print(f"  -> 股票 {stock_code} 未找到有效PB数据")
        except Exception as e:
            print(f"  -> 获取股票 {stock_code} 的PB数据时出错: {e}")
    
    if not available_stocks:
        print("没有可用的股票PB数据")
        return None
    
    # 对齐所有股票的日期并计算ETF的PB时间序列
    print("\n正在计算ETF的PB时间序列...")
    
    # 收集所有日期
    all_dates = set()
    for stock_code in available_stocks:
        stock_dates = all_stock_pb_data[stock_code]['data']['date']
        all_dates.update(stock_dates)
    
    # 确保日期在季度范围内
    all_dates = [date for date in all_dates if start_date <= date <= end_date]
    all_dates = sorted(list(all_dates))
    print(f"总共包含 {len(all_dates)} 个日期 (限定在季度日期范围内)")
    
    # 构建ETF的PB时间序列
    etf_pb_series = []
    
    for date in all_dates:
        weighted_pb_sum = 0
        total_weight = 0
        valid_stocks_count = 0
        
        for stock_code in available_stocks:
            stock_info = all_stock_pb_data[stock_code]
            stock_data = stock_info['data']
            weight = stock_info['weight']
            
            # 查找该日期的PB值
            date_data = stock_data[stock_data['date'] == date]
            if not date_data.empty:
                pb_value = date_data.iloc[0]['pb']
                if not np.isnan(pb_value) and pb_value > 0:
                    weighted_pb_sum += weight * pb_value
                    total_weight += weight
                    valid_stocks_count += 1
        
        # 只有当有权重时才计算
        if total_weight > 0:
            etf_pb = weighted_pb_sum / total_weight
            etf_pb_series.append({
                'date': date,
                'pb': etf_pb,
                'valid_stocks_count': valid_stocks_count,
                'total_weight': total_weight,
                'quarter': quarter_name
            })
    
    # 转换为DataFrame
    etf_pb_df = pd.DataFrame(etf_pb_series)
    
    if not etf_pb_df.empty:
        print(f"{quarter_name} 的PB时间序列计算完成，共 {len(etf_pb_df)} 条记录")
        return etf_pb_df
    else:
        print(f"无法计算 {quarter_name} 的PB时间序列")
        return None

def merge_quarterly_data_to_daily_curve(quarterly_pb_data, quarters_order):
    """
    将季度数据合并成按每日展示的单一曲线
    1-3月使用第一季度的PB序列
    4-6月使用第二季度的PB序列
    7-12月使用第三季度的PB序列
    
    Args:
        quarterly_pb_data (dict): 包含各季度PB数据的字典
        quarters_order (list): 季度名称的顺序列表
        
    Returns:
        pandas.DataFrame: 合并后的每日PB数据
    """
    if not quarterly_pb_data or not quarters_order:
        return None
    
    print("\n开始合并季度数据为每日曲线...")
    
    # 构建季度编号到数据的映射
    quarter_number_to_data = {}
    for quarter_name in quarters_order:
        quarter_num = extract_quarter_number(quarter_name)
        if quarter_num is not None and quarter_name in quarterly_pb_data:
            quarter_number_to_data[quarter_num] = quarterly_pb_data[quarter_name]
    
    # 获取所有日期范围
    all_dates = set()
    for pb_data in quarterly_pb_data.values():
        if pb_data is not None and not pb_data.empty:
            all_dates.update(pb_data['date'])
    
    if not all_dates:
        print("没有有效的日期数据")
        return None
    
    all_dates = sorted(list(all_dates))
    start_date = min(all_dates)
    end_date = max(all_dates)
    
    print(f"数据时间范围: {start_date.strftime('%Y-%m-%d')} 到 {end_date.strftime('%Y-%m-%d')}")
    
    # 构建每日数据
    daily_data = []
    
    # 按日处理
    for date in all_dates:
        year = date.year
        month = date.month
        
        # 确定该日期应使用的季度数据
        quarter_to_use = None
        if month in [1, 2, 3]:  # 1-3月
            quarter_to_use = 1
        elif month in [4, 5, 6]:  # 4-6月
            quarter_to_use = 2
        elif month in [7, 8, 9]:  # 7-9月
            quarter_to_use = 3
        elif month in [10, 11, 12]:  # 10-12月
            quarter_to_use = 4
        
        # 查找对应季度的数据
        pb_value = None
        if quarter_to_use in quarter_number_to_data:
            quarter_data = quarter_number_to_data[quarter_to_use]
            if quarter_data is not None and not quarter_data.empty:
                # 查找该日期的数据
                date_data = quarter_data[quarter_data['date'] == date]
                if not date_data.empty:
                    pb_value = date_data.iloc[0]['pb']
                else:
                    # 如果当天没有数据，查找最近的可用数据
                    earlier_data = quarter_data[quarter_data['date'] <= date]
                    if not earlier_data.empty:
                        pb_value = earlier_data.iloc[-1]['pb']
        
        if pb_value is not None:
            daily_data.append({
                'date': date,
                'pb': pb_value
            })
    
    merged_df = pd.DataFrame(daily_data)
    if not merged_df.empty:
        print(f"每日曲线合并完成，共 {len(merged_df)} 条记录")
        return merged_df
    else:
        print("每日曲线合并失败")
        return None

def plot_simple_etf_pb_time_series(pb_data, etf_code=None):
    """
    绘制简单的ETF PB时间序列图，并标注分位信息
    
    Args:
        pb_data (pandas.DataFrame): PB数据，包含[date, pb]列
        etf_code (str, optional): ETF代码，用于图表标题
    """
    try:
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        import numpy as np
        from scipy import stats
        plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
        plt.rcParams['axes.unicode_minus'] = False    # 用来正常显示负号
        
        # 创建图表
        fig, ax = plt.subplots(figsize=(14, 8))
        
        # 绘制PB时间序列
        ax.plot(pb_data['date'], pb_data['pb'], linewidth=2, color='black', label='ETF PB')
        
        # 计算分位数
        pb_values = pb_data['pb'].dropna()
        if len(pb_values) > 0:
            percentile_20 = np.percentile(pb_values, 20)
            percentile_80 = np.percentile(pb_values, 80)
            
            # 添加20%和80%分位线
            ax.axhline(y=percentile_20, color='green', linestyle='--', alpha=0.7, 
                      label=f'20%分位: {percentile_20:.2f}')
            ax.axhline(y=percentile_80, color='red', linestyle='--', alpha=0.7, 
                      label=f'80%分位: {percentile_80:.2f}')
            
            # 计算最新PB的百分位排名
            latest_pb = pb_data.iloc[-1]['pb']
            latest_percentile_rank = stats.percentileofscore(pb_values, latest_pb)
            
            # 添加最新PB值的标记
            latest_date = pb_data.iloc[-1]['date']
            ax.scatter(latest_date, latest_pb, color='blue', s=60, zorder=5)
            ax.annotate(f'最新: {latest_pb:.2f}\n({latest_percentile_rank:.1f}%分位)', 
                       xy=(latest_date, latest_pb), 
                       xytext=(10, 10), 
                       textcoords='offset points',
                       bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.7),
                       arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0'))
        
        # 设置图表标题和标签
        title = f'ETF {etf_code} PB时间序列' if etf_code else 'ETF PB时间序列'
        ax.set_title(title, fontsize=16, pad=20)
        ax.set_xlabel('日期', fontsize=12)
        ax.set_ylabel('PB值', fontsize=12)
        
        # 设置图例
        ax.legend(loc='upper left')
        
        # 格式化x轴日期显示
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
        
        # 添加网格
        ax.grid(True, alpha=0.3)
        
        # 调整布局
        plt.tight_layout()
        
        # 显示图表
        plt.show()
        
    except ImportError:
        print("未安装matplotlib库，无法绘制图表。请运行 'pip install matplotlib' 安装")
    except Exception as e:
        print(f"绘制图表时出错: {e}")

def append_pb_data(simple_pb_data, new_pb_data):
    """
    将新的PB数据追加到现有的PB数据中
    
    Args:
        simple_pb_data (pandas.DataFrame): 现有的PB数据，包含[date, pb]列
        new_pb_data (pandas.DataFrame): 新的PB数据，包含[date, pb]列
        
    Returns:
        pandas.DataFrame: 合并后的PB数据，按日期排序
    """
    if simple_pb_data is None or simple_pb_data.empty:
        return new_pb_data[['date', 'pb']].copy() if new_pb_data is not None and not new_pb_data.empty else None
    
    if new_pb_data is None or new_pb_data.empty:
        return simple_pb_data
    
    # 合并数据并按日期排序
    combined_data = pd.concat([simple_pb_data, new_pb_data[['date', 'pb']]], ignore_index=True)
    combined_data = combined_data.sort_values('date').drop_duplicates(subset='date', keep='last')
    
    return combined_data.reset_index(drop=True)


def analyze_quarterly_etf_pb(etf_code):
    """
    按季度分析ETF的PB时间序列
    
    Args:
        etf_code (str): ETF代码
    """
    # 读取ETF持仓数据
    etf_holdings = read_etf_holdings(etf_code)
    if etf_holdings is None or etf_holdings.empty:
        print("无法读取ETF持仓数据")
        return
    
    # 获取所有唯一的季度（按CSV中出现的顺序）
    quarters = etf_holdings['季度'].unique().tolist()
    print(f"ETF {etf_code} 包含 {len(quarters)} 个季度的数据:")
    for quarter in quarters:
        print(f"  - {quarter}")
    
    # 初始化简单的PB数据结构
    simple_pb_data = None
    
    # 按季度计算PB时间序列并追加数据
    for quarter in quarters:
        quarter_data = etf_holdings[etf_holdings['季度'] == quarter]
        pb_series = calculate_single_quarter_etf_pb_time_series(quarter_data, quarter)
        
        if pb_series is not None and not pb_series.empty:
            # 追加数据到简单结构中
            simple_pb_data = append_pb_data(simple_pb_data, pb_series)
    
    # 绘制合并后的ETF PB时间序列图（修改绘图函数以适应新数据结构）
    if simple_pb_data is not None and not simple_pb_data.empty:
        plot_simple_etf_pb_time_series(simple_pb_data, etf_code)
       


# 主程序入口
if __name__ == "__main__":
    etf_code = "512170"  # 指定ETF代码
    analyze_quarterly_etf_pb(etf_code)
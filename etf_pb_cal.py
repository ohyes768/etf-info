# etf_pb_calculator.py
import pandas as pd
import sqlite3
import numpy as np
from database import DB_FILENAME, get_pb_from_db
from datetime import datetime

def read_etf_holdings(etf_code):
    """
    读取ETF持仓CSV文件
    
    Args:
        etf_code (str): ETF代码
        
    Returns:
        pandas.DataFrame: ETF持仓数据
    """
    # 从etf-codes目录读取文件
    filename = f"etf-codes/hold_{etf_code}.csv"
    
    try:
        # 直接使用pandas读取CSV文件
        df = pd.read_csv(filename, encoding='utf-8-sig')
        
        # 清理数据
        df['股票代码'] = df['股票代码'].astype(str).str.strip()
        # 处理占净值比例列，去掉百分号并转换为小数
        df['占净值比例'] = df['占净值比例'].astype(str).str.rstrip('%').astype(float) / 100
        
        print(f"成功读取ETF {etf_code} 的持仓数据，共{len(df)}条记录")
        return df
        
    except FileNotFoundError:
        print(f"文件 {filename} 不存在")
        return None
    except Exception as e:
        print(f"读取CSV文件时出错: {e}")
        return None

def get_stock_pb_time_series(stock_code):
    """
    获取单个股票的历史PB时间序列
    
    Args:
        stock_code (str): 股票代码
        
    Returns:
        pandas.DataFrame: 股票的历史PB数据
    """
    try:
        # 获取股票的PB数据
        pb_data = get_pb_from_db(stock_code)
        return pb_data
    except Exception as e:
        print(f"获取股票 {stock_code} 的PB时间序列时出错: {e}")
        return pd.DataFrame()

def calculate_quarterly_etf_pb_series(etf_code):
    """
    按季度计算ETF的历史PB时间序列
    
    Args:
        etf_code (str): ETF代码
        
    Returns:
        pandas.DataFrame: ETF的季度PB时间序列
    """
    # 读取ETF持仓数据
    etf_holdings = read_etf_holdings(etf_code)
    if etf_holdings is None or etf_holdings.empty:
        return None
    
    print(f"\n开始按季度计算ETF {etf_code} 的PB时间序列...")
    
    # 获取所有唯一的季度
    quarters = etf_holdings['季度'].unique()
    print(f"共有 {len(quarters)} 个季度的数据")
    
    etf_pb_results = []
    
    # 按季度处理
    for quarter in quarters:
        quarter_data = etf_holdings[etf_holdings['季度'] == quarter]
        print(f"\n处理 {quarter} 的数据，包含 {len(quarter_data)} 只股票")
        
        # 存储该季度所有股票的PB数据
        quarter_pb_data = {}
        valid_stocks = []
        
        # 获取该季度每只股票的PB值
        for index, row in quarter_data.iterrows():
            stock_code = row['股票代码'].strip()
            weight = row['占净值比例']
            
            if weight <= 0:
                continue
                
            # 获取股票最新PB值（我们假设使用当前最新的PB值来代表该季度）
            try:
                pb_data = get_pb_from_db(stock_code)
                if not pb_data.empty:
                    # 获取最新的PB值
                    latest_pb = pb_data.iloc[-1]['pb']
                    if not np.isnan(latest_pb) and latest_pb > 0:
                        quarter_pb_data[stock_code] = {
                            'pb': latest_pb,
                            'weight': weight,
                            'name': row['股票名称']
                        }
                        valid_stocks.append(stock_code)
                else:
                    print(f"  未找到股票 {stock_code} 的PB数据")
            except Exception as e:
                print(f"  获取股票 {stock_code} 的PB值时出错: {e}")
        
        # 计算该季度的加权平均PB
        if valid_stocks:
            weighted_pb_sum = 0
            total_weight = 0
            
            for stock_code in valid_stocks:
                stock_info = quarter_pb_data[stock_code]
                weighted_pb_sum += stock_info['weight'] * stock_info['pb']
                total_weight += stock_info['weight']
            
            if total_weight > 0:
                weighted_avg_pb = weighted_pb_sum / total_weight
                etf_pb_results.append({
                    'quarter': quarter,
                    'pb': weighted_avg_pb,
                    'valid_stocks_count': len(valid_stocks),
                    'total_stocks_count': len(quarter_data),
                    'total_weight': total_weight
                })
                print(f"  {quarter} 加权平均PB: {weighted_avg_pb:.4f}")
            else:
                print(f"  {quarter} 无法计算加权平均PB（权重总和为0）")
        else:
            print(f"  {quarter} 没有有效的股票数据")
    
    # 转换为DataFrame
    if etf_pb_results:
        etf_pb_df = pd.DataFrame(etf_pb_results)
        # 按季度排序
        etf_pb_df = etf_pb_df.sort_values('quarter')
        print(f"\nETF {etf_code} 的季度PB序列计算完成，共 {len(etf_pb_df)} 个季度")
        return etf_pb_df
    else:
        print("无法计算ETF的季度PB序列")
        return None

def calculate_etf_pb_time_series(etf_code):
    """
    计算ETF的历史PB时间序列（基于个股历史PB数据）
    
    Args:
        etf_code (str): ETF代码
        
    Returns:
        pandas.DataFrame: ETF的历史PB时间序列
    """
    # 读取ETF持仓数据
    etf_holdings = read_etf_holdings(etf_code)
    if etf_holdings is None:
        return None
    
    print(f"\n开始计算ETF {etf_code} 的历史PB时间序列...")
    
    # 获取最新的季度数据（用于权重）
    latest_quarter = etf_holdings['季度'].iloc[-1]
    latest_quarter_data = etf_holdings[etf_holdings['季度'] == latest_quarter]
    print(f"使用 {latest_quarter} 的权重数据")
    
    # 获取所有股票的历史PB数据
    all_stock_pb_data = {}
    available_stocks = []
    
    for index, row in latest_quarter_data.iterrows():
        stock_code = row['股票代码'].strip()
        weight = row['占净值比例']
        
        print(f"正在获取股票 {stock_code} ({row['股票名称']}) 的历史PB数据...")
        
        # 获取股票历史PB数据
        pb_data = get_stock_pb_time_series(stock_code)
        
        if not pb_data.empty and len(pb_data) > 0:
            all_stock_pb_data[stock_code] = {
                'data': pb_data,
                'weight': weight,
                'name': row['股票名称']
            }
            available_stocks.append(stock_code)
            print(f"  -> 成功获取 {len(pb_data)} 条PB记录")
        else:
            print(f"  -> 未找到有效PB数据")
    
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
    
    all_dates = sorted(list(all_dates))
    print(f"总共包含 {len(all_dates)} 个日期")
    
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
                'total_weight': total_weight
            })
    
    # 转换为DataFrame
    etf_pb_df = pd.DataFrame(etf_pb_series)
    
    if not etf_pb_df.empty:
        print(f"ETF {etf_code} 的PB时间序列计算完成，共 {len(etf_pb_df)} 条记录")
        return etf_pb_df
    else:
        print("无法计算ETF的PB时间序列")
        return None

def calculate_percentile_rank(current_pb, pb_history):
    """
    计算当前PB值在历史数据中的百分位排名
    
    Args:
        current_pb (float): 当前PB值
        pb_history (list or pandas.Series): 历史PB值序列
        
    Returns:
        float: 百分位排名 (0-100)
    """
    if len(pb_history) == 0:
        return None
    
    # 计算百分位排名
    sorted_history = sorted(pb_history)
    percentile_rank = (np.searchsorted(sorted_history, current_pb, side='right') / len(sorted_history)) * 100
    
    return percentile_rank

def plot_etf_pb_time_series(etf_code, etf_pb_series):
    """
    绘制ETF的PB时间序列图
    
    Args:
        etf_code (str): ETF代码
        etf_pb_series (pandas.DataFrame): ETF的PB时间序列数据
    """
    try:
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
        plt.rcParams['axes.unicode_minus'] = False    # 用来正常显示负号
        
        # 创建图表
        fig, ax = plt.subplots(figsize=(12, 6))
        
        # 绘制PB时间序列
        ax.plot(etf_pb_series['date'], etf_pb_series['pb'], linewidth=1.5, color='blue', marker='o', markersize=3)
        
        # 添加最新PB值的标记
        latest_pb = etf_pb_series.iloc[-1]['pb']
        latest_date = etf_pb_series.iloc[-1]['date']
        ax.scatter(latest_date, latest_pb, color='red', s=50, zorder=5)
        ax.annotate(f'最新: {latest_pb:.2f}', 
                   xy=(latest_date, latest_pb), 
                   xytext=(10, 10), 
                   textcoords='offset points',
                   bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.7),
                   arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0'))
        
        # 计算统计指标用于绘制参考线
        mean_pb = etf_pb_series['pb'].mean()
        percentile_20 = np.percentile(etf_pb_series['pb'], 20)
        percentile_80 = np.percentile(etf_pb_series['pb'], 80)
        
        # 绘制参考线
        ax.axhline(y=mean_pb, color='green', linestyle='--', alpha=0.7, label=f'平均值: {mean_pb:.2f}')
        ax.axhline(y=percentile_20, color='orange', linestyle=':', alpha=0.7, label=f'20%分位: {percentile_20:.2f}')
        ax.axhline(y=percentile_80, color='orange', linestyle=':', alpha=0.7, label=f'80%分位: {percentile_80:.2f}')
        
        # 设置图表标题和标签
        ax.set_title(f'ETF {etf_code} PB历史时间序列', fontsize=16, pad=20)
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

def analyze_etf_pb_time_series(etf_code, plot=True):
    """
    分析ETF的PB时间序列并计算当前PB的百分位排名
    
    Args:
        etf_code (str): ETF代码
        plot (bool): 是否绘制图表
    """
    # 计算ETF的PB时间序列
    etf_pb_series = calculate_etf_pb_time_series(etf_code)
    
    if etf_pb_series is None or etf_pb_series.empty:
        print("无法获取ETF的PB时间序列数据")
        return
    
    # 获取最新的PB值
    latest_pb = etf_pb_series.iloc[-1]['pb']
    latest_date = etf_pb_series.iloc[-1]['date']
    
    # 计算历史PB的统计信息
    pb_history = etf_pb_series['pb'].dropna()
    
    if len(pb_history) == 0:
        print("没有有效的PB历史数据")
        return
    
    # 计算百分位排名
    percentile_rank = calculate_percentile_rank(latest_pb, pb_history)
    
    # 计算其他统计指标
    mean_pb = pb_history.mean()
    median_pb = pb_history.median()
    min_pb = pb_history.min()
    max_pb = pb_history.max()
    std_pb = pb_history.std()
    
    # 显示结果
    print("\n" + "=" * 60)
    print(f"ETF {etf_code} PB时间序列分析报告")
    print("=" * 60)
    print(f"最新日期: {latest_date.strftime('%Y-%m-%d')}")
    print(f"最新PB值: {latest_pb:.4f}")
    print(f"当前PB百分位: {percentile_rank:.2f}%")
    print("-" * 60)
    print(f"历史统计:")
    print(f"  平均值: {mean_pb:.4f}")
    print(f"  中位数: {median_pb:.4f}")
    print(f"  最小值: {min_pb:.4f}")
    print(f"  最大值: {max_pb:.4f}")
    print(f"  标准差: {std_pb:.4f}")
    print(f"  数据点数: {len(pb_history)}")
    print("-" * 60)
    
    # 估值评估
    if percentile_rank is not None:
        if percentile_rank < 20:
            evaluation = "处于历史低位，可能低估"
        elif percentile_rank < 50:
            evaluation = "处于历史中低位"
        elif percentile_rank < 80:
            evaluation = "处于历史中高位"
        else:
            evaluation = "处于历史高位，可能高估"
        
        print(f"估值评价: {evaluation}")
    
    # 显示最近10个交易日的数据
    print("\n最近10个交易日的PB值:")
    print("-" * 40)
    recent_data = etf_pb_series.tail(10)
    for index, row in recent_data.iterrows():
        print(f"{row['date'].strftime('%Y-%m-%d')}: {row['pb']:.4f}")
    
    # 绘制图表
    if plot:
        plot_etf_pb_time_series(etf_code, etf_pb_series)

def analyze_quarterly_etf_pb(etf_code):
    """
    分析ETF的季度PB数据并计算当前PB的百分位排名
    
    Args:
        etf_code (str): ETF代码
    """
    # 计算ETF的季度PB序列
    etf_pb_series = calculate_quarterly_etf_pb_series(etf_code)
    
    if etf_pb_series is None or etf_pb_series.empty:
        print("无法获取ETF的季度PB序列数据")
        return
    
    # 获取最新的PB值
    latest_pb = etf_pb_series.iloc[-1]['pb']
    latest_quarter = etf_pb_series.iloc[-1]['quarter']
    
    # 计算历史PB的统计信息
    pb_history = etf_pb_series['pb'].dropna()
    
    if len(pb_history) == 0:
        print("没有有效的PB历史数据")
        return
    
    # 计算百分位排名
    percentile_rank = calculate_percentile_rank(latest_pb, pb_history)
    
    # 计算其他统计指标
    mean_pb = pb_history.mean()
    median_pb = pb_history.median()
    min_pb = pb_history.min()
    max_pb = pb_history.max()
    std_pb = pb_history.std()
    
    # 显示结果
    print("\n" + "=" * 60)
    print(f"ETF {etf_code} 季度PB分析报告")
    print("=" * 60)
    print(f"最新季度: {latest_quarter}")
    print(f"最新PB值: {latest_pb:.4f}")
    print(f"当前PB百分位: {percentile_rank:.2f}%")
    print("-" * 60)
    print(f"历史统计:")
    print(f"  平均值: {mean_pb:.4f}")
    print(f"  中位数: {median_pb:.4f}")
    print(f"  最小值: {min_pb:.4f}")
    print(f"  最大值: {max_pb:.4f}")
    print(f"  标准差: {std_pb:.4f}")
    print(f"  数据点数: {len(pb_history)}")
    print("-" * 60)
    
    # 估值评估
    if percentile_rank is not None:
        if percentile_rank < 20:
            evaluation = "处于历史低位，可能低估"
        elif percentile_rank < 50:
            evaluation = "处于历史中低位"
        elif percentile_rank < 80:
            evaluation = "处于历史中高位"
        else:
            evaluation = "处于历史高位，可能高估"
        
        print(f"估值评价: {evaluation}")
    
    # 显示所有季度的数据
    print("\n所有季度的PB值:")
    print("-" * 60)
    for index, row in etf_pb_series.iterrows():
        print(f"{row['quarter']}: {row['pb']:.4f}")

def get_latest_etf_pb(etf_code):
    """
    获取ETF的最新PB值（用于向后兼容）
    
    Args:
        etf_code (str): ETF代码
        
    Returns:
        dict: 包含计算结果的字典
    """
    # 读取ETF持仓数据
    etf_holdings = read_etf_holdings(etf_code)
    if etf_holdings is None:
        return None
    
    # 使用最新的季度数据
    latest_quarter = etf_holdings['季度'].iloc[-1]
    latest_quarter_data = etf_holdings[etf_holdings['季度'] == latest_quarter]
    
    # 存储每只股票的PB值
    stock_pb_data = []
    total_weight = 0
    weighted_pb_sum = 0
    
    print(f"\n开始计算ETF {etf_code} 的PB值 (基于 {latest_quarter})...")
    print("=" * 50)
    
    # 遍历每只股票，获取其PB值
    for index, row in latest_quarter_data.iterrows():
        stock_code = row['股票代码'].strip()
        weight = row['占净值比例']
        
        print(f"正在处理股票 {stock_code} ({row['股票名称']})，权重: {weight*100:.2f}%")
        
        # 获取股票最新PB值
        try:
            pb_data = get_pb_from_db(stock_code)
            if not pb_data.empty:
                pb_value = pb_data.iloc[-1]['pb']
            else:
                pb_value = None
                print(f"未找到股票 {stock_code} 的PB数据")
        except Exception as e:
            pb_value = None
            print(f"获取股票 {stock_code} 的PB值时出错: {e}")
        
        if pb_value is not None and not np.isnan(pb_value):
            weighted_contribution = weight * pb_value
            total_weight += weight
            weighted_pb_sum += weighted_contribution
            
            stock_pb_data.append({
                '股票代码': stock_code,
                '股票名称': row['股票名称'],
                '权重': weight,
                'PB值': pb_value,
                '加权贡献': weighted_contribution
            })
            
            print(f"  -> PB值: {pb_value:.4f}, 加权贡献: {weighted_contribution:.6f}")
        else:
            print(f"  -> 无法获取PB值，跳过该股票")
    
    # 计算加权平均PB值
    if total_weight > 0:
        weighted_average_pb = weighted_pb_sum / total_weight
        
        result = {
            'etf_code': etf_code,
            'weighted_average_pb': weighted_average_pb,
            'total_weight': total_weight,
            'stock_count': len(stock_pb_data),
            'total_stock_count': len(latest_quarter_data),
            'stock_details': stock_pb_data
        }
        
        print("=" * 50)
        print(f"ETF {etf_code} 的PB计算完成:")
        print(f"  加权平均PB值: {weighted_average_pb:.4f}")
        print(f"  有效计算股票数: {len(stock_pb_data)}/{len(latest_quarter_data)}")
        print(f"  有效权重总和: {total_weight*100:.2f}%")
        
        return result
    else:
        print("没有有效的PB数据可用于计算")
        return None

def display_etf_pb_analysis(etf_code, plot=True):
    """
    显示ETF的PB分析结果（向后兼容的主函数）
    
    Args:
        etf_code (str): ETF代码
        plot (bool): 是否绘制图表
    """
    # 先尝试使用时间序列分析
    try:
        analyze_etf_pb_time_series(etf_code, plot=plot)
    except Exception as e:
        print(f"时间序列分析失败: {e}")
        print("使用简单PB计算方法...")
        # 如果时间序列分析失败，则使用原始方法
        result = get_latest_etf_pb(etf_code)
        
        if result:
            print("\n" + "=" * 60)
            print(f"ETF {result['etf_code']} PB分析报告")
            print("=" * 60)
            print(f"加权平均PB值: {result['weighted_average_pb']:.4f}")
            print(f"有效计算股票数: {result['stock_count']}/{result['total_stock_count']}")
            print(f"有效权重总和: {result['total_weight']*100:.2f}%")
            
            # 估值评估
            pb_value = result['weighted_average_pb']
            if pb_value < 1:
                evaluation = "低于1，可能表示ETF整体估值偏低"
            elif pb_value < 3:
                evaluation = "在1-3之间，属于正常估值范围"
            else:
                evaluation = "高于3，可能表示ETF整体估值偏高"
            
            print(f"估值评价: {evaluation}")
            
            # 显示前5只股票的详细信息
            print("\n前5只股票的PB详情:")
            print("-" * 60)
            print(f"{'股票代码':<10} {'股票名称':<10} {'权重':<8} {'PB值':<10} {'加权贡献':<10}")
            print("-" * 60)
            
            for i, stock in enumerate(result['stock_details'][:5]):
                print(f"{stock['股票代码']:<10} {stock['股票名称']:<10} "
                      f"{stock['权重']*100:>7.2f}% {stock['PB值']:>9.4f} "
                      f"{stock['加权贡献']:>9.6f}")

# 主程序入口
if __name__ == "__main__":
    etf_code = "588170"  # 指定ETF代码
    display_etf_pb_analysis(etf_code, plot=True)
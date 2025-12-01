# etf_pb_calculator.py
import pandas as pd
import sqlite3
from database import DB_FILENAME, get_pb_from_db

def read_etf_holdings(etf_code):
    """
    读取ETF持仓CSV文件
    
    Args:
        etf_code (str): ETF代码
        
    Returns:
        pandas.DataFrame: ETF持仓数据
    """
    filename = f"hold_{etf_code}.csv"
    
    try:
        # 读取原始文件内容
        with open(filename, 'r', encoding='utf-8') as file:
            content = file.read()
        
        # 按行分割
        lines = content.strip().split('\n')
        
        # 处理数据行（跳过标题行）
        processed_lines = [lines[0]]  # 保留标题行
        
        for line in lines[1:]:
            # 分割行
            parts = line.split(',')
            
            if len(parts) == 10:
                # 10列的情况，有一个数值字段被分割了
                new_line = ','.join([
                    parts[0],  # 序号
                    parts[1],  # 股票代码
                    parts[2],  # 股票名称
                    parts[3],  # 最新价
                    parts[4],  # 涨跌幅
                    parts[5],  # 相关资讯
                    parts[6],  # 占净值比例
                    parts[7],  # 持股数
                    parts[8] + parts[9]   # 合并的持仓市值
                ])
                processed_lines.append(new_line)
            else:
                # 其他情况直接保留
                processed_lines.append(line)
        
        # 使用pandas读取处理后的数据
        df = pd.DataFrame([line.split(',') for line in processed_lines][1:], 
                          columns=[col.strip() for col in processed_lines[0].split(',')])
        
        # 清理数据
        df['股票代码'] = df['股票代码'].astype(str).str.strip()
        df['占净值比例'] = df['占净值比例'].str.rstrip('%').astype(float) / 100
        
        print(f"成功读取ETF {etf_code} 的持仓数据，共{len(df)}只股票")
        return df
        
    except FileNotFoundError:
        print(f"文件 {filename} 不存在")
        return None
    except Exception as e:
        print(f"读取CSV文件时出错: {e}")
        return None

def get_latest_pb_for_stock(stock_code):
    """
    获取单个股票的最新PB值
    
    Args:
        stock_code (str): 股票代码
        
    Returns:
        float: 最新PB值，如果无数据返回None
    """
    try:
        # 获取股票的PB数据
        pb_data = get_pb_from_db(stock_code)
        
        if not pb_data.empty:
            # 返回最新的PB值
            latest_pb = pb_data.iloc[-1]['pb']
            return latest_pb
        else:
            print(f"未找到股票 {stock_code} 的PB数据")
            return None
            
    except Exception as e:
        print(f"获取股票 {stock_code} 的PB值时出错: {e}")
        return None

def calculate_etf_pb(etf_code):
    """
    根据ETF持仓和个股PB数据计算ETF的加权平均PB值
    
    Args:
        etf_code (str): ETF代码
        
    Returns:
        dict: 包含计算结果的字典
    """
    # 读取ETF持仓数据
    etf_holdings = read_etf_holdings(etf_code)
    if etf_holdings is None:
        return None
    
    # 存储每只股票的PB值
    stock_pb_data = []
    total_weight = 0
    weighted_pb_sum = 0
    
    print(f"\n开始计算ETF {etf_code} 的PB值...")
    print("=" * 50)
    
    # 遍历每只股票，获取其PB值
    for index, row in etf_holdings.iterrows():
        stock_code = row['股票代码'].strip()
        weight = row['占净值比例']
        
        print(f"正在处理股票 {stock_code} ({row['股票名称']})，权重: {weight*100:.2f}%")
        
        # 获取股票最新PB值
        pb_value = get_latest_pb_for_stock(stock_code)
        
        if pb_value is not None:
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
            'total_stock_count': len(etf_holdings),
            'stock_details': stock_pb_data
        }
        
        print("=" * 50)
        print(f"ETF {etf_code} 的PB计算完成:")
        print(f"  加权平均PB值: {weighted_average_pb:.4f}")
        print(f"  有效计算股票数: {len(stock_pb_data)}/{len(etf_holdings)}")
        print(f"  有效权重总和: {total_weight*100:.2f}%")
        
        return result
    else:
        print("没有有效的PB数据可用于计算")
        return None

def display_etf_pb_analysis(etf_code):
    """
    显示ETF的PB分析结果
    
    Args:
        etf_code (str): ETF代码
    """
    result = calculate_etf_pb(etf_code)
    
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
    display_etf_pb_analysis(etf_code)
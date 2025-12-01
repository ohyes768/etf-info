import pandas as pd
import os
import time
from io import StringIO
from stock_pbcal import init_database, calculate_daily_pb

def process_etf_stocks(etf_code):
    """
    处理ETF中的所有股票，计算并保存PB值到数据库
    
    Args:
        etf_code (str): ETF代码
    """
    # 构建文件路径
    filename = f"hold_{etf_code}.csv"
    
    # 检查文件是否存在
    if not os.path.exists(filename):
        print(f"文件 {filename} 不存在")
        return
    
    try:
        # 读取原始文件内容
        with open(filename, 'r', encoding='utf-8') as file:
            content = file.read()
        
        # 按行分割
        lines = content.strip().split('\n')
        
        # 处理数据行（跳过标题行）
        processed_lines = [lines[0]]  # 保留标题行
        
        for line in lines[1:]:
            # 使用正则表达式匹配并修复格式
            # 格式应该是: 序号,股票代码,股票名称,最新价,涨跌幅,相关资讯,占净值比例,持股数(万股),持仓市值(万元)
            # 但实际因为逗号分隔符问题变成了更多部分
            
            # 分割行
            parts = line.split(',')
            
            if len(parts) == 11:
                # 正确的情况应该是9列，但现在是11列，说明有两个数值字段被分割了
                # 重新组合: 第7和第8部分合并为持股数，第9和第10部分合并为持仓市值
                new_line = ','.join([
                    parts[0],  # 序号
                    parts[1],  # 股票代码
                    parts[2],  # 股票名称
                    parts[3],  # 最新价
                    parts[4],  # 涨跌幅
                    parts[5],  # 相关资讯
                    parts[6],  # 占净值比例
                    parts[7] + ',' + parts[8],  # 合并的持股数
                    parts[9] + ',' + parts[10]   # 合并的持仓市值
                ])
                processed_lines.append(new_line)
            elif len(parts) == 10:
                # 10列的情况，有一个数值字段被分割了
                # 判断哪个字段被分割（通常是较大的数值）
                new_line = ','.join([
                    parts[0],  # 序号
                    parts[1],  # 股票代码
                    parts[2],  # 股票名称
                    parts[3],  # 最新价
                    parts[4],  # 涨跌幅
                    parts[5],  # 相关资讯
                    parts[6],  # 占净值比例
                    parts[7],  # 合并的持股数
                    parts[8] + parts[9]   # 持仓市值
                ])
                processed_lines.append(new_line)
            else:
                # 其他情况直接保留
                processed_lines.append(line)
        
        # 使用StringIO创建文件对象供pandas读取
        processed_content = '\n'.join(processed_lines)
        df = pd.read_csv(StringIO(processed_content))
        
        # 初始化数据库
        init_database()
        
        # 提取所有股票代码
        stock_codes = df['股票代码'].tolist()
        
        print(f"list:{stock_codes}")
        print(f"ETF {etf_code} 包含 {len(stock_codes)} 只股票")
        print("开始计算各股票的PB值...")
        
        # 遍历所有股票代码并计算PB
        for i, stock_code in enumerate(stock_codes, 1):
            print(f"\n[{i}/{len(stock_codes)}] 正在处理股票: {stock_code}")
            try:
                pb_result = calculate_daily_pb(str(stock_code))
                #增加等待耗时5s，避免频繁调用
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
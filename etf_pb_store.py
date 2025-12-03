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
        # 使用pandas读取CSV文件，确保股票代码作为字符串处理
        df = pd.read_csv(filename, encoding='utf-8-sig', dtype={'股票代码': str})
        
        # 确保股票代码格式正确（6位数字，不足的前面补0）
        df['股票代码'] = df['股票代码'].apply(lambda x: x.zfill(6) if pd.notna(x) and str(x).isdigit() else x)
        
        # 过滤掉占净值比例为0.0的记录
        if '占净值比例' in df.columns:
            df = df[df['占净值比例'] > 0.0]
            print(f"过滤掉占净值比例为0.0的记录后，剩余 {len(df)} 条记录")
        
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
                # 调用calculate_daily_pb并检查是否从网络获取了数据
                result = calculate_daily_pb(str(stock_code))
                
                # 解包返回值（pb_df, from_database）
                if isinstance(result, tuple):
                    pb_result, from_database = result
                else:
                    # 兼容旧版本返回单一值的情况
                    pb_result, from_database = result, False
                
                # 只有在网络调用时才sleep，避免频繁请求
                if not from_database:
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

def main():
    """
    主函数 - 添加交互式输入ETF代码
    """
    print("=== ETF股票PB值计算工具 ===")
    
    # 交互式输入ETF基金代码
    etf_code = input("请输入ETF基金代码（直接回车使用默认代码588200）: ").strip()
    
    # 如果用户没有输入任何内容，使用默认代码
    if not etf_code:
        etf_code = "588200"
        print("使用默认代码: 588200")
    else:
        print(f"将处理ETF代码: {etf_code}")
    
    print(f"\n开始处理ETF {etf_code} 的股票PB值...")
    process_etf_stocks(etf_code)

# 主程序入口
if __name__ == "__main__":
    # 使用交互式主函数
    main()
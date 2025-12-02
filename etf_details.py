# etf_details.py
import akshare as ak
import pandas as pd
import os

def fetch_etf_holdings_to_csv(etf_code, start_year=2020, end_year=2025):
    """
    获取ETF持仓数据并保存到CSV文件（多年份）
    
    Args:
        etf_code (int or str): ETF基金代码
        start_year (int): 开始年份，默认为2020
        end_year (int): 结束年份，默认为2025
    
    Returns:
        bool: 是否成功保存
    """
    all_data = []  # 存储所有年份的数据
    success_years = []  # 记录成功获取数据的年份
    
    try:
        # 循环获取从start_year到end_year的持仓数据
        for year in range(start_year, end_year + 1):
            print(f"正在获取ETF {etf_code} {year}年的持仓数据...")
            try:
                holdings_data = ak.fund_portfolio_hold_em(etf_code, year)
                
                # 检查是否有数据
                if holdings_data is None or holdings_data.empty:
                    print(f"未获取到ETF {etf_code} {year}年的持仓数据")
                    continue
                
                # 添加年份列
                holdings_data['年份'] = year
                
                # 添加到总数据列表
                all_data.append(holdings_data)
                success_years.append(year)
                
                print(f"成功获取到 {year} 年 {len(holdings_data)} 条持仓记录")
                
            except Exception as e:
                print(f"获取ETF {etf_code} {year}年数据时出错: {e}")
                continue
        
        # 检查是否获取到任何数据
        if not all_data:
            print(f"未获取到ETF {etf_code} 任何年份的持仓数据")
            return False
        
        # 合并所有年份的数据
        combined_data = pd.concat(all_data, ignore_index=True)
        
        # 显示数据基本信息
        print(f"总共获取到 {len(success_years)} 个年份的数据: {success_years}")
        print("合并后数据总数:", len(combined_data))
        print("数据列名:", combined_data.columns.tolist())
        
        # 确保etf-codes目录存在
        directory = "etf-codes"
        if not os.path.exists(directory):
            os.makedirs(directory)
            print(f"创建目录: {directory}")
        
        # 生成文件路径
        filename = f"hold_{etf_code}.csv"
        filepath = os.path.join(directory, filename)
        
        # 保存到CSV文件
        combined_data.to_csv(filepath, index=False, encoding='utf-8-sig')
        print(f"数据已保存到 {filepath}")
        
        # 显示前几条数据预览
        print("\n数据预览:")
        print(combined_data.head())
        
        return True
        
    except Exception as e:
        print(f"获取或保存ETF {etf_code} 数据时出错: {e}")
        return False

def batch_fetch_etf_holdings(etf_codes, start_year=2020, end_year=2025):
    """
    批量获取多个ETF的持仓数据（多年份）
    
    Args:
        etf_codes (list): ETF基金代码列表
        start_year (int): 开始年份，默认为2020
        end_year (int): 结束年份，默认为2025
    """
    print(f"开始批量获取 {len(etf_codes)} 只ETF {start_year}-{end_year}年的持仓数据...")
    
    success_count = 0
    for etf_code in etf_codes:
        print(f"\n--- 处理 ETF {etf_code} ---")
        if fetch_etf_holdings_to_csv(etf_code, start_year, end_year):
            success_count += 1
    
    print(f"\n批量处理完成: 成功 {success_count}/{len(etf_codes)} 只ETF")

# 主程序入口
if __name__ == "__main__":
    # 单个ETF处理示例
    etf_code = 588200  # 你可以修改为其他ETF代码
    fetch_etf_holdings_to_csv(etf_code, 2020, 2025)
    
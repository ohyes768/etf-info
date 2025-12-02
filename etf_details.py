# etf_details.py
import akshare as ak
import pandas as pd
import os

def fetch_etf_holdings_to_csv(etf_code, year=2024):
    """
    获取ETF持仓数据并保存到CSV文件
    
    Args:
        etf_code (int or str): ETF基金代码
        year (int): 年份，默认为2024
    
    Returns:
        bool: 是否成功保存
    """
    try:
        # 获取ETF持仓数据
        print(f"正在获取ETF {etf_code} {year}年的持仓数据...")
        holdings_data = ak.fund_portfolio_hold_em(etf_code, year)
        
        # 检查是否有数据
        if holdings_data is None or holdings_data.empty:
            print(f"未获取到ETF {etf_code} 的持仓数据")
            return False
        
        # 显示数据基本信息
        print(f"成功获取到 {len(holdings_data)} 条持仓记录")
        print("数据列名:", holdings_data.columns.tolist())
        
        # 确保etf-codes目录存在
        directory = "etf-codes"
        if not os.path.exists(directory):
            os.makedirs(directory)
            print(f"创建目录: {directory}")
        
        # 生成文件路径
        filename = f"hold_{etf_code}.csv"
        filepath = os.path.join(directory, filename)
        
        # 保存到CSV文件
        holdings_data.to_csv(filepath, index=False, encoding='utf-8-sig')
        print(f"数据已保存到 {filepath}")
        
        # 显示前几条数据预览
        print("\n数据预览:")
        print(holdings_data.head())
        
        return True
        
    except Exception as e:
        print(f"获取或保存ETF {etf_code} 数据时出错: {e}")
        return False

def batch_fetch_etf_holdings(etf_codes, year=2024):
    """
    批量获取多个ETF的持仓数据
    
    Args:
        etf_codes (list): ETF基金代码列表
        year (int): 年份，默认为2024
    """
    print(f"开始批量获取 {len(etf_codes)} 只ETF的持仓数据...")
    
    success_count = 0
    for etf_code in etf_codes:
        print(f"\n--- 处理 ETF {etf_code} ---")
        if fetch_etf_holdings_to_csv(etf_code, year):
            success_count += 1
    
    print(f"\n批量处理完成: 成功 {success_count}/{len(etf_codes)} 只ETF")

# 主程序入口
if __name__ == "__main__":
    # 单个ETF处理示例
    etf_code = 588170  # 你可以修改为其他ETF代码
    fetch_etf_holdings_to_csv(etf_code, 2025)
    
    # 批量处理示例（取消注释以使用）
    # etf_list = [588170, 588000, 588080]  # 添加你需要的ETF代码
    # batch_fetch_etf_holdings(etf_list, 2024)
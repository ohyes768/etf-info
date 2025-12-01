# stock_code_utils.py
"""
股票代码工具模块
"""

def convert_stock_code_for_akshare(stock_code):
    """
    将股票代码转换为akshare接口所需的格式
    
    Args:
        stock_code (str): 原始股票代码，如 "688120"
    
    Returns:
        str: 转换后的股票代码，如 "688120.SH"
    """
    if not stock_code:
        return None
    
    # 如果已经包含市场后缀，则直接返回
    if '.' in stock_code:
        return stock_code
    
    # 根据股票代码前缀判断市场
    if stock_code.startswith(('6', '688')):
        return f"{stock_code}.SH"  # 上海证券交易所
    elif stock_code.startswith(('0', '3', '159')):
        return f"{stock_code}.SZ"  # 深圳证券交易所
    elif stock_code.startswith(('4', '8')):
        return f"{stock_code}.BJ"  # 北京证券交易所
    else:
        # 默认返回上海交易所格式（可以根据需要调整）
        return f"{stock_code}.SH"

def convert_stock_code_for_db(stock_code):
    """
    将股票代码转换为数据库存储格式（去除市场后缀）
    
    Args:
        stock_code (str): 带市场后缀的股票代码，如 "688120.SH"
    
    Returns:
        str: 不带市场后缀的股票代码，如 "688120"
    """
    if not stock_code:
        return None
    
    # 如果包含市场后缀，则去除后缀
    if '.' in stock_code:
        return stock_code.split('.')[0]
    
    # 如果不包含后缀，直接返回
    return stock_code

def get_market_prefix(stock_code):
    """
    获取股票代码的市场前缀（用于价格数据获取）
    
    Args:
        stock_code (str): 原始股票代码，如 "688120"
    
    Returns:
        str: 市场前缀，如 "sh" 或 "sz"
    """
    if not stock_code:
        return None
    
    # 根据股票代码前缀判断市场前缀
    if stock_code.startswith(('6', '688')):
        return "sh"  # 上海证券交易所
    elif stock_code.startswith(('0', '3', '159')):
        return "sz"  # 深圳证券交易所
    else:
        # 默认返回上海交易所前缀（可以根据需要调整）
        return "sh"
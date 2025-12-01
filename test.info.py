import akshare as ak
import pandas as pd
import numpy as np

stock_code= "688120.SH";
# x=ak.stock_individual_info_em(stock_code)
# x = ak.stock_individual_basic_info_xq(stock_code)
x= ak.stock_zh_a_gbjg_em(stock_code)
print(x)
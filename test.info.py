import akshare as ak
import pandas as pd
import numpy as np

stock_code= "688120.SH";
# x=ak.stock_individual_info_em(stock_code)
# x = ak.stock_individual_basic_info_xq(stock_code)
# x= ak.fund_announcement_report_em(588170)
# print(x)

y= ak.fund_portfolio_hold_em(588170,2024)
print(y)
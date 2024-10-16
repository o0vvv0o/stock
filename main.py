import baostock as bs
import pandas as pd
from datetime import datetime

# List of 34 analog chip companies with their stock codes and Chinese names
companies = [
    ("sz.300782", "卓胜微"),
    ("sh.603160", "汇顶科技"),
    ("sh.688220", "翱捷科技"),
    ("sh.688798", "艾为电子"),
    ("sz.300661", "圣邦股份"),
    ("sh.688484", "南芯科技"),
    ("sh.600171", "上海贝岭"),
    ("sh.688153", "唯捷创芯"),
    ("sh.688052", "纳芯微"),
    ("sh.688141", "杰华特"),
    ("sh.688368", "晶丰明源"),
    ("sh.688209", "英集芯"),
    ("sh.688536", "思瑞浦"),
    ("sh.600877", "电科芯片"),
    ("sh.688508", "芯朋微"),
    ("sh.688601", "力芯微"),
    ("sh.603068", "博通集成"),
    ("sh.688391", "钜泉科技"),
    ("sh.688045", "必易微"),
    ("sz.300671", "富满微"),
    ("sh.688699", "明微电子"),
    ("sh.688061", "灿瑞科技"),
    ("sh.688381", "帝奥微"),
    ("sh.688512", "慧智微"),
    ("sh.688173", "希荻微"),
    ("sh.603375", "盛景微"),
    ("sh.688653", "康希通信"),
    ("sh.688286", "敏芯股份"),
    ("sh.688458", "美芯晟"),
    ("sh.688325", "赛微微电"),
    ("sh.688515", "裕太微"),
    ("sh.688582", "芯动联科"),
    ("sh.688270", "臻镭科技"),
    ("sh.688130", "晶华微")
]

# Login to the system
lg = bs.login()
print('login respond error_code:' + lg.error_code)
print('login respond  error_msg:' + lg.error_msg)

# Get current year and quarter
current_year = datetime.now().year
current_quarter = (datetime.now().month - 1) // 3 + 1

# Initialize empty lists to store all profit and operation data
all_profit_data = []
all_op_data = []
all_grow_data = []
quart = 2
# Loop through each company
for code, name in companies:
    print(f"Fetching data for {name} ({code})")

    # Fetch profit data
    profit_list = []
    rs_profit = bs.query_profit_data(code=code, year=current_year, quarter=quart)
    while (rs_profit.error_code == '0') & rs_profit.next():
        row_data = rs_profit.get_row_data()
        row_data.append(name)  # Add Chinese name to the data
        profit_list.append(row_data)

    if profit_list:
        all_profit_data.extend(profit_list)
    else:
        print(f"No profit data available for {name} ({code})")

    # Fetch operation data
    operation_list = []
    rs_operation = bs.query_operation_data(code=code, year=current_year, quarter=quart)
    while (rs_operation.error_code == '0') & rs_operation.next():
        row_data_op = rs_operation.get_row_data()
        row_data_op.append(name)  # Add Chinese name to the data
        operation_list.append(row_data_op)

    if operation_list:
        all_op_data.extend(operation_list)
    else:
        print(f"No operation data available for {name} ({code})")

    # 成长能力
    growth_list = []
    rs_growth = bs.query_growth_data(code=code, year=current_year, quarter=quart)
    while (rs_growth.error_code == '0') & rs_growth.next():
        row_data_grow = rs_growth.get_row_data()
        row_data_grow.append(name)  # Add Chinese name to the data
        growth_list.append(row_data_grow)
    if growth_list:
        all_grow_data.extend(growth_list)
    else:
        print(f"No growth data available for {name} ({code})")

# Create DataFrames with all profit and operation data
profit_columns = rs_profit.fields + ['公司名称']
result_profit = pd.DataFrame(all_profit_data, columns=profit_columns)

op_columns = rs_operation.fields + ['公司名称']
result_operation = pd.DataFrame(all_op_data, columns=op_columns)

grow_columns = rs_growth.fields + ['公司名称']
result_growth = pd.DataFrame(all_grow_data, columns=grow_columns)

# Save results to CSV files
result_profit.to_csv("D:\\profit_data_34_companies.csv", encoding="utf-8-sig", index=False)
result_operation.to_csv("D:\\operation_data_34_companies.csv", encoding="utf-8-sig", index=False)
result_growth.to_csv("D:\\growth_data_34_companies.csv", encoding="utf-8-sig", index=False)
print("Data saved to profit_data_34_companies.csv, operation_data_34_companies.csv, growth_data_34_companies.csv")

# Logout from the system
bs.logout()
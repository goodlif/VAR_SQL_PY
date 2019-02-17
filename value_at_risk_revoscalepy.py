import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

from revoscalepy import RxComputeContext, RxInSqlServer, RxSqlServerData
from revoscalepy import rx_import


#Connection string to connect to SQL Server named instance
conn_str = 'Driver=SQL Server;Server=DESKTOP-NLR1KT5;Database=Apple_sql;Trusted_Connection=True;'

#Define the columns we wish to import
column_info = {"Date" : { "type" : "str" },
         "Open" : { "type" : "str" },
         "High" : { "type" : "str" },
         "Low" : { "type" : "str" },
         "Close" : { "type" : "str" },
         "Adj_Close" : { "type" : "str" },
         "Volume" : { "type" : "str" }
     }

#Get the data from SQL Server Table
data_source = RxSqlServerData(table="dbo.Apple_sql",
                               connection_string=conn_str, column_info=column_info)
computeContext = RxInSqlServer(
     connection_string = conn_str,
     num_tasks = 1,
     auto_cleanup = False
)

RxInSqlServer(connection_string=conn_str, num_tasks=1, auto_cleanup=False)

#testing dataset
#df = pd.read_csv('H:\\JSDev\\Python_SQL_Server_Value_At_Risk\\AAPL.csv', parse_dates=["Date"], index_col="Date")
#df.Close.resample('M').mean()
df = pd.DataFrame(rx_import(input_data = data_source))
print("Data frame:", df)

returns = df.pct_change()
returns = returns.dropna(how='any')

sorted_returns = sorted(returns["Close"])

varg = np.percentile(sorted_returns, 5)
print(varg)
plt.hist(sorted_returns,normed=True)
plt.xlabel('Returns')
plt.ylabel('Frequency')
plt.title(r'Asset Returns', fontsize=18, fontweight='bold')
plt.axvline(x=varg, color='r', linestyle='--', label='95% Confidence VaR: ' + "{0:.2f}%".format(varg * 100))
plt.legend(loc='upper right', fontsize = 'x-small')
plt.show() 

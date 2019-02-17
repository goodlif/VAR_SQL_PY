import pandas as pd
from sqlalchemy import create_engine
import pyodbc

# set db variables
dbname = 'stockdata'
schemaname = 'dbo'
servername = 'DESKTOP-NLR1KT5'
tablename = 'Apple_sql'

sqlcon = create_engine('mssql+pyodbc://@' + servername + '/' + dbname + '?driver=ODBC+Driver+13+for+SQL+Server')
df = pd.read_sql_table(tablename,con=sqlcon,schema=schemaname)

#print(df)

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
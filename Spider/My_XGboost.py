import pyodbc
import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.model_selection import train_test_split

cnxnstr = "DSN=Sample Cloudera Impala DSN;HOST=192.168.3.191;PORT=21050;UID=hive;AuthMech=3;PWD=hive;UseSasl=0"
conn = pyodbc.connect(cnxnstr, autocommit=True, timeout=240)
impala_cur = conn.cursor()


def get_list(arg):
    fetches = impala_cur.execute(str(arg))
    fetch_data = fetches.fetchall()
    d_list = []
    for data in fetch_data:
        d = []
        for i in data:
            d.append(i)
        d_list.append(d)
    return d_list


# 提取top10赔率公司
sql = 'select odd_comp from tmp.game_odds group by odd_comp order by count(*) desc limit 10'
rows = impala_cur.execute(sql)
top10 = rows.fetchall()
labels = []
for row in top10:
    labels.append("'" + row[0] + "'")

# 取出top10的赔率数据
sql1 = "select * from tmp.game_odds where odd_comp in (" + ",".join(labels) + ") and cast(id as int)<1500000"
data_list = get_list(sql1)
data_df = pd.DataFrame(data_list, columns=['id', 'company', 'fw', 'fd', 'fl', 'ow', 'od', 'ol'])
# data_piv = pd.pivot(data_df, index='id', columns='company', values=['fw', 'fd', 'fl', 'ow', 'od', 'ol'])
res = pd.DataFrame(pd.pivot(data_df, index='id', columns='company', values=['fw', 'fd', 'fl', 'ow', 'od', 'ol']))
# res.to_excel('test.xlsx')

# 取出赛果
sql2 = "select a.id,case when b.flat='胜' then 3 when b.flat='平' then 1 when b.flat='负' then 0 end " \
       "from tmp.game_odds a join tmp.game_record b on a.id=b.id where odd_comp in (" + ",".join(
        labels) + ") and cast(a.id as int)<1500000"
flat_list = get_list(sql2)
flat_df = pd.DataFrame(flat_list, columns=['id', 'flat'])
flat_df.to_excel('test2.xlsx')

new = pd.read_excel('test.xlsx',index_col='id')
new2 = pd.read_excel('test2.xlsx',index_col='id')

train = new2.join(new)
target = train['flat']


X_train, X_test, y_train, y_test = train_test_split(train, target, test_size=0.3, random_state=1)
model = xgb.XGBClassifier(max_depth=3, n_estimators=200, learn_rate=0.01)
model.fit(X_train, y_train)


"""
易胜博(安提瓜和巴布达)
10BET(英国)
澳门
明陞(菲律宾)
利记sbobet(英国)
威廉希尔(英国)
伟德(直布罗陀)
Coral(英国)
金宝博(马恩岛)
bet 365(英国)
"""
# 1.处理EXCEL
# 2.测试训练数据
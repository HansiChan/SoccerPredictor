import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn import preprocessing
from sklearn.model_selection import GridSearchCV
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import GradientBoostingClassifier
# from sklearn import cross_validation
from sklearn.preprocessing import LabelEncoder

import warnings

warnings.filterwarnings('ignore')

train = pd.read_csv('./train.csv')
test = pd.read_csv('./test.csv')
train.info()  # 打印训练数据的信息


def handle_na(train, test):  # 将Cabin特征删除
    fare_mean = train['Fare'].mean()  # 测试集的fare特征有缺失值，用训练数据的均值填充
    test.loc[pd.isnull(test.Fare), 'Fare'] = fare_mean

    embarked_mode = train['Embarked'].mode()  # 用众数填充
    train.loc[pd.isnull(train.Embarked), 'Embarked'] = embarked_mode[0]

    train.loc[pd.isnull(train.Age), 'Age'] = train['Age'].mean()  # 用均值填充年龄
    test.loc[pd.isnull(test.Age), 'Age'] = train['Age'].mean()
    return train, test


new_train, new_test = handle_na(train, test)  # 填充缺失值

# 对Embarked和male特征进行one-hot/get_dummies编码
new_train = pd.get_dummies(new_train, columns=['Embarked', 'Sex', 'Pclass'])
new_test = pd.get_dummies(new_test, columns=['Embarked', 'Sex', 'Pclass'])


target = new_train['Survived'].values
# 删除PassengerId，Name，Ticket，Cabin, Survived列
df_train = new_train.drop(['PassengerId','Name','Ticket','Cabin','Survived'], axis=1).values
df_test = new_test.drop(['PassengerId','Name','Ticket','Cabin'], axis=1).values



X_train, X_test, y_train, y_test = train_test_split(df_train, target, test_size=0.3, random_state=1)

model = xgb.XGBClassifier(max_depth=3, n_estimators=200, learn_rate=0.01)
model.fit(X_train, y_train)
test_score = model.score(X_test, y_test)
# print('test_score: {0}'.format(test_score))
pred = model.predict(df_test)
print(pred)
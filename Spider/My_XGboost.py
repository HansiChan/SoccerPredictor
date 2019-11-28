import joblib
import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.model_selection import train_test_split
from DAO.ImpalaCon import ImpalaCon


class Predict(object):

    def __init__(self):
        self.cur = ImpalaCon()
        self.flat_parse = "case when flat='胜' then '3' when flat='平' then '1' when flat='负' then '0' end"
        self.overunder_parse = "case when total_overunder='小' then '0' when total_overunder='大' then '1' end"

    # 训练胜负模型
    def train_flat(self):
        labels = self.get_top10('tmp.game_odds')  # 获取TOP10赔率公司
        # labels = ["'澳门'", "'威廉希尔(英国)'", "'bet 365(英国)'", "'伟德(直布罗陀)'"]
        sql = "select * from tmp.game_odds where odd_comp in (" + ",".join(labels) + ")"
        data_piv = self.get_data_df(sql)  # 取出标签的赔率数据
        game_list = self.get_game_list(data_piv)  # 获取比赛List
        result_df = self.get_result(game_list)  # 取出赛果
        train = data_piv.join(result_df)  # 整合赔率和赛果
        train.dropna(axis=0, how='any', inplace=True)   # 处理空值

        target = train['flat']
        train.drop(['flat', 'overunder'], axis=1, inplace=True)
        self.save_model(train, target, 'flat')

    # 保存预测模型
    @staticmethod
    def save_model(train, target, args):
        x_train, x_test, y_train, y_test = train_test_split(train.values, target, test_size=0.5, random_state=2)
        model = xgb.XGBClassifier(max_depth=1, n_estimators=100, learn_rate=0.1)
        model.fit(x_train, y_train)
        test_score = model.score(x_test, y_test)
        print('test_score: {0}'.format(test_score))
        joblib.dump(model, 'D:\Pyhton\SoccerPredictor\Model/19_%s.model' % (args))

    # 预测胜负
    def predict_flat(self):
        labels = self.get_top10('tmp.game_odds')  # 获取TOP10赔率公司
        sql = "select * from tmp.game_odds where odd_comp in (" + ",".join(labels) + ") and cast(id as int)>1500000"
        test = self.get_data_df(sql)  # 测试数据
        model = joblib.load('D:\Pyhton\SoccerPredictor\Model/19_flat.model')  # 调用模型
        preds = model.predict(test.values)
        print(preds)

    # 将List转为DataFrame
    def get_data_df(self, ags):
        data_list = self.cur.get_data_list(ags)
        data_df = pd.DataFrame(data_list, columns=['id', 'company', 'fw', 'fd', 'fl', 'ow', 'od', 'ol'])
        data_piv = pd.DataFrame(
            pd.pivot(data_df, index='id', columns='company', values=['fw', 'fd', 'fl', 'ow', 'od', 'ol']))  # 行列转换后聚合
        col_lis = [name[0] + '-' + name[1] for name in data_piv.columns]
        data_piv.columns = col_lis
        return data_piv

    # 提取top10赔率公司
    def get_top10(self, args):
        labels = []
        sql = 'select odd_comp from %s group by odd_comp order by count(*) desc limit 10' % (args)
        top10 = self.cur.get_data_list(sql)
        for row in top10:
            labels.append("'" + row[0] + "'")
        return labels

    # 取出赛果
    def get_result(self, game_list):
        sql = ("select distinct id,%s,%s  from tmp.game_record "
               "where id in (" + game_list + ")") % (self.flat_parse, self.overunder_parse)
        result_list = self.cur.get_data_list(sql)
        result_df = pd.DataFrame(result_list, columns=['id', 'flat', 'overunder'])
        result_df.set_index(['id'], inplace=True)
        # flat_df.to_excel('test2.xlsx') # 保存数据
        return result_df

    # 获取比赛List
    @staticmethod
    def get_game_list(data):
        game_list = ",".join(["'" + i + "'" for i in list(data.index)])
        return game_list


if __name__ == '__main__':
    predict = Predict()
    predict.train_flat()
    # predict.predict_flat()


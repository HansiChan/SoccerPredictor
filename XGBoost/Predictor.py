import joblib
import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.model_selection import train_test_split
from DAO.ImpalaCon import ImpalaCon


class Predictor(object):
    """
    基于XGBoost的机器学习预测算法，进行模型的训练和保存
    """
    def __init__(self):
        self.cur = ImpalaCon()
        self.flat_parse = "case when flat='胜' then '3' when flat='平' then '1' when flat='负' then '0' end"
        self.overunder_parse = "case when total_overunder='小' then '0' when total_overunder='大' then '1' end"

    def train_flat(self, team_id, hg):
        """
        训练胜负模型
        :param team_id: 队伍ID
        :param hg: 0：主场，1：客场
        """
        game_list = self.list2str(self.cur.get_game_list(team_id, hg))  # 获取所有比赛ID
        odd_df = self.get_label_odds(game_list, 'tmp.game_odds')  # 取出欧赔
        result_df = self.get_result(game_list)  # 取出赛果
        train = odd_df.join(result_df)  # 整合赔率和赛果
        train.dropna(axis=0, how='any', inplace=True)   # 处理空值

        target = train['flat']
        train.drop(['flat', 'overunder'], axis=1, inplace=True)
        self.save_model(train, target, team_id + '_' + str(hg) + '_flat')

    def train_ou(self, team_id, hg):
        """
        训练大小模型
        :param team_id: 队伍ID
        :param hg: 0：主场，1：客场
        """
        game_list = self.list2str(self.cur.get_game_list(team_id, hg))  # 获取所有比赛ID
        odd_df = self.get_label_odds(game_list, 'tmp.game_odds')  # 取出欧赔
        ou_df = self.get_label_odds(game_list, 'tmp.game_overunder')  # 取出大小盘赔
        result_df = self.get_result(game_list)  # 取出赛果
        train = odd_df.join(ou_df).join(result_df)  # 整合赔率和赛果
        train.dropna(axis=0, how='any', inplace=True)  # 处理空值

        target = train['overunder']
        train.drop(['flat', 'overunder'], axis=1, inplace=True)
        self.save_model(train, target, team_id + '_' + str(hg) + '_overunder')

    @staticmethod
    def save_model(train, target, args):
        """
        保存预测模型
        :param train: 元数据DF
        :param target: 结果集DF
        :param args: 类型(19_0_flat：team_id,主客场,预测类型)
        """
        x_train, x_test, y_train, y_test = train_test_split(train.values, target, test_size=0.6, random_state=2)
        model = xgb.XGBClassifier(max_depth=2, n_estimators=100, learn_rate=0.1)
        model.fit(x_train, y_train)
        test_score = model.score(x_test, y_test)
        print('test_score: {0}'.format(test_score))
        joblib.dump(model, 'D:\Pyhton\SoccerPredictor\Model/%s.model' % (args))

    # 预测胜负
    def predict_flat(self):
        labels = self.get_top10('tmp.game_odds')  # 获取TOP10赔率公司
        sql = "select * from tmp.game_odds where odd_comp in (%s) and cast(id as int)>1600000" % (",".join(labels))
        test = self.get_data_df(sql)  # 测试数据
        test.to_excel('test.xlsx')
        model = joblib.load('D:\Pyhton\SoccerPredictor\Model/19_0_flat.model')  # 调用模型
        preds = model.predict(test.values)
        print(preds)

    # 预测大小
    def predict_overunder(self):
        games = self.list2str(self.cur.get_game_list('19', 0))  # 获取所有比赛ID
        game_str = []
        for i in games.split(','):
            if int(i.replace('\'', ''))>1500000:
                game_str.append(i)
        game_list = ','.join(game_str)
        odd_df = self.get_label_odds(game_list, 'tmp.game_odds')  # 取出欧赔
        ou_df = self.get_label_odds(game_list, 'tmp.game_overunder')  # 取出大小盘赔
        test = odd_df.join(ou_df)
        test.to_excel('test.xlsx')
        model = joblib.load('D:\Pyhton\SoccerPredictor\Model/19_0_overunder.model')  # 调用模型
        preds = model.predict(test.values)
        for i in preds:
            print(i)

    def get_data_df(self, sql, flag):
        """
        将结果List转为DataFrame
        :param sql: SQL语句
        :param flag: 预测类型（flat）
        :return: 结果DataFrame
        """
        data_list = self.cur.get_data_list(sql)
        data_df = pd.DataFrame(data_list, columns=['id', 'company', 'f1', 'f2', 'f3', 'o1', 'o2', 'o3'])
        data_piv = pd.DataFrame(
            pd.pivot(data_df, index='id', columns='company', values=['f1', 'f2', 'f3', 'o1', 'o2', 'o3']))  # 行列转换后聚合
        col_lis = [name[0] + '_' + flag + '_' + name[1] for name in data_piv.columns]
        data_piv.columns = col_lis
        return data_piv

    def get_result(self, game_list):
        """
        取出赛果
        :param game_list: 比赛ID
        :return: 比赛结果DataFrame
        """
        sql = "select distinct id,%s,%s  from tmp.game_record where id in (%s)" \
              % (self.flat_parse, self.overunder_parse, game_list)
        result_list = self.cur.get_data_list(sql)
        result_df = pd.DataFrame(result_list, columns=['id', 'flat', 'overunder'])
        result_df.set_index(['id'], inplace=True)
        return result_df

    @staticmethod
    def list2str(oList):
        """
        获取比赛List
        :param oList: ID数组
        :return: 拼接后的ID字符串
        """
        games = ",".join(["'" + i + "'" for i in oList])
        return games

    def get_label_odds(self, game_list, table_name):
        """
        取出标签的赔率数据
        :param game_list: 比赛List
        :param table_name: 表名（tmp.game_odds）
        :return: 赔率数据DataFrame
        """
        flag = table_name.split('.')[1].split('_')[1]
        # 提取top10赔率公司
        labels = []
        sql = 'select odd_comp from %s group by odd_comp order by count(*) desc limit 10' % table_name
        top10 = self.cur.get_data_list(sql)
        for row in top10:
            labels.append("'" + row[0] + "'")
        # labels = ["'澳门'", "'威廉希尔(英国)'", "'bet 365(英国)'", "'伟德(直布罗陀)'"]

        # 取出标签的赔率数据
        sql = "select * from %s where odd_comp in (%s) and id in(%s)" % (table_name, ",".join(labels), game_list)
        data_df = self.get_data_df(sql, flag)
        return data_df


if __name__ == '__main__':
    predict = Predictor()
    # predict.train_flat('19', 0)
    # predict.train_ou('19', 0)
    # predict.predict_flat()
    predict.predict_overunder()


import joblib
import pandas as pd
import numpy as np
import xgboost as xgb
import os
import sys
import logging
from sklearn.model_selection import train_test_split

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from DAO.ImpalaCon import ImpalaCon
from config import MODEL, TABLES


class Predictor(object):
    """
    基于XGBoost的机器学习预测算法，进行模型的训练和保存
    """
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s")
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

        self.cur = ImpalaCon()
        self.flat_parse = "case when flat='胜' then '3' when flat='平' then '1' when flat='负' then '0' end"
        self.overunder_parse = "case when total_overunder='小' then '0' when total_overunder='大' then '1' end"
        # 使用配置文件中的模型目录
        self.model_dir = MODEL['DIR']
        self.logger.info(f"模型目录: {self.model_dir}")

    def train_flat(self, team_id, hg):
        """
        训练胜负模型
        :param team_id: 队伍ID
        :param hg: 0：主场，1：客场
        """
        try:
            self.logger.info(f"开始训练胜负模型 - 队伍ID: {team_id}, 主客场: {'主场' if hg == 0 else '客场'}")
            game_list = self.list2str(self.cur.get_game_list(team_id, hg))  # 获取所有比赛ID
            odd_df = self.get_label_odds(game_list, 'tmp.game_odds')  # 取出欧赔
            result_df = self.get_result(game_list)  # 取出赛果
            train = odd_df.join(result_df)  # 整合赔率和赛果
            rows_before = len(train)
            train.dropna(axis=0, how='any', inplace=True)   # 处理空值
            rows_after = len(train)
            if rows_before > rows_after:
                self.logger.info(f"删除了 {rows_before - rows_after} 行包含空值的数据")

            target = train['flat']
            train.drop(['flat', 'overunder'], axis=1, inplace=True)
            self.save_model(train, target, team_id + '_' + str(hg) + '_flat')
            self.logger.info("胜负模型训练完成")
        except Exception as e:
            self.logger.error(f"训练胜负模型失败: {str(e)}")
            raise

    def train_ou(self, team_id, hg):
        """
        训练大小模型
        :param team_id: 队伍ID
        :param hg: 0：主场，1：客场
        """
        try:
            self.logger.info(f"开始训练大小模型 - 队伍ID: {team_id}, 主客场: {'主场' if hg == 0 else '客场'}")
            game_list = self.list2str(self.cur.get_game_list(team_id, hg))  # 获取所有比赛ID
            odd_df = self.get_label_odds(game_list, 'tmp.game_odds')  # 取出欧赔
            ou_df = self.get_label_odds(game_list, 'tmp.game_overunder')  # 取出大小盘赔
            result_df = self.get_result(game_list)  # 取出赛果
            train = odd_df.join(ou_df).join(result_df)  # 整合赔率和赛果
            rows_before = len(train)
            train.dropna(axis=0, how='any', inplace=True)  # 处理空值
            rows_after = len(train)
            if rows_before > rows_after:
                self.logger.info(f"删除了 {rows_before - rows_after} 行包含空值的数据")

            target = train['overunder']
            train.drop(['flat', 'overunder'], axis=1, inplace=True)
            self.save_model(train, target, team_id + '_' + str(hg) + '_overunder')
            self.logger.info("大小模型训练完成")
        except Exception as e:
            self.logger.error(f"训练大小模型失败: {str(e)}")
            raise

    def save_model(self, train, target, args):
        """
        保存预测模型
        :param train: 元数据DF
        :param target: 结果集DF
        :param args: 类型(19_0_flat：team_id,主客场,预测类型)
        """
        try:
            self.logger.info(f"开始训练模型: {args}")
            self.logger.info(f"训练样本数: {len(train)}, 特征数: {len(train.columns)}")
            x_train, x_test, y_train, y_test = train_test_split(
                train.values, target,
                test_size=MODEL['TEST_SIZE'],
                random_state=MODEL['RANDOM_STATE']
            )
            self.logger.info(f"训练集大小: {len(x_train)}, 测试集大小: {len(x_test)}")

            model = xgb.XGBClassifier(
                max_depth=MODEL['MAX_DEPTH'],
                n_estimators=MODEL['N_ESTIMATORS'],
                learning_rate=MODEL['LEARNING_RATE']
            )
            model.fit(x_train, y_train)
            test_score = model.score(x_test, y_test)
            self.logger.info(f'模型测试得分: {test_score:.4f}')

            # 确保模型目录存在
            os.makedirs(self.model_dir, exist_ok=True)
            model_path = os.path.join(self.model_dir, f'{args}.model')
            joblib.dump(model, model_path)
            self.logger.info(f"模型已保存到: {model_path}")
        except Exception as e:
            self.logger.error(f"保存模型失败: {str(e)}")
            raise

    # 预测胜负
    def predict_flat(self):
        try:
            self.logger.info("开始预测胜负...")
            labels = self.get_top10('tmp.game_odds')  # 获取TOP10赔率公司
            sql = "select * from tmp.game_odds where odd_comp in (%s) and cast(id as int)>1600000" % (",".join(labels))
            test = self.get_data_df(sql, 'odds')  # 测试数据 - 修复缺失参数
            self.logger.info(f"测试数据量: {len(test)}")
            test.to_excel('test.xlsx')

            model_path = os.path.join(self.model_dir, '19_0_flat.model')
            if not os.path.exists(model_path):
                raise FileNotFoundError(f"模型文件不存在: {model_path}")
            model = joblib.load(model_path)  # 调用模型
            preds = model.predict(test.values)
            self.logger.info(f"预测完成，结果数量: {len(preds)}")
            print(preds)
            return preds
        except Exception as e:
            self.logger.error(f"预测胜负失败: {str(e)}")
            raise

    # 预测大小
    def predict_overunder(self):
        try:
            self.logger.info("开始预测大小...")
            games = self.list2str(self.cur.get_game_list('19', 0))  # 获取所有比赛ID
            game_str = []
            for i in games.split(','):
                if int(i.replace('\'', ''))>1500000:
                    game_str.append(i)
            game_list = ','.join(game_str)
            self.logger.info(f"筛选后的比赛数量: {len(game_str)}")

            odd_df = self.get_label_odds(game_list, 'tmp.game_odds')  # 取出欧赔
            ou_df = self.get_label_odds(game_list, 'tmp.game_overunder')  # 取出大小盘赔
            test = odd_df.join(ou_df)
            self.logger.info(f"测试数据量: {len(test)}")
            test.to_excel('test.xlsx')

            model_path = os.path.join(self.model_dir, '19_0_overunder.model')
            if not os.path.exists(model_path):
                raise FileNotFoundError(f"模型文件不存在: {model_path}")
            model = joblib.load(model_path)  # 调用模型
            preds = model.predict(test.values)
            self.logger.info(f"预测完成，结果数量: {len(preds)}")
            for i in preds:
                print(i)
            return preds
        except Exception as e:
            self.logger.error(f"预测大小失败: {str(e)}")
            raise

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

    def get_top10(self, table_name):
        """
        获取TOP10赔率公司
        :param table_name: 表名（tmp.game_odds）
        :return: TOP10公司名称列表（带引号）
        """
        labels = []
        # 验证表名
        allowed_tables = ['tmp.game_odds', 'tmp.game_overunder']
        if table_name not in allowed_tables:
            raise ValueError(f"Invalid table name: {table_name}")

        sql = f'select odd_comp from {table_name} group by odd_comp order by count(*) desc limit 10'
        top10 = self.cur.get_data_list(sql)
        for row in top10:
            labels.append("'" + row[0] + "'")
        self.logger.info(f"获取到TOP10公司: {labels}")
        return labels

    def get_label_odds(self, game_list, table_name):
        """
        取出标签的赔率数据
        :param game_list: 比赛List
        :param table_name: 表名（tmp.game_odds）
        :return: 赔率数据DataFrame
        """
        flag = table_name.split('.')[1].split('_')[1]
        # 提取top10赔率公司
        labels = self.get_top10(table_name)

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


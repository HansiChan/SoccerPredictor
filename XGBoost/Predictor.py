import joblib
import pandas as pd
import numpy as np
import xgboost as xgb
import os
import sys
import logging
from sklearn.model_selection import train_test_split

# Add the project root directory to the path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from DAO.ImpalaCon import ImpalaCon
from config import MODEL, TABLES


class Predictor(object):
    """
    Machine learning prediction algorithm based on XGBoost for model training and saving
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
        self.flat_parse = "case when flat='Win' then '3' when flat='Draw' then '1' when flat='Loss' then '0' end"
        self.overunder_parse = "case when total_overunder='Under' then '0' when total_overunder='Over' then '1' end"
        # Use the model directory from the configuration file
        self.model_dir = MODEL['DIR']
        self.logger.info(f"Model directory: {self.model_dir}")

    def train_flat(self, team_id, hg):
        """
        Train the win/loss model
        :param team_id: Team ID
        :param hg: 0: home, 1: away
        """
        try:
            self.logger.info(f"Start training win/loss model - Team ID: {team_id}, Home/Away: {'Home' if hg == 0 else 'Away'}")
            game_list = self.list2str(self.cur.get_game_list(team_id, hg))  # Get all game IDs
            odd_df = self.get_label_odds(game_list, 'tmp.game_odds')  # Get European odds
            result_df = self.get_result(game_list)  # Get game results
            train = odd_df.join(result_df)  # Integrate odds and results
            rows_before = len(train)
            train.dropna(axis=0, how='any', inplace=True)   # Handle null values
            rows_after = len(train)
            if rows_before > rows_after:
                self.logger.info(f"Deleted {rows_before - rows_after} rows containing null values")

            target = train['flat']
            train.drop(['flat', 'overunder'], axis=1, inplace=True)
            self.save_model(train, target, f'{team_id}_{hg}_flat')
            self.logger.info("Win/loss model training completed")
        except Exception as e:
            self.logger.error(f"Failed to train win/loss model: {e}")
            raise

    def train_ou(self, team_id, hg):
        """
        Train the over/under model
        :param team_id: Team ID
        :param hg: 0: home, 1: away
        """
        try:
            self.logger.info(f"Start training over/under model - Team ID: {team_id}, Home/Away: {'Home' if hg == 0 else 'Away'}")
            game_list = self.list2str(self.cur.get_game_list(team_id, hg))  # Get all game IDs
            odd_df = self.get_label_odds(game_list, 'tmp.game_odds')  # Get European odds
            ou_df = self.get_label_odds(game_list, 'tmp.game_overunder')  # Get over/under odds
            result_df = self.get_result(game_list)  # Get game results
            train = odd_df.join(ou_df).join(result_df)  # Integrate odds and results
            rows_before = len(train)
            train.dropna(axis=0, how='any', inplace=True)  # Handle null values
            rows_after = len(train)
            if rows_before > rows_after:
                self.logger.info(f"Deleted {rows_before - rows_after} rows containing null values")

            target = train['overunder']
            train.drop(['flat', 'overunder'], axis=1, inplace=True)
            self.save_model(train, target, f'{team_id}_{hg}_overunder')
            self.logger.info("Over/under model training completed")
        except Exception as e:
            self.logger.error(f"Failed to train over/under model: {e}")
            raise

    def save_model(self, train, target, args):
        """
        Save the prediction model
        :param train: Metadata DataFrame
        :param target: Result set DataFrame
        :param args: Type (e.g., 19_0_flat: team_id, home/away, prediction type)
        """
        try:
            self.logger.info(f"Start training model: {args}")
            self.logger.info(f"Number of training samples: {len(train)}, Number of features: {len(train.columns)}")
            x_train, x_test, y_train, y_test = train_test_split(
                train.values, target,
                test_size=MODEL['TEST_SIZE'],
                random_state=MODEL['RANDOM_STATE']
            )
            self.logger.info(f"Training set size: {len(x_train)}, Test set size: {len(x_test)}")

            model = xgb.XGBClassifier(
                max_depth=MODEL['MAX_DEPTH'],
                n_estimators=MODEL['N_ESTIMATORS'],
                learning_rate=MODEL['LEARNING_RATE']
            )
            model.fit(x_train, y_train)
            test_score = model.score(x_test, y_test)
            self.logger.info(f'Model test score: {test_score:.4f}')

            # Ensure the model directory exists
            os.makedirs(self.model_dir, exist_ok=True)
            model_path = os.path.join(self.model_dir, f'{args}.model')
            joblib.dump(model, model_path)
            self.logger.info(f"Model saved to: {model_path}")
        except Exception as e:
            self.logger.error(f"Failed to save model: {e}")
            raise

    # Predict win/loss
    def predict_flat(self):
        try:
            self.logger.info("Start predicting win/loss...")
            labels = self.get_top10('tmp.game_odds')  # Get TOP10 odds companies
            sql = "select * from tmp.game_odds where odd_comp in (%s) and cast(id as int)>1600000" % (",".join(labels))
            test = self.get_data_df(sql, 'odds', labels)  # Test data - fix missing parameter
            self.logger.info(f"Test data size: {len(test)}")
            test.to_excel('test.xlsx')

            model_path = os.path.join(self.model_dir, '19_0_flat.model')
            if not os.path.exists(model_path):
                raise FileNotFoundError(f"Model file not found: {model_path}")
            model = joblib.load(model_path)  # Load model
            preds = model.predict(test.values)
            self.logger.info(f"Prediction completed, number of results: {len(preds)}")
            print(preds)
            return preds
        except Exception as e:
            self.logger.error(f"Failed to predict win/loss: {e}")
            raise

    # Predict over/under
    def predict_overunder(self):
        try:
            self.logger.info("Start predicting over/under...")
            games = self.cur.get_game_list('19', 0)  # Get all game IDs
            game_str = [f"'{i}'" for i in games if int(i) > 1500000]
            game_list = ','.join(game_str)
            self.logger.info(f"Number of filtered games: {len(game_str)}")

            odd_df = self.get_label_odds(game_list, 'tmp.game_odds')  # Get European odds
            ou_df = self.get_label_odds(game_list, 'tmp.game_overunder')  # Get over/under odds
            test = odd_df.join(ou_df)
            self.logger.info(f"Test data size: {len(test)}")
            test.to_excel('test.xlsx')

            model_path = os.path.join(self.model_dir, '19_0_overunder.model')
            if not os.path.exists(model_path):
                raise FileNotFoundError(f"Model file not found: {model_path}")
            model = joblib.load(model_path)  # Load model
            preds = model.predict(test.values)
            self.logger.info(f"Prediction completed, number of results: {len(preds)}")
            for i in preds:
                print(i)
            return preds
        except Exception as e:
            self.logger.error(f"Failed to predict over/under: {e}")
            raise

    def get_data_df(self, sql, flag, columns):
        """
        Convert the result List to a DataFrame
        :param sql: SQL statement
        :param flag: Prediction type (e.g., 'flat')
        :return: Result DataFrame
        """
        data_list = self.cur.get_data_list(sql)
        data_df = pd.DataFrame(data_list, columns=columns)
        data_piv = pd.DataFrame(
            pd.pivot_table(data_df, index='id', columns='company', values=['f1', 'f2', 'f3', 'o1', 'o2', 'o3']))  # Pivot and aggregate
        col_lis = [f'{name[0]}_{flag}_{name[1]}' for name in data_piv.columns]
        data_piv.columns = col_lis
        return data_piv

    def get_result(self, game_list):
        """
        Get game results
        :param game_list: Game IDs
        :return: Game result DataFrame
        """
        sql = f"select distinct id,{self.flat_parse},{self.overunder_parse} from tmp.game_record where id in ({game_list})"
        result_list = self.cur.get_data_list(sql)
        result_df = pd.DataFrame(result_list, columns=['id', 'flat', 'overunder'])
        result_df.set_index(['id'], inplace=True)
        return result_df

    @staticmethod
    def list2str(oList):
        """
        Get game List
        :param oList: ID array
        :return: Concatenated ID string
        """
        return ",".join([f"'{i}'" for i in oList])

    def get_top10(self, table_name):
        """
        Get TOP10 odds companies
        :param table_name: table name (e.g., tmp.game_odds)
        :return: TOP10 company name list (with quotes)
        """
        # Validate table name
        allowed_tables = ['tmp.game_odds', 'tmp.game_overunder']
        if table_name not in allowed_tables:
            raise ValueError(f"Invalid table name: {table_name}")

        sql = f'select odd_comp from {table_name} group by odd_comp order by count(*) desc limit 10'
        top10 = self.cur.get_data_list(sql)
        labels = [f"'{row[0]}'" for row in top10]
        self.logger.info(f"Get TOP10 companies: {labels}")
        return labels

    def get_label_odds(self, game_list, table_name):
        """
        Get odds data for labels
        :param game_list: Game List
        :param table_name: table name (e.g., tmp.game_odds)
        :return: Odds data DataFrame
        """
        flag = table_name.split('.')[1].split('_')[1]
        # Extract top10 odds companies
        labels = self.get_top10(table_name)
        columns = ['id', 'company', 'f1', 'f2', 'f3', 'o1', 'o2', 'o3']
        # Get odds data for labels
        sql = f"select * from {table_name} where odd_comp in ({','.join(labels)}) and id in({game_list})"
        data_df = self.get_data_df(sql, flag, columns)
        return data_df


if __name__ == '__main__':
    predict = Predictor()
    # predict.train_flat('19', 0)
    # predict.train_ou('19', 0)
    # predict.predict_flat()
    predict.predict_overunder()


import pyodbc
import logging
import sys
import os

# Add the project root directory to the path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config import get_database_connection_string, DATABASE


class ImpalaCon(object):
    """
    Used to operate the database for CRUD operations
    """
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s")
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

        try:
            cnxnstr = get_database_connection_string()
            conn = pyodbc.connect(cnxnstr, autocommit=True, timeout=DATABASE['TIMEOUT'])
            self.impala_cur = conn.cursor()
            self.logger.info("Successfully connected to Impala database")
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {str(e)}")
            raise

    def get_game_list(self, team_id, hg):
        """
        Get home/away game records
        :param team_id: Team ID
        :param hg: 0: home, 1: away
        :return: Game List
        """
        try:
            game_list = []
            sql = "select distinct name from tmp.team_list where team_id=?"
            rows = self.impala_cur.execute(sql, (team_id,))
            results = rows.fetchall()
            if not results:
                self.logger.warning(f"No data found for team ID {team_id}")
                return game_list
            team_name = results[0][0]
            # Home or away
            field_type = "Home" if hg == 0 else "Away"
            if hg == 0:
                sql = "select id from tmp.game_record where host_t=?"
            else:
                sql = "select id from tmp.game_record where guest_t=?"
            rows = self.impala_cur.execute(sql, (team_name,))
            for row in rows.fetchall():
                game_list.append(row[0])
            self.logger.info(f"Retrieved {len(game_list)} {field_type} game records for {team_name}")
            return game_list
        except Exception as e:
            self.logger.error(f"Failed to get game list: {str(e)}")
            raise

    def save(self, sql, params=None):
        """
        Execute sql, no return value, for DDL operations
        :param sql: The sql statement to be executed
        """
        try:
            if params:
                self.impala_cur.execute(sql, params)
            else:
                self.impala_cur.execute(sql)
            self.logger.debug(f"Successfully executed SQL: {sql[:100]}...")
        except Exception as e:
            self.logger.error(f"Failed to execute SQL: {str(e)}, SQL: {sql[:100]}...")
            raise

    def get_data_list(self, sql, params=None):
        """
        Execute Sql and return the result List
        :param sql: The sql statement to be executed
        :return: Return result List
        """
        try:
            if params:
                fetches = self.impala_cur.execute(sql, params)
            else:
                fetches = self.impala_cur.execute(sql)
            fetch_data = fetches.fetchall()
            d_list = []
            for data in fetch_data:
                d = []
                for i in data:
                    if i is not None and isinstance(i, str) and '/' in i:
                        try:
                            i = (float(i.split('/')[0]) + float(i.split('/')[1])) / 2
                        except (ValueError, IndexError):
                            pass  # Keep original value
                    d.append(i)
                d_list.append(d)
            self.logger.debug(f"Query returned {len(d_list)} records")
            return d_list
        except Exception as e:
            self.logger.error(f"Failed to query data: {str(e)}, SQL: {str(sql)[:100]}...")
            raise
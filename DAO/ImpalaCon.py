import pyodbc
import logging
import sys
import os

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config import get_database_connection_string, DATABASE


class ImpalaCon(object):
    """
    用于操作数据库，进行增删改查
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
            self.logger.info("成功连接到Impala数据库")
        except Exception as e:
            self.logger.error(f"连接数据库失败: {str(e)}")
            raise

    def get_game_list(self, team_id, hg):
        """
        获取主/客场比赛记录
        :param team_id: 队伍ID
        :param hg: 0：主场，1：客场
        :return: 比赛List
        """
        try:
            game_list = []
            sql = "select distinct name from tmp.team_list where team_id=?"
            rows = self.impala_cur.execute(sql, (team_id,))
            results = rows.fetchall()
            if not results:
                self.logger.warning(f"未找到队伍ID为 {team_id} 的数据")
                return game_list
            team_name = results[0][0]
            # 主场or客场
            field_type = "主场" if hg == 0 else "客场"
            if hg == 0:
                sql = "select id from tmp.game_record where host_t=?"
            else:
                sql = "select id from tmp.game_record where guest_t=?"
            rows = self.impala_cur.execute(sql, (team_name,))
            for row in rows.fetchall():
                game_list.append(row[0])
            self.logger.info(f"获取到 {team_name} 的 {field_type} 比赛记录: {len(game_list)} 场")
            return game_list
        except Exception as e:
            self.logger.error(f"获取比赛列表失败: {str(e)}")
            raise

    def save(self, sql):
        """
        执行sql，无返回值，用于DDL操作
        :param sql: 要执行的sql语句
        """
        try:
            self.impala_cur.execute(sql)
            self.logger.debug(f"成功执行SQL: {sql[:100]}...")
        except Exception as e:
            self.logger.error(f"执行SQL失败: {str(e)}, SQL: {sql[:100]}...")
            raise

    def get_data_list(self, arg):
        """
        执行Sql并返回结果List
        :param arg: 要执行的sql语句
        :return: 返回结果List
        """
        try:
            fetches = self.impala_cur.execute(str(arg))
            fetch_data = fetches.fetchall()
            d_list = []
            for data in fetch_data:
                d = []
                for i in data:
                    if i is not None and isinstance(i, str) and '/' in i:
                        try:
                            i = (float(i.split('/')[0]) + float(i.split('/')[1])) / 2
                        except (ValueError, IndexError):
                            pass  # 保持原值
                    d.append(i)
                d_list.append(d)
            self.logger.debug(f"查询返回 {len(d_list)} 条记录")
            return d_list
        except Exception as e:
            self.logger.error(f"查询数据失败: {str(e)}, SQL: {str(arg)[:100]}...")
            raise
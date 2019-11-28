import pyodbc


class ImpalaCon(object):

    def __init__(self):
        cnxnstr = "DSN=Sample Cloudera Impala DSN;HOST=192.168.3.191;PORT=21050;UID=hive;AuthMech=3;PWD=hive;UseSasl=0"
        conn = pyodbc.connect(cnxnstr, autocommit=True, timeout=240)
        self.impala_cur = conn.cursor()

    # 获取主/客场比赛记录
    def get_game_list(self, team_id, hg):
        game_list = []
        sql = "select distinct name from tmp.team_list where team_id='%s'" % (team_id)
        rows = self.impala_cur.execute(sql)
        team_name = rows.fetchall()[0][0]
        # 主场or客场
        if hg == 0:
            sql = "select id from tmp.game_record where host_t='%s'" % (team_name)
        else:
            sql = "select id from tmp.game_record where guest_t='%s'" % (team_name)
        rows = self.impala_cur.execute(sql)
        for row in rows.fetchall():
            game_list.append(row[0])
        return game_list

    def save(self, sql):
        self.impala_cur.execute(sql)

    # 执行Sql并返回结果List
    def get_data_list(self, arg):
        fetches = self.impala_cur.execute(str(arg))
        fetch_data = fetches.fetchall()
        d_list = []
        for data in fetch_data:
            d = []
            for i in data:
                d.append(i)
            d_list.append(d)
        return d_list

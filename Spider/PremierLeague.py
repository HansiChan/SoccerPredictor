# coding=utf-8
from __future__ import print_function, division

import logging.handlers
import re
import time

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
from DAO.ImpalaCon import ImpalaCon
from sqlalchemy import create_engine


class Spider(object):

    def __init__(self):
        ## setup
        # self.base_url = base_url
        self.options = Options()
        self.options.add_argument('--headless')  # 爬取时隐藏浏览器
        self.driver = webdriver.Chrome(options=self.options)
        # self.driver = webdriver.Chrome()
        self.driver.implicitly_wait(30)
        self.verificationErrors = []
        self.accept_next_alert = True

        # 日志组件
        formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s")
        handler1 = logging.StreamHandler()
        handler1.setFormatter(formatter)
        self.logger = logging.getLogger("logger")
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(handler1)

        self.cur = ImpalaCon()

        # mysql连接信息
        # self.dbcon = create_engine('mysql+mysqlconnector://root:123456@localhost:3306/test?charset=utf8')

    # 获取赛季联赛球队名单及球队对应ID
    def get_team_ids(self, season, league):
        main_url = 'http://zq.win007.com/cn/TeamHeadPage/%s/%s.html' % (season, league)
        self.driver.get(main_url)
        teams = self.driver.find_elements_by_xpath("//td[@background='/Images/photo_s_2.gif' and @class='name01']")
        data = []
        for team in teams:
            team_id = team.find_element_by_xpath(".//a").get_attribute('href').split('/')[-1].split('.')[0]
            team_name = team.find_element_by_xpath(".//a").text
            data.append([team_id, team_name])
        self.driver.close()
        team_list = pd.DataFrame(data, columns=['队伍ID', '队伍名称'])
        self.logger.info(team_list)
        # self.to_sql('test.team_list', team_list)
        self.to_kudu('tmp.team_list', data)

    # 获取球队历史比赛数据
    def get_game_record(self, teamid, page_num):
        url = 'http://info.win0168.com/cn/team/TeamSche/%s.html' % teamid
        data = []
        ulist = []
        self.driver.get(url)
        for i in range(1, page_num + 1):
            self.logger.info('正在抓取第' + str(i) + '页:' + url)
            try:
                gamelist = self.driver.find_element_by_xpath('//div[@id="div_Table2" and @class="data"]')
                matches = gamelist.find_elements_by_xpath(".//tr")
                # 抓取比赛数据，并保存成DataFrame
                for x in matches:
                    matche_id = ''
                    url_list = []
                    if len(str(x.text).split(' ')) == 17:
                        league = str(x.text).split(' ')[0]
                        date = str(x.text).split(' ')[1]
                        times = str(x.text).split(' ')[2]
                        host_team = str(x.text).split(' ')[3]
                        full_score = str(x.text).split(' ')[4]
                        guest_team = str(x.text).split(' ')[5]
                        half_score = str(x.text).split(' ')[6]
                        aisa = str(x.text).split(' ')[7]
                        total_overunder = str(x.text).split(' ')[8]
                        flat = str(x.text).split(' ')[9]
                        # 爬取网页url
                        if x.find_elements_by_xpath('.//*[@href]'):
                            for y in x.find_elements_by_xpath('.//*[@href]'):
                                if 'analysis' in y.get_attribute('href'):
                                    matche_id = re.match('.*analysis/(\d+)', y.get_attribute('href')).group(1)
                                    url_list.insert(0, matche_id)
                                url_list.append(y.get_attribute('href'))
                        data.append(
                            [matche_id, league, date, times, host_team, full_score, guest_team, half_score, aisa,
                             total_overunder, flat])
                        ulist.append(url_list)
                # 模拟输入并跳转到指定页
                input = '//input[@type="text" and @name="pageNo" and @id="pageNo"]'
                self.driver.find_element_by_xpath(input).send_keys(i + 1)
                onclick = '//input[@onclick="SearchTeamSche(19)"]'
                self.driver.find_element_by_xpath(onclick).click()
                # 等待加载
                time.sleep(1)
                self.logger.info('链接成功')
            except Exception as e:
                self.logger.info('链接失败:' + str(e))
        self.driver.close()
        ddf = pd.DataFrame(data, columns=['ID', '赛事', '日期', '时间', '主队', '全场比分', '客队', '半场比分', '亚盘', '大小盘', '胜平负'])
        udf = pd.DataFrame(ulist, columns=['ID', '主队主页', '比赛记录', '客队主页', '比赛分析', '亚盘指数', '大小盘指数', '欧赔指数'])
        # self.to_sql('test.game_record',ddf)
        # self.to_sql('test.game_record_url',udf)
        self.to_kudu('tmp.game_record', data)
        self.to_kudu('tmp.game_record_url', ulist)

    # 获取主/客场赔率数据（主场：0/客场：1）
    def get_odds(self, game_id, hg):

        def get_oddList(arg):
            selector = Select(self.driver.find_element_by_id("sel_showType"))
            selector.select_by_visible_text(arg)
            odds_list = []
            odd_table = self.driver.find_element_by_xpath('//table[@id="oddsList_tab"]')
            odds = odd_table.find_elements_by_xpath('.//tr[.//td//a[@title="主流公司"]]')
            for i in odds:
                a = i.find_element_by_xpath('.//a').text
                span = re.findall(r'\d+\.\d+', i.text)[0:3]
                span.insert(0, a)
                odds_list.append(span)
            return odds_list

        game_list = self.cur.get_game_list(game_id, hg)
        self.logger.info('共 ' + str(len(game_list)) + ' 条记录')
        for gid in game_list:
            index = game_list.index(gid)
            url = 'http://vip.win0168.com/1x2/oddslist/%s.htm' % (gid)
            self.logger.info('正在爬取第 ' + str(index + 1) + ' 条记录数据 game_id:' + gid)
            self.driver.get(url)
            # 初盘
            orign_odd = get_oddList("初盘")
            # 终盘
            final_odd = get_oddList("即时盘")
            full_odd = []
            for f, o in zip(final_odd, orign_odd):
                f.insert(0, gid)
                full_odd.append(f + o[1:])
            self.to_kudu('tmp.game_odds', full_odd)
        self.driver.close()

    # 获取大小球赔率数据
    def get_overunder(self, game_id, hg):
        game_list = self.cur.get_game_list(game_id, hg)
        self.logger.info('共 ' + str(len(game_list)) + ' 条记录')
        for gid in game_list:
            index = game_list.index(gid)
            url = 'http://vip.win0168.com/OverDown_n.aspx?id=%s&l=0' % gid
            self.logger.info('正在爬取第 ' + str(index + 1) + ' 条记录数据 game_id:' + gid)
            self.driver.get(url)
            odd_table = self.driver.find_element_by_xpath('//table[@id="odds"]')
            odds = odd_table.find_elements_by_xpath('.//tr[.//td[@height="25"]]')
            odds_list = []
            for i in odds[:-2]:
                if len(i.text) > 0:
                    if '\n' in i.text:
                        comp = i.text.split('\n')[0]
                        odd = i.text.split('\n')[1].split(' ')[0:6]
                    else:
                        comp = i.text.split(' ')[0]
                        odd = i.text.split(' ')[1:7]
                    odd.insert(0, comp)
                    odd.insert(0, gid)
                    if len(odd) == 8:
                        odds_list.append(odd)
            self.to_kudu('tmp.game_overunder',odds_list)
        self.driver.close()

    # 保存到mysql
    def to_sql(self, table_name, data):
        table = table_name.split('.')[0]
        db = table_name.split('.')[1]
        data.to_sql(table, schema=db, con=self.dbcon, if_exists='replace', index=False)

    # 保存到kudu
    def to_kudu(self, table_name, data):
        for row in data:
            d = ''
            for l in row:
                d += "'" + str(l) + "',"
            sql = 'upsert into ' + table_name + ' values(' + d[:-1] + ')'
            self.cur.save(sql)


if __name__ == "__main__":
    spider = Spider()
    # spider.get_team_ids('2019-2020', '36')
    # spider.get_game_record(19, 54)
    # spider.get_odds('1646984')
    # spider.get_odds('19', 0)
    spider.get_overunder('19', 0)
# coding=utf-8
from __future__ import print_function, division

import logging.handlers
import re
import time
import sys
import os

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from DAO.ImpalaCon import ImpalaCon
from config import SPIDER, TABLES
from sqlalchemy import create_engine


class GameSpider(object):
    """
    爬虫程序，负责从网页爬取训练数据
    """
    def __init__(self):
        self.options = Options()
        if SPIDER['HEADLESS']:
            self.options.add_argument('--headless')  # 爬取时隐藏浏览器
        self.driver = webdriver.Chrome(options=self.options)
        self.driver.implicitly_wait(SPIDER['IMPLICIT_WAIT'])
        self.driver.set_page_load_timeout(SPIDER['PAGE_LOAD_TIMEOUT'])
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

    def get_team_ids(self, season, league):
        """
        获取赛季联赛球队名单及球队对应ID
        :param season: 赛季（2019-2020）
        :param league: 联赛编号（36）
        """
        main_url = 'http://zq.win007.com/cn/TeamHeadPage/%s/%s.html' % (season, league)
        self.driver.get(main_url)
        teams = self.driver.find_elements(By.XPATH, "//td[@background='/Images/photo_s_2.gif' and @class='name01']")
        data = []
        for team in teams:
            team_id = team.find_element(By.XPATH, ".//a").get_attribute('href').split('/')[-1].split('.')[0]
            team_name = team.find_element(By.XPATH, ".//a").text
            data.append([team_id, team_name])
        self.driver.close()
        team_list = pd.DataFrame(data, columns=['队伍ID', '队伍名称'])
        self.logger.info(team_list)
        self.to_kudu('tmp.team_list', data)


    def get_game_record(self, teamid, page_num):
        """
        获取球队历史比赛数据
        :param teamid: 队伍ID
        :param page_num: 爬取的页数范围（最大54）
        """
        url = 'http://info.win0168.com/cn/team/TeamSche/%s.html' % teamid
        data = []
        ulist = []
        self.driver.get(url)
        for i in range(1, page_num + 1):
            self.logger.info('正在抓取第' + str(i) + '页:' + url)
            try:
                gamelist = self.driver.find_element(By.XPATH, '//div[@id="div_Table2" and @class="data"]')
                matches = gamelist.find_elements(By.XPATH, ".//tr")
                # 抓取比赛数据，并保存成DataFrame
                for x in matches:
                    matche_id = ''
                    url_list = []
                    # 优化：只分割一次字符串
                    text_parts = str(x.text).split(' ')
                    if len(text_parts) == 17:
                        league = text_parts[0]
                        date = text_parts[1]
                        times = text_parts[2]
                        host_team = text_parts[3]
                        full_score = text_parts[4]
                        guest_team = text_parts[5]
                        half_score = text_parts[6]
                        aisa = text_parts[7]
                        total_overunder = text_parts[8]
                        flat = text_parts[9]
                        # 爬取网页url
                        links = x.find_elements(By.XPATH, './/*[@href]')
                        if links:
                            for y in links:
                                if 'analysis' in y.get_attribute('href'):
                                    matche_id = re.match('.*analysis/(\d+)', y.get_attribute('href')).group(1)
                                    url_list.insert(0, matche_id)
                                url_list.append(y.get_attribute('href'))
                        data.append(
                            [matche_id, league, date, times, host_team, full_score, guest_team, half_score, aisa,
                             total_overunder, flat])
                        ulist.append(url_list)
                # 模拟输入并跳转到指定页
                input_xpath = '//input[@type="text" and @name="pageNo" and @id="pageNo"]'
                self.driver.find_element(By.XPATH, input_xpath).send_keys(i + 1)
                onclick_xpath = '//input[@onclick="SearchTeamSche(19)"]'
                self.driver.find_element(By.XPATH, onclick_xpath).click()
                # 等待加载
                time.sleep(1)
                self.logger.info('链接成功')
            except Exception as e:
                self.logger.info('链接失败:' + str(e))
        self.driver.close()
        ddf = pd.DataFrame(data, columns=['ID', '赛事', '日期', '时间', '主队', '全场比分', '客队', '半场比分', '亚盘', '大小盘', '胜平负'])
        udf = pd.DataFrame(ulist, columns=['ID', '主队主页', '比赛记录', '客队主页', '比赛分析', '亚盘指数', '大小盘指数', '欧赔指数'])
        self.to_kudu('tmp.game_record', data)
        self.to_kudu('tmp.game_record_url', ulist)

    def get_odds(self, game_id, hg):
        """
        获取主/客场赔率数据
        :param game_id: 队伍ID
        :param hg: 0：主场，1：客场
        """
        def get_oddList(arg):
            selector = Select(self.driver.find_element(By.ID, "sel_showType"))
            selector.select_by_visible_text(arg)
            odds_list = []
            odd_table = self.driver.find_element(By.XPATH, '//table[@id="oddsList_tab"]')
            odds = odd_table.find_elements(By.XPATH, './/tr[.//td//a[@title="主流公司"]]')
            for i in odds:
                a = i.find_element(By.XPATH, './/a').text
                span = re.findall(r'\d+\.\d+', i.text)[0:3]
                span.insert(0, a)
                odds_list.append(span)
            return odds_list

        game_list = self.cur.get_game_list(game_id, hg)
        self.logger.info('共 ' + str(len(game_list)) + ' 条记录')
        # 优化：使用enumerate避免O(n²)复杂度
        for index, gid in enumerate(game_list):
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

    def get_overunder(self, game_id, hg):
        """
        获取大小球赔率数据
        :param game_id: 队伍ID
        :param hg: 0：主场，1：客场
        """
        game_list = self.cur.get_game_list(game_id, hg)
        self.logger.info('共 ' + str(len(game_list)) + ' 条记录')
        # 优化：使用enumerate避免O(n²)复杂度
        for index, gid in enumerate(game_list):
            url = 'http://vip.win0168.com/OverDown_n.aspx?id=%s&l=0' % gid
            self.logger.info('正在爬取第 ' + str(index + 1) + ' 条记录数据 game_id:' + gid)
            self.driver.get(url)
            odd_table = self.driver.find_element(By.XPATH, '//table[@id="odds"]')
            odds = odd_table.find_elements(By.XPATH, './/tr[.//td[@height="25"]]')
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
            self.to_kudu('tmp.game_overunder', odds_list)
        self.driver.close()

    def to_kudu(self, table_name, data):
        """
        保存到kudu
        :param table_name: 表名（tmp.game_record）
        :param data: 数据List
        """
        # 验证表名在允许列表中，防止SQL注入
        allowed_tables = list(TABLES.values())
        if table_name not in allowed_tables:
            raise ValueError(f"Invalid table name: {table_name}")

        for row in data:
            # 优化：使用join代替字符串拼接
            values = ','.join("'" + str(l) + "'" for l in row)
            sql = f'upsert into {table_name} values({values})'
            self.cur.save(sql)


if __name__ == "__main__":
    spider = GameSpider()
    # spider.get_team_ids('2019-2020', '36')
    # spider.get_game_record(19, 54)
    # spider.get_odds('1646984')
    # spider.get_odds('19', 0)
    spider.get_overunder('19', 0)

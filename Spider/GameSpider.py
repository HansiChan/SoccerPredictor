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

# Add the project root directory to the path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from DAO.ImpalaCon import ImpalaCon
from config import SPIDER, TABLES
from sqlalchemy import create_engine


class GameSpider(object):
    """
    Spider program, responsible for crawling training data from web pages
    """
    def __init__(self):
        self.options = Options()
        if SPIDER['HEADLESS']:
            self.options.add_argument('--headless')  # Hide browser while crawling
        self.driver = webdriver.Chrome(options=self.options)
        self.driver.implicitly_wait(SPIDER['IMPLICIT_WAIT'])
        self.driver.set_page_load_timeout(SPIDER['PAGE_LOAD_TIMEOUT'])
        self.verificationErrors = []
        self.accept_next_alert = True

        # Logging component
        formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s")
        handler1 = logging.StreamHandler()
        handler1.setFormatter(formatter)
        self.logger = logging.getLogger("logger")
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(handler1)

        self.cur = ImpalaCon()

    def get_team_ids(self, season, league):
        """
        Get the list of teams in the season and the corresponding team ID
        :param season: Season (2019-2020)
        :param league: League ID (36)
        """
        main_url = f'http://zq.win007.com/cn/TeamHeadPage/{season}/{league}.html'
        self.driver.get(main_url)
        teams = self.driver.find_elements(By.XPATH, "//td[@background='/Images/photo_s_2.gif' and @class='name01']")
        data = []
        for team in teams:
            team_id = team.find_element(By.XPATH, ".//a").get_attribute('href').split('/')[-1].split('.')[0]
            team_name = team.find_element(By.XPATH, ".//a").text
            data.append([team_id, team_name])
        self.driver.close()
        team_list = pd.DataFrame(data, columns=['TeamID', 'TeamName'])
        self.logger.info(team_list)
        self.to_kudu('tmp.team_list', data)


    def get_game_record(self, teamid, page_num):
        """
        Get team historical game data
        :param teamid: Team ID
        :param page_num: The range of pages to crawl (maximum 54)
        """
        url = f'http://info.win0168.com/cn/team/TeamSche/{teamid}.html'
        data = []
        ulist = []
        self.driver.get(url)
        for i in range(1, page_num + 1):
            self.logger.info(f'Crawling page {i}:' + url)
            try:
                gamelist = self.driver.find_element(By.XPATH, '//div[@id="div_Table2" and @class="data"]')
                matches = gamelist.find_elements(By.XPATH, ".//tr")
                # Crawl game data and save it as a DataFrame
                for x in matches:
                    matche_id = ''
                    url_list = []
                    # Optimization: split the string only once
                    text_parts = str(x.text).split(' ')
                    if len(text_parts) == 17:
                        league, date, times, host_team, full_score, guest_team, half_score, aisa, total_overunder, flat = text_parts[:10]
                        # Crawl web page url
                        links = x.find_elements(By.XPATH, './/*[@href]')
                        if links:
                            for y in links:
                                if 'analysis' in y.get_attribute('href'):
                                    match = re.match('.*analysis/(\d+)', y.get_attribute('href'))
                                    if match:
                                        matche_id = match.group(1)
                                        url_list.insert(0, matche_id)
                                url_list.append(y.get_attribute('href'))
                        data.append(
                            [matche_id, league, date, times, host_team, full_score, guest_team, half_score, aisa,
                             total_overunder, flat])
                        ulist.append(url_list)
                # Simulate input and jump to the specified page
                input_xpath = '//input[@type="text" and @name="pageNo" and @id="pageNo"]'
                self.driver.find_element(By.XPATH, input_xpath).send_keys(i + 1)
                onclick_xpath = f'//input[@onclick="SearchTeamSche({teamid})"]'
                self.driver.find_element(By.XPATH, onclick_xpath).click()
                # Wait for loading
                time.sleep(1)
                self.logger.info('Link successful')
            except Exception as e:
                self.logger.error(f'Link failed: {e}')
        self.driver.close()
        ddf = pd.DataFrame(data, columns=['ID', 'League', 'Date', 'Time', 'HostTeam', 'FullScore', 'GuestTeam', 'HalfScore', 'Asia', 'OverUnder', 'WinOrLose'])
        udf = pd.DataFrame(ulist, columns=['ID', 'HostHomepage', 'GameRecord', 'GuestHomepage', 'GameAnalysis', 'AsiaOdds', 'OverUnderOdds', 'EuroOdds'])
        self.to_kudu('tmp.game_record', data)
        self.to_kudu('tmp.game_record_url', ulist)

    def get_odds(self, game_id, hg):
        """
        Get home/away odds data
        :param game_id: Team ID
        :param hg: 0: home, 1: away
        """
        def get_oddList(arg):
            selector = Select(self.driver.find_element(By.ID, "sel_showType"))
            selector.select_by_visible_text(arg)
            odds_list = []
            odd_table = self.driver.find_element(By.XPATH, '//table[@id="oddsList_tab"]')
            odds = odd_table.find_elements(By.XPATH, './/tr[.//td//a[@title="Mainstream Companies"]]')
            for i in odds:
                a = i.find_element(By.XPATH, './/a').text
                span = re.findall(r'\d+\.\d+', i.text)[0:3]
                span.insert(0, a)
                odds_list.append(span)
            return odds_list

        game_list = self.cur.get_game_list(game_id, hg)
        self.logger.info(f'Total {len(game_list)} records')
        # Optimization: use enumerate to avoid O(n²) complexity
        for index, gid in enumerate(game_list):
            url = f'http://vip.win0168.com/1x2/oddslist/{gid}.htm'
            self.logger.info(f'Crawling record {index + 1} data game_id:{gid}')
            self.driver.get(url)
            # Initial odds
            orign_odd = get_oddList("Initial")
            # Final odds
            final_odd = get_oddList("Live")
            full_odd = []
            for f, o in zip(final_odd, orign_odd):
                f.insert(0, gid)
                full_odd.append(f + o[1:])
            self.to_kudu('tmp.game_odds', full_odd)
        self.driver.close()

    def get_overunder(self, game_id, hg):
        """
        Get over/under odds data
        :param game_id: Team ID
        :param hg: 0: home, 1: away
        """
        game_list = self.cur.get_game_list(game_id, hg)
        self.logger.info(f'Total {len(game_list)} records')
        # Optimization: use enumerate to avoid O(n²) complexity
        for index, gid in enumerate(game_list):
            url = f'http://vip.win0168.com/OverDown_n.aspx?id={gid}&l=0'
            self.logger.info(f'Crawling record {index + 1} data game_id:{gid}')
            self.driver.get(url)
            odd_table = self.driver.find_element(By.XPATH, '//table[@id="odds"]')
            odds = odd_table.find_elements(By.XPATH, './/tr[.//td[@height="25"]]')
            odds_list = []
            for i in odds[:-2]:
                if len(i.text) > 0:
                    if '\n' in i.text:
                        comp, *odd_parts = i.text.split('\n')[1].split(' ')
                    else:
                        comp, *odd_parts = i.text.split(' ')
                    odd = odd_parts[:6]
                    odd.insert(0, comp)
                    odd.insert(0, gid)
                    if len(odd) == 8:
                        odds_list.append(odd)
            self.to_kudu('tmp.game_overunder', odds_list)
        self.driver.close()

    def to_kudu(self, table_name, data):
        """
        Save to kudu
        :param table_name: table name (e.g. tmp.game_record)
        :param data: data List
        """
        # Verify that the table name is in the allowed list to prevent SQL injection
        allowed_tables = list(TABLES.values())
        if table_name not in allowed_tables:
            raise ValueError(f"Invalid table name: {table_name}")

        for row in data:
            # Optimization: use join instead of string concatenation
            values = ','.join(f"'{l}'" for l in row)
            sql = f'upsert into {table_name} values({values})'
            self.cur.save(sql)


if __name__ == "__main__":
    spider = GameSpider()
    # spider.get_team_ids('2019-2020', '36')
    # spider.get_game_record(19, 54)
    # spider.get_odds('1646984')
    # spider.get_odds('19', 0)
    spider.get_overunder('19', 0)

from __future__ import print_function, division

import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from sqlalchemy import create_engine
import pandas as pd
import re
import logging.handlers


class Spider(object):

    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.221 Safari/537.36 SE 2.X MetaSr 1.0'}

    def __init__(self):
        ## setup
        # self.base_url = base_url

        self.options = Options()
        self.options.add_argument('--headless') # 爬取时隐藏浏览器
        self.driver = webdriver.Chrome(options = self.options)
        # self.driver = webdriver.Chrome()
        self.driver.implicitly_wait(30)
        self.verificationErrors = []
        self.accept_next_alert = True
        self.dbcon = create_engine('mysql+mysqlconnector://root:123456@localhost:3306/test?charset=utf8')

        # 日志组件
        self.formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s")
        self.handler1 = logging.StreamHandler()
        self.handler1.setFormatter(self.formatter)
        self.logger = logging.getLogger("logger")
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(self.handler1)


    # 获取赛季联赛球队名单及球队对应ID
    def get_team_ids(self,season,league):
        main_url = 'http://zq.win007.com/cn/TeamHeadPage/%s/%s.html' % (season,league)
        self.driver.get(main_url)
        teams = self.driver.find_elements_by_xpath("//td[@background='/Images/photo_s_2.gif' and @class='name01']")
        data = []
        for team in teams:
            team_id = int(team.find_element_by_xpath(".//a").get_attribute('href').split('/')[-1].split('.')[0])
            team_name = team.find_element_by_xpath(".//a").text
            data.append([team_id, team_name])
        team_list = pd.DataFrame(data, columns=['team_id', 'team_name'])
        self.driver.close()
        print(team_list )
        team_list.to_sql('team_list', con=self.dbcon, if_exists='replace', index=False)

    # 获取球队历史比赛数据
    def get_game_record(self,teamid, page_num):
        url = 'http://info.win0168.com/cn/team/TeamSche/%s.html' % teamid
        data = []
        ulist = []
        id_list = []
        self.driver.get(url)
        for i in range(1, page_num+1):
            self.logger.info('正在抓取第' + str(i) + '页:' + url)
            try:
                gamelist = self.driver.find_element_by_xpath('//div[@id="div_Table2" and @class="data"]')
                matches = gamelist.find_elements_by_xpath(".//tr")
                # 抓取比赛数据，并保存成DataFrame
                for x in matches:
                    matche_id=''
                    url_list = []
                    if  len(str(x.text).split(' ')) in (15,16,17):
                        league = str(x.text).split(' ')[0]
                        date = str(x.text).split(' ')[1]
                        times = str(x.text).split(' ')[2]
                        host_team = str(x.text).split(' ')[3]
                        full_score = str(x.text).split(' ')[4]
                        guest_team = str(x.text).split(' ')[5]
                        half_score = str(x.text).split(' ')[6]
                        # 处理05年之前的数据
                        if len(str(x.text).split(' ')) == 15:
                            plate = ''
                            totle_overunder = ''
                            flat = str(x.text).split(' ')[7]
                        # 处理13年之前的数据
                        elif len(str(x.text).split(' ')) == 16:
                            plate = ''
                            totle_overunder = str(x.text).split(' ')[7]
                            flat = str(x.text).split(' ')[8]
                        else:
                            plate = str(x.text).split(' ')[7]
                            totle_overunder = str(x.text).split(' ')[8]
                            flat = str(x.text).split(' ')[9]
                        # 爬取网页url
                        if x.find_elements_by_xpath('.//*[@href]'):
                            for y in x.find_elements_by_xpath('.//*[@href]') :
                                if 'analysis' in y.get_attribute('href'):
                                    matche_id = re.match('.*analysis/(\d+)',y.get_attribute('href')).group(1)
                                    url_list.insert(0,matche_id)
                                url_list.append(y.get_attribute('href'))
                        data.append([matche_id,league,date,times,host_team,full_score,guest_team,half_score,plate,totle_overunder,flat])
                        ulist.append(url_list)
                # 模拟翻页
                # onclick='//a[@onclick="GetTeamSche(19,%s)"]' % (i+1)
                # 模拟输入并跳转到指定页
                input = '//input[@type="text" and @name="pageNo" and @id="pageNo"]'
                self.driver.find_element_by_xpath(input).send_keys(i+1)
                onclick='//input[@onclick="SearchTeamSche(19)"]'
                self.driver.find_element_by_xpath(onclick).click()
                # 等待加载
                time.sleep(1)
                self.logger.info('链接成功')
            except Exception as e :
                self.logger.info('链接失败:'+str(e))
        ddf = pd.DataFrame(data, columns=['ID','赛事', '日期', '时间', '主队', '全场比分', '客队', '半场比分', '亚盘', '大小盘', '胜平负'])
        udf = pd.DataFrame(ulist, columns=['ID', '主队主页', '比赛记录', '客队主页', '比赛分析', '亚盘指数', '大小盘指数', '欧赔指数'])
        ddf['ID'] = ddf['ID'].astype('int32')
        udf['ID'] = udf['ID'].astype('int32')
        ddf.to_sql('game_record',schema='test', con=self.dbcon2, if_exists='replace', index=False)
        udf.to_sql('game_record_url', con=self.dbcon2, if_exists='replace', index=False)
        self.driver.close()

    def x(self):
        return ''
if __name__ == "__main__":
    spider = Spider()
    # spider.get_team_ids('2019-2020','36')
    spider.get_game_record(19,3)
import os
import re
import time
import sys
import json
import logging
from glob import glob
from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from korean_romanizer.romanizer import Romanizer
from selenium.webdriver.common.action_chains import ActionChains

COLLECT_PATH = "./foodsafetykorea_crawl"


class Crawler:

    def __init__(self):
        self.driver = webdriver.Chrome()
        self.df_list = []
        self.do_print = True
        self.short_sleep = 3
        self.long_sleep = 20
        self.fail_sleep = 5

        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s [%(levelname)s] %(message)s',
                            handlers=[logging.StreamHandler()])
        self.logger = logging.getLogger(__name__)

    def _print(self, msg):
        if self.do_print:
            self.logger.info(msg)

    def retrieve_data(self, prod_container):
        res = []
        for row in prod_container.findAll("tr")[1:]:
            if row.text == "조회된 데이터가 없습니다.":
                return None

            row_parsed = {}
            for item in row.findAll("td"):
                span_list = item.findAll("span")
                row_parsed[span_list[0].text] = span_list[1].text

            item_page = row.find("a")["id"]
            row_parsed["item_page"] = item_page

            res.append(row_parsed)

        df = pd.DataFrame.from_dict(res)

        return df

    def move_to_item(self, item_page):
        """상품 페이지로 이동"""

        ## 마우스 호버
        #         self.driver.move_to_element(by=By.XPATH, value='//*[@id="tbody"]/tr[1]/td[6]/span[2]')
        target_elem = self.driver.find_element(by=By.XPATH, value='//*[@id="tbody"]/tr[1]/td[6]/span[2]')
        actions = ActionChains(self.driver).move_to_element(target_elem)
        actions.perform()

        elem = self.driver.find_element(by=By.XPATH, value=f'//*[@id="{item_page}"]')
        elem.click()
        time.sleep(self.short_sleep)

    def crawl_prod_info(self):
        """상품 페이지 크롤"""

        html = self.driver.page_source
        soup = BeautifulSoup(html, 'html.parser')

        ## Company info
        company_info = soup.findAll("table", attrs={"class": "mb-table"})[0]

        company_info_container = company_info.find("tr").findAll("td")[0].find("a")

        pattern = r"\((.*?)\)"
        company_no, company_stat = re.findall(pattern, company_info_container["onclick"].strip())[0].split()

        #         company_no = int(company_no.strip("',"))
        company_stat = company_stat.strip("'")
        company_address = company_info.find("tr").findAll("td")[1].text

        company_info_dict = {
            #             "회사번호": company_no,
            "회사상태": company_stat,
            "회사주소": company_address
        }

        ## Product info

        assert len(soup.findAll("table", attrs={"class": "mb-table"})) == 2

        prod_info_container = soup.findAll("table", attrs={"class": "mb-table"})[1]

        prod_info_dict = {}
        for row in prod_info_container.findAll("tr"):
            key_list = []
            value_list = []

            ## 여기에는 key 값들이
            for item in row.findAll("th"):
                key_list.append(item.text.strip())

            ## 여기에는 value 값들이
            for item in row.findAll("td"):
                value_list.append(item.text.strip())

            info_dict_temp = {k: v for k, v in zip(key_list, value_list)}

            assert len(set(prod_info_dict.keys()) & set(info_dict_temp.keys())) == 0

            prod_info_dict.update(info_dict_temp)

        ## 인허가, 수거, 성분
        container_list = soup.findAll("table", attrs={"class": "col table-sm"})
        assert len(container_list) in (2, 3)

        ## Authorize info
        authorize_info_container = container_list[0]

        authorize_info_table = authorize_info_container.findAll("tr")

        columns = [i.text for i in authorize_info_table[0].findAll("th")]

        row_value_list = []
        for row in authorize_info_table[1:]:
            row_values = [i.text.strip().replace("\t", "").replace("\n", "") for i in row.findAll("td")]
            row_value_list.append(row_values)

        authorize_info_df = pd.DataFrame(row_value_list, columns=columns)

        ## 수거 내역
        collection_info_container = container_list[1]  ## 수거

        collection_info_table = collection_info_container.findAll("tr")

        columns = [i.text for i in collection_info_table[0].findAll("th")]

        if len(collection_info_table) > 1:
            row_value_list = []
            for row in collection_info_table[1:]:
                row_values = [i.text.strip().replace("\t", "").replace("\n", "") for i in row.findAll("td")]
                row_value_list.append(row_values)

            collection_info_df = pd.DataFrame(row_value_list, columns=columns)

        else:
            row_value_list = None

        collection_info_df = pd.DataFrame(row_value_list, columns=columns)

        ## dict 병합
        assert len(set(company_info_dict.keys()) & set(prod_info_dict.keys())) == 0
        prod_info_agg = company_info_dict
        prod_info_agg.update(prod_info_dict)

        ## id 마킹
        authorize_info_df = authorize_info_df.assign(품목보고번호=prod_info_agg["품목보고번호"])
        collection_info_df = collection_info_df.assign(품목보고번호=prod_info_agg["품목보고번호"])

        now = datetime.now().timestamp()
        company_info_dict["last_updated_at"] = now
        authorize_info_df = authorize_info_df.assign(last_updated_at=now)
        collection_info_df = collection_info_df.assign(last_updated_at=now)

        authorize_info_df.to_parquet(f"{COLLECT_PATH}/authorize/{self.query_roman}_{prod_info_agg['품목보고번호']}.parquet")
        collection_info_df.to_parquet(f"{COLLECT_PATH}/collection/{self.query_roman}_{prod_info_agg['품목보고번호']}.parquet")

        ## 성분 및 원료
        if len(container_list) == 3:
            ingredient_container = container_list[2]  ## 성분

            rows = ingredient_container.findAll("tr")
            columns = [i.text for i in rows[0].findAll("th")]

            row_value_list = []
            for row in rows[1:]:
                row_values = [i.text.strip().replace("\t", "").replace("\n", "") for i in row.findAll("td")]
                row_value_list.append(row_values)

            ingredient_info_df = pd.DataFrame(row_value_list, columns=columns)

            ingredient_info_df = ingredient_info_df.assign(품목보고번호=prod_info_agg["품목보고번호"])
            ingredient_info_df = ingredient_info_df.assign(last_updated_at=now)
            ingredient_info_df.to_parquet(
                f"{COLLECT_PATH}/ingredient/{self.query_roman}_{prod_info_agg['품목보고번호']}.parquet")

        return prod_info_agg

    def parse_table_to_df(self, info_container):

        ## Haccp 예외처리
        if info_container is None:
            return None

        info_table = info_container.findAll("tr")

        columns = [i.text for i in info_table[0].findAll("th")]

        row_value_list = []
        for row in info_table[1:]:
            row_values = [i.text.strip().replace("\t", "").replace("\n", "") for i in row.findAll("td")]
            row_value_list.append(row_values)

        df = pd.DataFrame(row_value_list, columns=columns)
        return df

    def parse_comp_info(self, refer_item_no):
        """회사 정보 크롤"""
        self._print(f"{refer_item_no} 상품 회사 정보 크롤 시작")

        html = self.driver.page_source
        soup = BeautifulSoup(html, 'html.parser')

        company_info_container = soup.findAll("table", attrs={"class": "mb-table table-sm"})
        assert len(company_info_container) == 1
        company_info_container = company_info_container[0]

        company_info_dict = {}
        for row in company_info_container.findAll("tr"):
            key_list = []
            value_list = []

            ## 여기에는 key 값들이
            for item in row.findAll("th"):
                key_list.append(item.text.strip())

            ## 여기에는 value 값들이
            for item in row.findAll("td"):
                value_list.append(item.text.strip())

            info_dict_temp = {k: v for k, v in zip(key_list, value_list)}

            assert len(set(company_info_dict.keys()) & set(info_dict_temp.keys())) == 0

            company_info_dict.update(info_dict_temp)

        table_containers = soup.findAll("div", attrs={"class": "responsive-table"})

        if len(table_containers) == 3:
            haccp_info_container = None
            authorize_info_container = table_containers[0]
            enforce_info_container = table_containers[1]
            prod_list_info_container = table_containers[2]

        elif len(table_containers) == 4:
            haccp_info_container = table_containers[0]
            authorize_info_container = table_containers[1]
            enforce_info_container = table_containers[2]
            prod_list_info_container = table_containers[3]

        haccp_authorize_info_df = self.parse_table_to_df(haccp_info_container)
        comp_authorize_info_df = self.parse_table_to_df(authorize_info_container)
        enforce_df = self.parse_table_to_df(enforce_info_container)
        prod_list_df = self.parse_table_to_df(prod_list_info_container)

        comp_authorize_info_df = comp_authorize_info_df.assign(
            회사인허가번호=company_info_dict["인허가번호"],
            품목보고번호_refer=refer_item_no
        )
        enforce_df = enforce_df.assign(
            회사인허가번호=company_info_dict["인허가번호"],
            품목보고번호_refer=refer_item_no
        )
        prod_list_df = prod_list_df.assign(
            회사인허가번호=company_info_dict["인허가번호"],
            품목보고번호_refer=refer_item_no
        )

        ## 저장
        now = datetime.now().timestamp()
        company_info_dict["last_updated_at"] = now
        comp_authorize_info_df = comp_authorize_info_df.assign(last_updated_at=now)
        enforce_df = enforce_df.assign(last_updated_at=now)
        prod_list_df = prod_list_df.assign(last_updated_at=now)

        company_info_dict["refer_item_no"] = refer_item_no
        file_path = f"{COLLECT_PATH}/company/{self.query_roman}_{refer_item_no}_{company_info_dict['인허가번호']}.json"
        with open(file_path, 'w') as f:
            json.dump(company_info_dict, f)

        comp_authorize_info_df.to_parquet(
            f"{COLLECT_PATH}/authorize_company/{self.query_roman}_{refer_item_no}_{company_info_dict['인허가번호']}.parquet")
        enforce_df.to_parquet(
            f"{COLLECT_PATH}/enforce_company/{self.query_roman}_{refer_item_no}_{company_info_dict['인허가번호']}.parquet")
        prod_list_df.to_parquet(
            f"{COLLECT_PATH}/product_company/{self.query_roman}_{refer_item_no}_{company_info_dict['인허가번호']}.parquet")

        return company_info_dict

    def move_to_company(self):
        ## 이동
        elem = self.driver.find_element(
            By.XPATH,
            '/html/body/div[4]/div/div/div/div[2]/div[2]/table[1]/tbody/tr/td[1]/a'
        )
        elem.click()
        time.sleep(self.short_sleep)

    def move_back(self):
        try:
            ## 이전 페이지로
            elem = self.driver.find_element(by=By.XPATH, value='//*[@id="close"]')
            elem.click()
        except:
            pass

    def crawl_data_from_page(self):
        html = self.driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        prod_container = soup.find("table", attrs={"id": "tbl_prd_list"})

        ## 바깥 페이지 전체 데이터 캐싱
        df = self.retrieve_data(prod_container)

        for idx, row in df.iterrows():
            item_page = row.item_page
            item_no = row["품목보고번호"]

            page_success = False
            for _ in range(20):
                try:
                    file_path = f"{COLLECT_PATH}/product/{self.query}_{item_no}.json"

                    ## 이미 완료되었으면 패스
                    if os.path.exists(file_path):
                        #                         with open(file_path, 'w') as f:
                        #                             prod_info_agg_before = json.load(f)
                        page_success = True
                        break

                    self.move_to_item(item_page)
                    prod_info_agg = self.crawl_prod_info()
                    self.move_to_company()
                    company_info_dict = self.parse_comp_info(prod_info_agg['품목보고번호'])
                    prod_info_agg['회사인허가번호'] = company_info_dict["인허가번호"]
                    ## 여기까지 하면 끝!

                    prod_info_agg["last_updated_at"] = datetime.now().timestamp()
                    with open(file_path, 'w') as f:
                        json.dump(prod_info_agg, f)

                    ## 성공하고 초기화
                    self.move_back()
                    page_success = True
                    break

                except Exception as e:
                    ## 실패시 초기화
                    self.move_back()

                    # 페이지 이동 직후 연속적으로 많이 실패하는 경우가 존재
                    if _ < 5:
                        time.sleep(self.fail_sleep)
                    else:
                        time.sleep(self.long_sleep)
                    self._print(f"페이지 크롤 실패 {str(e)[:100]}")
                    continue

            ## 끝난 뒤 초기화. 안전빵이라서 안돼도 ㄱㅊ
            self.move_back()

            ## 10번 다 실패한 경우
            if page_success is False:
                raise Exception(f"{item_page} 페이지 상품 크롤 10번 실패")

            ## 이번 iteration 끝
            self._print(f"{item_page} 상품 회사 정보 크롤 완료")
        self.df_list.append(df)

        return df

    def init_page(self):
        ## 식품 안전나라 들어가기
        url = 'https://www.foodsafetykorea.go.kr/portal/specialinfo/searchInfoProduct.do?menu_grp=MENU_NEW04&menu_no=2815#page2'
        self.driver.get(url)

        ## 잠시 기다리기
        time.sleep(self.short_sleep)

    def input_query(self, query):

        self.query = query
        r = Romanizer(query)
        query_roman = r.romanize().replace(" ", "_")
        self.query_roman = query_roman

        ## 과자 입력
        elem = self.driver.find_element(by="id", value="prd_cd_nm")
        elem.send_keys(query)

        ## 검색 클릭
        elem = self.driver.find_element(by="id", value="srchBtn")
        elem.click()

        ## 20초 기다리기
        time.sleep(self.long_sleep)

        ## 현재 페이지 마킹
        self.cur_page = 1

    def expand_50_items(self):
        for _ in range(5):
            try:
                ## 50개로 확대
                elem = self.driver.find_element(By.XPATH, '//*[@id="a_list_cnt"]')
                elem.click()
                time.sleep(1)
                elem = self.driver.find_element(By.XPATH,
                                                '//*[@id="contents"]/main/section/div[2]/div[2]/div[2]/div[5]/ul/li[5]/a')
                elem.click()

                ## 20초 기다리기
                time.sleep(self.long_sleep)
                break
            except Exception as e:
                time.sleep(self.fail_sleep)
                self._print("50개로 확장")
                continue
            raise Exception("50개 아이템 확장 실패")

    def pagination(self):
        for _ in range(3):
            try:

                ## 아이템 개수가 201 ~ 250개이고, 4페이지로 가면 그냥 다음 페이지 눌러야 함
                if (self.total_count > 200) & (self.total_count <= 250) & (self.cur_page == 4):
                    xpath = '//*[@id="contents"]/main/section/div[2]/div[3]/div/ul/li[7]/a'
                ## 아이템 개수가 201 ~ 250개이고, 5페이지로 가면 그냥 다음 페이지 눌러야 함
                elif (self.total_count > 250) & (self.total_count <= 300) & (self.cur_page == 5):
                    xpath = '//*[@id="contents"]/main/section/div[2]/div[3]/div/ul/li[7]/a'
                elif self.cur_page < 4:
                    xpath = f'//*[@id="contents"]/main/section/div[2]/div[3]/div/ul/li[{self.cur_page + 2}]/a'
                else:
                    xpath = f'//*[@id="contents"]/main/section/div[2]/div[3]/div/ul/li[5]/a'

                ## 다음 페이지로
                pagination = self.driver.find_element(By.XPATH, xpath)
                pagination.click()
                self.cur_page += 1
                self._print(f"페이지 이동: {self.cur_page}")

                time.sleep(self.long_sleep)
                return
            except Exception as e:
                time.sleep(5)
                self._print(f"페이지 이동 실패: {str(e)[:100]}")

    def to_parquet_df_list(self, query):

        df = pd.concat(self.df_list)
        df.to_parquet(f"{COLLECT_PATH}/{self.query_roman.replace(' ', '_')}.parquet")
        self._print("저장 완료")

    def is_last_page(self):
        ## cur_page가 5이하이고, 현재 페이지에서 다음 페이지가 스팬이면
        if self.cur_page <= 5:
            try:
                xpath = f'//*[@id="contents"]/main/section/div[2]/div[3]/div/ul/li[{self.cur_page + 2}]/span'
                elem = self.driver.find_element(By.XPATH, xpath)
                return True
            except:
                return False
        ## cur_page가 5초과
        else:
            try:
                xpath = f'//*[@id="contents"]/main/section/div[2]/div[3]/div/ul/li[6]/span'
                elem = self.driver.find_element(By.XPATH, xpath)
                return True
            except:
                return False

    def execute(self, query):

        ## 초기 세팅
        self.init_page()
        self.input_query(query)
        self._print(f"{query} 작업 시작")
        self.expand_50_items()
        self.cur_page = 1

        ## 첫ㅔ이지 크롤링
        for _ in range(5):
            try:
                ## 처음 한 번 크롤해서 견적 뽑고
                df_temp = self.crawl_data_from_page()
                self.total_count = df_temp["번호"].astype(int).max()
                break
            except Exception as e:
                time.sleep(self.fail_sleep)
                self._print(f"첫 페이지 크롤 실패: {str(e)[:100]}")
                continue
            self._print(f"첫 페이지 크롤 실패 3회로 종료")
            raise Exception

        is_crawl_end = self.is_last_page()
        self.cur_page_crawled = True
        while is_crawl_end is False:
            ## 지금 페이지 다 됐으면 넘기기
            if self.cur_page_crawled is True:
                self.pagination()
                self.cur_page_crawled = False

            ## 네트워크가 느릴 때를 위한 버퍼
            for i in range(5):
                try:
                    df_temp = self.crawl_data_from_page()
                    self._print(f"{self.cur_page} 페이지 완료")
                    self.cur_page_crawled = True

                    ## 끝났으면 종료
                    if self.is_last_page():
                        is_crawl_end = True
                        self._print("크롤 완료")
                except Exception as e:
                    time.sleep(self.fail_sleep)
                    self._print(f"{self.cur_page} 페이지 크롤 실패: {str(e)[:100]}")

                # 이번 루프 완료
                break

        self.to_parquet_df_list(query)
        self._print(f"{query} 작업 완료")

if __name__ == "__main__":
    crawler = Crawler()
    query = sys.argv[1]
    crawler.execute(query)
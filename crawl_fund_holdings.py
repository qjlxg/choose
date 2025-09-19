# -*- coding: utf-8 -*-
"""
一个用于爬取天天基金网全市场基金持仓数据的Python脚本
"""
import os
import time
import requests
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
# 新增的导入，用于显式等待
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

# 配置Selenium
def setup_driver():
    """配置并返回一个无头模式的Chrome浏览器驱动。"""
    chrome_options = Options()
    chrome_options.add_argument('--headless')  # 无头模式，不在窗口中显示
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    
    # 修正后的代码：直接指定 chromedriver 的完整路径
    # 在 GitHub Actions 的 Ubuntu 环境中，chromedriver 通常位于此路径
    service = Service('/usr/lib/chromium-browser/chromedriver')

    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

# 爬取全市场基金代码列表并筛选C类基金
def get_all_fund_codes():
    """从天天基金网获取所有基金的代码列表，并筛选出C类基金。"""
    print("正在爬取全市场基金代码列表...")
    url = "http://fund.eastmoney.com/allfund.html"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        html = response.text
        soup = BeautifulSoup(html, 'lxml')
        
        all_fund_list = []
        # 文章中提到的 XPath 转换为 BeautifulSoup 的选择器
        # 这里使用 'div.ui-filter-item' 定位基金列表
        for div in soup.select('#code_content > div > ul > li > div'):
            a_tag = div.find('a')
            if a_tag:
                code_name_text = a_tag.get_text(strip=True)
                # 提取代码和名称，例如：(000689)前海开源新经济A
                if code_name_text and len(code_name_text) > 8:
                    code = code_name_text[1:7]
                    name = code_name_text[8:]
                    all_fund_list.append({'code': code, 'name': name})
        
        print(f"已获取 {len(all_fund_list)} 只基金的代码。")

        # 筛选出名称以 "C" 结尾的基金，即场外C类
        c_fund_list = [fund for fund in all_fund_list if fund['name'].endswith('C')]
        
        print(f"已筛选出 {len(c_fund_list)} 只场外C类基金。")
        return c_fund_list

    except requests.exceptions.RequestException as e:
        print(f"爬取基金代码列表失败：{e}")
        return []

# 爬取指定基金持仓数据
def get_fund_holdings(driver, fund_code, years_to_crawl):
    """
    爬取指定基金在近N年内的持仓数据。
    :param driver: Selenium WebDriver实例
    :param fund_code: 基金代码
    :param years_to_crawl: 爬取的年份列表
    :return: 包含持仓数据的DataFrame
    """
    fund_holdings = []
    base_url = f"https://fundf10.eastmoney.com/ccmx_{fund_code}.html"
    
    try:
        driver.get(base_url)
        # 用显式等待代替固定的 time.sleep()
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "cctable"))
        )
        
        for year in years_to_crawl:
            print(f"正在爬取基金 {fund_code} 的 {year} 年持仓数据...")
            try:
                # 寻找年份选择按钮并点击
                year_button_xpath = f"//*[@id='pagebar']/div/label[@value='{year}']"
                # 使用显式等待，等待按钮可点击
                year_button = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, year_button_xpath))
                )
                year_button.click()
                
                # 等待表格内容更新
                WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.ID, "cctable"))
                )
                
                # 获取页面HTML内容
                page_source = driver.page_source
                soup = BeautifulSoup(page_source, 'lxml')
                
                # 查找持仓表格
                holdings_table = soup.find(id="cctable")
                if not holdings_table:
                    print(f"未找到基金 {fund_code} 在 {year} 年的持仓表格，可能无持股。")
                    continue
                
                rows = holdings_table.find_all('tr')
                if not rows or len(rows) <= 1:
                    print(f"基金 {fund_code} 在 {year} 年无持仓数据。")
                    continue
                
                # 解析表格数据
                for row in rows[1:]:  # 跳过表头
                    cols = row.find_all('td')
                    if len(cols) >= 5:
                        data = {
                            'fund_code': fund_code,
                            'year': year,
                            'stock_code': cols[1].text.strip(),
                            'stock_name': cols[2].text.strip(),
                            'proportion': cols[3].text.strip(),
                            'shares': cols[4].text.strip(),
                            'market_value': cols[5].text.strip(),
                        }
                        fund_holdings.append(data)
                        
            # 如果找不到按钮或超时，则捕获异常并跳过
            except TimeoutException:
                print(f"基金 {fund_code} 在 {year} 年的持仓按钮或表格不存在，跳过。")
                continue
            except Exception as e:
                print(f"爬取基金 {fund_code} 的 {year} 年数据时发生错误：{e}")
                continue
                
    except Exception as e:
        print(f"访问基金 {fund_code} 页面失败：{e}")

    return pd.DataFrame(fund_holdings)


def main():
    """主函数，执行爬取任务。"""
    # 定义需要爬取的年份范围，根据文章，可爬取最新及历史持仓
    current_year = time.localtime().tm_year
    years_to_crawl = [str(current_year), str(current_year - 1), str(current_year - 2)]
    
    # 修改此处，只获取 C 类基金的代码列表
    all_fund_data = get_all_fund_codes()
    if not all_fund_data:
        print("无法获取基金代码列表，程序退出。")
        return

    # 设置一个文件路径来存储结果
    output_dir = "fund_data"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    output_filename = os.path.join(output_dir, f"fund_holdings_C_{time.strftime('%Y%m%d')}.csv")
    
    driver = setup_driver()
    all_holdings_df = pd.DataFrame()
    
    for fund in all_fund_data:
        fund_code = fund['code']
        holdings_df = get_fund_holdings(driver, fund_code, years_to_crawl)
        if not holdings_df.empty:
            all_holdings_df = pd.concat([all_holdings_df, holdings_df], ignore_index=True)
            
    driver.quit()
    
    if not all_holdings_df.empty:
        all_holdings_df.to_csv(output_filename, index=False, encoding='utf-8-sig')
        print(f"数据爬取完成，已保存到文件：{output_filename}")
    else:
        print("没有爬取到任何数据。")

if __name__ == '__main__':
    main()

# -*- coding: utf-8 -*-
"""
这是一个基于文章思路的基金持仓爬虫脚本。
它使用 Selenium 模拟浏览器操作，并结合 requests 和 BeautifulSoup4 来爬取
天天基金网的全市场基金持仓数据。
"""
import requests
from lxml import etree
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
import time
import re

def get_all_fund_codes():
    """
    爬取天天基金网，获取全市场所有基金的代码和名称。
    网址: http://fund.eastmoney.com/allfund.html
    Returns:
        list: 包含所有基金代码的列表，例如 ['000689', '001298', ...]
    """
    print("正在爬取全市场基金代码...")
    url = 'http://fund.eastmoney.com/allfund.html'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    response = requests.get(url, headers=headers)
    response.encoding = 'gbk'  # 天天基金网使用GBK编码
    html = etree.HTML(response.text)

    # 使用XPath定位所有基金代码和名称
    fund_list = html.xpath('//*[@id="code_content"]/div/ul/li/div/a[1]/text()')

    fund_codes = []
    for item in fund_list:
        # 使用正则表达式提取6位代码
        match = re.search(r'\((\d{6})\)', item)
        if match:
            fund_codes.append(match.group(1))

    print(f"成功获取 {len(fund_codes)} 个基金代码。")
    return fund_codes

def get_fund_holdings(driver, fund_code, num_years):
    """
    使用 Selenium 和 requests 爬取指定基金的持仓数据。
    Args:
        driver (webdriver.Chrome): Selenium WebDriver实例。
        fund_code (str): 基金的6位代码。
        num_years (int): 需要爬取的年数。
    Returns:
        pd.DataFrame: 基金持仓数据的DataFrame。
    """
    print(f"开始爬取基金 {fund_code} 的持仓数据...")
    url = f"https://fundf10.eastmoney.com/ccmx_{fund_code}.html"
    driver.get(url)

    # 获取基金名称
    try:
        fund_name = driver.find_element(By.CSS_SELECTOR, '#bodydiv > div.fundDetail-main > div.fundInfo > div > div.fund_name > a').text
    except Exception:
        fund_name = "未知基金"

    all_holdings = pd.DataFrame()

    # 循环点击年份按钮
    for i in range(num_years):
        try:
            # 获取所有年份的按钮元素
            year_buttons = driver.find_elements(By.CSS_SELECTOR, '#pagebar > div > label')
            if i >= len(year_buttons):
                break
            
            # 点击对应的年份按钮
            year_buttons[i].click()
            
            # 显式等待，确保页面内容加载完毕
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "cctable"))
            )
            
            # 获取网页源代码，并使用BeautifulSoup进行解析
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # 找到持仓表格
            table = soup.find(id='cctable')
            
            if not table:
                print(f"基金 {fund_code} 在当前年份没有股票持仓数据。")
                continue

            # 使用pandas的read_html直接解析表格
            tables = pd.read_html(str(table), encoding='utf-8')
            if tables:
                df = tables[0]
                # 获取报告期，并添加为新列
                report_date = soup.find('div', class_='ccmx-tbtm').text.strip().replace('（报告期）', '')
                df['报告期'] = report_date
                df['基金代码'] = fund_code
                df['基金名称'] = fund_name
                all_holdings = pd.concat([all_holdings, df], ignore_index=True)
                print(f"成功爬取基金 {fund_code} 的 {report_date} 持仓数据。")
                time.sleep(1) # 增加延迟，防止请求过快
        
        except Exception as e:
            print(f"爬取基金 {fund_code} 失败: {e}")
            continue

    return all_holdings

if __name__ == '__main__':
    # 请确保您已安装并配置好Chrome浏览器和对应的ChromeDriver。
    # 您可能需要指定ChromeDriver的路径，例如: executable_path='path/to/chromedriver'
    print("请确认您的Chrome浏览器和ChromeDriver已正确安装并匹配版本。")
    print("如果出现'WebDriverException'错误，通常是因为路径或版本问题。")

    options = webdriver.ChromeOptions()
    options.add_argument('--headless')  # 启用无头模式，不在界面上显示浏览器窗口
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    
    driver = webdriver.Chrome(options=options)
    
    # 示例用法：
    # 1. 获取全市场基金代码
    fund_codes = get_all_fund_codes()
    
    # 2. 爬取前5只基金近2年的持仓数据作为演示
    all_data = pd.DataFrame()
    for code in fund_codes[:5]:
        holdings = get_fund_holdings(driver, code, num_years=2)
        if not holdings.empty:
            all_data = pd.concat([all_data, holdings], ignore_index=True)
            
    driver.quit()
    
    print("\n爬取完成！")
    print("---"*20)
    print("爬取到的数据示例:")
    print(all_data.head())
    
    # 将数据保存到CSV文件
    # if not all_data.empty:
    #     all_data.to_csv("fund_holdings.csv", index=False, encoding='utf-8-sig')
    #     print("\n数据已保存到 fund_holdings.csv 文件中。")

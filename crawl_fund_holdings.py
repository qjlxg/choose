import requests
from lxml import etree
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import time
import os

# 可选：伪装 User-Agent（如果 requests 报错，可注释）
# from fake_useragent import UserAgent

def get_all_fund_codes():
    """爬取全市场基金代码列表（从 http://fund.eastmoney.com/allfund.html）"""
    url = "http://fund.eastmoney.com/allfund.html"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    fund_links = soup.select('#code_content > div > ul > li > div > a:nth-child(1)')
    fund_codes = []
    for link in fund_links:
        text = link.get_text()
        if '（' in text and '）' in text:
            code = text.split('（')[1].split('）')[0]
            fund_codes.append(code)
    return fund_codes[:100]  # 限制前100只，避免超时；生产环境可移除

def crawl_fund_holdings(fund_code, years_back=1):
    """爬取指定基金近 N 年持仓"""
    driver = webdriver.Chrome()  # 需下载 ChromeDriver
    wait = WebDriverWait(driver, 10)
    
    base_url = f"https://fundf10.eastmoney.com/ccmx_{fund_code}.html"
    driver.get(base_url)
    time.sleep(2)  # 等待加载
    
    # 解析基金基本信息（使用 CSS 选择器）
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    fund_info = {}
    try:
        fund_name = soup.select_one('#bodydiv > div > ...')  # 根据文章，需调整为实际选择器，如 '#fundInfo > h1'
        fund_info['name'] = fund_name.text.strip() if fund_name else 'Unknown'
        # 类似解析成立日、类型、经理等
    except:
        fund_info['name'] = 'Unknown'
    
    holdings = []
    current_year = 2025  # 当前年份，根据日期调整
    for year in range(current_year - years_back + 1, current_year + 1):
        try:
            # 点击年份按钮（XPath 如文章所述）
            year_button = wait.until(EC.element_to_be_clickable((By.XPATH, f"//*[@id='pagebar']/div/label[@value='{year}']")))
            year_button.click()
            time.sleep(3)  # 等待数据加载
            
            # 解析持仓表格（CSS 选择器）
            table = soup.select_one('#cctable > div > div')
            if table:
                rows = table.find_all('tr')[1:]  # 跳过表头
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) >= 4:
                        holding = {
                            'year': year,
                            'stock_code': cols[0].text.strip(),
                            'stock_name': cols[1].text.strip(),
                            'ratio': float(cols[2].text.strip().replace('%', '')) if cols[2].text.strip() != '-' else 0,
                            'shares': cols[3].text.strip()
                        }
                        holdings.append(holding)
        except Exception as e:
            print(f"年份 {year} 爬取失败: {e}")
            continue
    
    driver.quit()
    
    if holdings:
        df = pd.DataFrame(holdings)
        output_file = f"data/{fund_code}_holdings_{years_back}y.csv"
        os.makedirs('data', exist_ok=True)
        df.to_csv(output_file, index=False)
        print(f"持仓数据保存至 {output_file}")
        return df
    else:
        print(f"基金 {fund_code} 无股票持仓或爬取失败")
        return pd.DataFrame()

if __name__ == "__main__":
    # 示例：爬取单只基金
    df_single = crawl_fund_holdings('000689', years_back=1)
    
    # 示例：爬取全市场前10只基金
    all_codes = get_all_fund_codes()
    for code in all_codes[:10]:
        crawl_fund_holdings(code, years_back=1)
        time.sleep(5)  # 防反爬延时

# -*- coding: utf-8 -*-
"""
一个用于爬取天天基金网全市场基金持仓数据的Python脚本
"""
import os
import time
import requests
import pandas as pd
import re  # 新增：用于正则表达式匹配基金代码
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

# 爬取全市场基金代码列表（改进版：添加正则匹配和筛选C类基金）
def get_all_fund_codes():
    """从天天基金网获取所有基金的代码列表，并筛选出C类基金。"""
    print("正在爬取全市场基金代码列表...")
    url = "http://fund.eastmoney.com/allfund.html"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        html = response.text
        soup = BeautifulSoup(html, 'html.parser')  # 改进：使用html.parser更稳定
        
        fund_list = []
        # 保留原有选择器，并添加正则匹配作为备选
        for div in soup.select('#code_content > div > ul > li > div'):
            a_tag = div.find('a')
            if a_tag:
                code_name_text = a_tag.get_text(strip=True)
                # 原有提取方式
                if code_name_text and len(code_name_text) > 8:
                    code = code_name_text[1:7]
                    name = code_name_text[8:]
                    fund_list.append({'code': code, 'name': name})
                # 新增：使用正则表达式作为备选匹配
                else:
                    match = re.match(r'\((\d{6})\)(.+)', code_name_text)
                    if match:
                        code, name = match.groups()
                        fund_list.append({'code': code, 'name': name.strip()})
        
        print(f"已获取 {len(fund_list)} 只基金的代码。")
        
        # --- 新增的筛选逻辑 ---
        # 筛选出名称以 "C" 结尾的基金，即场外C类
        c_fund_list = [fund for fund in fund_list if fund['name'].endswith('C')]
        print(f"已筛选出 {len(c_fund_list)} 只场外C类基金。")
        return c_fund_list
        # --- 筛选逻辑结束 ---

    except requests.exceptions.RequestException as e:
        print(f"爬取基金代码列表失败：{e}")
        return []

# 新增：专门解析持仓表格的函数
def parse_holdings_table(soup, fund_code, year):
    """专门解析持仓表格的函数"""
    holdings_table = soup.find(id="cctable")
    if not holdings_table:
        return []
    
    holdings = []
    rows = holdings_table.find_all('tr')
    if not rows or len(rows) <= 1:
        return []
    
    # 解析表格数据（保留原有字段，并添加更多字段以增强）
    for row in rows[1:]:  # 跳过表头
        cols = row.find_all('td')
        if len(cols) >= 5:
            try:
                data = {
                    'fund_code': fund_code,
                    'year': year,
                    'stock_code': cols[1].text.strip() if len(cols) > 1 else '',
                    'stock_name': cols[2].text.strip() if len(cols) > 2 else '',
                    'proportion': cols[3].text.strip() if len(cols) > 3 else '',
                    'shares': cols[4].text.strip() if len(cols) > 4 else '',
                    'market_value': cols[5].text.strip() if len(cols) > 5 else '',
                    # 新增：尝试解析更多字段，如报告日期等
                    'report_date': cols[0].text.strip() if len(cols) > 0 else ''
                }
                holdings.append(data)
            except Exception as e:
                print(f"解析行数据失败: {e}")
                continue
    return holdings

# 爬取指定基金持仓数据（改进版：添加重试机制、多个选择器、分离解析函数）
def get_fund_holdings(driver, fund_code, years_to_crawl, max_retries=3):
    """
    爬取指定基金在近N年内的持仓数据。
    :param driver: Selenium WebDriver实例
    :param fund_code: 基金代码
    :param years_to_crawl: 爬取的年份列表
    :param max_retries: 最大重试次数
    :return: 包含持仓数据的DataFrame
    """
    fund_holdings = []
    base_url = f"https://fundf10.eastmoney.com/ccmx_{fund_code}.html"
    
    # 改进：添加页面加载重试
    for attempt in range(max_retries):
        try:
            print(f"尝试访问基金 {fund_code} (第{attempt+1}次)...")
            driver.get(base_url)
            # 用显式等待代替固定的 time.sleep()，并检查是否有数据
            WebDriverWait(driver, 15).until(
                EC.any_of(
                    EC.presence_of_element_located((By.ID, "cctable")),
                    EC.presence_of_element_located((By.CLASS_NAME, "placeholder"))
                )
            )
            
            # 检查是否有持仓数据
            page_source_check = driver.page_source
            if "暂无数据" in page_source_check or "没有找到" in page_source_check:
                print(f"基金 {fund_code} 暂无持仓数据")
                return pd.DataFrame()
            
            break  # 成功则跳出重试循环
            
        except TimeoutException:
            if attempt == max_retries - 1:
                print(f"基金 {fund_code} 页面加载失败，已重试{max_retries}次")
                return pd.DataFrame()
            time.sleep(2 ** attempt)  # 指数退避
        except Exception as e:
            print(f"访问基金 {fund_code} 页面失败：{e}")
            if attempt == max_retries - 1:
                return pd.DataFrame()
    
    # 原有年份循环逻辑，融入改进
    for year in years_to_crawl:
        print(f"正在爬取基金 {fund_code} 的 {year} 年持仓数据...")
        try:
            # 寻找年份选择按钮并点击（改进：多个XPath选择器）
            year_selectors = [
                f"//*[@id='pagebar']/div/label[@value='{year}']",
                f"//label[@value='{year}']",
                f"//input[@value='{year}']",
                f"//option[@value='{year}']"
            ]
            
            year_button = None
            for selector in year_selectors:
                try:
                    # 使用显式等待，等待按钮可点击
                    year_button = WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.XPATH, selector))
                    )
                    break
                except TimeoutException:
                    continue
            
            if not year_button:
                print(f"未找到基金 {fund_code} 在 {year} 年的持仓按钮，跳过。")
                continue
            
            # 改进：滚动到元素并点击
            driver.execute_script("arguments[0].scrollIntoView();", year_button)
            time.sleep(0.5)
            year_button.click()
            
            # 等待表格内容更新
            WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.ID, "cctable"))
            )
            
            # 获取页面HTML内容
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'lxml')
            
            # 使用新解析函数
            holdings = parse_holdings_table(soup, fund_code, year)
            fund_holdings.extend(holdings)
                    
        # 如果找不到按钮或超时，则捕获异常并跳过
        except TimeoutException:
            print(f"基金 {fund_code} 在 {year} 年的持仓按钮或表格不存在，跳过。")
            continue
        except Exception as e:
            print(f"爬取基金 {fund_code} 的 {year} 年数据时发生错误：{e}")
            continue
            
    return pd.DataFrame(fund_holdings)


def main():
    """主函数，执行爬取任务（改进版：添加配置、限制、统计和日志）。"""
    # 定义需要爬取的年份范围，根据文章，可爬取最新及历史持仓
    current_year = time.localtime().tm_year
    years_to_crawl = [str(current_year), str(current_year - 1), str(current_year - 2)]
    
    # 改进：配置参数
    max_funds = 50  # 新增：限制最大基金数量
    request_delay = 1  # 新增：请求延时
    
    print("=== 天天基金持仓数据爬取器 ===")
    print(f"目标年份: {', '.join(years_to_crawl)}")
    print(f"最大基金数量: {max_funds}")
    
    # 获取 C 类基金的代码列表
    all_fund_data = get_all_fund_codes()
    if not all_fund_data:
        print("无法获取基金代码列表，程序退出。")
        return

    # 限制爬取的基金数量，只处理列表中的前 max_funds 个
    if len(all_fund_data) > max_funds:
        all_fund_data = all_fund_data[:max_funds]
        print(f"注意：基金数量已限制为 {max_funds} 只。")
    
    print(f"📊 准备爬取 {len(all_fund_data)} 只基金")
    
    # 设置一个文件路径来存储结果
    output_dir = "fund_data"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    timestamp = time.strftime('%Y%m%d_%H%M%S')
    # 修改输出文件名，以区分 C 类基金数据
    output_filename = os.path.join(output_dir, f"fund_holdings_C_{timestamp}.csv")
    
    driver = setup_driver()
    all_holdings_df = pd.DataFrame()
    successful_funds = 0
    
    try:
        for i, fund in enumerate(all_fund_data, 1):
            fund_code = fund['code']
            fund_name = fund['name']
            
            print(f"\n[{i}/{len(all_fund_data)}] 正在处理: {fund_name} ({fund_code})")
            
            holdings_df = get_fund_holdings(driver, fund_code, years_to_crawl)
            if not holdings_df.empty:
                all_holdings_df = pd.concat([all_holdings_df, holdings_df], ignore_index=True)
                successful_funds += 1
                print(f"✅ 成功获取 {len(holdings_df)} 条持仓记录")
            else:
                print("❌ 未获取到数据")
            
            # 改进：适当延时，避免请求过快
            time.sleep(request_delay)
            
    finally:
        driver.quit()
    
    if not all_holdings_df.empty:
        all_holdings_df.to_csv(output_filename, index=False, encoding='utf-8-sig')
        print(f"\n🎉 数据爬取完成!")
        print(f"📁 已保存到文件：{output_filename}")
        print(f"📈 总记录数: {len(all_holdings_df)}")
        print(f"✅ 成功基金: {successful_funds}/{len(all_fund_data)}")
        
        # 新增：显示统计信息
        print("\n=== 数据概览 ===")
        if 'year' in all_holdings_df.columns:
            print(all_holdings_df.groupby('year').size())
    else:
        print("没有爬取到任何数据。")

if __name__ == '__main__':
    main()

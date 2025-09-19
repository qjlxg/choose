# -*- coding: utf-8 -*-
"""
一个用于爬取天天基金网全市场基金持仓数据的Python脚本
该版本增加了从本地Markdown文件解析指定基金代码的功能
"""
import os
import time
import requests
import pandas as pd
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from bs4 import BeautifulSoup
import logging

# --- 配置日志系统 ---
# 配置日志输出到控制台，并设置级别为 INFO
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 新增：解析Markdown文件，提取基金代码 ---
def parse_markdown_file(file_path):
    """
    解析Markdown文件，提取“弱买入”或“强买入”的基金代码。
    """
    if not os.path.exists(file_path):
        logging.error(f"❌ 错误：文件未找到 -> {file_path}")
        return []

    fund_codes = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # 使用正则表达式匹配表格行
        lines = content.strip().split('\n')
        for line in lines:
            # 匹配包含 '| 行动信号 |' 的表头，跳过
            if '| 行动信号' in line:
                continue
            
            # 使用更宽泛的匹配来处理表格内容
            parts = [p.strip() for p in line.split('|')]
            if len(parts) >= 9:
                fund_code = parts[1]
                action_signal = parts[8].lower() # 转换为小写，方便匹配

                if "弱买入" in action_signal or "强买入" in action_signal:
                    fund_codes.append({'code': fund_code, 'name': 'N/A'}) # 名称设为N/A，因为报告中未提供
        
        logging.info(f"✅ 从报告中成功提取了 {len(fund_codes)} 个目标基金代码。")
        return fund_codes

    except Exception as e:
        logging.error(f"❌ 解析Markdown文件时发生错误：{e}")
        return []

# --- 配置Selenium ---
def setup_driver():
    """配置并返回一个无头模式的Chrome浏览器驱动。"""
    logging.info("--- 正在启动 ChromeDriver ---")
    try:
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        
        chromedriver_path = os.getenv('CHROMEDRIVER_PATH', '/usr/lib/chromium-browser/chromedriver')
        service = Service(chromedriver_path)
        
        driver = webdriver.Chrome(service=service, options=chrome_options)
        logging.info("🎉 ChromeDriver 启动成功！")
        return driver
    except WebDriverException as e:
        logging.error(f"❌ ChromeDriver 启动失败：{e}")
        logging.error("请检查 ChromeDriver 路径、版本是否与 Chrome 浏览器匹配，以及系统依赖是否安装。")
        return None

# --- 爬取全市场基金代码列表（保留原功能，但新版本不会调用） ---
def get_all_fund_codes():
    """从天天基金网获取所有基金的代码列表，并筛选出C类基金。"""
    logging.info("正在爬取全市场基金代码列表...")
    url = "http://fund.eastmoney.com/allfund.html"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        html = response.text
        soup = BeautifulSoup(html, 'lxml')
        
        fund_list = []
        for a_tag in soup.select('#code_content a'):
            code_name_text = a_tag.get_text(strip=True)
            match = re.match(r'\((\d{6})\)(.+)', code_name_text)
            if match:
                code, name = match.groups()
                fund_list.append({'code': code, 'name': name.strip()})
        
        logging.info(f"已获取 {len(fund_list)} 只基金的代码。")
        
        c_fund_list = [fund for fund in fund_list if fund['name'].endswith('C')]
        logging.info(f"已筛选出 {len(c_fund_list)} 只场外C类基金。")
        return c_fund_list

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ 爬取基金代码列表失败：{e}")
        return []

# --- 专门解析持仓表格的函数 ---
def parse_holdings_table(soup, fund_code, year):
    """专门解析持仓表格的函数"""
    holdings_table = soup.find(id="cctable")
    if not holdings_table:
        return []
    
    holdings = []
    rows = holdings_table.find_all('tr')
    if not rows or len(rows) <= 1:
        return []
    
    for row in rows[1:]:
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
                    'report_date': cols[0].text.strip() if len(cols) > 0 else ''
                }
                holdings.append(data)
            except Exception as e:
                logging.warning(f"解析行数据失败: {e}")
                continue
    return holdings

# --- 爬取指定基金持仓数据 ---
def get_fund_holdings(driver, fund_code, years_to_crawl, max_retries=3):
    """
    爬取指定基金在近N年内的持仓数据。
    """
    if driver is None:
        logging.error("WebDriver 实例不存在，跳过爬取。")
        return pd.DataFrame()

    fund_holdings = []
    base_url = f"https://fundf10.eastmoney.com/ccmx_{fund_code}.html"

    logging.info(f"访问基金 {fund_code} 页面: {base_url}")
    
    for attempt in range(max_retries):
        try:
            logging.info(f"尝试访问页面 (第{attempt+1}次)...")
            driver.get(base_url)
            
            wait = WebDriverWait(driver, 30)
            wait.until(
                EC.any_of(
                    EC.presence_of_element_located((By.ID, "cctable")),
                    EC.presence_of_element_located((By.CLASS_NAME, "placeholder"))
                )
            )
            
            page_source_check = driver.page_source
            if "暂无数据" in page_source_check or "没有找到" in page_source_check:
                logging.info(f"基金 {fund_code} 暂无持仓数据")
                return pd.DataFrame()
            
            logging.info("页面加载成功，准备解析数据。")
            break
        except TimeoutException:
            logging.warning(f"页面加载超时，基金 {fund_code} (第{attempt+1}/{max_retries}次重试)")
            if attempt == max_retries - 1:
                logging.error(f"基金 {fund_code} 页面加载失败，已重试{max_retries}次，跳过。")
                return pd.DataFrame()
            time.sleep(2 ** attempt)
        except Exception as e:
            logging.error(f"访问基金 {fund_code} 页面时发生意外错误：{e}")
            if attempt == max_retries - 1:
                return pd.DataFrame()
            time.sleep(2 ** attempt)

    for year in years_to_crawl:
        try:
            logging.info(f"正在爬取 {year} 年持仓数据...")
            
            year_selectors = [
                f"//label[@value='{year}']",
                f"//div[@id='pagebar']//label[@value='{year}']",
            ]
            
            year_button = None
            for selector in year_selectors:
                try:
                    year_button = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.XPATH, selector))
                    )
                    break
                except TimeoutException:
                    continue
            
            if not year_button:
                logging.warning(f"未找到基金 {fund_code} 在 {year} 年的持仓按钮，跳过。")
                continue
            
            driver.execute_script("arguments[0].scrollIntoView();", year_button)
            time.sleep(1)
            year_button.click()
            
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "cctable"))
            )
            
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'lxml')
            
            holdings = parse_holdings_table(soup, fund_code, year)
            fund_holdings.extend(holdings)
            logging.info(f"✅ 成功获取 {len(holdings)} 条 {year} 年的持仓记录。")
            
        except TimeoutException:
            logging.warning(f"基金 {fund_code} 在 {year} 年的持仓按钮或表格加载超时，跳过。")
            continue
        except Exception as e:
            logging.error(f"爬取基金 {fund_code} 的 {year} 年数据时发生错误：{e}")
            continue
            
    return pd.DataFrame(fund_holdings)


def main():
    """主函数，执行爬取任务。"""
    # 定义需要爬取的年份范围
    current_year = time.localtime().tm_year
    years_to_crawl = [str(current_year), str(current_year - 1), str(current_year - 2)]
    
    # 改进：配置参数
    request_delay = 1  # 请求延时

    logging.info("=== 天天基金持仓数据爬取器 ===")
    logging.info(f"目标年份: {', '.join(years_to_crawl)}")
    
    # --- 核心修改：从Markdown文件获取基金代码列表 ---
    report_file = 'market_monitor_report.md'
    fund_list_to_crawl = parse_markdown_file(report_file)
    if not fund_list_to_crawl:
        logging.error(f"无法从文件 '{report_file}' 获取基金代码列表，程序退出。")
        return

    logging.info(f"📊 准备爬取 {len(fund_list_to_crawl)} 只指定基金")
    
    # 设置一个文件路径来存储结果
    output_dir = "fund_data"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    timestamp = time.strftime('%Y%m%d_%H%M%S')
    output_filename = os.path.join(output_dir, f"target_fund_holdings_{timestamp}.csv")
    
    # 尝试启动 WebDriver，如果失败则直接退出
    driver = setup_driver()
    if driver is None:
        return
        
    all_holdings_df = pd.DataFrame()
    successful_funds = 0
    
    try:
        for i, fund in enumerate(fund_list_to_crawl, 1):
            fund_code = fund['code']
            fund_name = fund['name']
            
            logging.info(f"\n--- [{i}/{len(fund_list_to_crawl)}] 正在处理: {fund_name} ({fund_code}) ---")
            
            holdings_df = get_fund_holdings(driver, fund_code, years_to_crawl)
            if not holdings_df.empty:
                all_holdings_df = pd.concat([all_holdings_df, holdings_df], ignore_index=True)
                successful_funds += 1
                logging.info(f"✅ 成功获取 {len(holdings_df)} 条持仓记录")
            else:
                logging.info("❌ 未获取到数据，继续下一只基金。")
            
            time.sleep(request_delay)
            
    finally:
        logging.info("爬取任务结束，关闭 WebDriver。")
        driver.quit()
    
    if not all_holdings_df.empty:
        logging.info("\n🎉 数据爬取完成!")
        logging.info(f"📁 已保存到文件：{output_filename}")
        logging.info(f"📈 总记录数: {len(all_holdings_df)}")
        logging.info(f"✅ 成功基金: {successful_funds}/{len(fund_list_to_crawl)}")
        
        try:
            all_holdings_df.to_csv(output_filename, index=False, encoding='utf-8-sig')
        except Exception as e:
            logging.error(f"保存文件时发生错误：{e}")
            
    else:
        logging.info("没有爬取到任何数据。")

if __name__ == '__main__':
    main()

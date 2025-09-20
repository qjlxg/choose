# -*- coding: utf-8 -*-
"""
一个用于爬取天天基金网全市场基金持仓数据的Python脚本
该版本增加了从本地Markdown文件解析指定基金代码的功能
并增加了爬取股票所属行业和主题信息的功能
优化了针对当前页面结构的解析逻辑
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
from selenium.common.exceptions import TimeoutException, WebDriverException, StaleElementReferenceException
from bs4 import BeautifulSoup
import logging
import random
from typing import List, Dict, Optional

# --- 配置日志系统 ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 新增：解析Markdown文件，提取基金代码 ---
def parse_markdown_file(file_path: str) -> List[Dict[str, str]]:
    """
    解析Markdown文件，提取"弱买入"或"强买入"的基金代码。
    """
    if not os.path.exists(file_path):
        logging.error(f"❌ 错误：文件未找到 -> {file_path}")
        return []
    
    fund_codes = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        lines = content.strip().split('\n')
        for line in lines:
            if '| 行动信号' in line:
                continue
            
            parts = [p.strip() for p in line.split('|')]
            if len(parts) >= 9:
                fund_code = parts[1]
                action_signal = parts[8].lower()

                if "弱买入" in action_signal or "强买入" in action_signal:
                    fund_codes.append({'code': fund_code, 'name': 'N/A'})
        
        logging.info(f"✅ 从报告中成功提取了 {len(fund_codes)} 个目标基金代码。")
        return fund_codes

    except Exception as e:
        logging.error(f"❌ 解析Markdown文件时发生错误：{e}")
        return []

# --- 新增：股票信息缓存 ---
stock_info_cache = {}

# --- 新增：User-Agent池 ---
user_agent_pool = [
    "Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36"
]

# --- 优化：获取股票行业和主题信息 ---
def get_stock_info(stock_code: str) -> Dict[str, str]:
    """
    根据股票代码爬取东方财富网，获取所属行业和概念主题。
    """
    if not stock_code or stock_code == '':
        return {'所属行业': '未知', '概念主题': '未知'}
    
    if stock_code in stock_info_cache:
        return stock_info_cache[stock_code]
    
    info = {'所属行业': '未知', '概念主题': '未知'}
    
    # 验证股票代码格式 (6位数字)
    if not re.match(r'^\d{6}$', stock_code):
        stock_info_cache[stock_code] = info
        return info
    
    url = f"https://wap.eastmoney.com/quote/stock/{stock_code}.html"
    headers = {
        "User-Agent": random.choice(user_agent_pool),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')

        # 优化：多种方式尝试获取所属行业
        industry_patterns = [
            ('div', re.compile(r'所属行业')),
            ('span', re.compile(r'所属行业')),
            ('label', re.compile(r'所属行业')),
        ]
        
        for tag, pattern in industry_patterns:
            industry_div = soup.find(tag, string=pattern)
            if industry_div:
                # 尝试多种方式获取行业名称
                next_elem = industry_div.find_next_sibling()
                if next_elem:
                    info['所属行业'] = next_elem.get_text().strip()
                elif industry_div.find_next():
                    info['所属行业'] = industry_div.find_next().get_text().strip()
                break
        
        # 优化：获取概念主题
        theme_patterns = [
            ('div', re.compile(r'概念')),
            ('span', re.compile(r'概念')),
            ('li', re.compile(r'概念')),
        ]
        
        for tag, pattern in theme_patterns:
            theme_div = soup.find(tag, string=pattern)
            if theme_div:
                # 查找主题链接
                theme_links = theme_div.find_all_next('a', limit=10)
                themes = []
                for link in theme_links:
                    if link.get_text().strip() and len(link.get_text().strip()) < 20:
                        themes.append(link.get_text().strip())
                if themes:
                    info['概念主题'] = ', '.join(themes[:5])  # 取前5个概念
                break

        stock_info_cache[stock_code] = info
        logging.debug(f"✅ 成功获取股票 {stock_code} 信息: {info}")

    except requests.exceptions.RequestException as e:
        logging.warning(f"❌ 爬取股票 {stock_code} 信息失败: {e}")
    except Exception as e:
        logging.warning(f"❌ 解析股票 {stock_code} 页面失败: {e}")
    
    # 动态延时策略
    time.sleep(random.uniform(0.8, 2.0))
    
    return info

# --- 配置Selenium ---
def setup_driver() -> Optional[webdriver.Chrome]:
    """配置并返回一个无头模式的Chrome浏览器驱动。"""
    logging.info("--- 正在启动 ChromeDriver ---")
    try:
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--disable-plugins')
        chrome_options.add_argument('--disable-images')  # 禁用图片加载加速
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        
        chromedriver_path = os.getenv('CHROMEDRIVER_PATH', '/usr/lib/chromium-browser/chromedriver')
        service = Service(chromedriver_path)
        
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # 设置隐式等待
        driver.implicitly_wait(10)
        
        logging.info("🎉 ChromeDriver 启动成功！")
        return driver
    except WebDriverException as e:
        logging.error(f"❌ ChromeDriver 启动失败：{e}")
        logging.error("请检查 ChromeDriver 路径、版本是否与 Chrome 浏览器匹配，以及系统依赖是否安装。")
        return None

# --- 爬取全市场基金代码列表（保留原功能，但新版本不会调用） ---
def get_all_fund_codes() -> List[Dict[str, str]]:
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

# --- 优化：专门解析持仓表格的函数 ---
def parse_holdings_table(soup: BeautifulSoup, fund_code: str, year: str) -> List[Dict]:
    """专门解析持仓表格的函数，优化了针对当前页面结构的解析逻辑"""
    holdings_table = soup.find(id="cctable")
    if not holdings_table:
        logging.warning(f"未找到持仓表格 #cctable")
        return []
    
    # 检查是否还在加载状态
    loading_div = holdings_table.find('div', style=re.compile(r'text-align:\s*center'))
    if loading_div:
        logging.warning(f"持仓表格仍在加载中，跳过 {fund_code} {year} 年数据")
        return []
    
    # 查找表格结构
    table_patterns = [
        holdings_table.find('table'),  # 标准表格
        holdings_table.find('div', class_=re.compile(r'table|grid')),  # div表格
        holdings_table.find_all('tr'),  # 直接查找行
    ]
    
    holdings = []
    table_rows = None
    
    # 尝试多种方式获取表格行
    for pattern in table_patterns:
        if pattern:
            if hasattr(pattern, 'find_all') and callable(pattern.find_all):
                table_rows = pattern.find_all('tr')
            elif isinstance(pattern, list):
                table_rows = pattern
            break
    
    if not table_rows or len(table_rows) <= 1:
        logging.warning(f"未找到有效的表格行数据，fund_code: {fund_code}, year: {year}")
        return []
    
    logging.debug(f"找到 {len(table_rows)} 行表格数据")
    
    for i, row in enumerate(table_rows[1:], 1):  # 跳过表头
        try:
            # 尝试多种方式解析列
            cols = row.find_all(['td', 'div', 'span'])
            if len(cols) < 5:
                continue
                
            # 提取股票代码（通常在第2列）
            stock_code = ''
            stock_name = ''
            
            # 查找股票代码（6位数字格式）
            for col in cols[1:3]:  # 通常在第2或第3列
                col_text = col.get_text().strip()
                code_match = re.search(r'(\d{6})', col_text)
                if code_match:
                    stock_code = code_match.group(1)
                    # 提取股票名称
                    name_match = re.search(r'([^\d\s]+)(?:\s*\d{6})?', col_text)
                    if name_match:
                        stock_name = name_match.group(1).strip()
                    break
            
            if not stock_code:
                continue
            
            # 获取股票行业和主题信息
            stock_info = get_stock_info(stock_code)
            
            # 提取持仓数据
            position_ratio = ''
            shares_held = ''
            market_value = ''
            report_date = ''
            
            # 尝试提取持仓占比（通常在第4列）
            for j, col in enumerate(cols[3:6]):
                col_text = col.get_text().strip()
                if '%' in col_text or re.match(r'\d+\.?\d*%', col_text):
                    position_ratio = col_text
                elif re.match(r'\d+,\d{3}', col_text) or '万' in col_text or '亿' in col_text:
                    if not shares_held:
                        shares_held = col_text
                    else:
                        market_value = col_text
                elif re.match(r'\d{4}-\d{2}-\d{2}', col_text):
                    report_date = col_text
            
            data = {
                '基金代码': fund_code,
                '年份': year,
                '股票代码': stock_code,
                '股票名称': stock_name or cols[2].get_text().strip() if len(cols) > 2 else '',
                '所属行业': stock_info['所属行业'],
                '概念主题': stock_info['概念主题'],
                '持仓占比': position_ratio,
                '持股数': shares_held,
                '市值': market_value,
                '报告日期': report_date or cols[0].get_text().strip() if len(cols) > 0 else ''
            }
            
            # 数据清洗
            data = {k: v.strip() if isinstance(v, str) and v else '' for k, v in data.items()}
            
            # 验证关键字段
            if stock_code and stock_name:
                holdings.append(data)
                
        except Exception as e:
            logging.warning(f"解析第 {i} 行数据失败: {e}")
            continue
    
    logging.info(f"解析完成: {len(holdings)} 条持仓记录")
    return holdings

# --- 优化：爬取指定基金持仓数据 ---
def get_fund_holdings(driver: webdriver.Chrome, fund_code: str, years_to_crawl: List[str], max_retries: int = 3) -> pd.DataFrame:
    """
    爬取指定基金在近N年内的持仓数据。
    优化了针对当前页面结构的等待和点击逻辑
    """
    if driver is None:
        logging.error("WebDriver 实例不存在，跳过爬取。")
        return pd.DataFrame()

    fund_holdings = []
    base_url = f"https://fundf10.eastmoney.com/ccmx_{fund_code}.html"

    logging.info(f"访问基金 {fund_code} 页面: {base_url}")
    
    # 页面加载重试机制
    for attempt in range(max_retries):
        try:
            logging.info(f"尝试访问页面 (第{attempt+1}次)...")
            driver.get(base_url)
            
            # 优化等待条件：等待页面主体加载完成
            wait = WebDriverWait(driver, 30)
            
            # 等待关键元素加载
            wait.until(
                EC.any_of(
                    EC.presence_of_element_located((By.ID, "cctable")),
                    EC.presence_of_element_located((By.ID, "pagebar")),
                    EC.presence_of_element_located((By.CLASS_NAME, "tit_h3"))
                )
            )
            
            # 额外等待JavaScript执行完成
            time.sleep(3)
            
            page_source_check = driver.page_source
            if "暂无数据" in page_source_check or "没有找到" in page_source_check:
                logging.info(f"基金 {fund_code} 暂无持仓数据")
                return pd.DataFrame()
            
            # 检查是否成功加载到正确的基金页面
            if f"ccmx_{fund_code}" not in page_source_check:
                logging.warning(f"页面加载可能异常，未找到基金 {fund_code} 的持仓标识")
                if attempt == max_retries - 1:
                    logging.error(f"基金 {fund_code} 页面加载失败，已重试{max_retries}次，跳过。")
                    return pd.DataFrame()
                time.sleep(2)
                continue
            
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

    # 年份数据爬取
    for year in years_to_crawl:
        try:
            logging.info(f"正在爬取 {year} 年持仓数据...")
            
            # 重试机制
            retries = 3
            success = False
            
            for retry in range(retries):
                try:
                    # 优化：多种XPath选择器
                    year_selectors = [
                        f"//label[@value='{year}']",
                        f"//input[@value='{year}']",
                        f"//select[@id='jjcc']//option[@value='{year}']",
                        f"//div[contains(@class, 'pagebar')]//label[@value='{year}']",
                        f"//div[contains(@class, 'pagebar')]//input[@value='{year}']",
                        f"//a[contains(text(), '{year}')]",
                    ]
                    
                    year_element = None
                    for selector in year_selectors:
                        try:
                            elements = driver.find_elements(By.XPATH, selector)
                            for element in elements:
                                if element.is_displayed() and element.is_enabled():
                                    year_element = element
                                    break
                            if year_element:
                                break
                        except:
                            continue
                    
                    if year_element:
                        # 滚动到元素位置
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", year_element)
                        time.sleep(1)
                        
                        # 尝试点击
                        driver.execute_script("arguments[0].click();", year_element)
                        time.sleep(2)
                        
                        # 验证点击成功
                        if "加载中" not in driver.page_source:
                            success = True
                            break
                    else:
                        logging.warning(f"未找到 {year} 年的选择器")
                        
                except StaleElementReferenceException:
                    logging.warning(f"检测到 StaleElementReferenceException，正在重新尝试... (第 {retry+1}/{retries} 次)")
                    time.sleep(2)
                except Exception as e:
                    logging.debug(f"点击 {year} 年按钮时发生错误: {e}")
                    time.sleep(1)
            
            if not success:
                logging.warning(f"无法选择 {year} 年数据，跳过")
                continue

            # 等待数据加载完成
            wait = WebDriverWait(driver, 15)
            wait.until_not(
                EC.presence_of_element_located((By.XPATH, "//img[@src*='loading2.gif']"))
            )
            
            # 解析页面
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'lxml')
            
            holdings = parse_holdings_table(soup, fund_code, year)
            fund_holdings.extend(holdings)
            logging.info(f"✅ 成功获取 {len(holdings)} 条 {year} 年的持仓记录。")
            
            # 随机延时
            time.sleep(random.uniform(1, 2))
            
        except TimeoutException:
            logging.warning(f"基金 {fund_code} 在 {year} 年的数据加载超时，跳过。")
            continue
        except Exception as e:
            logging.error(f"爬取基金 {fund_code} 的 {year} 年数据时发生错误：{e}")
            continue
            
    return pd.DataFrame(fund_holdings)


def main():
    """主函数，执行爬取任务。"""
    current_year = time.localtime().tm_year
    years_to_crawl = [str(current_year), str(current_year - 1), str(current_year - 2)]
    
    # 动态延时设置
    request_delay = random.uniform(1.5, 3.0)

    logging.info("=== 天天基金持仓数据爬取器 ===")
    logging.info(f"目标年份: {', '.join(years_to_crawl)}")
    logging.info(f"随机延时: {request_delay:.1f}秒")
    
    report_file = 'market_monitor_report.md'
    fund_list_to_crawl = parse_markdown_file(report_file)
    if not fund_list_to_crawl:
        logging.error(f"无法从文件 '{report_file}' 获取基金代码列表，程序退出。")
        return

    logging.info(f"📊 准备爬取 {len(fund_list_to_crawl)} 只指定基金")
    
    output_dir = "fund_data"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    timestamp = time.strftime('%Y%m%d_%H%M%S')
    output_filename = os.path.join(output_dir, f"target_fund_holdings_with_info_{timestamp}.csv")
    
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
            
            # 基金间延时
            time.sleep(request_delay)
            
    finally:
        logging.info("爬取任务结束，关闭 WebDriver。")
        if driver:
            driver.quit()
    
    # 数据保存和统计
    if not all_holdings_df.empty:
        logging.info("\n🎉 数据爬取完成!")
        logging.info(f"📁 已保存到文件：{output_filename}")
        logging.info(f"📈 总记录数: {len(all_holdings_df)}")
        logging.info(f"✅ 成功基金: {successful_funds}/{len(fund_list_to_crawl)}")
        
        # 数据质量统计
        unique_stocks = all_holdings_df['股票代码'].nunique()
        avg_holdings_per_fund = len(all_holdings_df) / len(fund_list_to_crawl)
        logging.info(f"📊 唯一股票数: {unique_stocks}")
        logging.info(f"📊 平均每基金持仓数: {avg_holdings_per_fund:.1f}")
        
        try:
            all_holdings_df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            logging.info(f"💾 CSV文件保存成功，大小: {os.path.getsize(output_filename) / 1024:.1f} KB")
        except Exception as e:
            logging.error(f"保存文件时发生错误：{e}")
            
    else:
        logging.info("❌ 没有爬取到任何数据。")
        # 创建空结果文件
        empty_df = pd.DataFrame(columns=['基金代码', '年份', '股票代码', '股票名称', '所属行业', 
                                       '概念主题', '持仓占比', '持股数', '市值', '报告日期'])
        empty_df.to_csv(output_filename, index=False, encoding='utf-8-sig')
        logging.info(f"📁 创建空结果文件: {output_filename}")


if __name__ == '__main__':
    main()

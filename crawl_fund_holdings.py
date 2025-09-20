# -*- coding: utf-8 -*-
"""
一个用于爬取天天基金网全市场基金持仓数据的Python脚本
终极修复版：针对GitHub Actions环境的深度优化
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
from typing import List, Dict, Optional, Tuple
import json

# --- 配置日志系统（详细版） ---
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('crawler.log', encoding='utf-8')
    ]
)

# --- 解析Markdown文件 ---
def parse_markdown_file(file_path: str) -> List[Dict[str, str]]:
    """解析Markdown文件，提取基金代码。"""
    if not os.path.exists(file_path):
        logging.error(f"❌ 文件未找到: {file_path}")
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
        
        logging.info(f"✅ 提取 {len(fund_codes)} 个基金代码")
        return fund_codes

    except Exception as e:
        logging.error(f"❌ Markdown解析错误: {e}")
        return []

# --- 股票信息缓存 ---
stock_info_cache = {}

# --- 增强User-Agent池 ---
user_agent_pool = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
]

# --- 修复：获取股票信息（降级策略） ---
def get_stock_info(stock_code: str) -> Dict[str, str]:
    """获取股票行业和概念信息，支持降级策略。"""
    if not stock_code or stock_code == '':
        return {'所属行业': '未知', '概念主题': '未知'}
    
    if stock_code in stock_info_cache:
        return stock_info_cache[stock_code]
    
    info = {'所属行业': '未知', '概念主题': '未知'}
    
    if not re.match(r'^\d{6}$', stock_code):
        stock_info_cache[stock_code] = info
        return info
    
    # 降级策略1：使用雪球API（更稳定）
    try:
        snowball_url = f"https://stock.xueqiu.com/v5/stock/quote.json?symbol={stock_code}&extend=detail"
        headers = {
            "User-Agent": random.choice(user_agent_pool),
            "Accept": "application/json",
            "Referer": "https://xueqiu.com/",
        }
        
        response = requests.get(snowball_url, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if 'data' in data and 'items' in data['data'] and data['data']['items']:
                item = data['data']['items'][0]
                if 'industry' in item:
                    info['所属行业'] = item['industry']
                if 'concept' in item:
                    info['概念主题'] = ', '.join(item['concept'][:5])
                stock_info_cache[stock_code] = info
                logging.debug(f"✅ 雪球API: {stock_code} -> {info['所属行业']}")
                return info
    except Exception as e:
        logging.debug(f"雪球API失败 {stock_code}: {e}")
    
    # 降级策略2：使用东方财富移动端（更稳定）
    try:
        if stock_code.startswith(('60', '68', '69')):
            market = 'sh'
        else:
            market = 'sz'
        
        em_url = f"https://push2.eastmoney.com/api/qt/stock/details/get?secid={market}{stock_code}"
        headers = {
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15",
            "Referer": "https://quote.eastmoney.com/",
        }
        
        response = requests.get(em_url, headers=headers, timeout=8)
        if response.status_code == 200:
            data = response.json()
            if 'data' in data:
                # 尝试从API响应中提取行业信息
                if 'industry' in data['data']:
                    info['所属行业'] = data['data']['industry']
                stock_info_cache[stock_code] = info
                logging.debug(f"✅ EM移动端: {stock_code} -> {info['所属行业']}")
                return info
    except Exception as e:
        logging.debug(f"EM移动端失败 {stock_code}: {e}")
    
    # 降级策略3：使用默认分类
    try:
        if stock_code.startswith('60'):
            info['所属行业'] = '沪主板-传统行业'
        elif stock_code.startswith('68'):
            info['所属行业'] = '科创板-科技成长'
        elif stock_code.startswith('30'):
            info['所属行业'] = '深创业板-成长型'
        elif stock_code.startswith('00'):
            info['所属行业'] = '深主板-成熟企业'
        elif stock_code.startswith('002'):
            info['所属行业'] = '深中小板-中小型企业'
        else:
            info['所属行业'] = '其他板块'
        
        stock_info_cache[stock_code] = info
        logging.debug(f"✅ 默认分类: {stock_code} -> {info['所属行业']}")
    except Exception as e:
        logging.debug(f"默认分类失败 {stock_code}: {e}")
    
    time.sleep(random.uniform(0.1, 0.3))  # 最小延时
    return info

# --- 配置Selenium（GitHub Actions优化） ---
def setup_driver() -> Optional[webdriver.Chrome]:
    """配置ChromeDriver，针对GitHub Actions环境优化。"""
    logging.info("🔧 配置ChromeDriver...")
    try:
        chrome_options = Options()
        
        # GitHub Actions特定配置
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--disable-plugins')
        chrome_options.add_argument('--disable-images')
        chrome_options.add_argument('--disable-javascript')  # 关键：禁用JS减少反爬
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        
        # 反检测配置
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # 性能优化
        chrome_options.add_argument('--disable-web-security')
        chrome_options.add_argument('--allow-running-insecure-content')
        chrome_options.add_argument('--disable-features=VizDisplayCompositor')
        
        # GitHub Actions路径
        chromedriver_path = '/usr/bin/chromedriver'
        service = Service(chromedriver_path)
        
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # 隐藏webdriver属性
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        driver.set_page_load_timeout(45)
        driver.implicitly_wait(8)
        
        logging.info("✅ ChromeDriver启动成功")
        return driver
        
    except WebDriverException as e:
        logging.error(f"❌ ChromeDriver启动失败: {e}")
        return None

# --- 增强：等待数据加载（多策略） ---
def wait_for_data_load(driver: webdriver.Chrome, fund_code: str, timeout: int = 45) -> bool:
    """多策略等待数据加载完成"""
    logging.info(f"⏳ 等待数据加载 (基金 {fund_code})...")
    
    start_time = time.time()
    max_wait = timeout
    
    while time.time() - start_time < max_wait:
        try:
            # 策略1：等待年份选择器填充
            select_elem = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.ID, "jjcc"))
            )
            
            # 检查是否有option
            options = driver.find_elements(By.CSS_SELECTOR, "#jjcc option")
            if len(options) > 1:
                logging.info(f"✅ 找到 {len(options)-1} 个年份选项")
                return True
            
            # 策略2：检查表格是否还在加载
            cctable = driver.find_element(By.ID, "cctable")
            if "数据加载中" not in cctable.text:
                # 检查是否有表格行
                rows = driver.find_elements(By.CSS_SELECTOR, "#cctable tr")
                if len(rows) > 1:
                    logging.info(f"✅ 表格加载完成，{len(rows)-1} 行数据")
                    return True
            
            # 策略3：检查pagebar是否加载
            pagebar = driver.find_element(By.ID, "pagebar")
            if pagebar.text.strip():
                logging.info("✅ 分页控件已加载")
                return True
                
        except TimeoutException:
            pass
        except Exception as e:
            logging.debug(f"等待检查异常: {e}")
        
        # 动态等待
        elapsed = time.time() - start_time
        remaining = max_wait - elapsed
        wait_time = min(3, remaining / 3)  # 自适应等待
        time.sleep(wait_time)
    
    logging.warning(f"⚠️  数据加载超时 ({timeout}s)")
    return False

# --- 修复：智能年份选择（增强版） ---
def select_year_intelligently(driver: webdriver.Chrome, target_year: str) -> Tuple[bool, str]:
    """智能选择年份，支持多种选择器"""
    try:
        logging.info(f"🎯 选择年份: {target_year}")
        
        # 等待选择器出现
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.ID, "jjcc"))
        )
        
        # 多种等待策略
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                # 策略1：直接查找option
                options = driver.find_elements(By.CSS_SELECTOR, "#jjcc option")
                available_years = []
                
                for option in options:
                    option_text = option.text.strip()
                    year_match = re.search(r'(\d{4})', option_text)
                    if year_match:
                        available_years.append(year_match.group(1))
                
                if available_years:
                    logging.info(f"📋 找到年份: {available_years}")
                    
                    # 智能匹配
                    target_int = int(target_year)
                    selected_year = None
                    
                    # 精确匹配
                    for year in [str(target_int), str(target_int-1), str(target_int-2)]:
                        if year in available_years:
                            selected_year = year
                            break
                    
                    # 最近年份
                    if not selected_year:
                        selected_year = max(available_years, key=lambda x: int(x))
                    
                    # 执行选择
                    select_elem = driver.find_element(By.ID, "jjcc")
                    option_index = available_years.index(selected_year)
                    
                    # JavaScript选择
                    driver.execute_script(f"""
                        var select = document.getElementById('jjcc');
                        select.selectedIndex = {option_index};
                        select.dispatchEvent(new Event('change', {{bubbles: true}}));
                    """)
                    
                    logging.info(f"✅ 选择成功: {selected_year}")
                    time.sleep(3)
                    return True, selected_year
                
                # 策略2：如果没有option，尝试点击其他控件
                logging.info(f"尝试策略 {attempt+1}: 查找其他年份控件")
                
                # 查找可能的年份按钮
                year_selectors = [
                    "//label[contains(text(), '202')]",
                    "//div[contains(@class, 'year')]//a[contains(text(), '202')]",
                    "//input[contains(@value, '202')]",
                    ".year-tab a",
                    "[data-year]",
                ]
                
                for selector in year_selectors:
                    try:
                        elements = driver.find_elements(By.XPATH, selector)
                        for element in elements:
                            if element.is_displayed() and element.is_enabled():
                                year_text = element.text.strip()
                                year_match = re.search(r'(\d{4})', year_text)
                                if year_match and str(year_match.group(1)) == target_year:
                                    driver.execute_script("arguments[0].click();", element)
                                    logging.info(f"✅ 点击年份按钮: {year_text}")
                                    time.sleep(4)
                                    return True, target_year
                    except:
                        continue
                
            except Exception as e:
                logging.debug(f"年份选择尝试 {attempt+1} 失败: {e}")
                time.sleep(2)
        
        logging.error("❌ 所有年份选择策略失败")
        return False, "选择失败"
        
    except Exception as e:
        logging.error(f"❌ 年份选择异常: {e}")
        return False, str(e)

# --- 增强：解析持仓表格（支持当前页面） ---
def parse_current_holdings(driver: webdriver.Chrome, fund_code: str, default_year: str = None) -> List[Dict]:
    """解析当前显示的持仓数据（备用策略）"""
    try:
        logging.info("📋 解析当前页面持仓数据...")
        
        # 等待表格加载
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.ID, "cctable"))
        )
        
        # 额外等待
        time.sleep(3)
        
        # 获取页面源码
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'lxml')
        
        # 查找表格
        cctable = soup.find(id="cctable")
        if not cctable:
            logging.warning("未找到cctable")
            return []
        
        # 检查是否还在加载
        if "数据加载中" in cctable.text:
            logging.warning("表格仍在加载")
            return []
        
        holdings = []
        rows = cctable.find_all('tr')
        
        if len(rows) <= 1:
            # 尝试其他结构
            table_divs = cctable.find_all('div', class_=re.compile(r'table|grid|row'))
            if table_divs:
                logging.info(f"使用div结构，找到 {len(table_divs)} 个元素")
                for div in table_divs[:30]:  # 限制数量
                    cols = div.find_all(['td', 'div', 'span'])
                    if len(cols) >= 4:
                        try:
                            # 提取股票代码
                            stock_code = ''
                            for col in cols[1:3]:
                                col_text = col.text.strip()
                                code_match = re.search(r'(\d{6})', col_text)
                                if code_match:
                                    stock_code = code_match.group(1)
                                    break
                            
                            if stock_code:
                                stock_name = cols[2].text.strip() if len(cols) > 2 else ''
                                stock_info = get_stock_info(stock_code)
                                
                                data = {
                                    '基金代码': fund_code,
                                    '年份': default_year or '当前年份',
                                    '股票代码': stock_code,
                                    '股票名称': stock_name,
                                    '所属行业': stock_info['所属行业'],
                                    '概念主题': stock_info['概念主题'],
                                    '持仓占比': cols[3].text.strip() if len(cols) > 3 else '',
                                    '持股数': cols[4].text.strip() if len(cols) > 4 else '',
                                    '市值': cols[5].text.strip() if len(cols) > 5 else '',
                                    '报告日期': cols[0].text.strip() if len(cols) > 0 else ''
                                }
                                holdings.append(data)
                        except Exception as e:
                            continue
            else:
                logging.warning("未找到表格数据")
                return []
        else:
            # 标准表格结构
            logging.info(f"使用table结构，找到 {len(rows)-1} 行数据")
            for row in rows[1:]:
                cols = row.find_all('td')
                if len(cols) >= 4:
                    try:
                        # 提取股票代码
                        stock_code = ''
                        for col in cols[1:3]:
                            col_text = col.text.strip()
                            code_match = re.search(r'(\d{6})', col_text)
                            if code_match:
                                stock_code = code_match.group(1)
                                break
                        
                        if stock_code:
                            stock_name = cols[2].text.strip() if len(cols) > 2 else ''
                            stock_info = get_stock_info(stock_code)
                            
                            data = {
                                '基金代码': fund_code,
                                '年份': default_year or '当前年份',
                                '股票代码': stock_code,
                                '股票名称': stock_name,
                                '所属行业': stock_info['所属行业'],
                                '概念主题': stock_info['概念主题'],
                                '持仓占比': cols[3].text.strip() if len(cols) > 3 else '',
                                '持股数': cols[4].text.strip() if len(cols) > 4 else '',
                                '市值': cols[5].text.strip() if len(cols) > 5 else '',
                                '报告日期': cols[0].text.strip() if len(cols) > 0 else ''
                            }
                            holdings.append(data)
                    except Exception as e:
                        logging.debug(f"行解析失败: {e}")
                        continue
        
        logging.info(f"✅ 当前页面解析: {len(holdings)} 条记录")
        return holdings
        
    except Exception as e:
        logging.error(f"当前页面解析失败: {e}")
        return []

# --- 终极修复：爬取基金持仓 ---
def get_fund_holdings(driver: webdriver.Chrome, fund_code: str, years_to_crawl: List[str], max_retries: int = 3) -> pd.DataFrame:
    """终极修复版：多策略数据获取"""
    if driver is None:
        logging.error("❌ WebDriver无效")
        return pd.DataFrame()

    fund_holdings = []
    base_url = f"https://fundf10.eastmoney.com/ccmx_{fund_code}.html"
    logging.info(f"🌐 访问: {base_url}")

    # 页面加载
    for attempt in range(max_retries):
        try:
            logging.info(f"🚀 加载尝试 {attempt+1}/{max_retries}")
            driver.get(base_url)
            
            # 等待基本框架
            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.ID, "cctable"))
            )
            
            # 关键等待：数据加载
            if not wait_for_data_load(driver, fund_code):
                if attempt == max_retries - 1:
                    logging.error(f"❌ 页面加载失败")
                    # 最后尝试：直接解析当前页面
                    current_data = parse_current_holdings(driver, fund_code)
                    if current_data:
                        logging.info(f"✅ 备用解析: {len(current_data)} 条记录")
                        return pd.DataFrame(current_data)
                    return pd.DataFrame()
                time.sleep(5)
                continue
            
            # 验证页面
            page_source = driver.page_source
            if fund_code not in page_source:
                logging.warning(f"⚠️ 页面验证失败")
                if attempt == max_retries - 1:
                    return pd.DataFrame()
                time.sleep(3)
                continue
            
            logging.info("✅ 页面加载成功")
            break
            
        except Exception as e:
            logging.error(f"加载异常: {e}")
            if attempt == max_retries - 1:
                return pd.DataFrame()
            time.sleep(2 ** attempt)

    # 策略1：尝试年份选择
    years_attempted = []
    for year in years_to_crawl:
        try:
            logging.info(f"📅 尝试年份: {year}")
            success, selected_year = select_year_intelligently(driver, year)
            
            if success:
                logging.info(f"✅ 年份选择成功: {selected_year}")
                
                # 等待数据刷新
                time.sleep(6)
                
                # 解析数据
                page_source = driver.page_source
                soup = BeautifulSoup(page_source, 'lxml')
                
                holdings = parse_holdings_table(soup, fund_code, selected_year)
                if holdings:
                    fund_holdings.extend(holdings)
                    years_attempted.append(selected_year)
                    logging.info(f"✅ {len(holdings)} 条 {selected_year} 年数据")
                else:
                    logging.warning(f"⚠️ {selected_year} 年无数据")
                
                time.sleep(2)
            else:
                logging.warning(f"❌ {year} 年选择失败: {selected_year}")
                
        except Exception as e:
            logging.error(f"{year} 年处理异常: {e}")
            continue

    # 策略2：如果年份选择失败，解析当前显示数据
    if not years_attempted:
        logging.info("🔄 备用策略：解析当前显示数据")
        current_data = parse_current_holdings(driver, fund_code, years_to_crawl[0])
        if current_data:
            fund_holdings.extend(current_data)
            logging.info(f"✅ 当前页面: {len(current_data)} 条记录")
        else:
            logging.warning("❌ 当前页面也无数据")

    return pd.DataFrame(fund_holdings)

# --- 解析持仓表格（简化版） ---
def parse_holdings_table(soup: BeautifulSoup, fund_code: str, year: str) -> List[Dict]:
    """解析持仓表格"""
    cctable = soup.find(id="cctable")
    if not cctable:
        return []
    
    if "数据加载中" in cctable.text:
        return []
    
    holdings = []
    rows = cctable.find_all('tr')
    
    if len(rows) > 1:
        for row in rows[1:]:
            cols = row.find_all('td')
            if len(cols) >= 4:
                try:
                    # 提取股票代码
                    stock_code = ''
                    for col in cols[1:3]:
                        col_text = col.text.strip()
                        code_match = re.search(r'(\d{6})', col_text)
                        if code_match:
                            stock_code = code_match.group(1)
                            break
                    
                    if stock_code:
                        stock_name = cols[2].text.strip() if len(cols) > 2 else ''
                        stock_info = get_stock_info(stock_code)
                        
                        data = {
                            '基金代码': fund_code,
                            '年份': year,
                            '股票代码': stock_code,
                            '股票名称': stock_name,
                            '所属行业': stock_info['所属行业'],
                            '概念主题': stock_info['概念主题'],
                            '持仓占比': cols[3].text.strip() if len(cols) > 3 else '',
                            '持股数': cols[4].text.strip() if len(cols) > 4 else '',
                            '市值': cols[5].text.strip() if len(cols) > 5 else '',
                            '报告日期': cols[0].text.strip() if len(cols) > 0 else ''
                        }
                        holdings.append(data)
                except Exception as e:
                    continue
    
    return holdings

# --- 主函数 ---
def main():
    """主函数"""
    current_year = time.localtime().tm_year
    years_to_crawl = [str(current_year), str(current_year - 1), str(current_year - 2)]
    
    request_delay = random.uniform(3, 5)

    logging.info("=== 天天基金持仓爬取器（终极修复版） ===")
    logging.info(f"🎯 目标年份: {', '.join(years_to_crawl)}")
    
    report_file = 'market_monitor_report.md'
    fund_list = parse_markdown_file(report_file)
    if not fund_list:
        logging.error("❌ 无基金列表")
        return

    logging.info(f"📊 爬取 {len(fund_list)} 只基金")
    
    output_dir = "fund_data"
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = time.strftime('%Y%m%d_%H%M%S')
    output_filename = os.path.join(output_dir, f"fund_holdings_{timestamp}.csv")
    
    driver = setup_driver()
    if driver is None:
        logging.error("❌ 浏览器启动失败")
        return
    
    all_holdings_df = pd.DataFrame()
    successful_funds = 0
    
    try:
        for i, fund in enumerate(fund_list, 1):
            fund_code = fund['code']
            
            logging.info(f"\n{'='*60}")
            logging.info(f"🔄 [{i}/{len(fund_list)}] {fund_code}")
            logging.info(f"{'='*60}")
            
            holdings_df = get_fund_holdings(driver, fund_code, years_to_crawl)
            
            if not holdings_df.empty:
                all_holdings_df = pd.concat([all_holdings_df, holdings_df], ignore_index=True)
                successful_funds += 1
                logging.info(f"✅ {len(holdings_df)} 条记录")
                
                # 预览
                if len(holdings_df) > 0:
                    sample = holdings_df.head(2)
                    for _, row in sample.iterrows():
                        logging.info(f"   {row['股票代码']} - {row['股票名称'][:20]}...")
            else:
                logging.warning(f"❌ {fund_code} 无数据")
            
            logging.info(f"💤 等待 {request_delay:.1f}s")
            time.sleep(request_delay)
            
    finally:
        driver.quit()
    
    # 保存结果
    if not all_holdings_df.empty:
        logging.info("\n" + "🎉" * 15)
        logging.info("📊 统计:")
        logging.info(f"   总记录: {len(all_holdings_df)}")
        logging.info(f"   成功基金: {successful_funds}/{len(fund_list)}")
        
        # 质量统计
        if '所属行业' in all_holdings_df.columns:
            industry_rate = (all_holdings_df['所属行业'] != '未知').mean()
            logging.info(f"   行业覆盖: {industry_rate:.1%}")
        
        try:
            all_holdings_df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            size = os.path.getsize(output_filename) / 1024
            logging.info(f"💾 保存: {size:.1f}KB")
        except Exception as e:
            logging.error(f"❌ 保存失败: {e}")
    else:
        logging.warning("❌ 无数据")
        # 创建空文件
        cols = ['基金代码', '年份', '股票代码', '股票名称', '所属行业', '概念主题', 
                '持仓占比', '持股数', '市值', '报告日期']
        pd.DataFrame(columns=cols).to_csv(output_filename, index=False, encoding='utf-8-sig')

if __name__ == '__main__':
    main()

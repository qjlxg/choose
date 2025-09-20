# -*- coding: utf-8 -*-
"""
一个用于爬取天天基金网全市场基金持仓数据的Python脚本
优化版：修复股票信息URL和年份选择逻辑
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

# --- 解析Markdown文件，提取基金代码 ---
def parse_markdown_file(file_path: str) -> List[Dict[str, str]]:
    """解析Markdown文件，提取"弱买入"或"强买入"的基金代码。"""
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

# --- 股票信息缓存 ---
stock_info_cache = {}

# --- User-Agent池 ---
user_agent_pool = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
]

# --- 修复：获取股票行业和主题信息 ---
def get_stock_info(stock_code: str) -> Dict[str, str]:
    """根据股票代码爬取东方财富网，获取所属行业和概念主题。"""
    if not stock_code or stock_code == '':
        return {'所属行业': '未知', '概念主题': '未知'}
    
    if stock_code in stock_info_cache:
        return stock_info_cache[stock_code]
    
    info = {'所属行业': '未知', '概念主题': '未知'}
    
    if not re.match(r'^\d{6}$', stock_code):
        stock_info_cache[stock_code] = info
        return info
    
    # 修复：正确的URL构造逻辑
    if stock_code.startswith(('60', '68')):
        # 上海主板/创业板/科创板
        url = f"https://quote.eastmoney.com/sh{stock_code}.html"
    elif stock_code.startswith('30'):
        # 深圳创业板
        url = f"https://quote.eastmoney.com/sz{stock_code}.html"
    elif stock_code.startswith('00') or stock_code.startswith('000'):
        # 深圳主板
        url = f"https://quote.eastmoney.com/sz{stock_code}.html"
    elif stock_code.startswith('002'):
        # 深圳中小板
        url = f"https://quote.eastmoney.com/sz{stock_code}.html"
    else:
        # 默认使用深圳
        url = f"https://quote.eastmoney.com/sz{stock_code}.html"
    
    headers = {
        "User-Agent": random.choice(user_agent_pool),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Referer": "https://www.eastmoney.com/",
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')

        # 获取行业信息（多种方式）
        industry_selectors = [
            ('span', re.compile(r'所属行业')),
            ('div', re.compile(r'行业分类')),
            ('td', re.compile(r'行业')),
        ]
        
        for tag, pattern in industry_selectors:
            industry_elem = soup.find(tag, string=pattern)
            if industry_elem:
                # 尝试获取后续的行业名称
                next_elem = industry_elem.find_next_sibling()
                if next_elem and next_elem.text.strip():
                    info['所属行业'] = next_elem.text.strip()
                    break
                # 或者查找附近的链接
                industry_link = industry_elem.find_next('a')
                if industry_link:
                    info['所属行业'] = industry_link.text.strip()
                    break
        
        # 获取概念信息
        concept_selectors = [
            ('div', re.compile(r'概念板块')),
            ('span', re.compile(r'概念')),
            ('h3', re.compile(r'概念')),
        ]
        
        for tag, pattern in concept_selectors:
            concept_section = soup.find(tag, string=pattern)
            if concept_section:
                # 查找后续的概念标签
                concept_container = concept_section.find_next_sibling(['div', 'ul', 'span'])
                if concept_container:
                    concept_links = concept_container.find_all('a', limit=8)
                    concepts = [link.text.strip() for link in concept_links if link.text.strip()]
                    if concepts:
                        info['概念主题'] = ', '.join(concepts[:5])  # 最多5个概念
                        break
                break

        if info['所属行业'] == '未知' and info['概念主题'] == '未知':
            logging.debug(f"股票 {stock_code} 未获取到详细信息")
        else:
            logging.debug(f"✅ 股票 {stock_code}: {info}")

        stock_info_cache[stock_code] = info

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            logging.debug(f"股票 {stock_code} 页面不存在 (404)，跳过")
        else:
            logging.warning(f"❌ 股票 {stock_code} HTTP错误 {e.response.status_code}")
    except requests.exceptions.Timeout:
        logging.warning(f"❌ 股票 {stock_code} 请求超时")
    except Exception as e:
        logging.warning(f"❌ 股票 {stock_code} 解析失败: {e}")
    
    # 动态延时
    time.sleep(random.uniform(0.3, 0.8))  # 缩短延时提高效率
    
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
        chrome_options.add_argument('--disable-images')
        chrome_options.add_argument('--disable-plugins-discovery')
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        
        chromedriver_path = os.getenv('CHROMEDRIVER_PATH', '/usr/bin/chromedriver')
        service = Service(chromedriver_path)
        
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(60)
        driver.implicitly_wait(10)
        
        logging.info("🎉 ChromeDriver 启动成功！")
        return driver
    except WebDriverException as e:
        logging.error(f"❌ ChromeDriver 启动失败：{e}")
        return None

# --- 等待LoadStockPos完成 ---
def wait_for_loadstockpos_complete(driver: webdriver.Chrome, fund_code: str, timeout: int = 60) -> bool:
    """等待LoadStockPos函数完成数据加载"""
    logging.info(f"⏳ 等待 LoadStockPos 完成加载 (基金 {fund_code})...")
    
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        try:
            # 检查年份选择器是否已填充
            year_select = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.ID, "jjcc"))
            )
            
            options = driver.find_elements(By.CSS_SELECTOR, "#jjcc option")
            if len(options) > 1:
                logging.info(f"✅ 年份选择器已填充，找到 {len(options)-1} 个年份选项")
                break
                
        except TimeoutException:
            pass
        
        # 检查加载动画
        try:
            loading_img = driver.find_element(By.XPATH, "//img[@src*='loading2.gif']")
            if not loading_img.is_displayed():
                logging.info("✅ 加载动画已消失")
                break
        except:
            break
        
        # 检查表格数据
        try:
            cctable = driver.find_element(By.ID, "cctable")
            if "数据加载中" not in cctable.text:
                logging.info("✅ 持仓表格已加载数据")
                break
        except:
            pass
        
        time.sleep(2)
    
    total_time = time.time() - start_time
    if total_time >= timeout:
        logging.warning(f"⚠️  LoadStockPos 加载超时 ({total_time:.1f}s)")
        return False
    
    logging.info(f"✅ LoadStockPos 加载完成，耗时 {total_time:.1f}s")
    return True

# --- 修复：智能年份选择 ---
def select_year_intelligently(driver: webdriver.Chrome, target_year: str) -> Tuple[bool, str]:
    """
    智能选择年份：修复字符串匹配问题
    返回 (成功, 实际选择的年份)
    """
    try:
        # 等待年份选项加载
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "jjcc"))
        )
        
        # 获取所有年份选项
        options = driver.find_elements(By.CSS_SELECTOR, "#jjcc option")
        available_years = []
        
        for option in options[1:]:  # 跳过默认选项
            year_text = option.text.strip()
            # 提取4位年份数字
            year_match = re.search(r'(\d{4})', year_text)
            if year_match:
                available_years.append(year_match.group(1))
        
        logging.info(f"📋 可用年份: {available_years}")
        
        if not available_years:
            logging.warning("❌ 未找到任何年份选项")
            return False, "无可用年份"
        
        # 智能匹配：优先精确匹配，其次最近年份
        selected_year = None
        for year in [target_year, str(int(target_year)-1), str(int(target_year)-2)]:
            if year in available_years:
                selected_year = year
                break
        
        if not selected_year:
            selected_year = available_years[0]  # 默认最新年份
            logging.warning(f"⚠️ 目标年份不可用，使用最新年份: {selected_year}")
        else:
            logging.info(f"🎯 选择年份: {selected_year}")
        
        # 执行选择
        year_option_xpath = f"//select[@id='jjcc']/option[contains(text(), '{selected_year}')]"
        year_option = driver.find_element(By.XPATH, year_option_xpath)
        
        # JavaScript方式选择
        select_element = driver.find_element(By.ID, "jjcc")
        driver.execute_script("arguments[0].selectedIndex = arguments[1];", select_element, available_years.index(selected_year))
        
        # 触发change事件
        driver.execute_script("""
            var event = new Event('change', {bubbles: true});
            arguments[0].dispatchEvent(event);
        """, select_element)
        
        logging.info(f"✅ 已选择年份: {selected_year}")
        
        # 等待数据加载
        time.sleep(5)
        return True, selected_year
        
    except Exception as e:
        logging.error(f"❌ 年份选择失败: {e}")
        return False, str(e)

# --- 解析持仓表格 ---
def parse_holdings_table(soup: BeautifulSoup, fund_code: str, year: str) -> List[Dict]:
    """专门解析持仓表格的函数"""
    holdings_table = soup.find(id="cctable")
    if not holdings_table:
        logging.warning(f"未找到持仓表格 #cctable")
        return []
    
    # 检查加载状态
    loading_div = holdings_table.find('div', string=re.compile(r'数据加载中'))
    if loading_div:
        logging.warning(f"持仓表格仍在加载，跳过 {fund_code} {year} 年数据")
        return []
    
    holdings = []
    rows = holdings_table.find_all('tr')
    
    if not rows or len(rows) <= 1:
        logging.warning(f"未找到表格行，使用备用解析")
        # 备用解析逻辑...
        return []
    
    for i, row in enumerate(rows[1:], 1):
        cols = row.find_all('td')
        if len(cols) >= 4:
            try:
                # 提取股票代码
                stock_code_match = None
                for col in cols[1:3]:
                    col_text = col.text.strip()
                    stock_code_match = re.search(r'(\d{6})', col_text)
                    if stock_code_match:
                        break
                
                if not stock_code_match:
                    continue
                    
                stock_code = stock_code_match.group(1)
                stock_name = cols[2].text.strip() if len(cols) > 2 else ''
                
                # 获取股票信息（异步执行，稍后处理）
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
                logging.debug(f"解析第{i}行失败: {e}")
                continue
    
    return holdings

# --- 爬取指定基金持仓数据 ---
def get_fund_holdings(driver: webdriver.Chrome, fund_code: str, years_to_crawl: List[str], max_retries: int = 3) -> pd.DataFrame:
    """爬取指定基金在近N年内的持仓数据。"""
    if driver is None:
        logging.error("WebDriver 实例不存在，跳过爬取。")
        return pd.DataFrame()

    fund_holdings = []
    base_url = f"https://fundf10.eastmoney.com/ccmx_{fund_code}.html"

    logging.info(f"🌐 访问基金 {fund_code} 页面: {base_url}")
    
    # 页面加载
    for attempt in range(max_retries):
        try:
            logging.info(f"🚀 尝试访问页面 (第{attempt+1}次)...")
            driver.get(base_url)
            
            # 等待基本结构
            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.ID, "cctable"))
            )
            
            # 等待数据加载
            if not wait_for_loadstockpos_complete(driver, fund_code):
                if attempt == max_retries - 1:
                    logging.error(f"❌ 页面加载失败，已重试{max_retries}次")
                    return pd.DataFrame()
                time.sleep(5)
                continue
            
            page_source = driver.page_source
            if fund_code not in page_source:
                logging.warning(f"页面可能加载错误")
                if attempt == max_retries - 1:
                    return pd.DataFrame()
                time.sleep(3)
                continue
            
            logging.info("✅ 页面加载成功！")
            break
            
        except Exception as e:
            logging.error(f"访问页面失败: {e}")
            if attempt == max_retries - 1:
                return pd.DataFrame()
            time.sleep(2 ** attempt)

    # 年份数据处理
    for year in years_to_crawl:
        try:
            logging.info(f"📅 处理 {year} 年数据...")
            
            success, selected_year = select_year_intelligently(driver, year)
            
            if not success:
                logging.warning(f"⚠️ 无法选择 {year} 年，跳过")
                continue
            
            # 等待数据加载
            time.sleep(5)
            wait_for_loadstockpos_complete(driver, fund_code, timeout=30)
            
            # 解析数据
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'lxml')
            
            holdings = parse_holdings_table(soup, fund_code, selected_year)
            fund_holdings.extend(holdings)
            
            if holdings:
                logging.info(f"✅ 获取 {len(holdings)} 条 {selected_year} 年记录")
            else:
                logging.warning(f"❌ {selected_year} 年无数据")
            
            time.sleep(random.uniform(1, 2))
            
        except Exception as e:
            logging.error(f"处理 {year} 年失败: {e}")
            continue
    
    return pd.DataFrame(fund_holdings)

# --- 主函数 ---
def main():
    """主函数，执行爬取任务。"""
    current_year = time.localtime().tm_year
    years_to_crawl = [str(current_year), str(current_year - 1), str(current_year - 2)]
    
    request_delay = random.uniform(2, 4)

    logging.info("=== 天天基金持仓数据爬取器（优化版） ===")
    logging.info(f"🎯 目标年份: {', '.join(years_to_crawl)}")
    
    report_file = 'market_monitor_report.md'
    fund_list_to_crawl = parse_markdown_file(report_file)
    if not fund_list_to_crawl:
        logging.error(f"❌ 无法获取基金列表")
        return

    logging.info(f"📊 准备爬取 {len(fund_list_to_crawl)} 只基金")
    
    output_dir = "fund_data"
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = time.strftime('%Y%m%d_%H%M%S')
    output_filename = os.path.join(output_dir, f"fund_holdings_{timestamp}.csv")
    
    driver = setup_driver()
    if driver is None:
        return
    
    all_holdings_df = pd.DataFrame()
    successful_funds = 0
    
    try:
        for i, fund in enumerate(fund_list_to_crawl, 1):
            fund_code = fund['code']
            
            logging.info(f"\n{'='*60}")
            logging.info(f"🔄 [{i}/{len(fund_list_to_crawl)}] {fund_code}")
            logging.info(f"{'='*60}")
            
            holdings_df = get_fund_holdings(driver, fund_code, years_to_crawl)
            
            if not holdings_df.empty:
                all_holdings_df = pd.concat([all_holdings_df, holdings_df], ignore_index=True)
                successful_funds += 1
                logging.info(f"✅ 获取 {len(holdings_df)} 条记录")
            else:
                logging.warning(f"❌ 无数据")
            
            time.sleep(request_delay)
            
    finally:
        driver.quit()
    
    # 保存结果
    if not all_holdings_df.empty:
        logging.info("\n" + "🎉" * 20)
        logging.info("📊 最终统计:")
        logging.info(f"   总记录: {len(all_holdings_df):,}")
        logging.info(f"   成功基金: {successful_funds}/{len(fund_list_to_crawl)}")
        logging.info(f"   唯一股票: {all_holdings_df['股票代码'].nunique()}")
        
        all_holdings_df.to_csv(output_filename, index=False, encoding='utf-8-sig')
        logging.info(f"💾 保存: {output_filename}")
    else:
        logging.warning("❌ 无数据生成")

if __name__ == '__main__':
    main()

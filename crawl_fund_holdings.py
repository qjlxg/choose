# -*- coding: utf-8 -*-
"""
一个用于爬取天天基金网全市场基金持仓数据的Python脚本
增强版：添加年份选择器诊断和自适应定位功能
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
from selenium.webdriver.common.action_chains import ActionChains

# --- 配置日志系统（增强版） ---
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawler_debug.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

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

# --- 获取股票行业和主题信息 ---
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

        # 尝试获取所属行业
        industry_div = soup.find('div', string=re.compile(r'所属行业'))
        if industry_div and industry_div.find_next_sibling('div'):
            info['所属行业'] = industry_div.find_next_sibling('div').text.strip()
        
        # 尝试获取概念主题
        theme_div = soup.find('div', string=re.compile(r'概念主题'))
        if theme_div and theme_div.find_next_sibling('div'):
            theme_links = theme_div.find_next_sibling('div').find_all('a')
            themes = [link.text.strip() for link in theme_links]
            info['概念主题'] = ', '.join(themes)

        stock_info_cache[stock_code] = info

    except requests.exceptions.RequestException as e:
        logging.warning(f"❌ 爬取股票 {stock_code} 信息失败: {e}")
    except Exception as e:
        logging.warning(f"❌ 解析股票 {stock_code} 页面失败: {e}")
    
    # 动态延时
    time.sleep(random.uniform(0.5, 1.5))
    
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
        chrome_options.add_argument('--disable-images')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        # 启用JavaScript性能日志
        chrome_options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
        
        chromedriver_path = os.getenv('CHROMEDRIVER_PATH', '/usr/lib/chromium-browser/chromedriver')
        service = Service(chromedriver_path)
        
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.implicitly_wait(10)
        
        logging.info("🎉 ChromeDriver 启动成功！")
        return driver
    except WebDriverException as e:
        logging.error(f"❌ ChromeDriver 启动失败：{e}")
        return None

# --- 新增：年份选择器诊断函数 ---
def diagnose_year_selectors(driver: webdriver.Chrome, fund_code: str, year: str) -> Tuple[bool, str]:
    """
    诊断年份选择器并尝试智能定位
    返回 (成功, 调试信息)
    """
    logging.info(f"🔍 开始诊断 {year} 年选择器...")
    
    # 等待页面完全加载
    time.sleep(5)
    
    # 1. 保存页面源码用于分析
    with open(f'debug_page_{fund_code}_{year}.html', 'w', encoding='utf-8') as f:
        f.write(driver.page_source)
    logging.info(f"💾 已保存调试页面: debug_page_{fund_code}_{year}.html")
    
    # 2. 多种选择器策略
    selector_strategies = [
        # 策略1：精确匹配
        [
            (By.XPATH, f"//label[@value='{year}']"),
            (By.XPATH, f"//input[@value='{year}']"),
            (By.XPATH, f"//option[@value='{year}']"),
        ],
        # 策略2：文本匹配
        [
            (By.XPATH, f"//*[contains(text(), '{year}') and (@class='active' or @class='current')]"),
            (By.XPATH, f"//a[contains(text(), '{year}')]"),
            (By.XPATH, f"//span[contains(text(), '{year}')]"),
            (By.XPATH, f"//div[contains(text(), '{year}')]"),
        ],
        # 策略3：模糊匹配
        [
            (By.XPATH, f"//*[contains(@class, 'year') or contains(@class, 'select') or contains(@class, 'tab')]/*[contains(text(), '{year}')]"),
            (By.CSS_SELECTOR, f"[data-year='{year}']"),
            (By.CSS_SELECTOR, f".year-{year}"),
        ],
        # 策略4：通用选择器
        [
            (By.ID, "jjcc"),
            (By.ID, "pagebar"),
            (By.CLASS_NAME, "selcc"),
        ],
    ]
    
    all_elements = []
    
    # 尝试所有策略
    for strategy_id, selectors in enumerate(selector_strategies, 1):
        logging.info(f"  策略 {strategy_id}: 测试 {len(selectors)} 个选择器")
        
        for selector_id, (by, value) in enumerate(selectors, 1):
            try:
                elements = driver.find_elements(by, value)
                logging.info(f"    选择器 {selector_id}: {by}={value[:50]}... -> 找到 {len(elements)} 个元素")
                
                for i, element in enumerate(elements):
                    try:
                        text = element.text.strip()
                        is_displayed = element.is_displayed()
                        is_enabled = element.is_enabled()
                        
                        element_info = {
                            'text': text,
                            'tag': element.tag_name,
                            'class': element.get_attribute('class'),
                            'value': element.get_attribute('value'),
                            'id': element.get_attribute('id'),
                            'displayed': is_displayed,
                            'enabled': is_enabled
                        }
                        
                        all_elements.append((element_info, element))
                        
                        # 如果元素包含目标年份且可交互，优先选择
                        if year in text and is_displayed and is_enabled:
                            logging.info(f"    🎯 找到目标元素: {text} (显示:{is_displayed}, 启用:{is_enabled})")
                            return True, f"策略{strategy_id}-{selector_id} 找到目标元素: {text}"
                            
                    except Exception as e:
                        logging.debug(f"    元素 {i} 分析失败: {e}")
                        
            except Exception as e:
                logging.debug(f"    选择器 {selector_id} 执行失败: {e}")
    
    # 3. 如果精确匹配失败，尝试通用交互
    logging.info("🔄 尝试通用交互策略...")
    
    # 查找所有可能的可点击元素
    clickable_selectors = [
        (By.TAG_NAME, "select"),
        (By.TAG_NAME, "button"),
        (By.CSS_SELECTOR, "input[type='button'], input[type='submit']"),
        (By.CSS_SELECTOR, ".btn, .button, [role='button']"),
        (By.XPATH, "//*[contains(@onclick, 'year') or contains(@onclick, 'select')]"),
    ]
    
    for by, value in clickable_selectors:
        try:
            elements = driver.find_elements(by, value)
            for element in elements:
                if element.is_displayed() and element.is_enabled():
                    text = element.text.strip()
                    logging.info(f"🔘 发现可点击元素: {text} (类型: {element.tag_name})")
                    
                    # 尝试点击
                    try:
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
                        time.sleep(1)
                        ActionChains(driver).move_to_element(element).click().perform()
                        time.sleep(2)
                        
                        # 检查点击后是否出现年份选项
                        new_elements = driver.find_elements(By.XPATH, f"//*[contains(text(), '{year}')]")
                        if new_elements:
                            logging.info(f"✅ 通用点击成功，找到 {len(new_elements)} 个年份元素")
                            return True, f"通用点击成功找到年份选项"
                            
                    except Exception as click_error:
                        logging.debug(f"点击元素失败: {click_error}")
                        
        except Exception as e:
            logging.debug(f"通用选择器执行失败: {e}")
    
    # 4. 最后的调试信息
    debug_info = f"未找到 {year} 年选择器\n"
    debug_info += f"总共发现 {len(all_elements)} 个相关元素\n"
    
    if all_elements:
        debug_info += "\n前5个发现的元素:\n"
        for i, (elem_info, _) in enumerate(all_elements[:5]):
            debug_info += f"  {i+1}. {elem_info['tag']}[{elem_info['text'][:30]}...] "
            debug_info += f"(显示:{elem_info['displayed']}, 启用:{elem_info['enabled']})\n"
    
    logging.warning(f"❌ {debug_info}")
    return False, debug_info

# --- 专门解析持仓表格的函数 ---
def parse_holdings_table(soup: BeautifulSoup, fund_code: str, year: str) -> List[Dict]:
    """专门解析持仓表格的函数"""
    holdings_table = soup.find(id="cctable")
    if not holdings_table:
        logging.warning(f"未找到持仓表格 #cctable")
        return []
    
    # 检查是否还在加载状态
    loading_div = holdings_table.find('div', style=re.compile(r'text-align:\s*center'))
    if loading_div and '数据加载中' in loading_div.get_text():
        logging.warning(f"持仓表格仍在加载中，跳过 {fund_code} {year} 年数据")
        return []
    
    holdings = []
    rows = holdings_table.find_all('tr')
    if not rows or len(rows) <= 1:
        # 尝试其他可能的表格结构
        div_rows = holdings_table.find_all('div', recursive=False)
        if div_rows:
            rows = [BeautifulSoup(f"<tr>{div_row}</tr>", 'lxml').find('tr') for div_row in div_rows]
        else:
            logging.warning(f"未找到有效的表格行数据")
            return []
    
    for i, row in enumerate(rows[1:], 1):
        cols = row.find_all('td')
        if len(cols) >= 5:
            try:
                stock_code = cols[1].text.strip() if len(cols) > 1 else ''
                
                # 提取6位股票代码
                code_match = re.search(r'(\d{6})', stock_code)
                if code_match:
                    stock_code = code_match.group(1)
                
                # 获取股票行业和主题信息
                stock_info = get_stock_info(stock_code)
                
                data = {
                    '基金代码': fund_code,
                    '年份': year,
                    '股票代码': stock_code,
                    '股票名称': cols[2].text.strip() if len(cols) > 2 else '',
                    '所属行业': stock_info['所属行业'],
                    '概念主题': stock_info['概念主题'],
                    '持仓占比': cols[3].text.strip() if len(cols) > 3 else '',
                    '持股数': cols[4].text.strip() if len(cols) > 4 else '',
                    '市值': cols[5].text.strip() if len(cols) > 5 else '',
                    '报告日期': cols[0].text.strip() if len(cols) > 0 else ''
                }
                holdings.append(data)
            except Exception as e:
                logging.warning(f"解析行数据失败: {e}")
                continue
    
    return holdings

# --- 优化：爬取指定基金持仓数据（增强版） ---
def get_fund_holdings(driver: webdriver.Chrome, fund_code: str, years_to_crawl: List[str], max_retries: int = 3) -> pd.DataFrame:
    """
    爬取指定基金在近N年内的持仓数据。
    增强版：添加选择器诊断和自适应点击
    """
    if driver is None:
        logging.error("WebDriver 实例不存在，跳过爬取。")
        return pd.DataFrame()

    fund_holdings = []
    base_url = f"https://fundf10.eastmoney.com/ccmx_{fund_code}.html"

    logging.info(f"访问基金 {fund_code} 页面: {base_url}")
    
    # 页面加载
    for attempt in range(max_retries):
        try:
            logging.info(f"尝试访问页面 (第{attempt+1}次)...")
            driver.get(base_url)
            
            wait = WebDriverWait(driver, 30)
            wait.until(
                EC.any_of(
                    EC.presence_of_element_located((By.ID, "cctable")),
                    EC.presence_of_element_located((By.ID, "pagebar")),
                    EC.presence_of_element_located((By.CLASS_NAME, "tit_h3"))
                )
            )
            
            # 等待JavaScript执行
            time.sleep(5)
            
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

    # 年份数据爬取（增强版）
    for year in years_to_crawl:
        try:
            logging.info(f"正在爬取 {year} 年持仓数据...")
            
            # 诊断并尝试选择年份
            success, debug_info = diagnose_year_selectors(driver, fund_code, year)
            
            if not success:
                logging.warning(f"❌ {debug_info}")
                
                # 尝试直接解析当前页面（可能默认显示最新年份）
                logging.info("🔄 尝试解析当前显示的持仓数据...")
                page_source = driver.page_source
                soup = BeautifulSoup(page_source, 'lxml')
                
                current_holdings = parse_holdings_table(soup, fund_code, year)
                if current_holdings:
                    logging.info(f"✅ 从当前页面解析到 {len(current_holdings)} 条记录")
                    fund_holdings.extend(current_holdings)
                else:
                    logging.warning(f"当前页面也无有效数据，跳过 {year} 年")
                continue
            else:
                logging.info(f"✅ {debug_info}")
            
            # 等待数据加载
            wait = WebDriverWait(driver, 15)
            try:
                wait.until_not(
                    EC.presence_of_element_located((By.XPATH, "//img[@src*='loading2.gif']"))
                )
            except TimeoutException:
                logging.warning("加载动画未消失，但继续解析...")
            
            # 解析数据
            time.sleep(3)  # 额外等待
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'lxml')
            
            holdings = parse_holdings_table(soup, fund_code, year)
            fund_holdings.extend(holdings)
            logging.info(f"✅ 成功获取 {len(holdings)} 条 {year} 年的持仓记录。")
            
        except Exception as e:
            logging.error(f"爬取基金 {fund_code} 的 {year} 年数据时发生错误：{e}")
            continue
            
    return pd.DataFrame(fund_holdings)


def main():
    """主函数，执行爬取任务。"""
    current_year = time.localtime().tm_year
    years_to_crawl = [str(current_year), str(current_year - 1), str(current_year - 2)]
    
    request_delay = random.uniform(1, 3)

    logging.info("=== 天天基金持仓数据爬取器（增强诊断版） ===")
    logging.info(f"目标年份: {', '.join(years_to_crawl)}")
    
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
            
            time.sleep(request_delay)
            
    finally:
        logging.info("爬取任务结束，关闭 WebDriver。")
        if driver:
            driver.quit()
    
    # 结果处理
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
        logging.info("❌ 没有爬取到任何数据。")

if __name__ == '__main__':
    main()

# -*- coding: utf-8 -*-
"""
一个用于爬取天天基金网全市场基金持仓数据的Python脚本
修复版：针对LoadStockPos异步加载机制的优化
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
import json

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

# --- 获取股票行业和主题信息 ---
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
    
    url = f"https://quote.eastmoney.com/sh{stock_code[:3]}.html" if stock_code.startswith(('60', '68')) else f"https://quote.eastmoney.com/sz{stock_code[3:]}.html"
    headers = {"User-Agent": random.choice(user_agent_pool)}
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')

        # 获取行业信息
        industry_elem = soup.find('span', string=re.compile(r'行业'))
        if industry_elem:
            industry_text = industry_elem.find_next_sibling('a')
            if industry_text:
                info['所属行业'] = industry_text.text.strip()

        # 获取概念信息
        concept_section = soup.find('div', string=re.compile(r'概念板块'))
        if concept_section:
            concepts = concept_section.find_next_sibling('div').find_all('a')
            info['概念主题'] = ', '.join([c.text.strip() for c in concepts[:5]])

        stock_info_cache[stock_code] = info

    except Exception as e:
        logging.warning(f"❌ 股票 {stock_code} 信息获取失败: {e}")
    
    time.sleep(random.uniform(0.5, 1.0))
    return info

# --- 配置Selenium（修复版） ---
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
        # 关键：启用性能日志以监控JavaScript执行
        chrome_options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
        
        chromedriver_path = os.getenv('CHROMEDRIVER_PATH', '/usr/bin/chromedriver')
        service = Service(chromedriver_path)
        
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(60)
        driver.implicitly_wait(15)
        
        logging.info("🎉 ChromeDriver 启动成功！")
        return driver
    except WebDriverException as e:
        logging.error(f"❌ ChromeDriver 启动失败：{e}")
        return None

# --- 新增：等待LoadStockPos完成 ---
def wait_for_loadstockpos_complete(driver: webdriver.Chrome, fund_code: str, timeout: int = 60) -> bool:
    """
    等待LoadStockPos函数完成数据加载
    通过监控页面变化和网络请求来判断
    """
    logging.info(f"⏳ 等待 LoadStockPos 完成加载 (基金 {fund_code})...")
    
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        try:
            # 1. 检查年份选择器是否已填充
            year_select = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.ID, "jjcc"))
            )
            
            # 检查select是否有option
            options = driver.find_elements(By.CSS_SELECTOR, "#jjcc option")
            if len(options) > 1:  # 至少有一个年份选项
                logging.info(f"✅ 年份选择器已填充，找到 {len(options)-1} 个年份选项")
                break
                
        except TimeoutException:
            pass
        
        # 2. 检查加载动画是否消失
        try:
            loading_img = driver.find_element(By.XPATH, "//img[@src*='loading2.gif']")
            if not loading_img.is_displayed():
                logging.info("✅ 加载动画已消失")
                break
        except:
            logging.info("✅ 无加载动画")
            break
        
        # 3. 检查cctable是否有实际数据
        try:
            cctable = driver.find_element(By.ID, "cctable")
            if "数据加载中" not in cctable.text:
                logging.info("✅ 持仓表格已加载数据")
                break
        except:
            pass
        
        # 4. 监控网络请求（检查是否有数据请求完成）
        logs = driver.get_log('performance')
        has_data_request = any(
            "fund.eastmoney.com" in log['message']['message']['params'].get('request', {}).get('url', '') 
            or "eastmoney.com" in log['message']['message']['params'].get('request', {}).get('url', '')
            for log in logs[-10:]  # 检查最近10条日志
        )
        
        if has_data_request:
            logging.info("🔄 检测到数据请求，等待响应...")
        
        time.sleep(2)
    
    total_time = time.time() - start_time
    if total_time >= timeout:
        logging.warning(f"⚠️  LoadStockPos 加载超时 ({total_time:.1f}s)")
        return False
    
    logging.info(f"✅ LoadStockPos 加载完成，耗时 {total_time:.1f}s")
    return True

# --- 新增：智能年份选择 ---
def select_year_intelligently(driver: webdriver.Chrome, target_year: str) -> bool:
    """
    智能选择年份：先获取所有可用年份，然后选择目标年份
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
            if re.match(r'\d{4}', year_text):
                available_years.append(year_text)
        
        logging.info(f"📋 可用年份: {available_years}")
        
        if not available_years:
            logging.warning("❌ 未找到任何年份选项")
            return False
        
        # 检查目标年份是否可用
        if target_year in available_years:
            logging.info(f"🎯 找到目标年份 {target_year}")
        else:
            # 选择最接近的年份
            target_year = available_years[0]  # 默认选择第一个（通常是最新）
            logging.warning(f"⚠️ 目标年份 {target_year} 不可用，使用 {target_year}")
        
        # 执行选择
        year_option = driver.find_element(By.XPATH, f"//select[@id='jjcc']/option[contains(text(), '{target_year}')]")
        
        # 滚动到选择器位置
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", year_option)
        time.sleep(1)
        
        # JavaScript点击避免Selenium的点击问题
        driver.execute_script("arguments[0].parentNode.selectedIndex = arguments[1];", 
                             driver.find_element(By.ID, "jjcc"), available_years.index(target_year))
        
        # 触发change事件
        driver.execute_script("arguments[0].dispatchEvent(new Event('change', {bubbles: true}));", 
                             driver.find_element(By.ID, "jjcc"))
        
        logging.info(f"✅ 已选择年份: {target_year}")
        
        # 等待数据重新加载
        time.sleep(5)
        return True
        
    except Exception as e:
        logging.error(f"❌ 年份选择失败: {e}")
        return False

# --- 解析持仓表格（优化版） ---
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
    # 查找表格行（支持多种结构）
    rows = holdings_table.find_all('tr') or []
    
    if not rows or len(rows) <= 1:
        # 尝试div表格结构
        div_rows = holdings_table.find_all('div', class_=re.compile(r'row|item'))
        if div_rows:
            logging.info(f"使用div表格结构，找到 {len(div_rows)} 行")
            for div_row in div_rows:
                cols = div_row.find_all(['span', 'div', 'td'])
                if len(cols) >= 4:  # 至少4列
                    try:
                        stock_code = re.search(r'(\d{6})', cols[1].text).group(1) if re.search(r'(\d{6})', cols[1].text) else ''
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
                        logging.debug(f"解析div行失败: {e}")
                        continue
        else:
            logging.warning(f"未找到有效的表格结构")
            return []
    else:
        # 标准table结构
        for row in rows[1:]:  # 跳过表头
            cols = row.find_all('td')
            if len(cols) >= 4:
                try:
                    stock_code = re.search(r'(\d{6})', cols[1].text).group(1) if re.search(r'(\d{6})', cols[1].text) else ''
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
                    logging.debug(f"解析行数据失败: {e}")
                    continue
    
    logging.info(f"✅ 解析完成: {len(holdings)} 条 {year} 年持仓记录")
    return holdings

# --- 修复版：爬取指定基金持仓数据 ---
def get_fund_holdings(driver: webdriver.Chrome, fund_code: str, years_to_crawl: List[str], max_retries: int = 3) -> pd.DataFrame:
    """
    爬取指定基金在近N年内的持仓数据。
    修复版：正确处理LoadStockPos异步加载
    """
    if driver is None:
        logging.error("WebDriver 实例不存在，跳过爬取。")
        return pd.DataFrame()

    fund_holdings = []
    base_url = f"https://fundf10.eastmoney.com/ccmx_{fund_code}.html"

    logging.info(f"🌐 访问基金 {fund_code} 页面: {base_url}")
    
    # 页面加载重试
    for attempt in range(max_retries):
        try:
            logging.info(f"🚀 尝试访问页面 (第{attempt+1}次)...")
            driver.get(base_url)
            
            # 等待基本页面结构
            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.ID, "cctable"))
            )
            
            # 关键：等待LoadStockPos完成
            if not wait_for_loadstockpos_complete(driver, fund_code):
                if attempt == max_retries - 1:
                    logging.error(f"❌ LoadStockPos 加载失败，已重试{max_retries}次")
                    return pd.DataFrame()
                time.sleep(5)
                continue
            
            # 验证页面是否正确加载
            page_source = driver.page_source
            if "017484" not in page_source and fund_code not in page_source:
                logging.warning(f"页面可能加载错误，未找到基金 {fund_code} 标识")
                if attempt == max_retries - 1:
                    return pd.DataFrame()
                time.sleep(3)
                continue
            
            logging.info("✅ 页面和数据加载成功！")
            break
            
        except TimeoutException:
            logging.warning(f"页面加载超时 (第{attempt+1}/{max_retries}次)")
            if attempt == max_retries - 1:
                return pd.DataFrame()
            time.sleep(2 ** attempt)
        except Exception as e:
            logging.error(f"访问页面时发生错误：{e}")
            if attempt == max_retries - 1:
                return pd.DataFrame()
            time.sleep(2 ** attempt)

    # 年份数据爬取
    for year in years_to_crawl:
        try:
            logging.info(f"📅 正在处理 {year} 年数据...")
            
            # 智能选择年份
            if not select_year_intelligently(driver, year):
                logging.warning(f"⚠️ 无法选择 {year} 年，尝试解析当前显示数据")
                # 解析当前页面数据
                page_source = driver.page_source
                soup = BeautifulSoup(page_source, 'lxml')
                current_holdings = parse_holdings_table(soup, fund_code, year)
                if current_holdings:
                    fund_holdings.extend(current_holdings)
                    logging.info(f"✅ 从当前页面获取 {len(current_holdings)} 条记录")
                continue
            
            # 等待年份切换后的数据加载
            time.sleep(5)
            
            # 再次等待LoadStockPos完成（年份切换触发）
            wait_for_loadstockpos_complete(driver, fund_code, timeout=30)
            
            # 解析数据
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'lxml')
            
            holdings = parse_holdings_table(soup, fund_code, year)
            fund_holdings.extend(holdings)
            
            if holdings:
                logging.info(f"✅ 成功获取 {len(holdings)} 条 {year} 年持仓记录")
            else:
                logging.warning(f"❌ {year} 年无持仓数据")
            
            time.sleep(random.uniform(1, 2))
            
        except Exception as e:
            logging.error(f"处理 {year} 年数据时出错：{e}")
            continue
    
    return pd.DataFrame(fund_holdings)

# --- 主函数 ---
def main():
    """主函数，执行爬取任务。"""
    current_year = time.localtime().tm_year
    years_to_crawl = [str(current_year), str(current_year - 1), str(current_year - 2)]
    
    request_delay = random.uniform(2, 4)

    logging.info("=== 天天基金持仓数据爬取器（修复版） ===")
    logging.info(f"🎯 目标年份: {', '.join(years_to_crawl)}")
    logging.info(f"⏱️  延时设置: {request_delay:.1f}秒")
    
    report_file = 'market_monitor_report.md'
    fund_list_to_crawl = parse_markdown_file(report_file)
    if not fund_list_to_crawl:
        logging.error(f"❌ 无法从 '{report_file}' 获取基金列表，程序退出")
        return

    logging.info(f"📊 准备爬取 {len(fund_list_to_crawl)} 只基金")
    
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
        for i, fund in enumerate(fund_list_to_crawl, 1):
            fund_code = fund['code']
            fund_name = fund['name']
            
            logging.info(f"\n{'='*60}")
            logging.info(f"🔄 [{i}/{len(fund_list_to_crawl)}] 处理: {fund_name} ({fund_code})")
            logging.info(f"{'='*60}")
            
            holdings_df = get_fund_holdings(driver, fund_code, years_to_crawl)
            
            if not holdings_df.empty:
                all_holdings_df = pd.concat([all_holdings_df, holdings_df], ignore_index=True)
                successful_funds += 1
                logging.info(f"✅ 成功获取 {len(holdings_df)} 条记录")
                
                # 显示数据预览
                logging.info(f"📋 数据预览:")
                for _, row in holdings_df.head(2).iterrows():
                    logging.info(f"   {row['股票代码']} - {row['股票名称'][:20]}... ({row['持仓占比']})")
            else:
                logging.info("❌ 未获取到数据")
            
            # 基金间延时
            logging.info(f"💤 等待 {request_delay:.1f} 秒...")
            time.sleep(request_delay)
            
    finally:
        logging.info("🔚 爬取任务结束，关闭浏览器")
        driver.quit()
    
    # 结果保存
    if not all_holdings_df.empty:
        logging.info("\n" + "🎉" * 20)
        logging.info("📊 爬取统计:")
        logging.info(f"   总记录数: {len(all_holdings_df):,}")
        logging.info(f"   成功基金: {successful_funds}/{len(fund_list_to_crawl)}")
        logging.info(f"   唯一股票: {all_holdings_df['股票代码'].nunique()}")
        
        try:
            all_holdings_df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            file_size = os.path.getsize(output_filename) / 1024
            logging.info(f"💾 保存成功: {output_filename} ({file_size:.1f} KB)")
        except Exception as e:
            logging.error(f"❌ 文件保存失败: {e}")
    else:
        logging.warning("❌ 未获取到任何数据，创建空文件")
        empty_df = pd.DataFrame(columns=['基金代码', '年份', '股票代码', '股票名称', '所属行业', 
                                        '概念主题', '持仓占比', '持股数', '市值', '报告日期'])
        empty_df.to_csv(output_filename, index=False, encoding='utf-8-sig')

if __name__ == '__main__':
    main()

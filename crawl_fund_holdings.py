import requests
from lxml import etree
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import time
import re
from fake_useragent import UserAgent
import os
from datetime import datetime
import logging
import signal
import sys

# 配置详细日志
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fund_crawler.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class FundDataCrawler:
    def __init__(self, output_dir='fund_data', timeout=30):
        self.session = None
        self.driver = None
        self.wait = None
        self.ua = UserAgent()
        self.output_dir = output_dir
        self.timeout = timeout
        self.session_delay = np.random.uniform(1, 3)
        
        logger.info(f"初始化爬虫，输出目录: {output_dir}, 超时时间: {timeout}s")
        self.setup_session()
        self.setup_driver()
        self.ensure_output_directory()
    
    def ensure_output_directory(self):
        """确保输出目录存在"""
        try:
            if not os.path.exists(self.output_dir):
                os.makedirs(self.output_dir)
                logger.info(f"已创建输出目录: {self.output_dir}")
            else:
                logger.info(f"输出目录已存在: {self.output_dir}")
        except Exception as e:
            logger.error(f"创建输出目录失败: {e}")
    
    def setup_session(self):
        """设置requests会话"""
        try:
            logger.info("正在设置requests会话...")
            headers = {
                'User-Agent': self.ua.random,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Cache-Control': 'max-age=0'
            }
            
            self.session = requests.Session()
            self.session.headers.update(headers)
            
            # 测试连接
            logger.info("测试网络连接...")
            test_response = self.session.get('https://www.baidu.com', timeout=10)
            logger.info(f"网络连接正常，状态码: {test_response.status_code}")
            
        except Exception as e:
            logger.error(f"设置requests会话失败: {e}")
            self.session = None
    
    def setup_driver(self):
        """初始化selenium浏览器驱动"""
        try:
            logger.info("正在初始化Selenium浏览器驱动...")
            
            chrome_options = Options()
            chrome_options.add_argument('--headless')  # 取消注释以显示浏览器窗口进行调试
            # chrome_options.add_argument('--no-headless')  # 取消注释以显示浏览器窗口
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument(f'--user-agent={self.ua.random}')
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            logger.info("下载并安装ChromeDriver...")
            service = Service(ChromeDriverManager().install())
            
            logger.info("启动Chrome浏览器...")
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # 执行反检测脚本
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            self.wait = WebDriverWait(self.driver, 10)
            
            # 测试浏览器
            logger.info("测试浏览器功能...")
            self.driver.get('https://www.baidu.com')
            logger.info(f"浏览器启动成功，当前URL: {self.driver.current_url}")
            
        except Exception as e:
            logger.error(f"浏览器驱动初始化失败: {e}")
            self.driver = None
    
    def close_driver(self):
        """关闭浏览器驱动"""
        try:
            if self.driver:
                logger.info("正在关闭浏览器驱动...")
                self.driver.quit()
                logger.info("浏览器驱动已关闭")
        except Exception as e:
            logger.error(f"关闭浏览器驱动时出错: {e}")
    
    def safe_request(self, url, max_retries=3):
        """安全的HTTP请求，带重试机制"""
        if not self.session:
            logger.error("requests会话不可用")
            return None
            
        for attempt in range(max_retries):
            try:
                logger.debug(f"HTTP请求尝试 {attempt + 1}/{max_retries}: {url}")
                response = self.session.get(url, timeout=self.timeout)
                response.raise_for_status()
                
                logger.debug(f"HTTP请求成功，状态码: {response.status_code}, 长度: {len(response.text)}")
                time.sleep(self.session_delay)
                return response
                
            except requests.exceptions.Timeout:
                logger.warning(f"请求超时 (尝试 {attempt + 1}/{max_retries}): {url}")
            except requests.exceptions.RequestException as e:
                logger.warning(f"请求异常 (尝试 {attempt + 1}/{max_retries}): {e}")
            except Exception as e:
                logger.error(f"未知错误 (尝试 {attempt + 1}/{max_retries}): {e}")
                
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2
                logger.info(f"等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)
        
        logger.error(f"请求最终失败: {url}")
        return None
    
    def get_all_fund_codes(self):
        """
        爬取天天基金网所有基金代码和名称
        返回: DataFrame格式的基金列表
        """
        url = "http://fund.eastmoney.com/allfund.html"
        
        logger.info(f"开始获取全市场基金列表: {url}")
        
        try:
            response = self.safe_request(url)
            if not response:
                logger.error("获取基金列表页面失败")
                return pd.DataFrame()
            
            logger.info("开始解析基金列表HTML...")
            html = etree.HTML(response.text)
            
            # XPath获取基金代码和名称
            fund_items = html.xpath('//*[@id="code_content"]/div/ul/li/div/a[1]/text()')
            logger.info(f"找到 {len(fund_items)} 个基金项目")
            
            fund_list = []
            for i, item in enumerate(fund_items):
                if i % 1000 == 0:
                    logger.debug(f"处理第 {i} 个基金项目...")
                    
                # 提取6位基金代码
                code_match = re.search(r'\((\d{6})\)', item)
                if code_match:
                    code = code_match.group(1)
                    name = re.sub(r'^\(.*?）', '', item).strip()
                    fund_list.append({
                        'fund_code': code,
                        'fund_name': name
                    })
                else:
                    logger.debug(f"无法解析基金代码: {item}")
            
            df = pd.DataFrame(fund_list)
            logger.info(f"成功获取 {len(df)} 只基金")
            
            # 保存到本地
            output_path = os.path.join(self.output_dir, 'all_fund_list.csv')
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
            logger.info(f"基金列表已保存至: {output_path}")
            return df
            
        except Exception as e:
            logger.error(f"获取基金列表失败: {e}", exc_info=True)
            return pd.DataFrame()
    
    def get_fund_info(self, fund_code):
        """
        获取单只基金的基本信息
        """
        url = f"https://fundf10.eastmoney.com/jbgk_{fund_code}.html"
        
        logger.info(f"获取基金信息: {fund_code} - {url}")
        
        try:
            response = self.safe_request(url)
            if not response:
                logger.error(f"获取基金 {fund_code} 页面失败")
                return {}
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 提取基金基本信息
            info = {}
            
            # 基金名称
            name_elem = soup.select_one('#bodydiv > div > div.fundInfo > div.title > h1')
            info['fund_name'] = name_elem.text.strip() if name_elem else ''
            logger.debug(f"基金 {fund_code} 名称: {info['fund_name']}")
            
            # 基金代码
            info['fund_code'] = fund_code
            
            # 其他信息（成立日期、基金经理等）
            info_table = soup.select_one('#bodydiv > div > div.fundInfo > div.info')
            if info_table:
                rows = info_table.find_all('p')
                logger.debug(f"找到 {len(rows)} 行信息")
                for row in rows:
                    text = row.get_text().strip()
                    if '成立日期' in text:
                        info['establish_date'] = text.split('：')[-1].strip()
                    elif '基金经理' in text:
                        info['manager'] = text.split('：')[-1].strip()
                    elif '基金公司' in text:
                        info['company'] = text.split('：')[-1].strip()
            
            logger.info(f"成功获取基金 {fund_code} 基本信息")
            return info
            
        except Exception as e:
            logger.error(f"获取基金 {fund_code} 信息失败: {e}", exc_info=True)
            return {}
    
    def get_fund_holdings(self, fund_code, years=None):
        """
        爬取指定基金的持仓数据
        years: 爬取的年份列表，None则爬取最新数据
        """
        if years is None:
            years = [datetime.now().year]
        
        logger.info(f"开始获取基金 {fund_code} 持仓数据，年份: {years}")
        
        if not self.driver:
            logger.error("浏览器驱动不可用，跳过动态加载")
            return pd.DataFrame()
        
        all_holdings = []
        
        for year in years:
            logger.info(f"正在爬取基金 {fund_code} {year}年持仓...")
            
            try:
                # 访问基金持仓页面
                url = f"https://fundf10.eastmoney.com/ccmx_{fund_code}.html"
                logger.info(f"访问持仓页面: {url}")
                
                start_time = time.time()
                self.driver.get(url)
                
                # 等待页面加载
                logger.info("等待页面加载...")
                self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                load_time = time.time() - start_time
                logger.info(f"页面加载完成，用时: {load_time:.2f}秒")
                
                time.sleep(3)
                
                # 检查并点击年份按钮
                logger.info(f"查找 {year} 年份按钮...")
                try:
                    year_button = self.wait.until(
                        EC.element_to_be_clickable(
                            (By.XPATH, f"//*[@id='pagebar']/div/label[@value='{year}']")
                        )
                    )
                    logger.info(f"找到 {year} 年份按钮，准备点击...")
                    
                    self.driver.execute_script("arguments[0].click();", year_button)
                    time.sleep(3)
                    logger.info(f"成功切换到 {year} 年份")
                    
                except Exception as e:
                    logger.warning(f"年份切换失败 {year}: {e}")
                    # 检查页面是否有任何年份按钮
                    try:
                        available_years = self.driver.find_elements(By.XPATH, "//*[@id='pagebar']/div/label")
                        year_texts = [elem.get_attribute('value') for elem in available_years]
                        logger.info(f"可用年份: {year_texts}")
                    except:
                        logger.warning("无法获取可用年份信息")
                    continue
                
                # 解析持仓表格
                logger.info("开始解析持仓表格...")
                soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                table = soup.select_one('#cctable > div > div')
                
                if not table:
                    logger.warning(f"未找到基金 {fund_code} {year}年持仓表格")
                    # 检查页面是否有表格
                    tables = soup.find_all('table')
                    logger.debug(f"页面中共有 {len(tables)} 个表格")
                    continue
                
                # 解析表格数据
                rows = table.find_all('tr')[1:]  # 跳过表头
                logger.info(f"找到 {len(rows)} 行持仓数据")
                
                for i, row in enumerate(rows):
                    if i % 10 == 0:
                        logger.debug(f"处理第 {i} 行持仓数据...")
                    
                    cols = row.find_all(['td', 'th'])
                    if len(cols) >= 7:
                        holding = {
                            'fund_code': fund_code,
                            'year': year,
                            'stock_code': cols[1].text.strip() if len(cols) > 1 and cols[1].text.strip() else '',
                            'stock_name': cols[2].text.strip() if len(cols) > 2 else '',
                            'hold_ratio': cols[3].text.strip() if len(cols) > 3 else '',
                            'hold_value': cols[4].text.strip() if len(cols) > 4 else '',
                            'stock_price': cols[5].text.strip() if len(cols) > 5 else '',
                            'hold_shares': cols[6].text.strip() if len(cols) > 6 else '',
                        }
                        all_holdings.append(holding)
                
                logger.info(f"基金 {fund_code} {year}年获取到 {len(rows)} 条持仓记录")
                time.sleep(1)  # 避免请求过快
                
            except Exception as e:
                logger.error(f"爬取基金 {fund_code} {year}年持仓失败: {e}", exc_info=True)
                continue
        
        # 转换为DataFrame
        if all_holdings:
            logger.info(f"基金 {fund_code} 总共获取到 {len(all_holdings)} 条持仓记录")
            df = pd.DataFrame(all_holdings)
            # 数据清洗
            df['hold_ratio'] = pd.to_numeric(df['hold_ratio'].str.replace('%', ''), errors='coerce')
            df['hold_value'] = pd.to_numeric(df['hold_value'].str.replace(',', ''), errors='coerce')
            df['stock_price'] = pd.to_numeric(df['stock_price'], errors='coerce')
            df['hold_shares'] = pd.to_numeric(df['hold_shares'].str.replace(',', ''), errors='coerce')
            return df
        else:
            logger.warning(f"基金 {fund_code} 未获取到任何持仓数据")
            return pd.DataFrame()
    
    def batch_crawl_fund_holdings(self, fund_list, max_funds=100, years=None):
        """
        批量爬取基金持仓数据
        """
        if years is None:
            years = [datetime.now().year]
        
        logger.info(f"开始批量爬取 {min(max_funds, len(fund_list))} 只基金的持仓数据")
        all_data = []
        
        for idx, fund in fund_list.iterrows():
            if idx >= max_funds:
                logger.info("达到最大基金数量限制，停止爬取")
                break
                
            fund_code = fund['fund_code']
            fund_name = fund.get('fund_name', '未知')
            logger.info(f"\n[{idx+1}/{min(max_funds, len(fund_list))}] 正在处理: {fund_name} ({fund_code})")
            
            start_time = time.time()
            try:
                # 获取基金基本信息
                logger.info(f"开始获取基金 {fund_code} 基本信息...")
                fund_info = self.get_fund_info(fund_code)
                
                # 获取持仓数据
                logger.info(f"开始获取基金 {fund_code} 持仓数据...")
                holdings = self.get_fund_holdings(fund_code, years)
                
                if not holdings.empty:
                    # 合并基本信息和持仓数据
                    holdings['fund_name'] = fund_info.get('fund_name', fund_name)
                    holdings['manager'] = fund_info.get('manager', '')
                    holdings['company'] = fund_info.get('company', '')
                    all_data.append(holdings)
                    logger.info(f"基金 {fund_code} 处理完成，获取到 {len(holdings)} 条记录")
                else:
                    logger.warning(f"基金 {fund_code} 未获取到持仓数据")
                
            except Exception as e:
                logger.error(f"处理基金 {fund_code} 时发生错误: {e}", exc_info=True)
                continue
            
            processing_time = time.time() - start_time
            logger.info(f"基金 {fund_code} 处理完成，用时: {processing_time:.2f}秒")
            
            # 避免请求过快
            time.sleep(2)
        
        # 合并所有数据
        if all_data:
            logger.info(f"开始合并 {len(all_data)} 个数据集...")
            result_df = pd.concat(all_data, ignore_index=True)
            
            # 保存结果
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'fund_holdings_{timestamp}.csv'
            output_path = os.path.join(self.output_dir, filename)
            result_df.to_csv(output_path, index=False, encoding='utf-8-sig')
            
            logger.info(f"\n批量爬取完成！")
            logger.info(f"总共获取 {len(result_df)} 条持仓记录")
            logger.info(f"数据已保存至: {output_path}")
            
            return result_df
        else:
            logger.warning("未获取到任何数据")
            return pd.DataFrame()
    
    def analyze_holdings(self, holdings_df):
        """
        分析持仓数据
        """
        if holdings_df.empty:
            logger.warning("没有数据可分析")
            return
        
        logger.info("\n=== 持仓数据分析 ===")
        
        # 1. 按基金公司统计
        logger.info("\n1. 各基金公司持仓股票数量统计:")
        try:
            company_stats = holdings_df.groupby('company').agg({
                'stock_name': 'nunique',
                'hold_value': 'sum'
            }).round(2)
            company_stats.columns = ['持仓股票数', '总持仓市值']
            top_companies = company_stats.sort_values('总持仓市值', ascending=False).head(10)
            logger.info(f"\n{top_companies}")
        except Exception as e:
            logger.error(f"公司统计分析失败: {e}")
        
        # 2. 热门股票统计
        logger.info("\n2. 热门持仓股票 Top 10:")
        try:
            hot_stocks = holdings_df.groupby('stock_name').agg({
                'fund_code': 'nunique',
                'hold_value': 'sum'
            }).round(2)
            hot_stocks.columns = ['持有基金数', '总持仓市值']
            top_stocks = hot_stocks.sort_values('总持仓市值', ascending=False).head(10)
            logger.info(f"\n{top_stocks}")
        except Exception as e:
            logger.error(f"热门股票分析失败: {e}")
        
        # 3. 持仓集中度分析
        logger.info("\n3. 各基金持仓集中度分析:")
        try:
            concentration = holdings_df.groupby('fund_code')['hold_ratio'].sum().sort_values(ascending=False)
            if len(concentration) > 0:
                logger.info(f"最高集中度基金: {concentration.index[0]} (集中度: {concentration.iloc[0]:.1f}%)")
                logger.info(f"平均集中度: {concentration.mean():.1f}%")
        except Exception as e:
            logger.error(f"集中度分析失败: {e}")

def get_fund_codes_from_report(file_path):
    """
    从市场监控报告中读取"弱买入"和"强买入"的基金代码。
    """
    logger.info(f"从报告文件读取基金代码: {file_path}")
    fund_codes = []
    try:
        if not os.path.exists(file_path):
            logger.error(f"报告文件不存在: {file_path}")
            return []
        
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            # 使用正则表达式匹配"弱买入"或"强买入"行
            pattern = re.compile(r'\|\s*(\d{6})\s*\|.*?\s*\|\s*(弱买入|强买入)\s*\|')
            matches = pattern.findall(content)
            for code, signal in matches:
                # 确保每个代码只添加一次
                if code not in fund_codes:
                    fund_codes.append(code)
        logger.info(f"从报告中获取到 {len(fund_codes)} 个待爬取的基金代码: {fund_codes}")
    except FileNotFoundError:
        logger.error(f"错误: 未找到文件 {file_path}")
        return []
    except Exception as e:
        logger.error(f"读取文件时出错: {e}")
        return []
    
    return fund_codes

def signal_handler(sig, frame):
    """处理Ctrl+C中断"""
    logger.info('用户中断程序 (Ctrl+C)')
    sys.exit(0)

def main():
    """主程序"""
    logger.info("=== 基金数据爬取系统启动 ===")
    
    # 设置信号处理
    signal.signal(signal.SIGINT, signal_handler)
    
    crawler = None
    try:
        # 步骤0: 初始化爬虫
        logger.info("=== 步骤0: 初始化爬虫 ===")
        crawler = FundDataCrawler(timeout=30)
        
        # 步骤1: 从报告文件中获取基金列表
        logger.info("\n=== 步骤1: 从报告中读取基金列表 ===")
        report_file = 'market_monitor_report.md'
        codes_to_crawl = get_fund_codes_from_report(report_file)
        
        if not codes_to_crawl:
            logger.error("未找到需要爬取的基金代码，程序退出")
            return
        
        # 将代码列表转换为DataFrame格式以适应原有函数
        fund_list_df = pd.DataFrame({'fund_code': codes_to_crawl, 'fund_name': ''})
        logger.info(f"准备爬取 {len(fund_list_df)} 只基金")
        
        # 步骤2: 批量爬取持仓数据
        logger.info("\n=== 步骤2: 批量爬取持仓数据 ===")
        years_to_crawl = [2024]  # 指定爬取年份
        holdings_data = crawler.batch_crawl_fund_holdings(
            fund_list_df, 
            max_funds=len(codes_to_crawl),
            years=years_to_crawl
        )
        
        # 步骤3: 数据分析
        if not holdings_data.empty:
            logger.info("\n=== 步骤3: 数据分析 ===")
            crawler.analyze_holdings(holdings_data)
        else:
            logger.warning("没有数据进行分析")
        
        logger.info("=== 程序执行完成 ===")
        
    except KeyboardInterrupt:
        logger.info("\n用户中断程序")
    except Exception as e:
        logger.error(f"程序执行出错: {e}", exc_info=True)
    finally:
        if crawler:
            crawler.close_driver()
        logger.info("程序结束")

if __name__ == "__main__":
    main()

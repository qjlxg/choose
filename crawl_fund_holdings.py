import logging
import requests
import time
import os
import re
import pandas as pd
from bs4 import BeautifulSoup
from lxml import etree
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from fake_useragent import UserAgent
from webdriver_manager.chrome import ChromeDriverManager

# 配置日志
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# Selenium debug log
selenium_logger = logging.getLogger('selenium')
selenium_logger.setLevel(logging.DEBUG)
webdriver_manager_logger = logging.getLogger('webdriver_manager')
webdriver_manager_logger.setLevel(logging.DEBUG)


class FundDataCrawler:
    """爬取基金持仓数据的类"""

    def __init__(self, output_dir='fund_data', timeout=30):
        """
        初始化爬虫
        :param output_dir: 数据保存的目录
        :param timeout: 请求超时时间
        """
        logger.info("初始化爬虫，输出目录: %s, 超时时间: %ss", output_dir, timeout)
        self.output_dir = output_dir
        self.timeout = timeout
        self.session = requests.Session()
        self.ua = UserAgent()
        self.driver = None
        self.wait = None
        self.total_funds_crawled = 0
        self.failed_funds = []
        
        # requests 基础配置
        logger.info("正在设置requests会话...")
        self.session.headers.update({'User-Agent': self.ua.random})
        self.session.mount('http://', requests.adapters.HTTPAdapter(max_retries=3))
        self.session.mount('https://', requests.adapters.HTTPAdapter(max_retries=3))

    def _test_network(self):
        """测试网络连接"""
        logger.info("测试网络连接...")
        try:
            response = self.session.get('https://www.baidu.com', timeout=5)
            response.raise_for_status()
            logger.info("网络连接正常，状态码: %s", response.status_code)
            return True
        except requests.exceptions.RequestException as e:
            logger.error("网络连接失败: %s", e)
            return False

    def setup_driver(self):
        """初始化selenium浏览器驱动"""
        try:
            logger.info("正在初始化Selenium浏览器驱动...")
            
            chrome_options = Options()
            # 基础无头模式配置
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument(f'--user-agent={self.ua.random}')
            
            # 新增：增强无头模式稳定性的参数
            chrome_options.add_argument('--disable-infobars')
            chrome_options.add_argument('--disable-extensions')
            chrome_options.add_argument('--disable-browser-side-navigation')
            chrome_options.add_argument('--disable-features=IsolateOrigins,site-per-process')
            chrome_options.add_argument('--single-process') # 确保所有进程在单个进程中运行
            
            # 反检测脚本
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            
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
        if self.driver:
            self.driver.quit()
            logger.info("浏览器驱动已关闭")

    def get_fund_info(self, fund_code):
        """
        获取基金的基本信息，如基金名称
        :param fund_code: 基金代码
        :return: 基金名称
        """
        url = f"https://fundf10.eastmoney.com/jbgk_{fund_code}.html"
        logger.info(f"获取基金信息: {fund_code} - {url}")
        
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            logger.debug(f"HTTP请求成功，状态码: {response.status_code}, 长度: {len(response.text)}")
            
            soup = BeautifulSoup(response.text, 'lxml')
            fund_name_tag = soup.select_one('.fundDetail-main .fund-name')
            if fund_name_tag:
                fund_name = fund_name_tag.text.strip().split('(')[0].strip()
                logger.debug(f"基金 {fund_code} 名称: {fund_name}")
                return fund_name
            else:
                logger.warning(f"无法找到基金 {fund_code} 名称")
                return ""

        except requests.exceptions.RequestException as e:
            logger.error(f"获取基金 {fund_code} 基本信息失败: {e}")
            return ""

    def get_fund_holdings(self, fund_code, fund_name, years):
        """
        获取基金的持仓数据
        :param fund_code: 基金代码
        :param fund_name: 基金名称
        :param years: 要爬取的年份列表
        :return: 基金持仓数据DataFrame
        """
        if not self.driver:
            logger.error("浏览器驱动不可用，跳过动态加载")
            return None

        data = []
        for year in years:
            try:
                url = f"https://fundf10.eastmoney.com/FundTopHoldings.aspx?type=block&fundCode={fund_code}&year={year}"
                logger.info(f"正在获取基金 {fund_code} 持仓数据，年份: {year} - {url}")
                self.driver.get(url)

                # 等待表格加载
                self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '#holder-container .boxContent')))
                
                # 获取表格HTML并解析
                page_source = self.driver.page_source
                soup = BeautifulSoup(page_source, 'lxml')
                tables = soup.select('#holder-container .boxContent table')
                
                for table in tables:
                    # 尝试解析季度信息，例如 "2024年一季度"
                    quarter_match = re.search(r'(\d{4}年.*?季度)', table.previous_sibling.get_text() if table.previous_sibling else '', re.IGNORECASE)
                    if quarter_match:
                        quarter_info = quarter_match.group(1)
                    else:
                        quarter_info = f"{year}年未知季度"
                        
                    df_quarter = pd.read_html(str(table), encoding='utf-8')[0]
                    df_quarter.columns = ['股票代码', '股票名称', '持仓市值(万元)', '持仓占净值比例(%)', '持股数(万股)', '备注']
                    df_quarter['基金代码'] = fund_code
                    df_quarter['基金名称'] = fund_name
                    df_quarter['报告期'] = quarter_info
                    data.append(df_quarter)

            except Exception as e:
                logger.warning(f"获取基金 {fund_code} 年份 {year} 的持仓数据失败: {e}")
                
        if not data:
            logger.warning(f"基金 {fund_code} 未获取到持仓数据")
            return None

        df_all_years = pd.concat(data, ignore_index=True)
        return df_all_years

    def read_fund_codes_from_report(self, report_path):
        """
        从市场监控报告文件中读取基金代码
        :param report_path: 报告文件路径
        :return: 基金代码列表
        """
        if not os.path.exists(report_path):
            logger.error(f"报告文件不存在: {report_path}")
            return []

        logger.info(f"从报告文件读取基金代码: {report_path}")
        with open(report_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 使用正则表达式匹配表格中的基金代码
        fund_codes = re.findall(r'\| ([\d]{6}) \|', content)
        
        if fund_codes:
            unique_codes = sorted(list(set(fund_codes)))
            logger.info(f"从报告中获取到 {len(unique_codes)} 个待爬取的基金代码: {unique_codes}")
            return unique_codes
        else:
            logger.warning("未能在报告中找到基金代码")
            return []

    def save_data(self, df, fund_code, fund_name):
        """
        将数据保存到CSV文件
        :param df: 要保存的DataFrame
        :param fund_code: 基金代码
        :param fund_name: 基金名称
        """
        if df is None or df.empty:
            logger.warning(f"基金 {fund_code} 无数据可保存")
            return
            
        # 移除名称中的特殊字符
        sanitized_name = re.sub(r'[\\/:*?"<>|]', '', fund_name)
        file_path = os.path.join(self.output_dir, f"{fund_code}_{sanitized_name}_持仓数据.csv")
        
        try:
            df.to_csv(file_path, index=False, encoding='utf-8-sig')
            logger.info(f"成功保存基金 {fund_code} 的数据到: {file_path}")
        except Exception as e:
            logger.error(f"保存基金 {fund_code} 数据失败: {e}")

    def run_single_fund_crawl(self, fund_code):
        """
        执行单个基金的爬取任务
        :param fund_code: 基金代码
        """
        start_time = time.time()
        logger.info(f"正在处理: {fund_code}")
        
        try:
            # 步骤1: 获取基金基本信息
            fund_name = self.get_fund_info(fund_code)
            logger.info(f"成功获取基金 {fund_code} 基本信息")

            # 步骤2: 获取基金持仓数据
            df_holdings = self.get_fund_holdings(fund_code, fund_name, years=[2024])
            
            # 步骤3: 保存数据
            self.save_data(df_holdings, fund_code, fund_name)
            
            self.total_funds_crawled += 1
            
        except Exception as e:
            logger.error(f"处理基金 {fund_code} 时发生错误: {e}")
            self.failed_funds.append(fund_code)

        end_time = time.time()
        logger.info(f"基金 {fund_code} 处理完成，用时: {end_time - start_time:.2f}秒")

def main():
    """主函数"""
    logger.info("=== 基金数据爬取系统启动 ===")
    
    # 步骤0: 初始化爬虫
    logger.info("=== 步骤0: 初始化爬虫 ===")
    crawler = FundDataCrawler()
    
    if not crawler._test_network():
        logger.error("网络连接失败，程序退出。")
        return
        
    # 步骤1: 从报告中读取基金列表
    logger.info("\n=== 步骤1: 从报告中读取基金列表 ===")
    fund_codes = crawler.read_fund_codes_from_report('market_monitor_report.md')
    
    if not fund_codes:
        logger.error("未找到基金代码，程序退出。")
        return
        
    logger.info(f"准备爬取 {len(fund_codes)} 只基金")

    # 步骤2: 批量爬取持仓数据
    logger.info("\n=== 步骤2: 批量爬取持仓数据 ===")
    crawler.setup_driver()
    if not crawler.driver:
        logger.error("无法启动浏览器驱动，无法进行动态网页爬取。")
    else:
        logger.info(f"开始批量爬取 {len(fund_codes)} 只基金的持仓数据")
        for i, code in enumerate(fund_codes, 1):
            logger.info(f"\n[{i}/{len(fund_codes)}] 正在处理: {code}")
            crawler.run_single_fund_crawl(code)
            time.sleep(2) # 礼貌性延迟

    # 步骤3: 总结
    crawler.close_driver()
    logger.info("\n=== 步骤3: 爬取任务总结 ===")
    logger.info(f"总计处理基金数: {crawler.total_funds_crawled}")
    if crawler.failed_funds:
        logger.warning(f"以下基金爬取失败: {', '.join(crawler.failed_funds)}")
    else:
        logger.info("所有基金均成功处理。")

if __name__ == "__main__":
    if not os.path.exists('fund_data'):
        os.makedirs('fund_data')
        logger.info("创建输出目录: fund_data")
    else:
        logger.info("输出目录已存在: fund_data")
        
    main()

import requests
from lxml import etree
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import time
import re
from fake_useragent import UserAgent
import os
from datetime import datetime
import re

class FundDataCrawler:
    def __init__(self, output_dir='fund_data'):
        self.session = requests.Session()
        self.ua = UserAgent()
        self.output_dir = output_dir
        self.setup_session()
        self.setup_driver()
        self.ensure_output_directory()
    
    def ensure_output_directory(self):
        """确保输出目录存在"""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            print(f"已创建输出目录: {self.output_dir}")
            
    def setup_session(self):
        """设置requests会话"""
        headers = {
            'User-Agent': self.ua.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        self.session.headers.update(headers)
    
    def setup_driver(self):
        """初始化selenium浏览器驱动"""
        chrome_options = Options()
        chrome_options.add_argument('--headless')  # 无头模式
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument(f'--user-agent={self.ua.random}')
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            self.wait = WebDriverWait(self.driver, 10)
        except Exception as e:
            print(f"浏览器驱动初始化失败: {e}")
            self.driver = None
    
    def close_driver(self):
        """关闭浏览器驱动"""
        if self.driver:
            self.driver.quit()
    
    def get_all_fund_codes(self):
        """
        爬取天天基金网所有基金代码和名称
        返回: DataFrame格式的基金列表
        """
        url = "http://fund.eastmoney.com/allfund.html"
        
        try:
            print("正在获取全市场基金列表...")
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            # 使用lxml解析
            html = etree.HTML(response.text)
            
            # XPath获取基金代码和名称
            fund_items = html.xpath('//*[@id="code_content"]/div/ul/li/div/a[1]/text()')
            
            fund_list = []
            for item in fund_items:
                # 提取6位基金代码
                code_match = re.search(r'\((\d{6})\)', item)
                if code_match:
                    code = code_match.group(1)
                    name = re.sub(r'^\(.*?）', '', item).strip()
                    fund_list.append({
                        'fund_code': code,
                        'fund_name': name
                    })
            
            df = pd.DataFrame(fund_list)
            print(f"成功获取 {len(df)} 只基金")
            
            # 保存到本地
            output_path = os.path.join(self.output_dir, 'all_fund_list.csv')
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
            print(f"基金列表已保存至: {output_path}")
            return df
            
        except Exception as e:
            print(f"获取基金列表失败: {e}")
            return pd.DataFrame()
    
    def get_fund_info(self, fund_code):
        """
        获取单只基金的基本信息
        """
        url = f"https://fundf10.eastmoney.com/jbgk_{fund_code}.html"
        
        try:
            response = self.session.get(url, timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 提取基金基本信息
            info = {}
            
            # 基金名称
            name_elem = soup.select_one('#bodydiv > div > div.fundInfo > div.title > h1')
            info['fund_name'] = name_elem.text.strip() if name_elem else ''
            
            # 基金代码
            info['fund_code'] = fund_code
            
            # 其他信息（成立日期、基金经理等）
            info_table = soup.select_one('#bodydiv > div > div.fundInfo > div.info')
            if info_table:
                rows = info_table.find_all('p')
                for row in rows:
                    text = row.get_text().strip()
                    if '成立日期' in text:
                        info['establish_date'] = text.split('：')[-1].strip()
                    elif '基金经理' in text:
                        info['manager'] = text.split('：')[-1].strip()
                    elif '基金公司' in text:
                        info['company'] = text.split('：')[-1].strip()
            
            return info
            
        except Exception as e:
            print(f"获取基金 {fund_code} 信息失败: {e}")
            return {}
    
    def get_fund_holdings(self, fund_code, years=None):
        """
        爬取指定基金的持仓数据
        years: 爬取的年份列表，None则爬取最新数据
        """
        if years is None:
            years = [datetime.now().year]
        
        all_holdings = []
        
        for year in years:
            try:
                print(f"正在爬取基金 {fund_code} {year}年持仓...")
                
                if not self.driver:
                    print("浏览器驱动不可用，跳过动态加载")
                    continue
                
                # 访问基金持仓页面
                url = f"https://fundf10.eastmoney.com/ccmx_{fund_code}.html"
                self.driver.get(url)
                time.sleep(3)
                
                # 切换到指定年份
                try:
                    year_button = self.wait.until(
                        EC.element_to_be_clickable(
                            (By.XPATH, f"//*[@id='pagebar']/div/label[@value='{year}']")
                        )
                    )
                    year_button.click()
                    time.sleep(3)
                except Exception as e:
                    print(f"年份切换失败 {year}: {e}")
                    continue
                
                # 解析持仓表格
                soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                table = soup.select_one('#cctable > div > div')
                
                if not table:
                    print(f"未找到基金 {fund_code} {year}年持仓表格")
                    continue
                
                # 解析表格数据
                rows = table.find_all('tr')[1:]  # 跳过表头
                for row in rows:
                    cols = row.find_all(['td', 'th'])
                    if len(cols) >= 7:
                        holding = {
                            'fund_code': fund_code,
                            'year': year,
                            'stock_code': cols[1].text.strip() if cols[1].text.strip() else '',
                            'stock_name': cols[2].text.strip(),
                            'hold_ratio': cols[3].text.strip(),
                            'hold_value': cols[4].text.strip(),
                            'stock_price': cols[5].text.strip(),
                            'hold_shares': cols[6].text.strip(),
                        }
                        all_holdings.append(holding)
                
                print(f"基金 {fund_code} {year}年获取到 {len(rows)} 条持仓记录")
                time.sleep(1)  # 避免请求过快
                
            except Exception as e:
                print(f"爬取基金 {fund_code} {year}年持仓失败: {e}")
                continue
        
        # 转换为DataFrame
        if all_holdings:
            df = pd.DataFrame(all_holdings)
            # 数据清洗
            df['hold_ratio'] = pd.to_numeric(df['hold_ratio'].str.replace('%', ''), errors='coerce')
            df['hold_value'] = pd.to_numeric(df['hold_value'].str.replace(',', ''), errors='coerce')
            df['stock_price'] = pd.to_numeric(df['stock_price'], errors='coerce')
            df['hold_shares'] = pd.to_numeric(df['hold_shares'].str.replace(',', ''), errors='coerce')
            return df
        else:
            return pd.DataFrame()
    
    def batch_crawl_fund_holdings(self, fund_list, max_funds=100, years=None):
        """
        批量爬取基金持仓数据
        """
        if years is None:
            years = [datetime.now().year]
        
        all_data = []
        
        for idx, fund in fund_list.iterrows():
            if idx >= max_funds:
                break
                
            fund_code = fund['fund_code']
            print(f"\n[{idx+1}/{min(max_funds, len(fund_list))}] 正在处理: {fund['fund_name']} ({fund_code})")
            
            # 获取基金基本信息
            fund_info = self.get_fund_info(fund_code)
            
            # 获取持仓数据
            holdings = self.get_fund_holdings(fund_code, years)
            
            if not holdings.empty:
                # 合并基本信息和持仓数据
                holdings['fund_name'] = fund_info.get('fund_name', fund['fund_name'])
                holdings['manager'] = fund_info.get('manager', '')
                holdings['company'] = fund_info.get('company', '')
                all_data.append(holdings)
            
            # 避免请求过快
            time.sleep(2)
        
        # 合并所有数据
        if all_data:
            result_df = pd.concat(all_data, ignore_index=True)
            
            # 保存结果
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'fund_holdings_{timestamp}.csv'
            output_path = os.path.join(self.output_dir, filename)
            result_df.to_csv(output_path, index=False, encoding='utf-8-sig')
            
            print(f"\n批量爬取完成！")
            print(f"总共获取 {len(result_df)} 条持仓记录")
            print(f"数据已保存至: {output_path}")
            
            return result_df
        else:
            print("未获取到任何数据")
            return pd.DataFrame()
    
    def analyze_holdings(self, holdings_df):
        """
        分析持仓数据
        """
        if holdings_df.empty:
            print("没有数据可分析")
            return
        
        print("\n=== 持仓数据分析 ===")
        
        # 1. 按基金类型统计
        print("\n1. 各基金公司持仓股票数量统计:")
        company_stats = holdings_df.groupby('company').agg({
            'stock_name': 'nunique',
            'hold_value': 'sum'
        }).round(2)
        company_stats.columns = ['持仓股票数', '总持仓市值']
        print(company_stats.sort_values('总持仓市值', ascending=False).head(10))
        
        # 2. 热门股票统计
        print("\n2. 热门持仓股票 Top 10:")
        hot_stocks = holdings_df.groupby('stock_name').agg({
            'fund_code': 'nunique',
            'hold_value': 'sum'
        }).round(2)
        hot_stocks.columns = ['持有基金数', '总持仓市值']
        print(hot_stocks.sort_values('总持仓市值', ascending=False).head(10))
        
        # 3. 持仓集中度分析
        print("\n3. 各基金持仓集中度分析:")
        concentration = holdings_df.groupby('fund_code').apply(
            lambda x: x['hold_ratio'].sum()
        ).sort_values(ascending=False)
        print(f"最高集中度基金: {concentration.index[0]} (集中度: {concentration.iloc[0]:.1f}%)")
        print(f"平均集中度: {concentration.mean():.1f}%")

def get_fund_codes_from_report(file_path):
    """
    从市场监控报告中读取“弱买入”和“强买入”的基金代码。
    """
    fund_codes = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            # 使用正则表达式匹配“弱买入”或“强买入”行
            pattern = re.compile(r'\|\s*(\d{6})\s*\|.*?\s*\|\s*(弱买入|强买入)\s*\|')
            matches = pattern.findall(content)
            for code, signal in matches:
                # 确保每个代码只添加一次
                if code not in fund_codes:
                    fund_codes.append(code)
        print(f"从报告中获取到 {len(fund_codes)} 个待爬取的基金代码。")
    except FileNotFoundError:
        print(f"错误: 未找到文件 {file_path}")
        return []
    except Exception as e:
        print(f"读取文件时出错: {e}")
        return []
    
    return fund_codes

def main():
    """主程序"""
    crawler = FundDataCrawler()
    
    try:
        # 步骤1: 从报告文件中获取基金列表
        print("=== 步骤1: 从报告中读取基金列表 ===")
        report_file = 'market_monitor_report.md'
        codes_to_crawl = get_fund_codes_from_report(report_file)
        
        if not codes_to_crawl:
            print("未找到需要爬取的基金代码，程序退出")
            return
        
        # 将代码列表转换为DataFrame格式以适应原有函数
        fund_list_df = pd.DataFrame({'fund_code': codes_to_crawl, 'fund_name': ''})
        
        # 步骤2: 批量爬取持仓数据
        print("\n=== 步骤2: 批量爬取持仓数据 ===")
        years_to_crawl = [2024]  # 指定爬取年份
        holdings_data = crawler.batch_crawl_fund_holdings(
            fund_list_df, 
            max_funds=len(codes_to_crawl),
            years=years_to_crawl
        )
        
        # 步骤3: 数据分析
        if not holdings_data.empty:
            print("\n=== 步骤3: 数据分析 ===")
            crawler.analyze_holdings(holdings_data)
        
    except KeyboardInterrupt:
        print("\n用户中断程序")
    except Exception as e:
        print(f"程序执行出错: {e}")
    finally:
        crawler.close_driver()
        print("程序结束")

if __name__ == "__main__":
    main()

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
import json
import ast

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
            'Referer': 'https://fundf10.eastmoney.com/',
        }
        self.session.headers.update(headers)
    
    def setup_driver(self):
        """初始化selenium浏览器驱动"""
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument(f'--user-agent={self.ua.random}')
        
        try:
            # 使用 webdriver-manager 自动下载并安装合适的 chromedriver
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            self.wait = WebDriverWait(self.driver, 10)
            print("浏览器驱动初始化成功")
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
        爬取指定基金的持仓数据（使用Selenium，适用于ccmx页面）
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

    def _parse_text_content(self, content, fund_code, year):
        """
        纯文本内容解析（私有方法）- 新增
        """
        holdings = []
        lines = content.split('\n')
        in_table = False
        current_quarter = ''
        data_rows = []
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # 识别季度标题
            quarter_match = re.search(rf'{year}年(\d+)季度股票投资明细', line)
            if quarter_match:
                # 处理上一季度数据
                if data_rows:
                    holdings.extend(self._parse_table_rows(data_rows, fund_code, year, current_quarter))
                    data_rows = []
                current_quarter = quarter_match.group(1)
                in_table = True
                continue
            
            # 识别数据行（以序号开头）
            if in_table and re.match(r'^\d+\t', line):
                data_rows.append(line)
            elif in_table and ('显示全部持仓明细' in line or '>>' in line):
                # 表格结束
                if data_rows:
                    holdings.extend(self._parse_table_rows(data_rows, fund_code, year, current_quarter))
                    data_rows = []
                in_table = False
        
        # 处理最后季度
        if data_rows:
            holdings.extend(self._parse_table_rows(data_rows, fund_code, year, current_quarter))
        
        return holdings

    def _parse_table_rows(self, rows, fund_code, year, quarter):
        """
        解析单季度表格行（私有方法）- 新增
        """
        holdings = []
        for row in rows:
            fields = [f.strip() for f in row.split('\t') if f.strip()]
            if len(fields) < 6 or not fields[0].isdigit():
                continue
            
            rank = int(fields[0])
            stock_code = fields[1]
            stock_name = fields[2]
            
            # 动态检测格式：Q2 有更多字段（最新价、涨跌幅、相关资讯合并）
            if len(fields) >= 9:  # Q2 格式（序号+代码+名称+最新价+涨跌幅+资讯+比例+持股+市值）
                hold_ratio = fields[6].replace('%', '')
                hold_shares = fields[7].replace(',', '')
                hold_value = fields[8].replace(',', '')
            else:  # Q1 格式（序号+代码+名称+资讯+比例+持股+市值）
                hold_ratio = fields[3].replace('%', '')
                hold_shares = fields[4].replace(',', '')
                hold_value = fields[5].replace(',', '')
            
            holding = {
                'fund_code': fund_code,
                'year': year,
                'quarter': quarter,
                'rank': rank,
                'stock_code': stock_code,
                'stock_name': stock_name,
                'hold_ratio': hold_ratio,
                'hold_shares': hold_shares,
                'hold_value': hold_value
            }
            holdings.append(holding)
        
        return holdings

    def get_fund_holdings_from_api(self, fund_code, years=None):
        """
        通过新的API链接爬取基金持仓数据 - 优化版（纯文本解析）
        """
        if years is None:
            years = [datetime.now().year]
        
        all_holdings = []
        
        for year in years:
            try:
                print(f"正在通过API爬取基金 {fund_code} {year}年持仓...")
                
                # 构建API链接
                url = f"https://fundf10.eastmoney.com/FundArchivesDatas.aspx?type=jjcc&code={fund_code}&topline=10&year={year}"
                response = self.session.get(url, timeout=10)
                response.raise_for_status()
                
                # 解析返回的JavaScript数据
                match = re.search(r'var apidata=(.*?);', response.text, re.DOTALL)
                if not match:
                    print(f"未在响应中找到基金 {fund_code} {year}年的数据")
                    continue
                
                # 使用正则表达式安全地提取 content 字符串（处理转义）
                content_match = re.search(r'content:"(.*)"', match.group(1), re.DOTALL | re.MULTILINE)
                content = content_match.group(1) if content_match else ''
                
                if not content.strip():
                    print(f"基金 {fund_code} {year}年 content 为空")
                    continue
                
                # 使用新的纯文本解析方法
                holdings = self._parse_text_content(content, fund_code, year)
                all_holdings.extend(holdings)
                print(f"基金 {fund_code} {year}年获取到 {len(holdings)} 条持仓记录")
                
                # 调试：打印前200字符查看内容
                if not holdings:
                    print(f"调试信息 - content 前200字符: {content[:200]}")
                
                time.sleep(1)  # 避免请求过快
                
            except Exception as e:
                print(f"爬取基金 {fund_code} {year}年持仓失败: {e}")
                import traceback
                traceback.print_exc()
                continue
        
        if all_holdings:
            df = pd.DataFrame(all_holdings)
            # 数据清洗为数值类型
            df['hold_ratio'] = pd.to_numeric(df['hold_ratio'], errors='coerce')
            df['hold_shares'] = pd.to_numeric(df['hold_shares'], errors='coerce')
            df['hold_value'] = pd.to_numeric(df['hold_value'], errors='coerce')
            
            # 数据质量检查
            print(f"  数据质量检查:")
            print(f"    - 总记录数: {len(df)}")
            if len(df) > 0:
                print(f"    - 平均持仓比例: {df['hold_ratio'].mean():.2f}%")
                print(f"    - 总持仓市值: {df['hold_value'].sum():,.2f}万元")
            
            return df
        else:
            print(f"基金 {fund_code} 未获取到任何持仓数据")
            return pd.DataFrame()
    
    def get_fund_holdings_from_api_backup(self, fund_code, years=None):
        """
        通过新的API链接爬取基金持仓数据 - 原始版本（作为备份）
        """
        if years is None:
            years = [datetime.now().year]
        
        all_holdings = []
        
        for year in years:
            try:
                print(f"正在通过API备份方法爬取基金 {fund_code} {year}年持仓...")
                
                # 构建API链接
                url = f"https://fundf10.eastmoney.com/FundArchivesDatas.aspx?type=jjcc&code={fund_code}&topline=10&year={year}"
                response = self.session.get(url, timeout=10)
                response.raise_for_status()
                
                # 解析返回的JavaScript数据
                match = re.search(r'var apidata=(.*?);', response.text, re.DOTALL)
                if not match:
                    print(f"未在响应中找到基金 {fund_code} {year}年的数据")
                    continue
                
                # 使用正则表达式安全地提取 content 字符串
                content_match = re.search(r'content:"(.*)"', match.group(1), re.DOTALL)
                content = content_match.group(1) if content_match else ''
                
                # 使用 BeautifulSoup 解析 HTML 内容
                soup = BeautifulSoup(content, 'html.parser')
                
                # 找到所有的表格
                tables = soup.find_all('table', {'class': 'w780'})
                
                if not tables:
                    print(f"未找到基金 {fund_code} {year}年持仓表格")
                    continue
                
                for table in tables:
                    # 获取季度信息
                    quarter_info_elem = table.find_previous_sibling('h3')
                    quarter_info = quarter_info_elem.text.strip().split('  ')[0] if quarter_info_elem else f"{year}年未知季度"
                    
                    # 遍历表格中的每一行
                    rows = table.find_all('tr')[1:] # 跳过表头
                    for row in rows:
                        cols = row.find_all('td')
                        if len(cols) >= 6:
                            holding = {
                                'fund_code': fund_code,
                                'year': year,
                                'quarter': quarter_info,
                                'stock_code': cols[1].text.strip(),
                                'stock_name': cols[2].text.strip(),
                                'hold_ratio': cols[3].text.strip().replace('%', ''),
                                'hold_shares': cols[4].text.strip().replace(',', ''),
                                'hold_value': cols[5].text.strip().replace(',', '')
                            }
                            all_holdings.append(holding)
                
                print(f"基金 {fund_code} {year}年获取到 {len(all_holdings)} 条持仓记录")

            except Exception as e:
                print(f"爬取基金 {fund_code} {year}年持仓失败: {e}")
                continue
        
        if all_holdings:
            df = pd.DataFrame(all_holdings)
            # 数据清洗
            df['hold_ratio'] = pd.to_numeric(df['hold_ratio'], errors='coerce')
            df['hold_value'] = pd.to_numeric(df['hold_value'], errors='coerce')
            df['hold_shares'] = pd.to_numeric(df['hold_shares'], errors='coerce')
            return df
        else:
            return pd.DataFrame()
    
    def test_api_parsing(self, fund_code='014192', year=2025):
        """
        测试API解析功能 - 新增测试方法
        """
        print(f"\n🧪 测试API解析 - 基金 {fund_code} {year}年")
        
        # 测试新方法
        print("\n1. 测试新纯文本解析方法:")
        holdings_new = self.get_fund_holdings_from_api(fund_code, years=[year])
        
        if not holdings_new.empty:
            print(f"✅ 新方法成功! 获取 {len(holdings_new)} 条记录")
            print("\n前5条记录:")
            display_cols = ['year', 'quarter', 'rank', 'stock_code', 'stock_name', 'hold_ratio', 'hold_value']
            print(holdings_new[display_cols].head().to_string(index=False))
        else:
            print("❌ 新方法失败")
        
        # 测试Selenium备份
        print("\n2. 测试Selenium备份方法:")
        holdings_selenium = self.get_fund_holdings(fund_code, years=[year])
        
        if not holdings_selenium.empty:
            print(f"✅ Selenium方法成功! 获取 {len(holdings_selenium)} 条记录")
            print("\n前5条记录:")
            display_cols_selenium = ['year', 'stock_code', 'stock_name', 'hold_ratio', 'hold_value']
            print(holdings_selenium[display_cols_selenium].head().to_string(index=False))
        else:
            print("❌ Selenium方法失败")
    
    def batch_crawl_fund_holdings(self, fund_list, max_funds=100, years=None, use_backup=False):
        """
        批量爬取基金持仓数据 - 增强版
        """
        if years is None:
            years = [datetime.now().year]
        
        all_data = []
        failed_funds = []
        successful_funds = []
        
        print(f"\n🔄 开始批量爬取 {min(max_funds, len(fund_list))} 只基金")
        print(f"   爬取年份: {', '.join(map(str, years))}")
        print(f"   使用方法: {'备份方法' if use_backup else '主方法'}")
        
        for idx, fund in fund_list.iterrows():
            if idx >= max_funds:
                break
                
            fund_code = fund['fund_code']
            fund_name = fund.get('fund_name', f'基金{fund_code}')
            print(f"\n[{idx+1}/{min(max_funds, len(fund_list))}] 正在处理: {fund_name} ({fund_code})")
            
            # 获取基金基本信息
            fund_info = self.get_fund_info(fund_code)
            full_fund_name = fund_info.get('fund_name', fund_name)
            company = fund_info.get('company', '未知')
            
            # 根据参数选择使用主方法还是备份方法
            if use_backup:
                holdings = self.get_fund_holdings_from_api_backup(fund_code, years)
            else:
                # 主方法：先尝试API新方法，如果失败则尝试Selenium
                holdings = self.get_fund_holdings_from_api(fund_code, years)
                
                if holdings.empty:
                    print(f"    API主方法失败，尝试Selenium备份...")
                    holdings = self.get_fund_holdings(fund_code, years)
                
                if holdings.empty:
                    print(f"    Selenium备份也失败，跳过此基金")
                    failed_funds.append(fund_code)
                    continue
            
            if not holdings.empty:
                # 合并基本信息和持仓数据
                holdings['fund_name'] = full_fund_name
                holdings['manager'] = fund_info.get('manager', '')
                holdings['company'] = company
                all_data.append(holdings)
                successful_funds.append(fund_code)
                print(f"    ✓ 成功获取 {len(holdings)} 条记录 (公司: {company})")
            else:
                failed_funds.append(fund_code)
                print(f"    ✗ 获取失败")
            
            # 避免请求过快
            time.sleep(2)
        
        # 合并所有数据
        if all_data:
            result_df = pd.concat(all_data, ignore_index=True)
            
            # 数据质量检查
            print(f"\n📊 批量爬取完成统计:")
            print(f"   成功基金: {len(successful_funds)} 只")
            print(f"   失败基金: {len(failed_funds)} 只")
            print(f"   总记录数: {len(result_df):,}")
            print(f"   涉及股票: {result_df['stock_name'].nunique()}")
            
            if len(result_df) > 0:
                print(f"   平均持仓比例: {result_df['hold_ratio'].mean():.2f}%")
                print(f"   总持仓市值: {result_df['hold_value'].sum():,.2f}万元")
            
            # 保存结果
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'fund_holdings_{timestamp}.csv'
            output_path = os.path.join(self.output_dir, filename)
            result_df.to_csv(output_path, index=False, encoding='utf-8-sig')
            
            # 保存成功基金列表
            success_file = os.path.join(self.output_dir, f'successful_funds_{timestamp}.txt')
            with open(success_file, 'w', encoding='utf-8') as f:
                for code in successful_funds:
                    fund_info = self.get_fund_info(code)
                    f.write(f"{code}\t{fund_info.get('fund_name', '未知')}\n")
            
            # 保存失败基金列表
            if failed_funds:
                failed_file = os.path.join(self.output_dir, f'failed_funds_{timestamp}.txt')
                with open(failed_file, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(failed_funds))
                print(f"   失败基金列表已保存至: {failed_file}")
            
            print(f"\n💾 数据已保存至: {output_path}")
            print(f"   成功基金列表已保存至: {success_file}")
            
            return result_df
        else:
            print("❌ 未获取到任何数据")
            return pd.DataFrame()
    
    def analyze_holdings(self, holdings_df):
        """
        分析持仓数据 - 增强版
        """
        if holdings_df.empty:
            print("❌ 没有数据可分析")
            return
        
        print("\n" + "="*80)
        print("                    基金持仓数据分析报告")
        print("="*80)
        
        # 1. 基本统计
        print(f"\n📊 基本统计信息:")
        print(f"   总记录数: {len(holdings_df):,}")
        print(f"   涉及基金数: {holdings_df['fund_code'].nunique()}")
        print(f"   涉及股票数: {holdings_df['stock_name'].nunique()}")
        print(f"   数据年份范围: {holdings_df['year'].min()}-{holdings_df['year'].max()}")
        
        if 'quarter' in holdings_df.columns:
            print(f"   季度分布: {holdings_df['quarter'].value_counts().to_dict()}")
        
        # 2. 按基金公司统计
        if 'company' in holdings_df.columns and holdings_df['company'].notna().any():
            print(f"\n🏢 各基金公司持仓概览 (Top 5):")
            company_stats = holdings_df.groupby('company').agg({
                'stock_name': 'nunique',
                'hold_value': 'sum',
                'fund_code': 'nunique'
            }).round(2)
            company_stats.columns = ['持仓股票数', '总持仓市值(万元)', '管理基金数']
            company_stats = company_stats.sort_values('总持仓市值(万元)', ascending=False).head(5)
            print(company_stats.to_string())
        
        # 3. 热门股票统计
        print(f"\n🔥 热门持仓股票 Top 10:")
        hot_stocks = holdings_df.groupby('stock_name').agg({
            'fund_code': 'nunique',
            'hold_value': 'sum'
        }).round(2)
        hot_stocks.columns = ['持有基金数', '总持仓市值(万元)']
        hot_stocks = hot_stocks.sort_values('总持仓市值(万元)', ascending=False).head(10)
        print(hot_stocks.to_string())
        
        # 4. 持仓集中度分析
        print(f"\n🎯 持仓集中度分析 (Top 5):")
        concentration = holdings_df.groupby('fund_code').apply(
            lambda x: x['hold_ratio'].sum() if not x.empty else 0
        ).sort_values(ascending=False).head(5)
        
        for fund_code, conc in concentration.items():
            fund_name = holdings_df[holdings_df['fund_code'] == fund_code]['fund_name'].iloc[0] if 'fund_name' in holdings_df.columns else f'基金{fund_code}'
            print(f"   {fund_name} ({fund_code}): {conc:.1f}%")
        
        print(f"   平均集中度: {concentration.mean():.1f}%")
        
        # 5. 市场分布分析
        if 'stock_code' in holdings_df.columns:
            print(f"\n📈 股票市场分布:")
            def classify_stock(code):
                if pd.isna(code) or not str(code).isdigit():
                    return '港股/其他'
                code_str = str(code).zfill(6)  # 补齐6位
                if code_str.startswith('0'):
                    if code_str.startswith('002'):
                        return '中小板'
                    elif code_str.startswith('300'):
                        return '创业板'
                    else:
                        return '主板'
                elif code_str.startswith('6') or code_str.startswith('688'):
                    return '主板'
                else:
                    return '港股/其他'
            
            holdings_df['market'] = holdings_df['stock_code'].apply(classify_stock)
            
            market_dist = holdings_df.groupby('market').agg({
                'hold_value': 'sum',
                'stock_name': 'nunique'
            }).round(2)
            market_dist.columns = ['总持仓市值(万元)', '股票数']
            if len(market_dist) > 0:
                market_dist['占比'] = (market_dist['总持仓市值(万元)'] / market_dist['总持仓市值(万元)'].sum() * 100).round(1)
                print(market_dist.sort_values('总持仓市值(万元)', ascending=False).to_string())

def get_fund_codes_from_report(file_path):
    """
    从市场监控报告中读取"弱买入"和"强买入"的基金代码。
    """
    fund_codes = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            # 使用正则表达式匹配"弱买入"或"强买入"行
            pattern = re.compile(r'\|\s*(\d{6})\s*\|.*?\s*\|\s*(弱买入|强买入)\s*\|')
            matches = pattern.findall(content)
            for code, signal in matches:
                # 确保每个代码只添加一次
                if code not in fund_codes:
                    fund_codes.append(code)
        print(f"✅ 从报告中获取到 {len(fund_codes)} 个待爬取的基金代码。")
        if fund_codes:
            print(f"   示例: {fund_codes[:5]}...")
    except FileNotFoundError:
        print(f"❌ 错误: 未找到文件 {file_path}")
        # 提供默认测试基金代码
        print("   使用默认测试基金代码: 014192")
        return ['014192']
    except Exception as e:
        print(f"❌ 读取文件时出错: {e}")
        return []
    
    return fund_codes

def main():
    """主程序 - 增强版"""
    crawler = FundDataCrawler()
    
    try:
        print("🚀 启动基金持仓数据爬取系统")
        print("=" * 60)
        
        # === 测试模式（可选） ===
        # 取消下面这行的注释来运行测试
        # crawler.test_api_parsing('014192', 2025)
        # return
        
        # 步骤1: 从报告文件中获取基金列表
        print("\n📋 步骤1: 从报告中读取基金列表")
        report_file = 'market_monitor_report.md'
        codes_to_crawl = get_fund_codes_from_report(report_file)
        
        if not codes_to_crawl:
            print("❌ 未找到需要爬取的基金代码，使用默认测试基金")
            fund_list_df = pd.DataFrame({
                'fund_code': ['014192'], 
                'fund_name': ['广发先进制造股票发起式C']
            })
        else:
            # 将代码列表转换为DataFrame格式以适应原有函数
            fund_list_df = pd.DataFrame({'fund_code': codes_to_crawl, 'fund_name': ''})
        
        print(f"📈 准备处理 {len(fund_list_df)} 只基金")
        
        # 步骤2: 批量爬取持仓数据
        print(f"\n🔍 步骤2: 批量爬取持仓数据")
        years_to_crawl = [2025, 2024, 2023]  # 指定爬取年份，爬取近三年数据
        
        # 可以设置 use_backup=True 使用备份方法进行测试
        holdings_data = crawler.batch_crawl_fund_holdings(
            fund_list_df, 
            max_funds=len(fund_list_df),
            years=years_to_crawl,
            use_backup=False  # False使用主方法，True使用备份方法
        )
        
        # 步骤3: 数据分析
        if not holdings_data.empty:
            print(f"\n📊 步骤3: 数据分析")
            crawler.analyze_holdings(holdings_data)
        else:
            print("❌ 未获取到任何持仓数据")
            print("💡 建议: 运行测试模式检查API是否正常")
            print("   在main()函数开头取消 test_api_parsing() 的注释")
        
    except KeyboardInterrupt:
        print("\n⚠️  用户中断程序")
    except Exception as e:
        print(f"❌ 程序执行出错: {e}")
        import traceback
        traceback.print_exc()
    finally:
        crawler.close_driver()
        print("\n👋 程序结束，感谢使用！")

def quick_test():
    """快速测试函数"""
    print("🧪 快速测试 - 单个基金")
    crawler = FundDataCrawler()
    
    # 测试单个基金
    fund_code = '014192'
    holdings = crawler.get_fund_holdings_from_api(fund_code, years=[2025])
    
    if not holdings.empty:
        print(f"✅ 测试成功! 获取 {len(holdings)} 条记录")
        print("\n前10条持仓:")
        display_cols = ['year', 'quarter', 'rank', 'stock_code', 'stock_name', 'hold_ratio', 'hold_shares', 'hold_value']
        print(holdings[display_cols].head(10).to_string(index=False))
        
        # 简单分析
        print(f"\n📈 快速分析:")
        print(f"   总持仓比例: {holdings['hold_ratio'].sum():.2f}%")
        print(f"   总持仓市值: {holdings['hold_value'].sum():,.2f}万元")
        print(f"   Top3持仓:")
        top3 = holdings.nlargest(3, 'hold_value')[['stock_name', 'hold_ratio', 'hold_value']]
        for _, row in top3.iterrows():
            print(f"     {row['stock_name']}: {row['hold_ratio']:.1f}% ({row['hold_value']:,.0f}万元)")
    else:
        print("❌ 测试失败")
        print("💡 尝试使用备份方法:")
        holdings_backup = crawler.get_fund_holdings_from_api_backup(fund_code, years=[2025])
        if not holdings_backup.empty:
            print(f"✅ 备份方法成功: {len(holdings_backup)} 条记录")
        else:
            print("❌ 所有方法都失败，请检查网络连接")
    
    crawler.close_driver()

if __name__ == "__main__":
    # 运行快速测试
    # quick_test()
    
    # 运行主程序
    main()

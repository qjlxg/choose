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
import logging

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
            logging.info(f"已创建输出目录: {self.output_dir}")
            
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
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            self.wait = WebDriverWait(self.driver, 10)
            logging.info("浏览器驱动初始化成功")
        except Exception as e:
            logging.error(f"浏览器驱动初始化失败: {e}")
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
            logging.info("正在获取全市场基金列表...")
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
            logging.info(f"成功获取 {len(df)} 只基金")
            
            # 保存到本地
            output_path = os.path.join(self.output_dir, 'all_fund_list.csv')
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
            logging.info(f"基金列表已保存至: {output_path}")
            return df
            
        except Exception as e:
            logging.error(f"获取基金列表失败: {e}")
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
            logging.error(f"获取基金 {fund_code} 信息失败: {e}")
            return {}
    
    def parse_holding_table_from_content(self, content, year, quarter=None):
        """
        从HTML内容中解析持仓表格数据
        """
        holdings = []
        
        # 构建季度标识
        if quarter:
            quarter_pattern = f'{year}年{quarter}季度股票投资明细'
        else:
            quarter_pattern = f'{year}年.*?季度股票投资明细'
        
        # 按季度分割内容
        sections = re.split(quarter_pattern, content, flags=re.DOTALL)
        
        # 取匹配的季度内容
        target_section = None
        for i, section in enumerate(sections):
            if re.search(quarter_pattern, section, re.DOTALL):
                target_section = sections[i + 1] if i + 1 < len(sections) else ''
                break
        
        if not target_section:
            # 如果指定季度没找到，取最新的季度
            quarter_matches = re.findall(r'(\d+)年(\d+)季度股票投资明细', content)
            if quarter_matches:
                latest_quarter = max(quarter_matches, key=lambda x: (int(x[0]), int(x[1])))
                target_section = re.split(f'{latest_quarter[0]}年{latest_quarter[1]}季度股票投资明细', content)[-1]
        
        if not target_section:
            logging.warning(f"未找到 {year} 年的持仓数据")
            return holdings
        
        # 解析表格行
        # 匹配表格行：序号|股票代码|股票名称|...|占净值比例|持股数|持仓市值
        table_row_pattern = r'<tr>.*?<td>(\d+)</td>.*?<td>(\d{5,6})</td>.*?<td>(.*?)</td>.*?<td>([\d.]+%)</td>.*?<td>([\d.,]+)</td>.*?<td>([\d.,]+)</td>'
        
        # 查找所有匹配的行
        rows = re.findall(table_row_pattern, target_section, re.DOTALL)
        
        for row in rows:
            try:
                holding = {
                    'year': year,
                    'quarter': quarter if quarter else None,
                    'rank': int(row[0]),
                    'stock_code': row[1].strip(),
                    'stock_name': re.sub(r'<.*?>', '', row[2]).strip(),  # 移除HTML标签
                    'hold_ratio': float(row[3].replace('%', '')),
                    'hold_shares': float(row[4].replace(',', '')),
                    'hold_value': float(row[5].replace(',', '')),
                }
                holdings.append(holding)
            except (ValueError, IndexError) as e:
                logging.debug(f"解析行数据失败: {row}, 错误: {e}")
                continue
        
        logging.info(f"解析到 {len(holdings)} 条 {year}年持仓记录")
        return holdings
    
    def get_fund_holdings_from_api(self, fund_code, years=None, quarter=None):
        """
        通过API链接爬取基金持仓数据 - 优化版本
        """
        if years is None:
            years = [datetime.now().year]
        
        all_holdings = []
        
        for year in years:
            try:
                logging.info(f"正在通过API爬取基金 {fund_code} {year}年持仓...")
                
                # 构建API链接
                url = f"https://fundf10.eastmoney.com/FundArchivesDatas.aspx?type=jjcc&code={fund_code}&topline=10&year={year}"
                response = self.session.get(url, timeout=10)
                response.raise_for_status()
                
                # 解析返回的JavaScript数据
                match = re.search(r'var apidata=(.*?);', response.text, re.DOTALL)
                if not match:
                    logging.warning(f"未在响应中找到基金 {fund_code} {year}年的数据")
                    continue
                
                # 提取数据字符串
                api_data_str = match.group(1).strip()
                
                # 更robust的JSON修复方法
                try:
                    # 提取content部分
                    content_match = re.search(r'content:"(.*?)"', api_data_str, re.DOTALL)
                    if content_match:
                        content_value = content_match.group(1)
                        # 清理content中的HTML标签和特殊字符
                        content_cleaned = content_value.replace('\r', '').replace('\t', ' ')
                        
                        # 构建完整的JSON字符串
                        arryear_match = re.search(r'arryear:\[(.*?)\]', api_data_str)
                        arryear_value = arryear_match.group(1) if arryear_match else '[]'
                        
                        cur_year_match = re.search(r'curyear:(\d+)', api_data_str)
                        cur_year_value = cur_year_match.group(1) if cur_year_match else str(year)
                        
                        # 构建JSON
                        json_data = {
                            "content": content_cleaned,
                            "arryear": [int(y.strip()) for y in arryear_value.split(',') if y.strip().isdigit()],
                            "curyear": int(cur_year_value)
                        }
                    else:
                        logging.warning(f"无法提取content数据: {fund_code} {year}")
                        continue
                        
                except Exception as json_error:
                    logging.error(f"JSON解析失败 {fund_code} {year}: {json_error}")
                    continue
                
                # 解析持仓数据
                holdings = self.parse_holding_table_from_content(json_data['content'], year, quarter)
                
                # 添加基金信息
                for holding in holdings:
                    holding['fund_code'] = fund_code
                    holding['fund_name'] = ''  # 后续合并时添加
                
                all_holdings.extend(holdings)
                time.sleep(1)  # 避免请求过快
                
            except Exception as e:
                logging.error(f"爬取基金 {fund_code} {year}年持仓失败: {e}")
                continue
        
        if all_holdings:
            df = pd.DataFrame(all_holdings)
            # 确保数值列为数值类型
            numeric_columns = ['hold_ratio', 'hold_shares', 'hold_value']
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            logging.info(f"基金 {fund_code} 总共获取 {len(df)} 条持仓记录")
            return df
        else:
            logging.warning(f"基金 {fund_code} 未获取到任何持仓数据")
            return pd.DataFrame()
    
    def get_fund_holdings_selenium_backup(self, fund_code, years=None):
        """
        Selenium备份方法，当API方法失败时使用
        """
        if years is None:
            years = [datetime.now().year]
        
        if not self.driver:
            logging.warning("浏览器驱动不可用")
            return pd.DataFrame()
        
        all_holdings = []
        
        for year in years:
            try:
                logging.info(f"使用Selenium爬取基金 {fund_code} {year}年持仓...")
                
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
                    logging.warning(f"年份切换失败 {year}: {e}")
                    continue
                
                # 解析页面内容
                soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                
                # 查找所有持仓表格
                tables = soup.select('#cctable > div > div table')
                
                if not tables:
                    logging.warning(f"未找到基金 {fund_code} {year}年持仓表格")
                    continue
                
                # 解析表格
                for table in tables:
                    rows = table.find_all('tr')[1:]  # 跳过表头
                    for row in rows:
                        cols = row.find_all(['td', 'th'])
                        if len(cols) >= 7:
                            holding = {
                                'fund_code': fund_code,
                                'year': year,
                                'rank': len(all_holdings) + 1,
                                'stock_code': cols[1].text.strip() if len(cols) > 1 else '',
                                'stock_name': cols[2].text.strip() if len(cols) > 2 else '',
                                'hold_ratio': cols[3].text.strip() if len(cols) > 3 else '',
                                'hold_shares': cols[4].text.strip() if len(cols) > 4 else '',
                                'hold_value': cols[5].text.strip() if len(cols) > 5 else '',
                            }
                            
                            # 数据清洗
                            holding['hold_ratio'] = float(holding['hold_ratio'].replace('%', '')) if holding['hold_ratio'] else 0
                            holding['hold_shares'] = float(holding['hold_shares'].replace(',', '')) if holding['hold_shares'] else 0
                            holding['hold_value'] = float(holding['hold_value'].replace(',', '')) if holding['hold_value'] else 0
                            
                            all_holdings.append(holding)
                
                logging.info(f"基金 {fund_code} {year}年获取到 {len(rows)} 条持仓记录")
                time.sleep(2)
                
            except Exception as e:
                logging.error(f"Selenium爬取基金 {fund_code} {year}年持仓失败: {e}")
                continue
        
        if all_holdings:
            df = pd.DataFrame(all_holdings)
            return df
        else:
            return pd.DataFrame()
    
    def batch_crawl_fund_holdings(self, fund_list, max_funds=100, years=None):
        """
        批量爬取基金持仓数据 - 优化版本
        """
        if years is None:
            years = [datetime.now().year]
        
        all_data = []
        failed_funds = []
        
        for idx, fund in fund_list.iterrows():
            if idx >= max_funds:
                break
                
            fund_code = fund['fund_code']
            fund_name = fund.get('fund_name', '')
            
            logging.info(f"\n[{idx+1}/{min(max_funds, len(fund_list))}] 正在处理: {fund_name} ({fund_code})")
            
            # 获取基金基本信息
            fund_info = self.get_fund_info(fund_code)
            full_fund_name = fund_info.get('fund_name', fund_name)
            company = fund_info.get('company', '')
            
            # 首先尝试API方法
            holdings = self.get_fund_holdings_from_api(fund_code, years)
            
            # 如果API方法失败，尝试Selenium方法
            if holdings.empty:
                logging.warning(f"API方法失败，尝试Selenium方法...")
                holdings = self.get_fund_holdings_selenium_backup(fund_code, years)
            
            if not holdings.empty:
                # 合并基本信息和持仓数据
                holdings['fund_name'] = full_fund_name
                holdings['manager'] = fund_info.get('manager', '')
                holdings['company'] = company
                all_data.append(holdings)
                logging.info(f"✓ 成功获取 {len(holdings)} 条记录")
            else:
                failed_funds.append(fund_code)
                logging.error(f"✗ 获取失败: {fund_code}")
            
            # 避免请求过快
            time.sleep(2)
        
        # 合并所有数据
        if all_data:
            result_df = pd.concat(all_data, ignore_index=True)
            
            # 数据质量检查
            logging.info(f"数据质量检查:")
            logging.info(f"  - 总记录数: {len(result_df)}")
            logging.info(f"  - 空股票代码记录: {(result_df['stock_code'] == '').sum()}")
            logging.info(f"  - 平均持仓比例: {result_df['hold_ratio'].mean():.2f}%")
            
            # 保存结果
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'fund_holdings_{timestamp}.csv'
            output_path = os.path.join(self.output_dir, filename)
            result_df.to_csv(output_path, index=False, encoding='utf-8-sig')
            
            # 保存失败的基金列表
            if failed_funds:
                failed_file = os.path.join(self.output_dir, f'failed_funds_{timestamp}.txt')
                with open(failed_file, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(failed_funds))
                logging.info(f"失败的基金已保存至: {failed_file}")
            
            logging.info(f"\n批量爬取完成！")
            logging.info(f"成功处理 {len(all_data)} 只基金")
            logging.info(f"总共获取 {len(result_df)} 条持仓记录")
            logging.info(f"数据已保存至: {output_path}")
            
            return result_df
        else:
            logging.error("未获取到任何数据")
            return pd.DataFrame()
    
    def analyze_holdings(self, holdings_df):
        """
        分析持仓数据 - 增强版
        """
        if holdings_df.empty:
            logging.warning("没有数据可分析")
            return
        
        print("\n" + "="*60)
        print("                基金持仓数据分析报告")
        print("="*60)
        
        # 1. 基本统计
        print(f"\n📊 基本统计:")
        print(f"   总记录数: {len(holdings_df):,}")
        print(f"   涉及基金数: {holdings_df['fund_code'].nunique()}")
        print(f"   涉及股票数: {holdings_df['stock_name'].nunique()}")
        print(f"   数据时间跨度: {holdings_df['year'].min()}-{holdings_df['year'].max()}")
        
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
            'hold_value': 'sum',
            'hold_ratio': lambda x: (x * holdings_df.loc[x.index, 'hold_shares'].sum()).sum() / len(x)
        }).round(2)
        hot_stocks.columns = ['持有基金数', '总持仓市值(万元)', '平均持仓比例(%)']
        hot_stocks = hot_stocks.sort_values('总持仓市值(万元)', ascending=False).head(10)
        print(hot_stocks.to_string())
        
        # 4. 持仓集中度分析
        print(f"\n🎯 持仓集中度分析 (Top 5):")
        concentration = holdings_df.groupby('fund_code').apply(
            lambda x: x['hold_ratio'].sum() if not x.empty else 0
        ).sort_values(ascending=False).head(5)
        
        for fund_code, conc in concentration.items():
            fund_name = holdings_df[holdings_df['fund_code'] == fund_code]['fund_name'].iloc[0]
            print(f"   {fund_name} ({fund_code}): {conc:.1f}%")
        
        # 5. 行业/板块分布（如果有分类信息）
        if 'stock_code' in holdings_df.columns:
            # 简单的前十大/中小板/创业板分类
            def classify_stock(code):
                if pd.isna(code):
                    return '未知'
                code_str = str(code)
                if code_str.startswith('0') and len(code_str) >= 6:
                    if code_str.startswith('002'):
                        return '中小板'
                    elif code_str.startswith('300'):
                        return '创业板'
                    else:
                        return '主板'
                elif code_str.startswith('6') or code_str.startswith('688'):
                    return '主板'
                elif code_str.startswith('11') or code_str.startswith('12'):
                    return '港股'
                else:
                    return '其他'
            
            holdings_df['market'] = holdings_df['stock_code'].apply(classify_stock)
            
            print(f"\n📈 市场分布:")
            market_dist = holdings_df.groupby('market').agg({
                'hold_value': 'sum',
                'stock_name': 'nunique'
            }).round(2)
            market_dist.columns = ['总持仓市值(万元)', '股票数']
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
                if code not in fund_codes:
                    fund_codes.append(code)
        logging.info(f"从报告中获取到 {len(fund_codes)} 个待爬取的基金代码。")
    except FileNotFoundError:
        logging.error(f"错误: 未找到文件 {file_path}")
        return []
    except Exception as e:
        logging.error(f"读取文件时出错: {e}")
        return []
    
    return fund_codes

def main():
    """主程序 - 增强版"""
    crawler = FundDataCrawler()
    
    try:
        # 步骤1: 从报告文件中获取基金列表
        print("🚀 启动基金持仓数据爬取系统")
        print("=" * 50)
        
        print("\n📋 步骤1: 从报告中读取基金列表")
        report_file = 'market_monitor_report.md'
        codes_to_crawl = get_fund_codes_from_report(report_file)
        
        if not codes_to_crawl:
            print("❌ 未找到需要爬取的基金代码，尝试获取全市场前100只基金...")
            # 备用方案：获取全市场前100只基金
            all_funds = crawler.get_all_fund_codes()
            if not all_funds.empty:
                fund_list_df = all_funds.head(100)
                print(f"📈 使用全市场前100只基金进行测试")
            else:
                print("❌ 无法获取任何基金数据，程序退出")
                return
        else:
            fund_list_df = pd.DataFrame({'fund_code': codes_to_crawl, 'fund_name': ''})
        
        # 步骤2: 批量爬取持仓数据
        print(f"\n🔍 步骤2: 批量爬取持仓数据")
        print(f"   目标基金数: {len(fund_list_df)}")
        years_to_crawl = [2025, 2024, 2023]  # 指定爬取年份
        print(f"   爬取年份: {', '.join(map(str, years_to_crawl))}")
        
        holdings_data = crawler.batch_crawl_fund_holdings(
            fund_list_df, 
            max_funds=len(fund_list_df),
            years=years_to_crawl
        )
        
        # 步骤3: 数据分析
        if not holdings_data.empty:
            print(f"\n📊 步骤3: 数据分析")
            crawler.analyze_holdings(holdings_data)
        else:
            print("❌ 未获取到任何持仓数据")
        
    except KeyboardInterrupt:
        print("\n⚠️  用户中断程序")
    except Exception as e:
        logging.error(f"程序执行出错: {e}")
        import traceback
        traceback.print_exc()
    finally:
        crawler.close_driver()
        print("\n👋 程序结束")

if __name__ == "__main__":
    main()

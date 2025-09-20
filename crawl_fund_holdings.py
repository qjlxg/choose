import requests
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
                    print(f"    年份切换失败 {year}: {e}，尝试直接解析当前页面...")
                    # 如果年份切换失败，继续解析当前页面
                    pass
                
                # 解析持仓表格 - 增强版解析逻辑
                soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                
                # 尝试多种选择器找到持仓表格
                table_selectors = [
                    '#cctable > div > div',
                    '.tablebg2',
                    'table.comm',
                    '#table',
                    'table'
                ]
                
                table = None
                for selector in table_selectors:
                    table = soup.select_one(selector)
                    if table and table.find_all('tr'):
                        break
                
                if not table:
                    print(f"    未找到基金 {fund_code} {year}年持仓表格")
                    # 尝试查找任何包含股票数据的表格
                    all_tables = soup.find_all('table')
                    for t in all_tables:
                        if t.find_all('tr') and len(t.find_all('tr')) > 1:
                            table = t
                            print(f"    使用备用表格选择器找到表格")
                            break
                    
                    if not table:
                        print(f"    所有表格选择器都失败")
                        continue
                
                # 解析表格数据 - 智能解析
                rows = table.find_all('tr')[1:]  # 跳过表头
                year_holdings = []
                
                for row_idx, row in enumerate(rows, 1):
                    cols = row.find_all(['td', 'th'])
                    if len(cols) >= 4:  # 至少需要序号、代码、名称、比例
                        try:
                            # 提取基本信息
                            rank_text = cols[0].get_text().strip()
                            rank = int(rank_text) if rank_text.isdigit() else row_idx
                            
                            # 提取股票代码（支持多种格式）
                            stock_code = ''
                            if len(cols) > 1:
                                # 从链接中提取代码
                                code_link = cols[1].find('a')
                                if code_link and code_link.get('href'):
                                    code_match = re.search(r'r/([.\d]+)', code_link.get('href'))
                                    if code_match:
                                        stock_code = code_match.group(1).replace('.', '')
                                # 如果链接中没有，从文本提取
                                if not stock_code:
                                    stock_code = cols[1].get_text().strip()
                            
                            # 提取股票名称
                            stock_name = ''
                            if len(cols) > 2:
                                name_elem = cols[2].find('a') or cols[2]
                                stock_name = name_elem.get_text().strip()
                            
                            # 查找持仓比例（通常在第4列或更靠后的列）
                            hold_ratio = ''
                            hold_shares = ''
                            hold_value = ''
                            
                            # 智能查找数值列
                            for col_idx in range(3, len(cols)):
                                col_text = cols[col_idx].get_text().strip()
                                # 匹配持仓比例（包含%符号）
                                if '%' in col_text and any(char.isdigit() for char in col_text):
                                    hold_ratio = col_text.replace('%', '')
                                # 匹配持股数（小数格式）
                                elif '.' in col_text and col_text.replace('.', '').replace(',', '').isdigit():
                                    if not hold_shares:
                                        hold_shares = col_text.replace(',', '')
                                    elif not hold_value:
                                        hold_value = col_text.replace(',', '')
                                # 匹配持仓市值（大数值）
                                elif ',' in col_text and col_text.replace(',', '').replace('.', '').isdigit():
                                    hold_value = col_text.replace(',', '')
                            
                            # 如果没找到持仓比例，尝试其他位置
                            if not hold_ratio:
                                for col_idx in range(3, len(cols)):
                                    col_text = cols[col_idx].get_text().strip()
                                    if '%' in col_text:
                                        hold_ratio = col_text.replace('%', '')
                                        break
                            
                            # 如果没找到持股数和市值，使用最后两列
                            if not hold_shares and len(cols) >= 5:
                                hold_shares = cols[-2].get_text().strip().replace(',', '')
                            if not hold_value and len(cols) >= 6:
                                hold_value = cols[-1].get_text().strip().replace(',', '')
                            
                            holding = {
                                'fund_code': fund_code,
                                'year': year,
                                'quarter': '未知',  # Selenium页面通常不显示具体季度
                                'rank': rank,
                                'stock_code': stock_code,
                                'stock_name': stock_name,
                                'hold_ratio': hold_ratio,
                                'hold_shares': hold_shares,
                                'hold_value': hold_value,
                                'method': 'selenium'
                            }
                            
                            # 验证数据完整性
                            if stock_code and stock_name and hold_ratio:
                                year_holdings.append(holding)
                            
                        except Exception as row_error:
                            print(f"    解析第 {row_idx} 行失败: {row_error}")
                            continue
                
                all_holdings.extend(year_holdings)
                print(f"    基金 {fund_code} {year}年获取到 {len(year_holdings)} 条有效持仓记录")
                time.sleep(1)  # 避免请求过快
                
            except Exception as e:
                print(f"    爬取基金 {fund_code} {year}年持仓失败: {e}")
                continue
        
        # 转换为DataFrame
        if all_holdings:
            df = pd.DataFrame(all_holdings)
            # 数据清洗
            df['hold_ratio'] = pd.to_numeric(df['hold_ratio'], errors='coerce')
            df['hold_value'] = pd.to_numeric(df['hold_value'], errors='coerce')
            df['hold_shares'] = pd.to_numeric(df['hold_shares'], errors='coerce')
            
            # 数据质量检查
            valid_records = df[df['hold_ratio'].notna()]
            print(f"    总数据质量: {len(valid_records)}/{len(df)} 条有效记录")
            
            return valid_records
        else:
            print(f"    基金 {fund_code} 未获取到有效数据")
            return pd.DataFrame()
    
    def test_selenium_parsing(self, fund_code='014192', year=2025):
        """
        测试Selenium解析功能
        """
        print(f"\n🧪 测试Selenium解析 - 基金 {fund_code} {year}年")
        
        holdings = self.get_fund_holdings(fund_code, years=[year])
        
        if not holdings.empty:
            print(f"✅ Selenium方法成功! 获取 {len(holdings)} 条记录")
            print("\n前5条记录:")
            display_cols = ['year', 'rank', 'stock_code', 'stock_name', 'hold_ratio', 'hold_shares', 'hold_value']
            print(holdings[display_cols].head().to_string(index=False))
            
            # 简单统计
            print(f"\n📊 快速统计:")
            print(f"   总持仓比例: {holdings['hold_ratio'].sum():.2f}%")
            print(f"   总持仓市值: {holdings['hold_value'].sum():,.2f}万元")
            print(f"   平均单股比例: {holdings['hold_ratio'].mean():.2f}%")
        else:
            print("❌ Selenium方法失败")
            print("💡 检查: 1.网络连接 2.基金代码是否正确 3.ChromeDriver版本")
    
    def batch_crawl_fund_holdings(self, fund_list, max_funds=100, years=None):
        """
        批量爬取基金持仓数据 - 纯Selenium版本，只针对报告中的弱买入和强买入基金
        """
        if years is None:
            years = [datetime.now().year, datetime.now().year-1, datetime.now().year-2]
        
        all_data = []
        failed_funds = []
        successful_funds = []
        
        print(f"\n🔄 开始批量爬取 {min(max_funds, len(fund_list))} 只基金 (Selenium方法，仅弱买入/强买入基金)")
        print(f"   爬取年份: {', '.join(map(str, years))}")
        
        for idx, fund in fund_list.iterrows():
            if idx >= max_funds:
                break
                
            fund_code = fund['fund_code']
            fund_name = fund.get('fund_name', f'基金{fund_code}')
            print(f"\n[{idx+1}/{min(max_funds, len(fund_list))}] 正在处理: {fund_name} ({fund_code})")
            
            # 获取基金基本信息
            print(f"    获取基金基本信息...")
            fund_info = self.get_fund_info(fund_code)
            full_fund_name = fund_info.get('fund_name', fund_name)
            company = fund_info.get('company', '未知')
            print(f"    基金名称: {full_fund_name}")
            print(f"    管理公司: {company}")
            
            # 使用Selenium获取持仓数据
            print(f"    开始爬取持仓数据...")
            holdings = self.get_fund_holdings(fund_code, years)
            
            if not holdings.empty:
                # 合并基本信息和持仓数据
                holdings['fund_name'] = full_fund_name
                holdings['manager'] = fund_info.get('manager', '')
                holdings['company'] = company
                all_data.append(holdings)
                successful_funds.append(fund_code)
                print(f"    ✓ 成功获取 {len(holdings)} 条记录")
                print(f"      - 覆盖年份: {sorted(holdings['year'].unique())}")
                print(f"      - 总持仓市值: {holdings['hold_value'].sum():,.2f}万元")
            else:
                failed_funds.append(fund_code)
                print(f"    ✗ 获取失败 - 无有效持仓数据")
            
            # 避免请求过快
            print(f"    等待3秒后继续...")
            time.sleep(3)
        
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
                print(f"   数据年份分布: {result_df['year'].value_counts().sort_index().to_dict()}")
            
            # 保存结果
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'fund_holdings_report_{timestamp}.csv'
            output_path = os.path.join(self.output_dir, filename)
            result_df.to_csv(output_path, index=False, encoding='utf-8-sig')
            
            # 保存成功基金列表
            success_file = os.path.join(self.output_dir, f'successful_funds_report_{timestamp}.txt')
            with open(success_file, 'w', encoding='utf-8') as f:
                f.write(f"报告基金成功爬取列表 ({len(successful_funds)} 只)\n")
                f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("="*50 + "\n")
                for code in successful_funds:
                    fund_info = self.get_fund_info(code)
                    name = fund_info.get('fund_name', '未知')
                    f.write(f"{code}\t{name}\n")
            
            # 保存失败基金列表
            if failed_funds:
                failed_file = os.path.join(self.output_dir, f'failed_funds_report_{timestamp}.txt')
                with open(failed_file, 'w', encoding='utf-8') as f:
                    f.write(f"报告基金失败爬取列表 ({len(failed_funds)} 只)\n")
                    f.write(f"可能原因: 1.基金无持仓数据 2.网络问题 3.页面结构变化\n")
                    f.write("="*50 + "\n")
                    for code in failed_funds:
                        f.write(f"{code}\n")
                print(f"   失败基金列表已保存至: {failed_file}")
            
            print(f"\n💾 主数据文件已保存至: {output_path}")
            print(f"   成功基金列表已保存至: {success_file}")
            
            return result_df
        else:
            print("❌ 未获取到任何数据")
            print("💡 建议: 检查网络连接、ChromeDriver版本、目标基金代码")
            return pd.DataFrame()
    
    def analyze_holdings(self, holdings_df):
        """
        分析持仓数据 - 增强版
        """
        if holdings_df.empty:
            print("❌ 没有数据可分析")
            return
        
        print("\n" + "="*80)
        print("                    基金持仓数据分析报告 (报告基金专用)")
        print("="*80)
        
        # 1. 基本统计
        print(f"\n📊 基本统计信息:")
        print(f"   总记录数: {len(holdings_df):,}")
        print(f"   涉及基金数: {holdings_df['fund_code'].nunique()}")
        print(f"   涉及股票数: {holdings_df['stock_name'].nunique()}")
        print(f"   数据年份范围: {holdings_df['year'].min()}-{holdings_df['year'].max()}")
        
        year_dist = holdings_df['year'].value_counts().sort_index()
        print(f"   年份分布: {dict(year_dist)}")
        
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
        else:
            print(f"\n🏢 基金公司信息: 部分基金信息获取失败")
        
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
            record_count = len(holdings_df[holdings_df['fund_code'] == fund_code])
            print(f"   {fund_name} ({fund_code}): {conc:.1f}% ({record_count}条记录)")
        
        print(f"   平均集中度: {concentration.mean():.1f}%")
        
        # 5. 市场分布分析
        if 'stock_code' in holdings_df.columns:
            print(f"\n📈 股票市场分布:")
            def classify_stock(code):
                if pd.isna(code) or not str(code):
                    return '未知'
                code_str = re.sub(r'^[0-9.]+\.', '', str(code)).zfill(6)  # 清理前缀并补齐6位
                if code_str.startswith('002'):
                    return '中小板'
                elif code_str.startswith('300'):
                    return '创业板'
                elif code_str.startswith('60') or code_str.startswith('688'):
                    return '主板'
                elif code_str.startswith('00') or code_str.startswith('399'):
                    return '指数'
                else:
                    return '其他'
            
            holdings_df['market'] = holdings_df['stock_code'].apply(classify_stock)
            
            market_dist = holdings_df.groupby('market').agg({
                'hold_value': 'sum',
                'stock_name': 'nunique'
            }).round(2)
            market_dist.columns = ['总持仓市值(万元)', '股票数']
            if len(market_dist) > 0:
                market_dist['占比'] = (market_dist['总持仓市值(万元)'] / market_dist['总持仓市值(万元)'].sum() * 100).round(1)
                print(market_dist.sort_values('总持仓市值(万元)', ascending=False).to_string())
        
        # 6. 数据质量报告
        print(f"\n🔍 数据质量报告:")
        null_ratios = {
            'hold_ratio': holdings_df['hold_ratio'].isna().sum(),
            'hold_value': holdings_df['hold_value'].isna().sum(),
            'stock_code': holdings_df['stock_code'].isna().sum(),
            'stock_name': holdings_df['stock_name'].isna().sum()
        }
        print(f"   空值统计: {null_ratios}")
        print(f"   完整记录率: {(len(holdings_df) - sum(null_ratios.values())) / len(holdings_df) * 100:.1f}%")

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
        print(f"✅ 从报告中获取到 {len(fund_codes)} 个待爬取的基金代码（弱买入/强买入）。")
        if fund_codes:
            print(f"   示例: {fund_codes[:5]}...")
    except FileNotFoundError:
        print(f"❌ 错误: 未找到文件 {file_path}")
        # 提供默认测试基金代码
        print("   使用默认测试基金代码: 014192, 005959")
        return ['014192', '005959']
    except Exception as e:
        print(f"❌ 读取文件时出错: {e}")
        return ['014192', '005959']  # 默认测试代码
    
    return fund_codes

def main():
    """主程序 - 纯Selenium版本，仅报告基金"""
    crawler = FundDataCrawler()
    
    try:
        print("🚀 启动基金持仓数据爬取系统 (Selenium版，仅弱买入/强买入基金)")
        print("=" * 60)
        
        # === 测试模式（推荐先运行测试） ===
        print("\n🧪 运行单基金测试...")
        crawler.test_selenium_parsing('014192', 2025)
        
        # 如果测试失败，程序提前结束
        # return
        
        # 步骤1: 从报告文件中获取基金列表（仅弱买入/强买入）
        print(f"\n📋 步骤1: 从报告中读取弱买入/强买入基金列表")
        report_file = 'market_monitor_report.md'
        codes_to_crawl = get_fund_codes_from_report(report_file)
        
        if not codes_to_crawl:
            print("❌ 未找到需要爬取的基金代码，使用默认测试基金")
            fund_list_df = pd.DataFrame({
                'fund_code': ['014192', '005959'], 
                'fund_name': ['广发先进制造股票发起式C', '前海开源沪深300指数']
            })
        else:
            # 将代码列表转换为DataFrame格式以适应原有函数
            fund_list_df = pd.DataFrame({'fund_code': codes_to_crawl, 'fund_name': ''})
        
        print(f"📈 准备处理 {len(fund_list_df)} 只基金（仅弱买入/强买入）")
        
        # 步骤2: 批量爬取持仓数据
        print(f"\n🔍 步骤2: 批量爬取持仓数据")
        years_to_crawl = [2025, 2024, 2023]  # 当前日期2025-09-20，爬取近三年
        
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
            print("💡 可能原因:")
            print("   1. 网络连接问题")
            print("   2. ChromeDriver版本不匹配")
            print("   3. 目标基金无公开持仓数据")
            print("   4. 网站页面结构发生变化")
        
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
    """快速测试函数 - 纯Selenium版"""
    print("🧪 快速测试 - 单个基金 (Selenium)")
    crawler = FundDataCrawler()
    
    # 测试单个基金
    fund_code = '014192'
    year = 2025
    print(f"测试基金: {fund_code} ({year}年)")
    
    holdings = crawler.get_fund_holdings(fund_code, years=[year])
    
    if not holdings.empty:
        print(f"✅ 测试成功! 获取 {len(holdings)} 条记录")
        print("\n前5条持仓:")
        display_cols = ['year', 'rank', 'stock_code', 'stock_name', 'hold_ratio', 'hold_shares', 'hold_value']
        print(holdings[display_cols].head().to_string(index=False))
        
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
        print("💡 故障排除:")
        print("   1. 检查网络连接")
        print("   2. 确认Chrome浏览器已安装")
        print("   3. 运行: pip install --upgrade webdriver-manager")
        print("   4. 检查防火墙设置")
    
    crawler.close_driver()

if __name__ == "__main__":
    # 运行快速测试
    # quick_test()
    
    # 运行主程序
    main()

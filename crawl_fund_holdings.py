import requests
import json
import pandas as pd
import numpy as np
import time
import os
import re
from bs4 import BeautifulSoup
from urllib.parse import urlencode
from datetime import datetime
import argparse

class FundHoldingsCrawler:
    def __init__(self, base_url="https://fundf10.eastmoney.com"):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Referer': 'https://fund.eastmoney.com/'
        })
    
    def extract_json_from_jsonp(self, jsonp_response):
        """从JSONP响应中提取JSON数据"""
        try:
            # 移除JSONP包装，提取 var apidata={...};
            pattern = r'var\s+apidata\s*=\s*({.*?});?\s*$'
            match = re.search(pattern, jsonp_response, re.DOTALL)
            if match:
                json_str = match.group(1)
                return json.loads(json_str)
            else:
                # 尝试直接解析（如果已经是JSON格式）
                return json.loads(jsonp_response)
        except json.JSONDecodeError as e:
            print(f"JSON解析失败: {e}")
            print(f"响应预览: {jsonp_response[:200]}...")
            return None
    
    def crawl_fund_holdings_by_year(self, fund_code, year, topline=10):
        """爬取指定基金指定年份的持仓数据"""
        url = f"{self.base_url}/FundArchivesDatas.aspx"
        params = {
            'type': 'jjcc',  # 基金持仓
            'code': fund_code,
            'topline': topline,
            'year': year
        }
        
        try:
            print(f"📡 正在请求 {fund_code} {year}年数据...")
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            # 解析JSONP响应
            data = self.extract_json_from_jsonp(response.text)
            if not data or 'content' not in data:
                print(f"❌ 未获取到 {fund_code} {year}年数据")
                return []
            
            # 解析HTML内容
            soup = BeautifulSoup(data['content'], 'html.parser')
            holdings = []
            
            # 查找所有季度表格
            quarter_sections = soup.find_all('div', class_='box')
            print(f"📊 找到 {len(quarter_sections)} 个季度数据段")
            
            for i, section in enumerate(quarter_sections, 1):
                try:
                    # 提取季度标题（2023年X季度）
                    title_elem = section.find('h4', class_='t')
                    if title_elem:
                        title_text = title_elem.get_text().strip()
                        # 提取季度信息，如 "2023年4季度股票投资明细"
                        quarter_match = re.search(r'(\d{4}年)(\d)季度', title_text)
                        if quarter_match:
                            year_str, quarter = quarter_match.groups()
                            quarter = int(quarter)
                        else:
                            # 默认当前年份的第一个季度
                            year_str = str(year)
                            quarter = 1
                    else:
                        year_str = str(year)
                        quarter = i
                    
                    # 查找表格
                    table = section.find('table', class_=re.compile(r'.*tzxq.*'))
                    if not table:
                        print(f"⚠️  未找到第{i}个季度的表格")
                        continue
                    
                    # 解析表格行
                    rows = table.find('tbody').find_all('tr') if table.find('tbody') else table.find_all('tr')
                    
                    for row in rows:
                        cols = row.find_all(['td', 'th'])
                        if len(cols) < 7:  # 至少7列：序号、代码、名称、资讯、比例、持股数、市值
                            continue
                        
                        try:
                            # 提取股票代码（从链接中获取）
                            code_link = cols[1].find('a')
                            stock_code = ''
                            if code_link and 'href' in code_link.attrs:
                                href = code_link['href']
                                # 从URL中提取代码，如 "1.688608" -> "688608"
                                code_match = re.search(r'r/[\d.]+(\d+)', href)
                                if code_match:
                                    stock_code = code_match.group(1)
                            
                            # 提取数据
                            holding = {
                                'fund_code': fund_code,
                                'year': year_str,
                                'quarter': quarter,
                                'report_date': title_elem.find('font', class_='px12').get_text().strip() if title_elem and title_elem.find('font', class_='px12') else f"{year}-Q{quarter}",
                                'stock_code': stock_code,
                                'stock_name': cols[2].get_text().strip() if len(cols) > 2 else '',
                                'ratio': cols[4].get_text().strip() if len(cols) > 4 else '',
                                'shares': cols[5].get_text().strip() if len(cols) > 5 else '',
                                'market_value': cols[6].get_text().strip().replace(',', '') if len(cols) > 6 else ''
                            }
                            
                            # 数据清洗
                            if holding['ratio']:
                                holding['ratio_clean'] = holding['ratio'].replace('%', '').strip()
                            if holding['market_value']:
                                holding['market_value_clean'] = holding['market_value'].replace(',', '').strip()
                            if holding['shares']:
                                holding['shares_clean'] = holding['shares'].strip()
                            
                            # 只保留有效的股票持仓
                            if stock_code and stock_code.isdigit():
                                holdings.append(holding)
                                
                        except Exception as row_error:
                            print(f"⚠️  解析第{i}季度第{len(holdings)+1}行失败: {row_error}")
                            continue
                    
                    print(f"✅ 第{i}季度解析完成: {len([h for h in holdings[-10:] if 'stock_code' in h])} 条记录")
                    
                except Exception as section_error:
                    print(f"❌ 解析第{i}个季度失败: {section_error}")
                    continue
            
            print(f"🎉 {fund_code} {year}年共获取 {len(holdings)} 条持仓记录")
            return holdings
            
        except requests.RequestException as e:
            print(f"❌ 请求 {fund_code} {year}年数据失败: {e}")
            return []
        except Exception as e:
            print(f"❌ 解析 {fund_code} {year}年数据失败: {e}")
            return []
    
    def crawl_fund_holdings(self, fund_code, years_back=1, topline=10):
        """爬取指定基金近N年的持仓数据"""
        print(f"🚀 开始爬取基金 {fund_code} 近 {years_back} 年持仓数据")
        
        current_year = datetime.now().year
        all_holdings = []
        
        for year_offset in range(years_back):
            year = current_year - year_offset
            year_holdings = self.crawl_fund_holdings_by_year(fund_code, year, topline)
            all_holdings.extend(year_holdings)
            
            # 防反爬延时
            if year_offset < years_back - 1:
                wait_time = np.random.uniform(1, 3)
                print(f"⏳ 等待 {wait_time:.1f} 秒...")
                time.sleep(wait_time)
        
        # 保存结果
        if all_holdings:
            df = pd.DataFrame(all_holdings)
            
            # 数据类型转换
            numeric_cols = ['ratio_clean', 'shares_clean', 'market_value_clean']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 创建输出目录
            output_dir = 'data'
            os.makedirs(output_dir, exist_ok=True)
            
            # 保存详细数据
            output_file = f"{output_dir}/{fund_code}_holdings_{years_back}y.csv"
            df.to_csv(output_file, index=False, encoding='utf-8-sig')
            
            # 保存汇总统计
            summary_stats = df.groupby(['year', 'quarter']).agg({
                'stock_code': 'count',
                'ratio_clean': ['sum', 'mean', 'max']
            }).round(4)
            summary_file = f"{output_dir}/{fund_code}_summary_{years_back}y.csv"
            summary_stats.to_csv(summary_file)
            
            print(f"💾 数据已保存:")
            print(f"   📋 详细持仓: {output_file} ({len(df)} 条记录)")
            print(f"   📊 季度汇总: {summary_file}")
            
            # 显示前10条数据预览
            print(f"\n📈 数据预览 (前10条):")
            print(df[['year', 'quarter', 'stock_code', 'stock_name', 'ratio', 'market_value']].head(10).to_string(index=False))
            
            return df
        else:
            print(f"❌ 基金 {fund_code} 未获取到任何持仓数据")
            return pd.DataFrame()
    
    def get_fund_basic_info(self, fund_code):
        """获取基金基本信息"""
        url = f"{self.base_url}/F10/{fund_code}.html"
        try:
            response = self.session.get(url, timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 尝试提取基金名称
            name_selectors = [
                'h1',
                '.fund-name',
                '#fundInfo h1',
                '[class*="fund-name"]'
            ]
            
            fund_name = 'Unknown'
            for selector in name_selectors:
                name_elem = soup.select_one(selector)
                if name_elem:
                    fund_name = name_elem.get_text().strip()
                    break
            
            # 尝试提取基金类型
            fund_type = 'Unknown'
            type_selectors = [
                '.fund-type',
                '[class*="fund-type"]',
                '.info-table td:contains("类型") + td'
            ]
            
            return {
                'fund_code': fund_code,
                'fund_name': fund_name,
                'fund_type': fund_type
            }
            
        except Exception as e:
            print(f"⚠️  获取 {fund_code} 基本信息失败: {e}")
            return {'fund_code': fund_code, 'fund_name': 'Unknown', 'fund_type': 'Unknown'}
    
    def get_all_fund_codes(self, max_codes=50):
        """获取全市场基金代码列表"""
        print("📋 获取全市场基金列表...")
        
        # 使用天天基金的基金排行接口
        rank_url = "https://fund.eastmoney.com/data/rankhandler.aspx"
        params = {
            'op': 'ph',
            'dt': 'kf',  # 全部基金
            'ft': 'gp',  # 股票型
            'rs': '',
            'gs': '0',
            'sc': 'jn',  # 近一年收益
            'st': 'desc',
            'pi': '1',
            'pn': str(max_codes * 2),  # 多取一些
            'dx': '0'
        }
        
        try:
            response = self.session.get(rank_url, params=params, timeout=10)
            data = self.extract_json_from_jsonp(response.text)
            
            if data and 'datas' in data:
                # 解析返回的基金列表
                fund_list = data['datas'].split('|') if isinstance(data['datas'], str) else []
                fund_codes = []
                
                for item in fund_list:
                    if '|' in item:
                        parts = item.split('|')
                        if len(parts) >= 2 and parts[0].isdigit() and len(parts[0]) == 6:
                            fund_codes.append(parts[0])
                
                print(f"✅ 获取到 {len(fund_codes)} 只基金")
                return fund_codes[:max_codes]
            else:
                print("⚠️  使用示例基金代码")
                return ['002580', '000689', '001298', '000001', '000002'][:max_codes]
                
        except Exception as e:
            print(f"❌ 获取基金列表失败: {e}")
            return ['002580', '000689', '001298', '000001', '000002'][:max_codes]

def main():
    parser = argparse.ArgumentParser(description='天天基金网持仓数据爬取工具')
    parser.add_argument('--fund-code', type=str, default='002580', 
                       help='基金代码 (默认: 002580)')
    parser.add_argument('--years', type=int, default=1, 
                       help='爬取年数 (默认: 1)')
    parser.add_argument('--topline', type=int, default=10, 
                       help='每季度前N只持仓 (默认: 10)')
    parser.add_argument('--all', action='store_true', 
                       help='爬取所有基金 (前20只)')
    parser.add_argument('--max-codes', type=int, default=20,
                       help='批量模式最大基金数量 (默认: 20)')
    
    args = parser.parse_args()
    
    # 初始化爬虫
    crawler = FundHoldingsCrawler()
    
    if args.all:
        # 批量模式
        print("🚀 批量爬取模式启动")
        fund_codes = crawler.get_all_fund_codes(args.max_codes)
        print(f"📋 将处理 {len(fund_codes)} 只基金")
        
        all_results = []
        success_count = 0
        
        for i, code in enumerate(fund_codes, 1):
            print(f"\n{'='*60}")
            print(f"[{i:2d}/{len(fund_codes)}] 正在处理: {code}")
            print(f"{'='*60}")
            
            # 获取基本信息
            info = crawler.get_fund_basic_info(code)
            print(f"📄 基金名称: {info['fund_name']}")
            
            # 爬取持仓
            df = crawler.crawl_fund_holdings(code, args.years, args.topline)
            
            if not df.empty:
                # 添加基金基本信息
                df['fund_name'] = info['fund_name']
                df['fund_type'] = info['fund_type']
                all_results.append(df)
                success_count += 1
                
                print(f"✅ {code} 处理成功: {len(df)} 条记录")
            else:
                print(f"❌ {code} 处理失败")
            
            # 延时防反爬
            if i < len(fund_codes):
                wait_time = np.random.uniform(2, 5)
                print(f"⏳ 等待 {wait_time:.1f} 秒...")
                time.sleep(wait_time)
        
        # 合并所有结果
        if all_results:
            combined_df = pd.concat(all_results, ignore_index=True)
            
            # 保存总汇总
            total_file = f"data/all_funds_holdings_{args.years}y_{datetime.now().strftime('%Y%m%d')}.csv"
            combined_df.to_csv(total_file, index=False, encoding='utf-8-sig')
            
            # 生成分析报告
            print(f"\n🎉 批量任务完成！")
            print(f"✅ 成功: {success_count}/{len(fund_codes)} 只基金")
            print(f"📊 总记录数: {len(combined_df):,}")
            print(f"💾 总汇总: {total_file}")
            
            # 显示统计
            print(f"\n📈 按基金统计:")
            fund_stats = combined_df.groupby('fund_code').size().reset_index(name='record_count')
            print(fund_stats.to_string(index=False))
            
    else:
        # 单基金模式
        print(f"🎯 单基金模式: {args.fund_code}")
        df = crawler.crawl_fund_holdings(args.fund_code, args.years, args.topline)
        
        if not df.empty:
            print(f"\n✅ 爬取完成！共 {len(df)} 条记录")
        else:
            print(f"\n❌ 爬取失败")

if __name__ == "__main__":
    main()

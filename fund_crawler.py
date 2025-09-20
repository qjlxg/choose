import requests
import pandas as pd
import numpy as np
import time
import os
import re
import ast  # 用于安全解析
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
        """从JSONP响应中提取并解析JSON数据（使用ast.literal_eval安全解析）"""
        try:
            # 匹配 var XXX = {...};
            pattern = r'var\s+\w+\s*=\s*({.*?});?\s*$'
            match = re.search(pattern, jsonp_response, re.DOTALL | re.MULTILINE)
            if match:
                json_str = match.group(1)
                # ast.literal_eval 能处理单引号和简单结构
                data = ast.literal_eval(json_str)
                return data
            else:
                print(f"未匹配到JSONP模式: {jsonp_response[:100]}...")
                return None
        except (ast.literal_eval, SyntaxError, ValueError) as e:
            print(f"解析失败: {e}")
            print(f"响应预览: {jsonp_response[:200]}...")
            return None
    
    def crawl_fund_holdings_by_year(self, fund_code, year, topline=10):
        """爬取指定基金指定年份的持仓数据"""
        url = f"{self.base_url}/FundArchivesDatas.aspx"
        params = {
            'type': 'jjcc',
            'code': fund_code,
            'topline': topline,
            'year': year
        }
        
        try:
            print(f"📡 请求 {fund_code} {year}年数据...")
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = self.extract_json_from_jsonp(response.text)
            if not data or 'content' not in data:
                print(f"❌ 无 {fund_code} {year}年数据")
                return []
            
            soup = BeautifulSoup(data['content'], 'html.parser')
            holdings = []
            
            quarter_sections = soup.find_all('div', class_='box')
            print(f"📊 找到 {len(quarter_sections)} 个季度")
            
            for i, section in enumerate(quarter_sections, 1):
                title_elem = section.find('h4', class_='t')
                if title_elem:
                    title_text = title_elem.get_text().strip()
                    quarter_match = re.search(r'(\d{4}年)(\d)季度', title_text)
                    if quarter_match:
                        year_str, quarter = quarter_match.groups()
                        quarter = int(quarter)
                    else:
                        year_str = str(year)
                        quarter = i
                    report_date = title_elem.find('font', class_='px12').get_text().strip() if title_elem.find('font', class_='px12') else f"{year}-Q{quarter}"
                else:
                    year_str = str(year)
                    quarter = i
                    report_date = f"{year}-Q{quarter}"
                
                table = section.find('table', class_=re.compile(r'.*tzxq.*'))
                if not table:
                    continue
                
                rows = table.find('tbody').find_all('tr') if table.find('tbody') else table.find_all('tr')
                
                for row in rows:
                    cols = row.find_all(['td', 'th'])
                    if len(cols) < 7:
                        continue
                    
                    code_link = cols[1].find('a')
                    stock_code = ''
                    if code_link and 'href' in code_link.attrs:
                        href = code_link['href']
                        code_match = re.search(r'r/[\d.]+(\d+)', href)
                        if code_match:
                            stock_code = code_match.group(1)
                    
                    holding = {
                        'fund_code': fund_code,
                        'year': year_str,
                        'quarter': quarter,
                        'report_date': report_date,
                        'stock_code': stock_code,
                        'stock_name': cols[2].get_text().strip() if len(cols) > 2 else '',
                        'ratio': cols[4].get_text().strip() if len(cols) > 4 else '',
                        'shares': cols[5].get_text().strip() if len(cols) > 5 else '',
                        'market_value': cols[6].get_text().strip().replace(',', '') if len(cols) > 6 else ''
                    }
                    
                    if holding['ratio']:
                        holding['ratio_clean'] = holding['ratio'].replace('%', '').strip()
                    if holding['market_value']:
                        holding['market_value_clean'] = holding['market_value'].replace(',', '').strip()
                    if holding['shares']:
                        holding['shares_clean'] = holding['shares'].strip()
                    
                    if stock_code and stock_code.isdigit():
                        holdings.append(holding)
            
            print(f"✅ {fund_code} {year}年: {len(holdings)} 条记录")
            return holdings
            
        except Exception as e:
            print(f"❌ {fund_code} {year}年失败: {e}")
            return []
    
    def crawl_fund_holdings(self, fund_code, years_back=1, topline=10):
        """爬取近N年持仓（从2024年开始）"""
        print(f"🚀 爬取 {fund_code} 近 {years_back} 年")
        base_year = 2024  # 固定从2024开始
        all_holdings = []
        
        for year_offset in range(years_back):
            year = base_year - year_offset
            year_holdings = self.crawl_fund_holdings_by_year(fund_code, year, topline)
            all_holdings.extend(year_holdings)
            time.sleep(np.random.uniform(1, 2))
        
        if all_holdings:
            df = pd.DataFrame(all_holdings)
            numeric_cols = ['ratio_clean', 'shares_clean', 'market_value_clean']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            os.makedirs('data', exist_ok=True)
            output_file = f"data/{fund_code}_holdings_{years_back}y.csv"
            df.to_csv(output_file, index=False, encoding='utf-8-sig')
            
            print(f"💾 保存 {len(df)} 条到 {output_file}")
            print(df[['year', 'quarter', 'stock_code', 'stock_name', 'ratio']].head(5).to_string(index=False))
            return df
        return pd.DataFrame()
    
    def get_fund_basic_info(self, fund_code):
        """获取基金基本信息（修复编码）"""
        url = f"https://fund.eastmoney.com/{fund_code}.html"
        try:
            response = self.session.get(url, timeout=10)
            soup = BeautifulSoup(response.content, 'html.parser', from_encoding='utf-8')
            
            name_elem = soup.select_one('h1') or soup.find('title')
            fund_name = name_elem.get_text().strip() if name_elem else 'Unknown'
            
            return {'fund_code': fund_code, 'fund_name': fund_name, 'fund_type': 'Unknown'}
        except Exception as e:
            print(f"⚠️ {fund_code} 信息失败: {e}")
            return {'fund_code': fund_code, 'fund_name': 'Unknown', 'fund_type': 'Unknown'}
    
    def get_all_fund_codes(self, max_codes=5):
        """获取基金列表（修复JSONP）"""
        rank_url = "https://fund.eastmoney.com/data/rankhandler.aspx"
        params = {
            'op': 'ph', 'dt': 'kf', 'ft': 'gp', 'rs': '', 'gs': '0',
            'sc': 'jn', 'st': 'desc', 'pi': '1', 'pn': str(max_codes * 2), 'dx': '0'
        }
        
        try:
            response = self.session.get(rank_url, params=params, timeout=10)
            data = self.extract_json_from_jsonp(response.text)
            
            if data and 'datas' in data:
                fund_list = data['datas'].split(',') if isinstance(data['datas'], str) else data['datas']
                fund_codes = []
                for item in fund_list:
                    parts = re.split(r'[,|]', item)
                    if len(parts) >= 2 and parts[0].isdigit() and len(parts[0]) == 6:
                        fund_codes.append(parts[0])
                print(f"✅ 获取 {len(fund_codes)} 只基金")
                return fund_codes[:max_codes]
        except Exception as e:
            print(f"❌ 列表失败: {e}")
        
        return ['002580', '000689', '001298', '000001', '000002'][:max_codes]

def main():
    parser = argparse.ArgumentParser(description='基金持仓爬取')
    parser.add_argument('--fund-code', type=str, default='002580')
    parser.add_argument('--years', type=int, default=1)
    parser.add_argument('--topline', type=int, default=10)
    parser.add_argument('--all', action='store_true')
    parser.add_argument('--max-codes', type=int, default=5)
    
    args = parser.parse_args()
    crawler = FundHoldingsCrawler()
    
    if args.all:
        fund_codes = crawler.get_all_fund_codes(args.max_codes)
        all_results = []
        for i, code in enumerate(fund_codes, 1):
            print(f"\n[ {i}/{len(fund_codes)} ] {code}")
            info = crawler.get_fund_basic_info(code)
            print(f"📄 {info['fund_name']}")
            df = crawler.crawl_fund_holdings(code, args.years, args.topline)
            if not df.empty:
                df['fund_name'] = info['fund_name']
                all_results.append(df)
            time.sleep(np.random.uniform(2, 4))
        
        if all_results:
            combined = pd.concat(all_results, ignore_index=True)
            combined.to_csv(f"data/all_holdings_{args.years}y.csv", index=False, encoding='utf-8-sig')
            print(f"\n🎉 总 {len(combined)} 条保存")
    else:
        crawler.crawl_fund_holdings(args.fund_code, args.years, args.topline)

if __name__ == "__main__":
    main()

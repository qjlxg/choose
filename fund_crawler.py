import requests
import pandas as pd
import numpy as np
import time
import os
import re
import json  # 用于更稳定的JSON解析
from bs4 import BeautifulSoup
from datetime import datetime

class FundSignalCrawler:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://fund.eastmoney.com/'
        })
    
    def parse_signals_from_md(self, md_file='market_monitor_report.md'):
        """从 MD 文件解析买入信号基金"""
        if not os.path.exists(md_file):
            print(f"❌ 未找到 {md_file} 文件")
            return []
        
        with open(md_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        print("📖 解析 MD 文件...")
        
        # 查找 "行动信号" 下的 "弱买入" 或 "强买入" 段落
        signal_pattern = r'行动信号.*?(弱买入|强买入).*?(?=(行动信号|$))'
        signals = re.findall(signal_pattern, content, re.DOTALL | re.IGNORECASE)
        
        # 提取基金代码（6位数字）
        fund_codes = re.findall(r'\b\d{6}\b', ' '.join([s[0] for s in signals]))
        
        # 去重并过滤有效代码
        fund_codes = list(set([code for code in fund_codes if len(code) == 6 and code.isdigit()]))
        
        print(f"✅ 找到 {len(signals)} 个买入信号，{len(fund_codes)} 只基金: {fund_codes}")
        return fund_codes
    
    def extract_json_from_jsonp(self, text):
        """提取并解析 JSONP（修复单引号问题）"""
        try:
            # 匹配 var apidata = {...};
            pattern = r'var\s+apidata\s*=\s*({.*?});?\s*$'
            match = re.search(pattern, text, re.DOTALL)
            if match:
                json_str = match.group(1)
                # 替换单引号为双引号（处理 HTML 中的单引号）
                json_str = re.sub(r"(\w+)'?": r'"\1":', json_str)
                json_str = re.sub(r":\s*'([^']*)'?", r': "\1"', json_str)
                # 清理 HTML 转义
                json_str = json_str.replace('\\u003c', '<').replace('\\u003e', '>')
                return json.loads(json_str)
        except Exception as e:
            print(f"解析失败: {e}")
        return None
    
    def get_fund_name(self, fund_code):
        """获取基金名称"""
        url = f"https://fund.eastmoney.com/{fund_code}.html"
        try:
            response = self.session.get(url, timeout=10)
            soup = BeautifulSoup(response.content, 'html.parser')
            name_elem = soup.select_one('h1') or soup.find('title')
            if name_elem:
                name = name_elem.get_text().strip().split(' - ')[0]
                return name
        except:
            pass
        return f"基金{fund_code}"
    
    def crawl_year_holdings(self, fund_code, year):
        """爬取单年持仓"""
        url = "https://fundf10.eastmoney.com/FundArchivesDatas.aspx"
        params = {'type': 'jjcc', 'code': fund_code, 'topline': '10', 'year': year}
        
        try:
            response = self.session.get(url, params=params, timeout=10)
            data = self.extract_json_from_jsonp(response.text)
            
            if not data or 'content' not in data:
                return []
            
            soup = BeautifulSoup(data['content'], 'html.parser')
            holdings = []
            
            boxes = soup.find_all('div', class_='box')
            for box in boxes:
                title = box.find('h4')
                if not title:
                    continue
                
                title_text = title.get_text()
                quarter_match = re.search(r'(\d{4}年)(\d)季度', title_text)
                if not quarter_match:
                    continue
                
                year_str, quarter = quarter_match.groups()
                quarter = int(quarter)
                
                table = box.find('table')
                if not table:
                    continue
                
                rows = table.find_all('tr')[1:]  # 跳过表头
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) < 7:
                        continue
                    
                    code_link = cols[1].find('a')
                    stock_code = ''
                    if code_link:
                        href = code_link.get('href', '')
                        code_match = re.search(r'r/[\d.]+(\d+)', href)
                        if code_match:
                            stock_code = code_match.group(1)
                    
                    if not stock_code or not stock_code.isdigit():
                        continue
                    
                    holding = {
                        'fund_code': fund_code,
                        'year': year_str,
                        'quarter': quarter,
                        'stock_code': stock_code,
                        'stock_name': cols[2].get_text().strip(),
                        'ratio': cols[4].get_text().strip(),
                        'shares': cols[5].get_text().strip(),
                        'market_value': cols[6].get_text().strip().replace(',', '')
                    }
                    
                    # 清洗数值
                    holding['ratio_clean'] = float(holding['ratio'].replace('%', '')) if holding['ratio'] else 0
                    holding['market_value_clean'] = float(holding['market_value']) if holding['market_value'] else 0
                    holding['shares_clean'] = float(holding['shares']) if holding['shares'] else 0
                    
                    holdings.append(holding)
            
            return holdings
        except Exception as e:
            print(f"❌ {fund_code}-{year} 失败: {e}")
            return []
    
    def crawl_fund(self, fund_code):
        """爬取单基金（优先2024年）"""
        print(f"\n📈 爬取 {fund_code}...")
        fund_name = self.get_fund_name(fund_code)
        print(f"   名称: {fund_name}")
        
        years_to_try = [2024, 2023]
        all_holdings = []
        
        for year in years_to_try:
            print(f"   📅 {year}年...")
            year_holdings = self.crawl_year_holdings(fund_code, year)
            if year_holdings:
                all_holdings.extend(year_holdings)
                print(f"   ✅ {len(year_holdings)} 条")
                break
            time.sleep(1)
        
        if not all_holdings:
            print(f"   ❌ 无数据")
            return pd.DataFrame()
        
        df = pd.DataFrame(all_holdings)
        df['fund_name'] = fund_name
        
        os.makedirs('data', exist_ok=True)
        filename = f"data/{fund_code}_{fund_name[:20]}_signal_holdings.csv"
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        
        print(f"   💾 {len(df)} 条 → {filename}")
        print(df[['year', 'quarter', 'stock_code', 'stock_name', 'ratio']].head(3).to_string(index=False))
        
        return df

def main():
    """主函数 - 只爬买入信号基金"""
    print("🚀 买入信号基金持仓爬取")
    print("📖 从 market_monitor_report.md 解析弱/强买入基金")
    print("-" * 50)
    
    crawler = FundSignalCrawler()
    fund_codes = crawler.parse_signals_from_md()
    
    if not fund_codes:
        print("❌ 未找到买入信号基金")
        return
    
    all_data = []
    success_count = 0
    
    for i, code in enumerate(fund_codes, 1):
        print(f"\n[{i}/{len(fund_codes)}] {code}")
        df = crawler.crawl_fund(code)
        
        if not df.empty:
            all_data.append(df)
            success_count += 1
        
        if i < len(fund_codes):
            wait = np.random.uniform(2, 4)
            print(f"   ⏳ 等待 {wait:.1f}s...")
            time.sleep(wait)
    
    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        summary_file = f"data/买入信号汇总_{datetime.now().strftime('%Y%m%d')}.csv"
        combined.to_csv(summary_file, index=False, encoding='utf-8-sig')
        
        print(f"\n🎉 完成！")
        print(f"✅ 成功: {success_count}/{len(fund_codes)} 只")
        print(f"📊 总记录: {len(combined)} 条")
        print(f"💾 汇总: {summary_file}")
        
        print("\n📈 统计:")
        stats = combined.groupby('fund_code').agg({'stock_code': 'count', 'ratio_clean': 'sum'}).round(2)
        stats.columns = ['持仓数', '总占比(%)']
        print(stats.to_string())

if __name__ == "__main__":
    main()

import requests
import pandas as pd
import numpy as np
import time
import os
import re
import ast
from bs4 import BeautifulSoup
from datetime import datetime

class FundHoldingsCrawler:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://fund.eastmoney.com/'
        })
    
    def extract_json_from_jsonp(self, text):
        """提取JSONP中的JSON数据"""
        try:
            # 匹配 var XXX = {...};
            pattern = r'var\s+\w+\s*=\s*({.*?});?\s*$'
            match = re.search(pattern, text, re.DOTALL)
            if match:
                json_str = match.group(1)
                # 替换单引号为双引号，处理HTML中的单引号问题
                json_str = re.sub(r"'([^']*)':", r'"\1":', json_str)
                json_str = re.sub(r":\s*'([^']*)'", r': "\1"', json_str)
                return json.loads(json_str)
        except Exception as e:
            print(f"解析失败: {e}")
            return None
        return None
    
    def get_fund_list(self):
        """获取基金列表"""
        print("📋 获取基金列表...")
        url = "https://fund.eastmoney.com/data/rankhandler.aspx"
        params = {
            'op': 'ph', 'dt': 'kf', 'ft': 'gp', 'rs': '', 'gs': '0',
            'sc': 'jn', 'st': 'desc', 'pi': '1', 'pn': '50', 'dx': '0'
        }
        
        try:
            response = self.session.get(url, params=params, timeout=10)
            data = self.extract_json_from_jsonp(response.text)
            
            if data and 'datas' in data:
                fund_list = data['datas'].split('|') if isinstance(data['datas'], str) else []
                fund_codes = []
                for item in fund_list:
                    if '|' in item:
                        parts = item.split('|')
                        if len(parts) >= 1 and parts[0].isdigit() and len(parts[0]) == 6:
                            fund_codes.append(parts[0])
                print(f"✅ 获取到 {len(fund_codes)} 只基金")
                return fund_codes[:5]  # 默认前5只
        except Exception as e:
            print(f"❌ 获取列表失败: {e}")
        
        # 默认基金列表
        print("📋 使用默认基金列表")
        return ['002580', '000689', '001298', '000001', '000002']
    
    def get_fund_name(self, fund_code):
        """获取基金名称"""
        url = f"https://fund.eastmoney.com/{fund_code}.html"
        try:
            response = self.session.get(url, timeout=10)
            soup = BeautifulSoup(response.content, 'html.parser')
            name_elem = soup.select_one('h1') or soup.find('title')
            if name_elem:
                name = name_elem.get_text().strip()
                # 清理标题中的多余文字
                if ' - ' in name:
                    name = name.split(' - ')[0]
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
            
            # 查找所有季度
            boxes = soup.find_all('div', class_='box')
            for box in boxes:
                # 提取季度信息
                title = box.find('h4')
                if not title:
                    continue
                
                title_text = title.get_text()
                quarter_match = re.search(r'(\d{4}年)(\d)季度', title_text)
                if not quarter_match:
                    continue
                
                year_str, quarter = quarter_match.groups()
                quarter = int(quarter)
                
                # 查找表格
                table = box.find('table')
                if not table:
                    continue
                
                rows = table.find_all('tr')[1:]  # 跳过表头
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) < 7:
                        continue
                    
                    # 提取股票代码
                    code_link = cols[1].find('a')
                    stock_code = ''
                    if code_link:
                        href = code_link.get('href', '')
                        code_match = re.search(r'r/[\d.]+(\d+)', href)
                        if code_match:
                            stock_code = code_match.group(1)
                    
                    if not stock_code or not stock_code.isdigit():
                        continue
                    
                    # 提取数据
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
                    
                    # 数据清洗
                    if holding['ratio']:
                        holding['ratio_clean'] = float(holding['ratio'].replace('%', ''))
                    if holding['market_value']:
                        holding['market_value_clean'] = float(holding['market_value'])
                    if holding['shares']:
                        holding['shares_clean'] = float(holding['shares'])
                    
                    holdings.append(holding)
            
            return holdings
        except Exception as e:
            print(f"❌ {fund_code}-{year}失败: {e}")
            return []
    
    def crawl_fund(self, fund_code):
        """爬取单只基金（默认2024年）"""
        print(f"\n📈 正在爬取 {fund_code}...")
        fund_name = self.get_fund_name(fund_code)
        print(f"   基金名称: {fund_name}")
        
        # 尝试2024年，如果没有则尝试2023年
        years_to_try = [2024, 2023]
        all_holdings = []
        
        for year in years_to_try:
            print(f"   📅 尝试{year}年数据...")
            year_holdings = self.crawl_year_holdings(fund_code, year)
            if year_holdings:
                all_holdings.extend(year_holdings)
                print(f"   ✅ {year}年获取 {len(year_holdings)} 条")
                break  # 成功获取就停止
            time.sleep(1)
        
        if not all_holdings:
            print(f"   ❌ {fund_code} 无可用数据")
            return pd.DataFrame()
        
        # 保存数据
        df = pd.DataFrame(all_holdings)
        df['fund_name'] = fund_name
        
        os.makedirs('data', exist_ok=True)
        filename = f"data/{fund_code}_{fund_name[:20]}_holdings.csv"
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        
        print(f"   💾 保存 {len(df)} 条到 {filename}")
        print(f"   📊 前3条预览:")
        preview_cols = ['year', 'quarter', 'stock_code', 'stock_name', 'ratio']
        print(df[preview_cols].head(3).to_string(index=False))
        
        return df

def main():
    """主函数 - 无参数直接运行"""
    print("🚀 基金持仓爬取工具启动")
    print("📅 默认配置: 前5只基金, 2024年数据, 每季度前10只持仓")
    print("-" * 50)
    
    crawler = FundHoldingsCrawler()
    
    # 获取基金列表
    fund_codes = crawler.get_fund_list()
    
    # 爬取所有基金
    all_data = []
    success_count = 0
    
    for i, code in enumerate(fund_codes, 1):
        print(f"\n[{i}/{len(fund_codes)}] {code}")
        df = crawler.crawl_fund(code)
        
        if not df.empty:
            all_data.append(df)
            success_count += 1
        
        # 延时防反爬
        if i < len(fund_codes):
            wait = np.random.uniform(2, 4)
            print(f"   ⏳ 等待 {wait:.1f}秒...")
            time.sleep(wait)
    
    # 生成汇总报告
    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        summary_file = f"data/汇总_{datetime.now().strftime('%Y%m%d')}.csv"
        combined.to_csv(summary_file, index=False, encoding='utf-8-sig')
        
        print(f"\n🎉 任务完成！")
        print(f"✅ 成功: {success_count}/{len(fund_codes)} 只基金")
        print(f"📊 总记录: {len(combined)} 条")
        print(f"💾 汇总文件: {summary_file}")
        
        # 显示总体统计
        print("\n📈 按基金统计:")
        stats = combined.groupby('fund_code').agg({
            'stock_code': 'count',
            'ratio_clean': 'sum'
        }).round(2)
        stats.columns = ['持仓数量', '总占比']
        print(stats.to_string())
    else:
        print("\n❌ 没有获取到任何数据")

if __name__ == "__main__":
    main()

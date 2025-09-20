import requests
import pandas as pd
import numpy as np
import time
import os
import re
import json
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
        """从 Markdown 表格解析买入信号基金（增强版）"""
        if not os.path.exists(md_file):
            print(f"❌ 未找到 {md_file} 文件")
            # 调试：显示当前目录文件
            print("📁 当前目录文件:")
            for f in os.listdir('.'):
                print(f"   - {f}")
            return []
        
        with open(md_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        print("📖 解析 Markdown 表格...")
        print(f"📄 文件大小: {len(content)} 字符")
        print(f"🔍 前200字符预览:\n{content[:200]}...")
        
        # 更宽松的表格匹配
        # 查找包含"基金代码"和"行动信号"的表格
        table_pattern = r'(?s).*?\|.*?(?:基金代码).*?\|.*?\|.*?\|.*?\|.*?\|.*?\|.*?\|.*?\|.*?\|.*?\|.*?行动信号.*?\|.*?(?=\n\n|\Z)'
        table_match = re.search(table_pattern, content, re.DOTALL | re.IGNORECASE)
        
        if not table_match:
            print("❌ 正则匹配失败，尝试逐行解析...")
            
            # 备用方案：逐行解析
            lines = content.split('\n')
            in_table = False
            table_lines = []
            
            for line in lines:
                line = line.strip()
                if line.startswith('|') and '基金代码' in line and '行动信号' in line:
                    in_table = True
                    table_lines = [line]
                    print(f"✅ 找到表头: {line[:80]}...")
                    continue
                if in_table:
                    if line.startswith('|') and len(line.split('|')) > 8:
                        table_lines.append(line)
                    elif not line.startswith('|') and len(table_lines) > 1:
                        in_table = False
            
            if table_lines:
                print(f"✅ 找到 {len(table_lines)} 行表格数据")
                return self._parse_table_lines(table_lines)
            else:
                print("❌ 备用解析也失败")
                print("🔍 文件内容预览:")
                for i, line in enumerate(lines[:10]):
                    print(f"   {i+1}: {repr(line)[:60]}")
                return []
        
        table_content = table_match.group(0)
        print(f"✅ 找到表格: {len(table_content)} 字符")
        
        lines = [line.strip() for line in table_content.split('\n') if line.strip()]
        print(f"📊 表格行数: {len(lines)}")
        
        # 找到表头
        header_line = None
        for i, line in enumerate(lines):
            if line.startswith('|') and '基金代码' in line and '行动信号' in line:
                header_line = i
                print(f"✅ 表头行 {i}: {line}")
                break
        
        if header_line is None:
            print("❌ 未找到表头行")
            return []
        
        return self._parse_table_lines(lines[header_line:])
    
    def _parse_table_lines(self, table_lines):
        """解析表格行"""
        fund_signals = []
        
        # 跳过表头和分隔线
        data_start = 2 if len(table_lines) > 2 and '|---' in table_lines[1] else 1
        
        print(f"📊 开始解析数据行 (从第 {data_start} 行)")
        
        for i, line in enumerate(table_lines[data_start:], data_start):
            if not line.startswith('|'):
                continue
            
            # 分割单元格
            parts = line.split('|')
            if len(parts) < 10:  # 至少10个 | 分隔符
                print(f"⚠️  行 {i} 格式错误: {line[:50]}...")
                continue
            
            # 提取单元格内容
            cells = [part.strip() for part in parts[1:-1]]  # 去掉首尾空单元格
            
            if len(cells) < 8:
                print(f"⚠️  行 {i} 单元格不足: {len(cells)} 个")
                continue
            
            fund_code = cells[0].strip()
            action_signal = cells[-1].strip()  # 最后一列
            
            print(f"🔍 行 {i}: 代码={fund_code}, 信号={action_signal}")
            
            # 验证格式
            if re.match(r'^\d{6}$', fund_code) and '买入' in action_signal:
                fund_signals.append({
                    'fund_code': fund_code,
                    'signal': action_signal
                })
                print(f"   ✅ 添加: {fund_code} ({action_signal})")
            else:
                print(f"   ❌ 跳过: 代码={fund_code}, 信号={action_signal}")
        
        fund_codes = [fs['fund_code'] for fs in fund_signals]
        print(f"📊 最终结果: {len(fund_signals)} 只买入信号基金")
        if fund_codes:
            print(f"   📋 基金列表: {', '.join(fund_codes[:5])}{'...' if len(fund_codes) > 5 else ''}")
        
        return fund_codes
    
    def extract_json_from_jsonp(self, text):
        """提取 JSONP 数据"""
        try:
            # 匹配 var apidata = {...};
            pattern = r'var\s+apidata\s*=\s*(\{.*?\});?\s*$'
            match = re.search(pattern, text, re.DOTALL)
            if not match:
                print("❌ 未找到 apidata 变量")
                return None
            
            json_str = match.group(1)
            
            # 处理引号问题
            json_str = re.sub(r"(\b\w+\b)'?\s*:", r'"\1":', json_str)
            json_str = re.sub(r":\s*'([^']*)'?", r': "\1"', json_str)
            json_str = json_str.replace('\\"', '"').replace("\\'", "'")
            
            # 清理 HTML 转义
            json_str = re.sub(r'\\u003c', '<', json_str)
            json_str = re.sub(r'\\u003e', '>', json_str)
            
            return json.loads(json_str)
            
        except json.JSONDecodeError as e:
            print(f"❌ JSON 解析失败: {e}")
            print(f"🔍 尝试解析: {json_str[:100] if 'json_str' in locals() else text[:100]}...")
        except Exception as e:
            print(f"❌ 其他错误: {e}")
        
        return None
    
    def get_fund_name(self, fund_code):
        """获取基金名称"""
        url = f"https://fund.eastmoney.com/{fund_code}.html"
        try:
            response = self.session.get(url, timeout=10)
            soup = BeautifulSoup(response.content, 'html.parser')
            name_elem = soup.select_one('h1') or soup.find('title')
            if name_elem:
                name = name_elem.get_text().strip()
                if ' - ' in name:
                    name = name.split(' - ')[0]
                return name
        except Exception:
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
                title = box.find('h4', class_='t')
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
                
                rows = table.find_all('tr')[1:]
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) < 7:
                        continue
                    
                    code_link = cols[1].find('a')
                    stock_code = ''
                    if code_link and code_link.get('href'):
                        href = code_link['href']
                        code_match = re.search(r'r/[\d.]+(\d+)', href)
                        if code_match:
                            stock_code = code_match.group(1)
                    
                    if stock_code and stock_code.isdigit():
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
                        
                        holding['ratio_clean'] = float(holding['ratio'].replace('%', '')) if holding['ratio'] else 0
                        holding['market_value_clean'] = float(holding['market_value']) if holding['market_value'] else 0
                        holding['shares_clean'] = float(holding['shares']) if holding['shares'] else 0
                        
                        holdings.append(holding)
            
            return holdings
            
        except Exception as e:
            print(f"   ❌ {year}年失败: {e}")
            return []
    
    def crawl_fund(self, fund_code):
        """爬取单基金"""
        print(f"\n📈 [{fund_code}] 爬取...")
        fund_name = self.get_fund_name(fund_code)
        print(f"   📋 {fund_name}")
        
        years_to_try = [2024, 2023]
        all_holdings = []
        
        for year in years_to_try:
            year_holdings = self.crawl_year_holdings(fund_code, year)
            if year_holdings:
                all_holdings.extend(year_holdings)
                print(f"   ✅ {year}年: {len(year_holdings)} 条")
                break
            time.sleep(0.5)
        
        if not all_holdings:
            print(f"   ❌ 无数据")
            return pd.DataFrame()
        
        df = pd.DataFrame(all_holdings)
        df['fund_name'] = fund_name
        
        os.makedirs('data', exist_ok=True)
        safe_name = re.sub(r'[^\w\s-]', '', fund_name)[:20]
        filename = f"data/{fund_code}_{safe_name}_买入信号.csv"
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        
        print(f"   💾 {len(df)} 条 → {filename}")
        return df

def main():
    """主函数"""
    print("🚀 买入信号基金持仓分析")
    print("=" * 50)
    
    crawler = FundSignalCrawler()
    fund_codes = crawler.parse_signals_from_md()
    
    if not fund_codes:
        print("\n💡 建议检查:")
        print("1. market_monitor_report.md 是否在根目录")
        print("2. 文件中是否包含正确的表格格式")
        print("3. 表格是否有 '基金代码' 和 '行动信号' 列")
        print("4. '行动信号' 列是否包含 '弱买入' 或 '强买入'")
        return
    
    print(f"\n🎯 开始爬取 {len(fund_codes)} 只基金")
    
    all_data = []
    for i, code in enumerate(fund_codes, 1):
        df = crawler.crawl_fund(code)
        if not df.empty:
            all_data.append(df)
        
        if i < len(fund_codes):
            time.sleep(2)
    
    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M')
        summary_file = f"data/买入信号汇总_{timestamp}.csv"
        combined.to_csv(summary_file, index=False, encoding='utf-8-sig')
        
        print(f"\n🎉 完成！总 {len(combined)} 条记录")
        print(f"💾 汇总: {summary_file}")
    else:
        print("\n❌ 无数据")

if __name__ == "__main__":
    main()

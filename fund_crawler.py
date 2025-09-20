import requests
import pandas as pd
import numpy as np
import time
import os
import re
import json
from bs4 import BeautifulSoup
from datetime import datetime

# 检查并导入所需的库
try:
    import requests
    import pandas as pd
    import numpy as np
    from bs4 import BeautifulSoup
except ImportError as e:
    print(f"❌ 缺少必要的Python库：{e}")
    print("请使用以下命令安装：pip install requests pandas beautifulsoup4 lxml")
    exit()

class FundSignalCrawler:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://fund.eastmoney.com/'
        })

    def parse_signals_from_md(self, md_file='market_monitor_report.md'):
        """从 Markdown 表格解析买入信号基金（增强版）"""
        print("🔍 正在检查 'market_monitor_report.md' 文件...")
        if not os.path.exists(md_file):
            print(f"❌ 未找到 {md_file} 文件")
            print("请确保该文件与脚本在同一个目录下。")
            return []

        with open(md_file, 'r', encoding='utf-8') as f:
            content = f.read()

        print("📖 解析 Markdown 表格...")
        print(f"📄 文件大小: {len(content)} 字符")

        # 匹配包含"基金代码"和"行动信号"的表格
        table_pattern = r'(?s).*?\|.*?基金代码.*?\|.*?\|.*?\|.*?\|.*?\|.*?\|.*?\|.*?\|.*?\|.*?\|.*?行动信号.*?\|.*?(?=\n\n|\Z)'
        table_match = re.search(table_pattern, content, re.DOTALL | re.IGNORECASE)

        if not table_match:
            print("❌ 正则匹配失败，尝试逐行解析...")
            lines = content.split('\n')
            in_table = False
            table_lines = []

            for line in lines:
                line = line.strip()
                if line.startswith('|') and '基金代码' in line and '行动信号' in line:
                    in_table = True
                    table_lines = [line]
                    print(f"✅ 找到表头...")
                    continue
                if in_table:
                    if line.startswith('|') and len(line.split('|')) > 8:
                        table_lines.append(line)
                    elif not line.strip() or not line.startswith('|'):
                        in_table = False

            if table_lines:
                print(f"✅ 找到 {len(table_lines) - 1} 行表格数据")
                return self._parse_table_lines(table_lines)
            else:
                print("❌ 备用解析也失败")
                return []

        table_content = table_match.group(0)
        print(f"✅ 找到表格内容，总计 {len(table_content)} 字符")

        lines = [line.strip() for line in table_content.split('\n') if line.strip()]

        header_line_index = -1
        for i, line in enumerate(lines):
            if line.startswith('|') and '基金代码' in line and '行动信号' in line:
                header_line_index = i
                break

        if header_line_index == -1:
            print("❌ 未找到表头行")
            return []

        return self._parse_table_lines(lines[header_line_index:])

    def _parse_table_lines(self, table_lines):
        """解析表格行"""
        fund_codes = []
        data_lines = [line for line in table_lines if line.strip().count('|') > 8 and '|-' not in line]

        print(f"📊 正在解析 {len(data_lines)} 行数据...")

        for line in data_lines:
            parts = [p.strip() for p in line.split('|')]
            if len(parts) >= 10:
                fund_code = parts[1]
                action_signal = parts[-2]
                if re.match(r'^\d{6}$', fund_code) and '买入' in action_signal:
                    fund_codes.append(fund_code)

        print(f"📊 最终找到 {len(fund_codes)} 只买入信号基金")
        if fund_codes:
            print(f"📋 基金列表: {', '.join(fund_codes[:5])}{'...' if len(fund_codes) > 5 else ''}")
        
        return fund_codes

    def extract_json_from_jsonp(self, text):
        """提取 JSONP 数据"""
        try:
            pattern = r'var\s+apidata\s*=\s*(\{.*?\});?\s*$'
            match = re.search(pattern, text, re.DOTALL)
            if not match:
                print("❌ 未找到 apidata 变量")
                return None

            json_str = match.group(1)
            json_str = json_str.replace("'", '"')
            json_str = re.sub(r'(\w+):', r'"\1":', json_str)
            return json.loads(json_str)

        except json.JSONDecodeError as e:
            print(f"❌ JSON 解析失败: {e}")
            return None
        except Exception as e:
            print(f"❌ 其他错误: {e}")
            return None

    def get_fund_name(self, fund_code):
        """获取基金名称"""
        url = f"https://fund.eastmoney.com/{fund_code}.html"
        try:
            response = self.session.get(url, timeout=10)
            soup = BeautifulSoup(response.content, 'lxml')
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
            print(f"  请求 {year} 年持仓数据...")
            response = self.session.get(url, params=params, timeout=10)
            data = self.extract_json_from_jsonp(response.text)

            if not data or 'content' not in data:
                print(f"  ❌ {year}年无数据")
                return []

            soup = BeautifulSoup(data['content'], 'lxml')
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
            print(f"  ❌ 爬取 {year} 年数据失败: {e}")
            return []

    def crawl_fund(self, fund_code):
        """爬取单基金"""
        print(f"\n📈 [{fund_code}] 正在爬取...")
        fund_name = self.get_fund_name(fund_code)
        print(f"  📋 基金名称: {fund_name}")
        
        years_to_try = [datetime.now().year, datetime.now().year - 1]
        all_holdings = []
        
        for year in years_to_try:
            year_holdings = self.crawl_year_holdings(fund_code, year)
            if year_holdings:
                all_holdings.extend(year_holdings)
                print(f"  ✅ {year}年: 找到 {len(year_holdings)} 条记录")
                break
            time.sleep(0.5)
        
        if not all_holdings:
            print(f"  ❌ 无数据")
            return pd.DataFrame()
        
        df = pd.DataFrame(all_holdings)
        df['fund_name'] = fund_name
        
        os.makedirs('data', exist_ok=True)
        safe_name = re.sub(r'[^\w\s-]', '', fund_name)[:20]
        filename = f"data/{fund_code}_{safe_name}_买入信号.csv"
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        
        print(f"  💾 {len(df)} 条记录 → {filename}")
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
        print("4. '行动信号' 列是否包含 '买入' 关键词")
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
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        summary_file = f"data/买入信号汇总_{timestamp}.csv"
        combined.to_csv(summary_file, index=False, encoding='utf-8-sig')
        
        print(f"\n🎉 完成！总 {len(combined)} 条记录")
        print(f"💾 汇总文件已保存至: {summary_file}")
    else:
        print("\n❌ 爬取完成，但无有效数据。")

if __name__ == "__main__":
    main()

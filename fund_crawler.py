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
        """从 Markdown 表格解析买入信号基金"""
        if not os.path.exists(md_file):
            print(f"❌ 未找到 {md_file} 文件")
            return []
        
        with open(md_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        print("📖 解析 Markdown 表格...")
        
        # 查找表格
        table_pattern = r'\|.*基金代码.*\|.*行动信号.*\|(?:\n\|.*\|.*\|)*'
        table_match = re.search(table_pattern, content, re.DOTALL)
        
        if not table_match:
            print("❌ 未找到基金表格")
            return []
        
        table_content = table_match.group(0)
        
        # 分割行
        lines = [line.strip() for line in table_content.split('\n') if line.strip()]
        
        # 找到表头行（包含"基金代码"和"行动信号"）
        header_line = None
        for i, line in enumerate(lines):
            if '基金代码' in line and '行动信号' in line:
                header_line = i
                break
        
        if header_line is None:
            print("❌ 未找到表头行")
            return []
        
        # 解析数据行
        fund_signals = []
        for line in lines[header_line + 2:]:  # 跳过表头、分隔线
            if not line.startswith('|') or line.endswith('|'):
                continue
                
            # 清理 Markdown 表格格式
            cells = [cell.strip() for cell in line.split('|')[1:-1]]  # 去掉首尾的空单元格
            
            if len(cells) >= 8:  # 确保有足够的列
                fund_code = cells[0].strip()  # 第一列：基金代码
                action_signal = cells[7].strip()  # 最后一列：行动信号
                
                # 验证基金代码格式
                if re.match(r'^\d{6}$', fund_code):
                    if '买入' in action_signal:  # 包含"弱买入"或"强买入"
                        fund_signals.append({
                            'fund_code': fund_code,
                            'signal': action_signal
                        })
                        print(f"   ✅ {fund_code}: {action_signal}")
        
        fund_codes = [fs['fund_code'] for fs in fund_signals]
        print(f"📊 找到 {len(fund_signals)} 只买入信号基金: {fund_codes}")
        
        return fund_codes
    
    def extract_json_from_jsonp(self, text):
        """提取并解析 JSONP"""
        try:
            # 匹配 var apidata = {...};
            pattern = r'var\s+apidata\s*=\s*({.*?});?\s*$'
            match = re.search(pattern, text, re.DOTALL)
            if match:
                json_str = match.group(1)
                # 处理单引号问题
                json_str = re.sub(r"(\w+)'?": r'"\1":', json_str)
                json_str = re.sub(r":\s*'([^']*)'?", r': "\1"', json_str)
                # 清理转义字符
                json_str = json_str.replace('\\"', '"').replace("\\'", "'")
                return json.loads(json_str)
        except Exception as e:
            print(f"JSON解析失败: {e}")
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
        except Exception as e:
            print(f"   ⚠️ 获取 {fund_code} 名称失败: {e}")
        return f"基金{fund_code}"
    
    def crawl_year_holdings(self, fund_code, year):
        """爬取单年持仓数据"""
        url = "https://fundf10.eastmoney.com/FundArchivesDatas.aspx"
        params = {
            'type': 'jjcc', 
            'code': fund_code, 
            'topline': '10',  # 前10只持仓
            'year': year
        }
        
        try:
            print(f"      📡 请求 {year}年数据...")
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = self.extract_json_from_jsonp(response.text)
            if not data or 'content' not in data:
                print(f"      ❌ 无 {year}年数据")
                return []
            
            soup = BeautifulSoup(data['content'], 'html.parser')
            holdings = []
            
            # 查找所有季度块
            boxes = soup.find_all('div', class_='box')
            print(f"      📊 找到 {len(boxes)} 个季度")
            
            for box in boxes:
                # 提取季度标题
                title = box.find('h4', class_='t')
                if not title:
                    continue
                
                title_text = title.get_text().strip()
                quarter_match = re.search(r'(\d{4}年)(\d)季度', title_text)
                if not quarter_match:
                    continue
                
                year_str, quarter = quarter_match.groups()
                quarter = int(quarter)
                
                # 提取报告日期
                date_elem = title.find('font', class_='px12')
                report_date = date_elem.get_text().strip() if date_elem else f"{year_str}Q{quarter}"
                
                # 查找表格
                table = box.find('table', class_=re.compile(r'tzxq'))
                if not table:
                    continue
                
                # 解析表格行
                tbody = table.find('tbody')
                if tbody:
                    rows = tbody.find_all('tr')
                else:
                    rows = table.find_all('tr')[1:]  # 跳过表头
                
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) < 7:
                        continue
                    
                    # 提取股票代码
                    code_link = cols[1].find('a')
                    stock_code = ''
                    if code_link and code_link.get('href'):
                        href = code_link['href']
                        code_match = re.search(r'r/[\d.]+(\d+)', href)
                        if code_match:
                            stock_code = code_match.group(1)
                    
                    if not stock_code or not stock_code.isdigit():
                        continue
                    
                    # 提取持仓信息
                    holding = {
                        'fund_code': fund_code,
                        'year': year_str,
                        'quarter': quarter,
                        'report_date': report_date,
                        'stock_code': stock_code,
                        'stock_name': cols[2].get_text().strip(),
                        'ratio': cols[4].get_text().strip(),
                        'shares': cols[5].get_text().strip(),
                        'market_value': cols[6].get_text().strip().replace(',', '')
                    }
                    
                    # 数值清洗
                    holding['ratio_clean'] = float(holding['ratio'].replace('%', '')) if holding['ratio'] else 0
                    holding['market_value_clean'] = float(holding['market_value']) if holding['market_value'] else 0
                    holding['shares_clean'] = float(holding['shares']) if holding['shares'] else 0
                    
                    holdings.append(holding)
            
            print(f"      ✅ {year}年: {len(holdings)} 条记录")
            return holdings
            
        except requests.RequestException as e:
            print(f"      ❌ 请求失败: {e}")
            return []
        except Exception as e:
            print(f"      ❌ 解析失败: {e}")
            return []
    
    def crawl_fund(self, fund_code):
        """爬取单只基金的持仓"""
        print(f"\n📈 [{fund_code}] 正在爬取...")
        fund_name = self.get_fund_name(fund_code)
        print(f"   📋 名称: {fund_name}")
        
        # 优先尝试最近年份
        years_to_try = [2024, 2023, 2022]
        all_holdings = []
        
        for year in years_to_try:
            year_holdings = self.crawl_year_holdings(fund_code, year)
            if year_holdings:
                all_holdings.extend(year_holdings)
                print(f"   🎯 {year}年成功: {len(year_holdings)} 条")
                break  # 成功就停止尝试更早年份
            time.sleep(0.5)  # 短暂延时
        
        if not all_holdings:
            print(f"   ❌ 无可用持仓数据")
            return pd.DataFrame()
        
        # 保存单基金数据
        df = pd.DataFrame(all_holdings)
        df['fund_name'] = fund_name
        
        os.makedirs('data', exist_ok=True)
        safe_name = re.sub(r'[^\w\s-]', '', fund_name)[:20]  # 清理文件名
        filename = f"data/{fund_code}_{safe_name}_买入信号.csv"
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        
        print(f"   💾 保存 {len(df)} 条 → {filename}")
        
        # 显示前5条预览
        preview_cols = ['year', 'quarter', 'stock_code', 'stock_name', 'ratio']
        print(f"   📊 前5条:")
        print(df[preview_cols].head().to_string(index=False))
        
        return df

def main():
    """主函数 - 直接运行"""
    print("🚀 买入信号基金持仓分析工具")
    print("=" * 60)
    
    crawler = FundSignalCrawler()
    fund_codes = crawler.parse_signals_from_md()
    
    if not fund_codes:
        print("❌ MD 文件中未找到弱买入/强买入信号的基金")
        print("请检查 market_monitor_report.md 文件格式")
        return
    
    print(f"\n🎯 目标: 爬取 {len(fund_codes)} 只买入信号基金")
    print("-" * 60)
    
    all_data = []
    success_count = 0
    
    for i, code in enumerate(fund_codes, 1):
        print(f"\n[{i:2d}/{len(fund_codes)}] {code}")
        df = crawler.crawl_fund(code)
        
        if not df.empty:
            all_data.append(df)
            success_count += 1
        
        # 防反爬延时
        if i < len(fund_codes):
            wait_time = np.random.uniform(2, 4)
            print(f"   ⏳ 等待 {wait_time:.1f} 秒...")
            time.sleep(wait_time)
    
    # 生成汇总报告
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        
        # 创建汇总文件名
        timestamp = datetime.now().strftime('%Y%m%d_%H%M')
        summary_file = f"data/买入信号基金汇总_{timestamp}.csv"
        combined_df.to_csv(summary_file, index=False, encoding='utf-8-sig')
        
        print(f"\n🎉 任务完成！")
        print(f"✅ 成功: {success_count}/{len(fund_codes)} 只基金")
        print(f"📊 总持仓记录: {len(combined_df):,} 条")
        print(f"💾 汇总文件: {summary_file}")
        
        # 生成统计报告
        print(f"\n📈 买入信号基金统计:")
        stats = combined_df.groupby(['fund_code', 'fund_name']).agg({
            'stock_code': 'count',
            'ratio_clean': ['sum', 'mean']
        }).round(2)
        stats.columns = ['持仓数量', '总占比(%)', '平均占比(%)']
        print(stats.to_string())
        
        # 生成简单分析
        print(f"\n💡 快速分析:")
        total_funds = len(fund_codes)
        avg_holdings = len(combined_df) / success_count if success_count > 0 else 0
        print(f"   • 总基金数: {total_funds} 只")
        print(f"   • 成功获取: {success_count} 只")
        print(f"   • 平均每基金: {avg_holdings:.1f} 条持仓")
        
    else:
        print("\n❌ 没有成功获取任何基金数据")
        print("可能原因: 网络问题、API变化、或基金无持仓数据")

if __name__ == "__main__":
    main()

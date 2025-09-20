import pandas as pd
import requests
from datetime import datetime
import os
import time
from io import StringIO

def fetch_fund_holdings(fund_code, year):
    """
    从东方财富网获取特定年份的基金持仓信息
    """
    url = f"http://fundf10.eastmoney.com/FundArchivesDatas.aspx?type=jjcc&code={fund_code}&topline=10&year={year}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        # 使用 StringIO 包装字符串，以消除 FutureWaring
        tables = pd.read_html(StringIO(response.text), encoding='utf-8')
        if tables:
            holdings_table = tables[0]
            print(f"✅ 成功获取基金 {fund_code} 在 {year} 年的持仓数据。")
            return holdings_table
        else:
            print(f"⚠️ 无法从 {fund_code} 获取 {year} 年的表格数据。")
            return None
    except Exception as e:
        print(f"❌ 获取基金 {fund_code} 在 {year} 年的数据时出错：{e}")
        return None

def main():
    """
    主函数：读取基金代码并抓取多年度数据
    """
    today_date = datetime.now().strftime('%Y%m%d')
    input_csv_path = f'data/买入信号基金_{today_date}.csv'
    
    print(f"🚀 正在检查输入文件路径: {input_csv_path}")
    
    if not os.path.exists(input_csv_path):
        print(f"❌ 输入文件 {input_csv_path} 不存在。")
        return
    else:
        print(f"✅ 找到输入文件：{input_csv_path}")
        
    output_dir = 'fund_data'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"✅ 创建输出目录: {output_dir}")
        
    try:
        df = pd.read_csv(input_csv_path)
        fund_codes = df['fund_code'].unique()
        print(f"✅ 成功读取到 {len(fund_codes)} 个基金代码。")
    except Exception as e:
        print(f"❌ 读取 CSV 文件时出错：{e}")
        return

    years_to_fetch = [2023, 2024, 2025]
    for code in fund_codes:
        for year in years_to_fetch:
            print(f"----------------------------------------")
            print(f"🔍 正在处理基金代码: {code}，年份: {year}")
            holdings_df = fetch_fund_holdings(str(code).zfill(6), year)
            
            if holdings_df is not None:
                output_path = os.path.join(output_dir, f'持仓_{code}_{year}.csv')
                holdings_df.to_csv(output_path, index=False, encoding='utf-8-sig')
                print(f"✅ 持仓数据已保存至 {output_path}")
            
            time.sleep(2)
        
    print(f"----------------------------------------")
    print(f"✅ 所有基金和年份处理完毕。")

if __name__ == "__main__":
    main()

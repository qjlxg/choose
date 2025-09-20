import pandas as pd
import glob
import os

def analyze_holdings():
    """
    遍历 fund_data 目录，对每个基金代码的持仓数据进行合并和分析，
    并将结果输出到 analysis_report.md 文件中。
    """
    base_path = 'fund_data'
    # 动态获取 fund_data 目录下的所有 CSV 文件
    all_files = glob.glob(os.path.join(base_path, "*.csv"))

    if not all_files:
        print("未在 'fund_data' 目录中找到任何 CSV 文件。")
        return

    # 根据文件名中的基金代码分组
    fund_files = {}
    for f in all_files:
        try:
            fund_code = os.path.basename(f).split('_')[1]
            if fund_code not in fund_files:
                fund_files[fund_code] = []
            fund_files[fund_code].append(f)
        except IndexError:
            print(f"文件名格式不正确，跳过：{f}")
            continue

    if not fund_files:
        print("未找到任何有效基金文件。")
        return

    report = []

    # 逐个基金进行分析
    for fund_code, files in fund_files.items():
        df_list = []
        for f in files:
            try:
                df = pd.read_csv(f)
                df['基金代码'] = fund_code
                df_list.append(df)
            except Exception as e:
                print(f"读取文件 {f} 时出错：{e}")
        
        if not df_list:
            continue
            
        combined_df = pd.concat(df_list, ignore_index=True)
        
        # 确保列名一致，并重命名以方便处理
        combined_df.rename(columns={'占净值 比例': '占净值比例', '持仓市值 （万元）': '持仓市值', '股票代码': '股票代码'}, inplace=True)

        # 针对 2025 年文件，处理额外的列
        if '最新价' in combined_df.columns:
            combined_df.drop(columns=['最新价', '涨跌幅'], inplace=True, errors='ignore')
            
        # 清理季度列，提取年份和季度信息
        combined_df['季度'] = combined_df['季度'].str.replace('年', '-Q')
        combined_df['年份'] = combined_df['季度'].str.split('-').str[0].astype(int)
        combined_df['季度编号'] = combined_df['季度'].str.split('-').str[1].str.replace('季度', '')
        
        # 按时间排序
        combined_df.sort_values(by=['年份', '季度编号'], inplace=True)

        report.append(f"## 基金代码: {fund_code} 持仓分析报告")
        report.append("---")

        # 1. 重仓股变动分析
        report.append("### 1. 重仓股变动")
        quarters = combined_df['季度'].unique()
        for i in range(len(quarters) - 1):
            current_q = quarters[i]
            next_q = quarters[i+1]
            
            current_holdings = set(combined_df[combined_df['季度'] == current_q]['股票代码'])
            next_holdings = set(combined_df[combined_df['季度'] == next_q]['股票代码'])
            
            new_additions = next_holdings - current_holdings
            removed = current_holdings - next_holdings
            
            report.append(f"#### 从 {current_q} 到 {next_q} 的变动")
            if new_additions:
                new_add_stocks = combined_df[(combined_df['季度'] == next_q) & (combined_df['股票代码'].isin(new_additions))]['股票名称'].tolist()
                report.append(f"- **新增股票**：{', '.join(new_add_stocks)}")
            if removed:
                removed_stocks = combined_df[(combined_df['季度'] == current_q) & (combined_df['股票代码'].isin(removed))]['股票名称'].tolist()
                report.append(f"- **移除股票**：{', '.join(removed_stocks)}")
        
        # 2. 行业偏好分析
        report.append("\n### 2. 行业偏好和持仓集中度")
        sector_mapping = {
            '688': '科创板',
            '300': '创业板',
            '002': '中小板',
            '000': '主板',
            '600': '主板',
            '601': '主板',
            '603': '主板',
            '605': '主板',
            '005': '主板',
            '006': '主板',
        }
        
        combined_df['板块'] = combined_df['股票代码'].astype(str).str[:3].map(sector_mapping).fillna('其他')

        sector_summary = combined_df.groupby(['年份', '板块'])['占净值比例'].sum().unstack().fillna(0)
        report.append("#### 板块偏好（占净值比例之和）")
        report.append(sector_summary.to_markdown())

        # 3. 集中度分析
        concentration_summary = combined_df.groupby('季度')['占净值比例'].sum()
        report.append("\n#### 前十大持仓集中度（占净值比例之和）")
        report.append(concentration_summary.to_markdown())

    # 将报告写入 Markdown 文件
    with open('analysis_report.md', 'w', encoding='utf-8') as f:
        f.write('\n'.join(report))

if __name__ == "__main__":
    analyze_holdings()
    print("分析报告已生成：analysis_report.md")

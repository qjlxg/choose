import pandas as pd
import glob
import os

def analyze_holdings():
    """
    遍历 fund_data 目录，对每个基金代码的持仓数据进行合并和分析，
    并将结果输出到 analysis_report.md 文件中。
    """
    base_path = 'fund_data'
    all_files = glob.glob(os.path.join(base_path, "*.csv"))

    if not all_files:
        print("未在 'fund_data' 目录中找到任何 CSV 文件。")
        return

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

    for fund_code, files in fund_files.items():
        df_list = []
        for f in files:
            try:
                df = pd.read_csv(f, engine='python')
                df.rename(columns={'占净值 比例': '占净值比例', '持仓市值 （万元）': '持仓市值'}, inplace=True)
                
                if '最新价' in df.columns:
                    df = df.loc[:, ['序号', '股票代码', '股票名称', '相关资讯', '占净值比例', '持股数 （万股）', '持仓市值', '季度']]
                    
                df['基金代码'] = fund_code
                df_list.append(df)
            except Exception as e:
                print(f"读取文件 {f} 时出错：{e}")
        
        if not df_list:
            continue
            
        combined_df = pd.concat(df_list, ignore_index=True)
        combined_df['季度'] = combined_df['季度'].str.replace('年', '-Q')
        combined_df['年份'] = combined_df['季度'].str.split('-').str[0].astype(int)
        combined_df['季度编号'] = combined_df['季度'].str.split('-').str[1].str.replace('季度', '')
        combined_df.sort_values(by=['年份', '季度编号'], inplace=True)

        report.append(f"## 基金代码: {fund_code} 持仓分析报告")
        report.append("---")

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
        
        formatted_sector_summary = sector_summary.copy()
        for col in formatted_sector_summary.columns:
            formatted_sector_summary[col] = formatted_sector_summary[col].apply(lambda x: f"{x:.2f}%" if x > 0 else "")
        report.append("#### 板块偏好（占净值比例之和）")
        report.append(formatted_sector_summary.to_markdown())

        concentration_summary = combined_df.groupby('季度')['占净值比例'].sum()
        
        formatted_concentration_summary = pd.DataFrame(concentration_summary)
        formatted_concentration_summary['占净值比例'] = formatted_concentration_summary['占净值比例'].apply(lambda x: f"{x:.2f}%")
        report.append("\n#### 前十大持仓集中度（占净值比例之和）")
        report.append(formatted_concentration_summary.to_markdown())

        # 3. 趋势总结分析
        report.append("\n### 3. 趋势总结")
        
        if len(concentration_summary) > 1:
            first_q = concentration_summary.index[0]
            last_q = concentration_summary.index[-1]
            first_concentration = concentration_summary.iloc[0]
            last_concentration = concentration_summary.iloc[-1]
            
            trend = "上升" if last_concentration > first_concentration else "下降" if last_concentration < first_concentration else "保持稳定"
            report.append(f"- **持仓集中度**：从 {first_q} 到 {last_q}，前十大持仓集中度从 {first_concentration:.2f}% {trend}到 {last_concentration:.2f}%。这表明基金经理在分析期内，**{ '更倾向于' if trend == '上升' else '降低了' }** 投资集中度。")

        if len(sector_summary) > 1:
            first_year_summary = sector_summary.iloc[0].idxmax()
            last_year_summary = sector_summary.iloc[-1].idxmax()
            if first_year_summary != last_year_summary:
                report.append(f"- **板块偏好**：基金的投资偏好在分析期内发生了显著变化。最初主要集中在**{first_year_summary}**，而最新季度则转向了**{last_year_summary}**。这可能反映了基金经理对市场热点或行业前景的最新判断。")
            else:
                report.append(f"- **板块偏好**：基金在分析期内保持了较为稳定的投资风格，主要偏向于**{first_year_summary}**板块。")

        # 4. 投资建议和风险防范
        report.append("\n### 4. 投资建议和风险防范")
        report.append("> **免责声明**：本报告基于历史持仓数据进行分析，不构成任何投资建议。投资有风险，入市需谨慎。")
        report.append("\n基于以上分析，你可以进一步思考和研究以下方面：")
        report.append("\n**评估基金风格与个人风险偏好**")
        report.append("- **集中度**：持仓集中度高意味着基金经理重仓少数股票，潜在收益和风险都更高。如果你的风险偏好较低，可能需要规避这类基金。")
        report.append("- **板块偏好**：基金的持仓板块反映了其投资风格。你是否看好这些行业的未来前景？这些行业是否符合你的风险承受能力？")
        report.append("\n**关注基金经理的策略变化**")
        report.append("- **重仓股变动**：如果基金频繁更换重仓股，可能意味着基金经理在进行短线操作，投资风格偏向激进，这可能带来较高的波动性。")
        report.append("- **板块变动**：如果板块配置在不同年份间发生显著变化，可能反映了基金经理对宏观经济或行业周期的判断。")
        report.append("\n**综合考量其他关键因素**")
        report.append("- **基金业绩**：历史持仓数据不能完全代表基金未来表现。请结合基金的长期业绩、同类基金排名等指标进行综合评估。")
        report.append("- **费率结构**：不同基金的管理费、托管费和申赎费率都会影响你的实际投资收益。")
        report.append("\n**防范风险建议**")
        report.append("- **分散投资**：不要将所有资金都投入到单一基金中，通过配置不同风格和资产类别的基金，可以有效分散风险。")


    with open('analysis_report.md', 'w', encoding='utf-8') as f:
        f.write('\n'.join(report))

if __name__ == "__main__":
    analyze_holdings()
    print("分析报告已生成：analysis_report.md")

```python
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
                
                # 检查并重命名列，以处理不同文件中的列名差异
                column_mapping = {
                    '占净值 比例': '占净值比例',
                    '持仓市值 （万元）': '持仓市值'
                }
                df.rename(columns=column_mapping, inplace=True)

                if '最新价' in df.columns:
                    df = df.loc[:, ['序号', '股票代码', '股票名称', '相关资讯', '占净值比例', '持股数 （万股）', '持仓市值', '季度']]
                
                df['基金代码'] = fund_code
                df_list.append(df)
            except KeyError as e:
                print(f"读取文件 {f} 时出错：缺少关键列 {e}")
                continue # 跳过当前文件，继续处理下一个
            except Exception as e:
                print(f"读取文件 {f} 时出错：{e}")
                continue
        
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
        
        report.append("\n### 2. 行业偏好、主题热点和持仓集中度")
        
        # 原板块映射（保留，作为补充）
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
        
        # 新增：行业映射（基于申万一级行业，示例数据，可扩展）
        industry_mapping = {
            '600519': '食品饮料',  # 贵州茅台
            '300750': '电力设备',  # 宁德时代
            '601318': '非银金融',  # 中国平安
            '000858': '食品饮料',  # 五粮液
            '000333': '家用电器',  # 美的集团
            '002594': '汽车',      # 比亚迪
            '601166': '银行',      # 兴业银行
            # 添加更多股票代码 -> 行业映射，根据您的基金持仓扩展
        }
        combined_df['行业'] = combined_df['股票代码'].astype(str).map(industry_mapping).fillna('未知')
        
        # 新增：主题热点映射（基于关键词或代码，示例）
        theme_mapping = {
            '600519': '消费',      # 贵州茅台 - 白酒消费
            '300750': '新能源',    # 宁德时代 - 新能源电池
            '002594': '新能源',    # 比亚迪 - 新能源汽车
            '000333': '智能家居',  # 美的集团 - 家电智能
            # 添加更多：如如果名称含'光伏' -> '新能源'，或手动映射
        }
        combined_df['主题热点'] = combined_df['股票代码'].astype(str).map(theme_mapping).fillna('无特定主题')
        
        # 原板块总结（保留）
        sector_summary = combined_df.groupby(['年份', '板块'])['占净值比例'].sum().unstack().fillna(0)
        report.append("#### 板块偏好（占净值比例之和）")
        formatted_sector_summary = sector_summary.map(lambda x: f"{x:.2f}%" if x > 0 else "")
        report.append(formatted_sector_summary.to_markdown())
        
        # 新增：行业偏好总结
        industry_summary = combined_df.groupby(['年份', '行业'])['占净值比例'].sum().unstack().fillna(0)
        report.append("\n#### 行业偏好（占净值比例之和）")
        formatted_industry_summary = industry_summary.map(lambda x: f"{x:.2f}%" if x > 0 else "")
        report.append(formatted_industry_summary.to_markdown())
        
        # 新增：主题热点总结
        theme_summary = combined_df.groupby(['年份', '主题热点'])['占净值比例'].sum().unstack().fillna(0)
        report.append("\n#### 主题热点偏好（占净值比例之和）")
        formatted_theme_summary = theme_summary.map(lambda x: f"{x:.2f}%" if x > 0 else "")
        report.append(formatted_theme_summary.to_markdown())
        
        # 原集中度总结（不变）
        concentration_summary = combined_df.groupby('季度')['占净值比例'].sum()
        report.append("\n#### 前十大持仓集中度（占净值比例之和）")
        formatted_concentration_summary = pd.DataFrame(concentration_summary)
        formatted_concentration_summary['占净值比例'] = formatted_concentration_summary['占净值比例'].map(lambda x: f"{x:.2f}%")
        report.append(formatted_concentration_summary.to_markdown())

        # 3. 动态趋势总结和投资建议
        report.append("\n### 3. 趋势总结和投资建议")
        report.append("> **免责声明**：本报告基于历史持仓数据进行分析，不构成任何投资建议。投资有风险，入市需谨慎。")
        report.append(f"\n基于对基金 **{fund_code}** 的历史持仓数据分析，本报告得出以下关键观察结果：")
        
        # 集中度变化分析 (使用原始数值数据)
        if len(concentration_summary) > 1:
            first_concentration = concentration_summary.iloc[0]
            last_concentration = concentration_summary.iloc[-1]
            concentration_diff = last_concentration - first_concentration
            
            if concentration_diff > 10:
                report.append("- **持仓集中度**：在分析期内，该基金的持仓集中度显著**上升**。这表明基金经理正将资金集中到其看好的少数股票上。这可能带来更高的回报，但同时也伴随着更高的风险。")
            elif concentration_diff < -10:
                report.append("- **持仓集中度**：在分析期内，该基金的持仓集中度显著**下降**。这表明基金经理正在分散投资，这通常有助于降低风险，但可能牺牲部分超额收益。")
            else:
                report.append("- **持仓集中度**：该基金的持仓集中度在分析期内相对**稳定**。这可能表明基金经理的投资风格较为稳健，并坚持其既定的投资策略。")

        # 板块偏好变化分析 (使用原始数值数据)
        if len(sector_summary) > 1:
            first_year_summary = sector_summary.iloc[0]
            last_year_summary = sector_summary.iloc[-1]
            
            first_dominant_sector = first_year_summary.idxmax()
            last_dominant_sector = last_year_summary.idxmax()
            
            if first_dominant_sector != last_dominant_sector:
                report.append(f"- **板块偏好**：基金的投资偏好在分析期内发生了明显变化，从最初主要集中在**{first_dominant_sector}**转向了**{last_dominant_sector}**。这可能反映了基金经理对市场热点或宏观经济的最新判断。")
            else:
                report.append(f"- **板块偏好**：该基金在分析期内保持了相对稳定的投资风格，主要偏向于**{first_dominant_sector}**板块。")
        
        # 增加通用建议
        report.append("\n**总结与建议：**")
        report.append("  在考虑投资该基金时，建议将上述分析结果与其他因素结合考量，例如基金的过往业绩、基金经理的管理经验、基金规模以及费率等。")


    with open('analysis_report.md', 'w', encoding='utf-8') as f:
        f.write('\n'.join(report))

if __name__ == "__main__":
    analyze_holdings()
    print("分析报告已生成：analysis_report.md")
```

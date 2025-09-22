import pandas as pd
import glob
import os
import sys

def load_stock_categories(category_path):
    """
    遍历指定目录，加载所有 .xlsx 格式的股票分类表。
    
    Args:
        category_path (str): 包含分类表的目录路径。
        
    Returns:
        dict: 一个字典，键为股票代码，值为其所属的分类。
    """
    all_categories = {}
    xlsx_files = glob.glob(os.path.join(category_path, "*.xlsx"))
    
    if not xlsx_files:
        print(f"未在 '{category_path}' 目录中找到任何 XLSX 文件。")
        return all_categories

    for f in xlsx_files:
        try:
            category_name = os.path.basename(f).split('.')[0]
            
            df = pd.read_excel(f, header=0, engine='openpyxl')
            
            if '股票代码' not in df.columns or '股票名称' not in df.columns:
                print(f"文件 {f} 缺少关键列 '股票代码' 或 '股票名称'，跳过。")
                continue
            
            df['股票代码'] = df['股票代码'].astype(str).str.zfill(6)
            
            for code in df['股票代码']:
                all_categories[code] = category_name
        except Exception as e:
            print(f"读取分类文件 {f} 时出错: {e}")
            continue
            
    return all_categories

def analyze_holdings():
    """
    遍历 fund_data 目录，对每个基金代码的持仓数据进行合并和分析，
    并将结果输出到 analysis_report.md 文件中。
    """
    base_path = 'fund_data'
    category_path = '分类表'
    all_files = glob.glob(os.path.join(base_path, "*.csv"))

    if not all_files:
        print("未在 'fund_data' 目录中找到任何 CSV 文件。")
        return

    stock_categories = load_stock_categories(category_path)
    if not stock_categories:
        print("未加载到任何股票分类数据，将使用默认板块分析。")
        sector_mapping = {
            '688': '科创板', '300': '创业板', '002': '中小板',
            '000': '主板', '600': '主板', '601': '主板',
            '603': '主板', '605': '主板', '005': '主板', '006': '主板',
        }
        use_detailed_categories = False
    else:
        use_detailed_categories = True

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
                
                column_mapping = {
                    '占净值 比例': '占净值比例', '持仓市值 （万元）': '持仓市值',
                    '持仓市值': '持仓市值', '占净值比例': '占净值比例'
                }
                df.rename(columns=column_mapping, inplace=True)
                
                required_cols = ['股票代码', '股票名称', '占净值比例', '持仓市值', '季度']
                missing_cols = [col for col in required_cols if col not in df.columns]
                if missing_cols:
                    raise KeyError(f"缺少关键列 {missing_cols}")

                df['占净值比例'] = df['占净值比例'].astype(str).str.replace('%', '', regex=False).str.replace(',', '', regex=False)
                df['占净值比例'] = pd.to_numeric(df['占净值比例'], errors='coerce')
                
                df['股票代码'] = df['股票代码'].astype(str).str.zfill(6)
                
                if use_detailed_categories:
                    df['行业'] = df['股票代码'].map(stock_categories).fillna('其他')
                else:
                    df['行业'] = df['股票代码'].astype(str).str[:3].map(sector_mapping).fillna('其他')
                
                df['基金代码'] = fund_code
                df_list.append(df)
            except KeyError as e:
                print(f"读取文件 {f} 时出错：缺少关键列 {e}")
                continue
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
        
        report.append("\n### 2. 行业偏好和持仓集中度")
        sector_summary = combined_df.groupby(['年份', '行业'])['占净值比例'].sum().unstack().fillna(0)
        
        sector_summary = sector_summary.astype(float)
        
        report.append("#### 行业偏好（占净值比例之和）")
        formatted_sector_summary = sector_summary.map(lambda x: f"{x:.2f}%" if x > 0 else "")
        report.append(formatted_sector_summary.to_markdown())

        concentration_summary = combined_df.groupby('季度')['占净值比例'].sum()
        
        report.append("\n#### 前十大持仓集中度（占净值比例之和）")
        formatted_concentration_summary = pd.DataFrame(concentration_summary)
        formatted_concentration_summary['占净值比例'] = formatted_concentration_summary['占净值比例'].map(lambda x: f"{x:.2f}%")
        report.append(formatted_concentration_summary.to_markdown())

        report.append("\n### 3. 趋势总结和投资建议")
        report.append("> **免责声明**：本报告基于历史持仓数据进行分析，不构成任何投资建议。投资有风险，入市需谨慎。")
        report.append(f"\n基于对基金 **{fund_code}** 的历史持仓数据分析，本报告得出以下关键观察结果：")
        
        if len(concentration_summary) > 1:
            first_concentration = concentration_summary.iloc[0]
            last_concentration = concentration_summary.iloc[-1]
            concentration_diff = last_concentration - first_concentration
            
            if concentration_diff > 10:
                report.append("- **持仓集中度**：在分析期内，该基金的持仓集中度显著**上升**。")
            elif concentration_diff < -10:
                report.append("- **持仓集中度**：在分析期内，该基金的持仓集中度显著**下降**。")
            else:
                report.append("- **持仓集中度**：该基金的持仓集中度在分析期内相对**稳定**。")

        if len(sector_summary) > 1:
            first_year_summary = sector_summary.iloc[0]
            last_year_summary = sector_summary.iloc[-1]
            
            try:
                first_dominant_sector = first_year_summary.idxmax()
                last_dominant_sector = last_year_summary.idxmax()
                
                if first_dominant_sector != last_dominant_sector:
                    report.append(f"- **行业偏好**：基金的投资偏好发生了明显变化，从**{first_dominant_sector}**转向了**{last_dominant_sector}**。")
                else:
                    report.append(f"- **行业偏好**：该基金在分析期内主要偏向于**{first_dominant_sector}**行业。")
            except ValueError:
                report.append("- **行业偏好**：由于数据不足，无法分析行业偏好变化。")
        
        report.append("\n**总结与建议：**")
        report.append("  在考虑投资该基金时，建议将上述分析结果与其他因素结合考量，例如基金的过往业绩、基金经理的管理经验、基金规模以及费率等。")


    with open('analysis_report.md', 'w', encoding='utf-8') as f:
        f.write('\n'.join(report))

if __name__ == "__main__":
    analyze_holdings()
    print("分析报告已生成：analysis_report.md")

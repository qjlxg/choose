import pandas as pd
import glob
import os
import sys
from datetime import datetime

def load_stock_categories(category_path):
    """
    加载股票分类数据，保持原有逻辑但增强错误处理和性能
    """
    all_categories = {}
    xlsx_files = glob.glob(os.path.join(category_path, "*.xlsx"))
    
    if not xlsx_files:
        print(f"⚠️  在 '{category_path}' 目录中未找到 XLSX 文件")
        return all_categories

    for f in xlsx_files:
        try:
            category_name = os.path.basename(f).split('.')[0]
            df = pd.read_excel(f, header=0, engine='openpyxl')
            
            # 严格检查必要列
            if '股票代码' not in df.columns or '股票名称' not in df.columns:
                print(f"⚠️  文件 {f} 缺少必要列，跳过")
                continue
            
            # 批量处理，避免循环
            df['股票代码'] = df['股票代码'].astype(str).str.strip().str.zfill(6)
            new_categories = dict(zip(df['股票代码'], [category_name] * len(df)))
            
            # 合并分类，优先使用已存在的
            for code, cat in new_categories.items():
                if code not in all_categories:
                    all_categories[code] = cat
                    
        except Exception as e:
            print(f"❌ 读取 {f} 出错: {e}")
            continue
    
    print(f"✅ 加载了 {len(all_categories)} 只股票的分类")
    return all_categories

def clean_dataframe(df, fund_code, use_detailed_categories, stock_categories, sector_mapping):
    """
    集中处理数据清洗逻辑
    """
    # 列名标准化
    column_mapping = {
        '占净值 比例': '占净值比例', 
        '占净值比例': '占净值比例',
        '持仓市值 （万元）': '持仓市值',
        '持仓市值': '持仓市值',
        '市值': '持仓市值',
        '持仓市值 （万元人民币）': '持仓市值',
        '股票名称': '股票名称',
        '股票代码': '股票代码',
        '季度': '季度'
    }
    
    df.columns = [column_mapping.get(col, col) for col in df.columns]

    # 验证必要列
    required_cols = ['股票代码', '股票名称', '占净值比例', '持仓市值', '季度']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"缺少列: {missing_cols}")

    # 清洗占净值比例
    df['占净值比例'] = (df['占净值比例']
                       .astype(str)
                       .str.replace('%', '', regex=False)
                       .str.replace(',', '', regex=False)
                       .pipe(pd.to_numeric, errors='coerce'))
    
    # 清洗股票代码
    df['股票代码'] = df['股票代码'].astype(str).str.strip().str.zfill(6)
    
    # 添加行业分类
    if use_detailed_categories and stock_categories:
        df['行业'] = df['股票代码'].map(stock_categories).fillna('其他')
    else:
        df['行业'] = df['股票代码'].str[:3].map(sector_mapping).fillna('其他')
    
    df['基金代码'] = fund_code
    return df

def process_temporal_data(df):
    """
    处理时间维度数据
    """
    df = df.copy()
    
    # 标准化季度格式
    df['季度'] = df['季度'].astype(str).str.replace('年', '-Q', regex=False)
    
    # 提取年份和季度编号
    df['年份'] = df['季度'].str.split('-').str[0].astype(int)
    df['季度编号'] = (df['季度']
                     .str.split('-')
                     .str[1]
                     .str.replace('季度', '')
                     .astype(int))
    
    # 按时间排序
    df = df.sort_values(['年份', '季度编号']).reset_index(drop=True)
    return df

def analyze_single_fund(fund_code, files, stock_categories, use_detailed_categories, sector_mapping):
    """
    分析单个基金的核心逻辑
    """
    df_list = []
    
    for f in files:
        try:
            df = pd.read_csv(f, encoding='utf-8')
            df = clean_dataframe(df, fund_code, use_detailed_categories, 
                               stock_categories, sector_mapping)
            df_list.append(df)
        except Exception as e:
            print(f"⚠️  处理 {f} 出错: {e}")
            continue
    
    if not df_list:
        return []
    
    # 合并并处理时间数据
    combined_df = pd.concat(df_list, ignore_index=True)
    combined_df = process_temporal_data(combined_df)
    
    return generate_fund_report(fund_code, combined_df)

def generate_fund_report(fund_code, df):
    """
    生成单个基金的报告，保持原有逻辑
    """
    report = [f"## 基金代码: {fund_code} 持仓分析报告", "---"]
    
    # 未分类股票
    unclassified_stocks = df[df['行业'] == '其他']
    if not unclassified_stocks.empty:
        report.extend(generate_unclassified_section(unclassified_stocks))
    
    # 重仓股变动
    report.extend(generate_stock_changes_section(df))
    
    # 行业偏好和集中度
    report.extend(generate_sector_analysis_section(df))
    
    # 趋势总结
    report.extend(generate_trend_summary_section(df))
    
    return report

def generate_unclassified_section(unclassified_stocks):
    """生成未分类股票部分"""
    lines = ["\n### 未能匹配到行业分类的股票列表", "---"]
    
    for quarter, group in unclassified_stocks.groupby('季度'):
        lines.append(f"#### {quarter}")
        for _, row in group.iterrows():
            lines.append(f"- **{row['股票名称']}** ({row['股票代码']}): {row['占净值比例']:.2f}%")
    
    lines.append("---\n")
    return lines

def generate_stock_changes_section(df):
    """生成重仓股变动部分"""
    lines = ["### 1. 重仓股变动"]
    quarters = sorted(df['季度'].unique())
    
    for i in range(len(quarters) - 1):
        current_q = quarters[i]
        next_q = quarters[i + 1]
        
        current_holdings = set(df[df['季度'] == current_q]['股票代码'])
        next_holdings = set(df[df['季度'] == next_q]['股票代码'])
        
        new_additions = next_holdings - current_holdings
        removed = current_holdings - next_holdings
        
        lines.append(f"\n#### 从 {current_q} 到 {next_q} 的变动")
        
        if new_additions:
            new_stocks = (df[(df['季度'] == next_q) & 
                           (df['股票代码'].isin(new_additions))]['股票名称']
                         .tolist())
            lines.append(f"- **新增股票**：{', '.join(new_stocks)}")
        
        if removed:
            removed_stocks = (df[(df['季度'] == current_q) & 
                               (df['股票代码'].isin(removed))]['股票名称']
                             .tolist())
            lines.append(f"- **移除股票**：{', '.join(removed_stocks)}")
    
    return lines

def generate_sector_analysis_section(df):
    """生成行业分析部分"""
    lines = ["### 2. 行业偏好和持仓集中度"]
    
    # 行业汇总
    sector_summary = df.groupby(['年份', '行业'])['占净值比例'].sum().unstack().fillna(0)
    
    lines.append("\n#### 行业偏好（占净值比例之和）")
    # 格式化表格输出
    formatted_table = sector_summary.round(2).applymap(lambda x: f"{x:.2f}%" if x > 0 else "")
    lines.append(formatted_table.to_markdown())
    
    # 集中度
    concentration_summary = df.groupby('季度')['占净值比例'].sum()
    
    lines.append("\n#### 前十大持仓集中度（占净值比例之和）")
    conc_df = pd.DataFrame({
        '季度': concentration_summary.index,
        '集中度': concentration_summary.round(2).apply(lambda x: f"{x:.2f}%")
    })
    lines.append(conc_df.to_markdown(index=False))
    
    return lines

def generate_trend_summary_section(df):
    """生成趋势总结部分"""
    lines = ["\n### 3. 趋势总结和投资建议"]
    
    # 免责声明
    lines.append("> **免责声明**：本报告基于历史持仓数据进行分析，不构成任何投资建议。投资有风险，入市需谨慎。")
    
    # 集中度分析
    concentration_summary = df.groupby('季度')['占净值比例'].sum()
    if len(concentration_summary) > 1:
        first_concentration = concentration_summary.iloc[0]
        last_concentration = concentration_summary.iloc[-1]
        concentration_diff = last_concentration - first_concentration
        
        if concentration_diff > 10:
            trend_desc = "显著**上升**"
        elif concentration_diff < -10:
            trend_desc = "显著**下降**"
        else:
            trend_desc = "相对**稳定**"
        
        lines.append(f"- **持仓集中度**：在分析期内，该基金的持仓集中度{trend_desc}。")
    
    # 行业偏好分析
    sector_summary = df.groupby(['年份', '行业'])['占净值比例'].sum().unstack().fillna(0)
    if len(sector_summary) > 1:
        try:
            first_year_summary = sector_summary.iloc[0]
            last_year_summary = sector_summary.iloc[-1]
            
            first_dominant_sector = first_year_summary.idxmax()
            last_dominant_sector = last_year_summary.idxmax()
            
            if first_dominant_sector != last_dominant_sector:
                lines.append(f"- **行业偏好**：基金的投资偏好发生了明显变化，从**{first_dominant_sector}**转向了**{last_dominant_sector}**。")
            else:
                lines.append(f"- **行业偏好**：该基金在分析期内主要偏向于**{first_dominant_sector}**行业。")
        except ValueError:
            lines.append("- **行业偏好**：由于数据不足，无法分析行业偏好变化。")
    
    # 投资建议
    lines.extend([
        "",
        "**总结与建议：**",
        "  在考虑投资该基金时，建议将上述分析结果与其他因素结合考量，例如基金的过往业绩、基金经理的管理经验、基金规模以及费率等。"
    ])
    
    return lines

def analyze_holdings():
    """
    主分析函数，优化后的版本
    """
    base_path = 'fund_data'
    category_path = '分类表'
    
    print("🔄 开始分析基金持仓数据...")
    
    # 检查数据文件
    all_files = glob.glob(os.path.join(base_path, "*.csv"))
    if not all_files:
        print("❌ 在 'fund_data' 目录中未找到 CSV 文件")
        return

    # 加载分类
    stock_categories = load_stock_categories(category_path)
    use_detailed_categories = bool(stock_categories)
    
    if not use_detailed_categories:
        print("⚠️  使用默认板块分类")
        sector_mapping = {
            '688': '科创板', '300': '创业板', '002': '中小板',
            '000': '主板', '600': '主板', '601': '主板',
            '603': '主板', '605': '主板', '005': '主板', '006': '主板',
        }
    else:
        sector_mapping = None

    # 按基金分组文件
    fund_files = {}
    for f in all_files:
        try:
            # 解析文件名：假设格式为 *_{fund_code}_*.csv
            filename = os.path.basename(f)
            if '_' not in filename:
                continue
                
            parts = filename.split('_')
            # 查找基金代码（通常是6位数字）
            fund_code = None
            for part in parts:
                if part.isdigit() and len(part) == 6:
                    fund_code = part
                    break
            
            if fund_code and fund_code not in fund_files:
                fund_files[fund_code] = []
            
            if fund_code:
                fund_files[fund_code].append(f)
                
        except Exception:
            continue

    if not fund_files:
        print("❌ 未找到有效基金文件")
        return

    print(f"📊 发现 {len(fund_files)} 只基金，开始分析...")

    # 生成报告
    report = [f"# 基金持仓分析报告", 
              f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
              "", "---", ""]

    for fund_code, files in fund_files.items():
        print(f"  分析基金: {fund_code} ({len(files)} 个季度)")
        fund_report = analyze_single_fund(fund_code, files, stock_categories, 
                                        use_detailed_categories, sector_mapping)
        report.extend(fund_report)

    # 保存报告
    with open('analysis_report.md', 'w', encoding='utf-8') as f:
        f.write('\n'.join(report))
    
    print(f"✅ 分析完成，报告已保存: analysis_report.md")

if __name__ == "__main__":
    analyze_holdings()

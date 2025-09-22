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

    # 新增：从分类表目录加载所有行业分类表（31个申万一级行业）
    classification_dir = '分类表'
    stock_to_industry = {}
    
    if os.path.exists(classification_dir):
        xlsx_files = glob.glob(os.path.join(classification_dir, '*分类表.xlsx'))
        print(f"找到 {len(xlsx_files)} 个分类表文件")
        
        for file in xlsx_files:
            try:
                # 从文件名提取行业名称，例如 "交通运输分类表.xlsx" -> "交通运输"
                filename = os.path.basename(file)
                # 移除 "分类表.xlsx" 后缀
                industry_name = filename.replace('分类表.xlsx', '').strip()
                
                # 读取第一个sheet（标题页），跳过前两行（标题和空行）
                try:
                    title_df = pd.read_excel(file, sheet_name=0, skiprows=1, nrows=31)
                    # 假设第一列是行业名称
                    if not title_df.empty and len(title_df.columns) > 0:
                        # 验证当前文件的行业名称是否在标题列表中
                        title_industries = title_df.iloc[:, 0].dropna().str.strip().tolist()
                        if industry_name in title_industries:
                            print(f"✓ 加载分类表：{industry_name} ({len(title_industries)} 个行业)")
                        else:
                            print(f"⚠ 警告：文件名 '{industry_name}' 不在标题列表中")
                except Exception as e:
                    print(f"读取标题页 {file} 时出错：{e}")
                
                # 读取数据页（股票数据），假设在同一个sheet中，跳过标题页部分
                # 根据你的描述，数据从标题后开始，尝试读取更多行
                data_df = pd.read_excel(file, sheet_name=0, skiprows=32)  # 跳过标题部分
                
                if not data_df.empty:
                    # 假设列名为 '行业名称', '股票代码', '股票名称', '起始时间', '结束时间'
                    # 处理可能的列名问题
                    if '股票代码' in data_df.columns:
                        stock_col = '股票代码'
                    elif len(data_df.columns) > 1 and data_df.columns[1] == '股票代码':
                        stock_col = data_df.columns[1]
                    else:
                        stock_col = 1  # 第二列
                        data_df = data_df.iloc[:, :5]  # 取前5列
                        data_df.columns = ['行业名称', '股票代码', '股票名称', '起始时间', '结束时间']
                    
                    for _, row in data_df.iterrows():
                        try:
                            stock_code = str(int(row[stock_col])).zfill(6)  # 转换为6位字符串
                            # 只映射到当前文件的行业
                            if pd.notna(row.get('行业名称', '')):
                                industry = str(row['行业名称']).strip()
                                stock_to_industry[stock_code] = industry
                        except (ValueError, KeyError) as e:
                            continue
                            
            except Exception as e:
                print(f"加载分类文件 {file} 时出错：{e}")
                continue
    else:
        print(f"警告：分类表目录 '{classification_dir}' 不存在")
    
    print(f"成功加载 {len(stock_to_industry)} 个股票的行业分类")
    
    # 如果没有加载到分类数据，提供一个简单的fallback（可选）
    if not stock_to_industry:
        print("警告：未加载到行业分类，将使用股票代码前缀简单分类")

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
                    # 修复：只选择实际存在的列
                    required_cols = ['序号', '股票代码', '股票名称', '相关资讯', '占净值比例', '持股数 （万股）', '持仓市值', '季度']
                    available_cols = [col for col in required_cols if col in df.columns]
                    if len(available_cols) < 4:  # 至少要有股票代码、名称、比例、季度
                        print(f"文件 {f} 缺少必要列，跳过")
                        continue
                    df = df.loc[:, available_cols]
                
                # 修复：清理占净值比例列中的百分比符号和单位（使用原始字符串）
                if '占净值比例' in df.columns:
                    # 移除百分比符号和可能的中文单位
                    df['占净值比例'] = df['占净值比例'].astype(str).str.replace(r'[\%,％]', '', regex=True)
                    df['占净值比例'] = df['占净值比例'].str.replace(r'[(（].*[））]', '', regex=True).str.strip()
                    # 转换为数值型（百分比形式，100% = 100.0）
                    df['占净值比例'] = pd.to_numeric(df['占净值比例'], errors='coerce')
                
                # 确保有季度列
                if '季度' not in df.columns:
                    print(f"文件 {f} 缺少季度列，跳过")
                    continue
                
                df['基金代码'] = fund_code
                df_list.append(df)
            except KeyError as e:
                print(f"读取文件 {f} 时出错：缺少关键列 {e}")
                continue # 跳过当前文件，继续处理下一个
            except Exception as e:
                print(f"读取文件 {f} 时出错：{e}")
                continue
        
        if not df_list:
            print(f"基金 {fund_code} 没有有效的数据文件，跳过")
            continue
            
        combined_df = pd.concat(df_list, ignore_index=True)
        
        # 再次确保占净值比例是数值型
        if '占净值比例' in combined_df.columns:
            combined_df['占净值比例'] = pd.to_numeric(combined_df['占净值比例'], errors='coerce')
            # 移除NaN值
            combined_df = combined_df.dropna(subset=['占净值比例'])
        
        # 检查是否有足够的数据
        if combined_df.empty:
            print(f"基金 {fund_code} 处理后数据为空，跳过")
            continue
        
        combined_df['季度'] = combined_df['季度'].astype(str).str.replace('年', '-Q')
        combined_df['年份'] = combined_df['季度'].str.split('-').str[0].astype(int)
        combined_df['季度编号'] = combined_df['季度'].str.split('-').str[1].str.replace('季度', '')
        combined_df.sort_values(by=['年份', '季度编号'], inplace=True)

        report.append(f"## 基金代码: {fund_code} 持仓分析报告")
        report.append("---")

        report.append("### 1. 重仓股变动")
        quarters = combined_df['季度'].unique()
        if len(quarters) > 1:
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
        else:
            report.append("#### 仅有一个季度数据，无法分析变动")
        
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
        
        # 确保数据类型为数值型
        for col in sector_summary.columns:
            sector_summary[col] = pd.to_numeric(sector_summary[col], errors='coerce').fillna(0)
        sector_summary = sector_summary.astype(float)
        
        report.append("#### 板块偏好（占净值比例之和）")
        def format_percentage(x):
            if pd.isna(x) or x == 0:
                return ""
            return f"{float(x):.2f}%"
        formatted_sector_summary = sector_summary.map(format_percentage)
        report.append(formatted_sector_summary.to_markdown())

        concentration_summary = combined_df.groupby('季度')['占净值比例'].sum()
        
        report.append("\n#### 前十大持仓集中度（占净值比例之和）")
        formatted_concentration_summary = pd.DataFrame(concentration_summary)
        formatted_concentration_summary['占净值比例'] = formatted_concentration_summary['占净值比例'].map(lambda x: f"{float(x):.2f}%")
        report.append(formatted_concentration_summary.to_markdown())

        # 新增：行业和主题热点分析（在原有分析后添加）
        report.append("\n#### 行业偏好分析（申万一级行业）")
        
        # 使用从分类表加载的stock_to_industry映射
        # 如果没有找到分类，则使用简单的代码前缀分类作为fallback
        def get_industry_fallback(stock_code):
            """fallback分类函数"""
            code_str = str(stock_code).zfill(6)
            if code_str.startswith('688'):
                return '科创板股票'
            elif code_str.startswith('300'):
                return '创业板股票'
            elif code_str.startswith('002'):
                return '中小板股票'
            else:
                return '主板股票'
        
        combined_df['股票代码_str'] = combined_df['股票代码'].astype(str).str.zfill(6)
        combined_df['行业'] = combined_df['股票代码_str'].map(stock_to_industry)
        # 对于没有找到分类的股票，使用fallback
        mask = combined_df['行业'].isna()
        combined_df.loc[mask, '行业'] = combined_df.loc[mask, '股票代码_str'].apply(get_industry_fallback)
        combined_df['行业'] = combined_df['行业'].fillna('其他行业')
        combined_df.drop('股票代码_str', axis=1, inplace=True)
        
        # 扩展完整版：主题热点映射（基于关键词或代码，扩展更多示例）
        theme_mapping = {
            # 新能源相关
            '300750': '新能源',    # 宁德时代 - 新能源电池
            '002594': '新能源',    # 比亚迪 - 新能源汽车
            '300274': '新能源',    # 阳光电源 - 光伏
            '002129': '新能源',    # 中环股份 - 光伏
            '300118': '新能源',    # 东方日升 - 光伏
            '000792': '新能源',    # 盐湖股份 - 锂资源
            '601012': '新能源',    # 隆基绿能 - 光伏
            '300763': '新能源',    # 锦浪科技 - 光伏
            '600732': '新能源',    # 爱康科技 - 光伏
            '600900': '新能源',    # 长江电力 - 水电
            '000027': '新能源',    # 深圳能源 - 清洁能源
            
            # 消费相关
            '600519': '消费',      # 贵州茅台 - 白酒
            '000858': '消费',      # 五粮液 - 白酒
            '000333': '消费',      # 美的集团 - 家电
            '000651': '消费',      # 格力电器 - 家电
            '002304': '消费',      # 洋河股份 - 白酒
            '600809': '消费',      # 山西汾酒 - 白酒
            '600600': '消费',      # 青岛啤酒 - 啤酒
            '002024': '消费',      # 苏宁易购 - 零售
            '600827': '消费',      # 百联股份 - 零售
            
            # 科技成长
            '002415': '科技成长',  # 海康威视 - 智能安防
            '002230': '科技成长',  # 科大讯飞 - AI
            '300059': '科技成长',  # 东方财富 - 互联网金融
            '300502': '科技成长',  # 新易盛 - 光模块
            '300124': '科技成长',  # 汇川技术 - 自动化
            '002410': '科技成长',  # 广联达 - 建筑信息
            '600570': '科技成长',  # 恒生电子 - 金融软件
            '688981': '科技成长',  # 中芯国际 - 半导体
            '603986': '科技成长',  # 兆易创新 - 芯片
            '300223': '科技成长',  # 北京君正 - 芯片
            
            # 医药健康
            '600276': '医药健康', # 恒瑞医药 - 创新药
            '300760': '医药健康', # 迈瑞医疗 - 器械
            '300015': '医药健康', # 爱尔眼科 - 服务
            '000538': '医药健康', # 云南白药 - 中药
            '600436': '医药健康', # 片仔癀 - 中药
            '300957': '医药健康', # 贝泰妮 - 护肤
            '603605': '医药健康', # 珀莱雅 - 护肤
            
            # 金融
            '601318': '金融',      # 中国平安 - 保险
            '601166': '金融',      # 兴业银行 - 银行
            '600036': '金融',      # 招商银行 - 银行
            '600030': '金融',      # 中信证券 - 证券
            '601688': '金融',      # 华泰证券 - 证券
            '000776': '金融',      # 广发证券 - 证券
            
            # 半导体
            '688981': '半导体',    # 中芯国际
            '002185': '半导体',    # 华天科技
            '603986': '半导体',    # 兆易创新
            '300223': '半导体',    # 北京君正
            '002079': '半导体',    # 苏州固锝
            
            # AI（人工智能）
            '002230': 'AI',         # 科大讯飞
            '300059': 'AI',         # 东方财富
            '002415': 'AI',         # 海康威视
            '600570': 'AI',         # 恒生电子
            '603019': 'AI',         # 中科曙光
            
            # 5G通信
            '000063': '5G通信',     # 中兴通讯
            '600050': '5G通信',     # 中国联通
            '002792': '5G通信',     # 通宇通讯
            '300312': '5G通信',     # 邦讯技术
            
            # 工业4.0/智能制造
            '300124': '工业4.0',    # 汇川技术
            '002050': '工业4.0',    # 三花智控
            '000651': '工业4.0',    # 格力电器
            '600031': '工业4.0',    # 三一重工
            
            # 绿色能源/环保
            '600900': '绿色能源',   # 长江电力
            '000027': '绿色能源',   # 深圳能源
            '300070': '绿色能源',   # 碧水源
            '000826': '绿色能源',   # 启迪环境
        }
        
        combined_df['主题热点'] = combined_df['股票代码'].astype(str).map(theme_mapping).fillna('无特定主题')
        
        # 行业偏好总结
        industry_summary = combined_df.groupby(['年份', '行业'])['占净值比例'].sum().unstack().fillna(0)
        # 确保数据类型为数值型
        for col in industry_summary.columns:
            industry_summary[col] = pd.to_numeric(industry_summary[col], errors='coerce').fillna(0)
        industry_summary = industry_summary.astype(float)
        report.append("\n##### 申万一级行业分布")
        formatted_industry_summary = industry_summary.map(format_percentage)
        report.append(formatted_industry_summary.to_markdown())
        
        # 主题热点总结
        theme_summary = combined_df.groupby(['年份', '主题热点'])['占净值比例'].sum().unstack().fillna(0)
        # 确保数据类型为数值型
        for col in theme_summary.columns:
            theme_summary[col] = pd.to_numeric(theme_summary[col], errors='coerce').fillna(0)
        theme_summary = theme_summary.astype(float)
        report.append("\n##### 主题热点分布")
        formatted_theme_summary = theme_summary.map(format_percentage)
        report.append(formatted_theme_summary.to_markdown())

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
        else:
            report.append("- **持仓集中度**：仅有一个季度数据，无法分析变化趋势")

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
        else:
            first_year_summary = sector_summary.iloc[0] if len(sector_summary) > 0 else pd.Series()
            if not first_year_summary.empty:
                dominant_sector = first_year_summary.idxmax()
                report.append(f"- **板块偏好**：该基金主要偏向于**{dominant_sector}**板块。")
            else:
                report.append("- **板块偏好**：无法分析板块分布")
        
        # 新增：行业偏好变化分析
        if len(industry_summary) > 1:
            first_year_industry = industry_summary.iloc[0]
            last_year_industry = industry_summary.iloc[-1]
            
            first_dominant_industry = first_year_industry.idxmax()
            last_dominant_industry = last_year_industry.idxmax()
            
            if first_dominant_industry != last_dominant_industry:
                report.append(f"- **行业偏好**：基金的行业配置在分析期内发生了明显变化，从最初主要集中在**{first_dominant_industry}**行业转向了**{last_dominant_industry}**行业。")
            else:
                report.append(f"- **行业偏好**：该基金在分析期内保持了相对稳定的行业配置，主要偏向于**{first_dominant_industry}**行业。")
        else:
            first_year_industry = industry_summary.iloc[0] if len(industry_summary) > 0 else pd.Series()
            if not first_year_industry.empty:
                dominant_industry = first_year_industry.idxmax()
                report.append(f"- **行业偏好**：该基金主要偏向于**{dominant_industry}**行业。")
            else:
                report.append("- **行业偏好**：无法分析行业分布")
        
        # 新增：主题热点变化分析
        if len(theme_summary) > 1:
            first_year_theme = theme_summary.iloc[0]
            last_year_theme = theme_summary.iloc[-1]
            
            first_dominant_theme = first_year_theme.idxmax()
            last_dominant_theme = last_year_theme.idxmax()
            
            if first_dominant_theme != last_dominant_theme:
                report.append(f"- **主题热点**：基金的主题投资在分析期内发生了明显变化，从最初主要集中在**{first_dominant_theme}**主题转向了**{last_dominant_theme}**主题。")
            else:
                report.append(f"- **主题热点**：该基金在分析期内保持了相对稳定的主题投资，主要偏向于**{first_dominant_theme}**主题。")
        else:
            first_year_theme = theme_summary.iloc[0] if len(theme_summary) > 0 else pd.Series()
            if not first_year_theme.empty:
                dominant_theme = first_year_theme.idxmax()
                report.append(f"- **主题热点**：该基金主要偏向于**{dominant_theme}**主题。")
            else:
                report.append("- **主题热点**：无法分析主题分布")
        
        # 增加通用建议
        report.append("\n**总结与建议：**")
        report.append("  在考虑投资该基金时，建议将上述分析结果与其他因素结合考量，例如基金的过往业绩、基金经理的管理经验、基金规模以及费率等。")
        
        report.append("\n---\n")  # 在每个基金报告后添加分隔线


    with open('analysis_report.md', 'w', encoding='utf-8') as f:
        f.write('# 基金持仓分析报告\n\n')
        f.write('\n'.join(report))

if __name__ == "__main__":
    analyze_holdings()
    print("分析报告已生成：analysis_report.md")

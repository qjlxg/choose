import pandas as pd
import glob
import os
import sys
import logging
from datetime import datetime
import yaml
from dataclasses import dataclass
from typing import Optional, Dict, List, Tuple
import re

# 配置日志
def setup_logging():
    """设置日志配置"""
    log_filename = f'fund_analysis_{datetime.now().strftime("%Y%m%d")}.log'
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    logging.info("日志系统已启动")

@dataclass
class AnalysisConfig:
    """分析配置类"""
    data_path: str = 'fund_data'
    category_path: str = '分类表'
    output_file: str = 'analysis_report.md'
    min_weight_threshold: float = 0.5  # 最小权重阈值
    top_n_holdings: int = 10  # 分析前N大持仓
    concentration_threshold: float = 10.0  # 集中度变化阈值
    
    @classmethod
    def from_file(cls, config_path: str = 'config.yaml'):
        """从配置文件加载配置"""
        try:
            if os.path.exists(config_path):
                with open(config_path, 'r', encoding='utf-8') as f:
                    config_dict = yaml.safe_load(f) or {}
                logging.info(f"成功加载配置文件: {config_path}")
                return cls(**config_dict)
            else:
                logging.warning(f"配置文件 {config_path} 不存在，使用默认配置")
                return cls()
        except Exception as e:
            logging.error(f"加载配置文件失败: {e}，使用默认配置")
            return cls()

def safe_read_excel(file_path, **kwargs):
    """安全的Excel文件读取函数"""
    try:
        return pd.read_excel(file_path, **kwargs)
    except FileNotFoundError:
        logging.error(f"文件不存在: {file_path}")
        return None
    except PermissionError:
        logging.error(f"文件权限错误: {file_path}")
        return None
    except Exception as e:
        logging.error(f"读取文件 {file_path} 时发生未知错误: {e}")
        return None

def safe_read_csv(file_path, **kwargs):
    """安全的CSV文件读取函数"""
    try:
        # 尝试不同的编码
        encodings = ['utf-8', 'gbk', 'utf-8-sig']
        for encoding in encodings:
            try:
                return pd.read_csv(file_path, encoding=encoding, **kwargs)
            except UnicodeDecodeError:
                continue
        # 如果所有编码都失败，抛出异常
        raise UnicodeDecodeError("csv", b"", 0, 0, "无法解码")
    except FileNotFoundError:
        logging.error(f"文件不存在: {file_path}")
        return None
    except PermissionError:
        logging.error(f"文件权限错误: {file_path}")
        return None
    except Exception as e:
        logging.error(f"读取文件 {file_path} 时发生未知错误: {e}")
        return None

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
        logging.warning(f"未在 '{category_path}' 目录中找到任何 XLSX 文件。")
        return all_categories

    for f in xlsx_files:
        try:
            category_name = os.path.basename(f).split('.')[0]
            logging.info(f"正在加载分类文件: {category_name}")
            
            df = safe_read_excel(f, header=0, engine='openpyxl')
            if df is None:
                continue
                
            if '股票代码' not in df.columns or '股票名称' not in df.columns:
                logging.warning(f"文件 {f} 缺少关键列 '股票代码' 或 '股票名称'，跳过。")
                continue
            
            # 数据清洗
            df = df.dropna(subset=['股票代码'])
            df['股票代码'] = df['股票代码'].astype(str).str.strip().str.zfill(6)
            
            # 避免重复覆盖，保留第一个分类
            added_count = 0
            for code in df['股票代码']:
                if code not in all_categories:
                    all_categories[code] = category_name
                    added_count += 1
            
            logging.info(f"从 {category_name} 加载了 {added_count} 个股票分类")
        except Exception as e:
            logging.error(f"读取分类文件 {f} 时出错: {e}")
            continue
            
    logging.info(f"总共加载了 {len(all_categories)} 个股票的分类信息")
    return all_categories

def preprocess_fund_data(df: pd.DataFrame, fund_code: str) -> Optional[pd.DataFrame]:
    """
    统一的数据预处理函数
    
    Args:
        df: 原始数据框
        fund_code: 基金代码
        
    Returns:
        处理后的数据框
    """
    try:
        # 列名映射
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
        required_cols = ['股票代码', '股票名称', '占净值比例', '季度']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise KeyError(f"缺少关键列: {missing_cols}")

        # 删除空行
        df = df.dropna(subset=required_cols)
        if df.empty:
            logging.warning(f"数据预处理后为空，跳过文件")
            return None

        # 股票代码标准化
        df['股票代码'] = df['股票代码'].astype(str).str.strip().str.zfill(6)
        
        # 占净值比例清洗
        df['占净值比例'] = (df['占净值比例']
                           .astype(str)
                           .str.replace(r'[%，]', '', regex=True)
                           .str.replace(',', '', regex=False)
                           .pipe(pd.to_numeric, errors='coerce'))
        
        # 删除无效的占净值比例数据
        df = df.dropna(subset=['占净值比例'])
        df = df[df['占净值比例'] >= 0]  # 过滤负值
        
        # 季度格式标准化
        df['季度'] = df['季度'].astype(str).str.strip()
        df['季度'] = df['季度'].str.replace('年', '-Q')
        df['季度'] = df['季度'].str.replace('第', '').str.replace('季度', 'Q')
        
        # 添加基金代码
        df['基金代码'] = fund_code
        
        # 过滤最小权重
        df = df[df['占净值比例'] >= 0.01]  # 过滤权重小于0.01%的持仓
        
        logging.info(f"数据预处理完成，保留 {len(df)} 条有效记录")
        return df
        
    except Exception as e:
        logging.error(f"数据预处理失败: {e}")
        return None

def get_stock_industry(stock_code: str, stock_categories: Dict, sector_mapping: Dict) -> str:
    """获取股票行业分类，支持多级分类"""
    code_str = str(stock_code).zfill(6)
    
    # 优先使用详细分类
    if stock_categories and code_str in stock_categories:
        return stock_categories[code_str]
    
    # 使用板块分类
    prefix = code_str[:3]
    return sector_mapping.get(prefix, '其他')

def parse_filename(filename: str) -> Optional[Tuple[str, str]]:
    """
    解析文件名，提取季度和基金代码
    
    Args:
        filename: 文件名
        
    Returns:
        (季度, 基金代码) 元组，如果解析失败返回 None
    """
    try:
        # 期望格式：季度_基金代码_描述.csv
        # 或者：基金代码_季度_描述.csv
        parts = filename.split('_')
        if len(parts) < 2:
            return None
        
        # 尝试识别季度（包含年份或季度信息）
        quarter_pattern = re.compile(r'\d{4}|Q[1-4]')
        
        quarter_candidate = None
        fund_code_candidate = None
        
        for i, part in enumerate(parts):
            if quarter_pattern.search(part):
                quarter_candidate = part
                fund_code_candidate = parts[i-1] if i > 0 else parts[0]
                break
            elif len(part) == 6 and part.isdigit():  # 6位数字可能是基金代码
                fund_code_candidate = part
                # 检查前面的部分是否包含季度信息
                if i > 0 and quarter_pattern.search(parts[i-1]):
                    quarter_candidate = parts[i-1]
        
        if quarter_candidate and fund_code_candidate:
            return quarter_candidate, fund_code_candidate
        else:
            # 回退到原逻辑
            fund_code = parts[1] if len(parts) > 1 else parts[0]
            return "未知季度", fund_code
            
    except Exception as e:
        logging.warning(f"解析文件名 {filename} 失败: {e}")
        return None

def analyze_holdings():
    """
    遍历 fund_data 目录，对每个基金代码的持仓数据进行合并和分析，
    并将结果输出到 analysis_report.md 文件中。
    """
    # 加载配置
    config = AnalysisConfig.from_file()
    setup_logging()
    
    logging.info(f"开始基金持仓分析，数据路径: {config.data_path}")
    logging.info(f"分类表路径: {config.category_path}")
    
    base_path = config.data_path
    category_path = config.category_path
    all_files = glob.glob(os.path.join(base_path, "*.csv"))

    if not all_files:
        logging.error("未在 'fund_data' 目录中找到任何 CSV 文件。")
        return

    # 加载股票分类
    stock_categories = load_stock_categories(category_path)
    if not stock_categories:
        logging.warning("未加载到任何股票分类数据，将使用默认板块分析。")
        sector_mapping = {
            '688': '科创板', '300': '创业板', '002': '中小板',
            '000': '主板', '600': '主板', '601': '主板',
            '603': '主板', '605': '主板', '005': '主板', '006': '主板',
        }
        use_detailed_categories = False
    else:
        sector_mapping = {
            '688': '科创板', '300': '创业板', '002': '中小板',
            '000': '主板', '600': '主板', '601': '主板',
            '603': '主板', '605': '主板', '005': '主板', '006': '主板',
        }
        use_detailed_categories = True

    # 按基金代码分组文件（增强版）
    fund_files = {}
    for f in all_files:
        try:
            filename = os.path.basename(f)
            parsed = parse_filename(filename)
            
            if parsed:
                quarter, fund_code = parsed
                if fund_code not in fund_files:
                    fund_files[fund_code] = {}
                if quarter not in fund_files[fund_code]:
                    fund_files[fund_code][quarter] = []
                fund_files[fund_code][quarter].append(f)
            else:
                # 回退到原逻辑
                fund_code = os.path.basename(f).split('_')[1]
                if fund_code not in fund_files:
                    fund_files[fund_code] = {}
                if '未知季度' not in fund_files[fund_code]:
                    fund_files[fund_code]['未知季度'] = []
                fund_files[fund_code]['未知季度'].append(f)
                
        except IndexError:
            logging.warning(f"文件名格式不正确，跳过：{f}")
            continue

    if not fund_files:
        logging.error("未找到任何有效基金文件。")
        return

    logging.info(f"发现 {len(fund_files)} 个基金，{sum(len(files) for files in fund_files.values())} 个文件")

    report = []
    report.append(f"# 基金持仓综合分析报告")
    report.append(f"**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append(f"**分析基金数量**: {len(fund_files)}")
    report.append(f"**数据路径**: {base_path}")
    report.append(f"**分类数据**: {'详细分类' if use_detailed_categories else '默认板块分类'}")
    report.append("---")

    for fund_code, quarter_files in fund_files.items():
        logging.info(f"正在分析基金 {fund_code}，涉及 {len(quarter_files)} 个季度")
        
        df_list = []
        for quarter, files in quarter_files.items():
            for f in files:
                try:
                    df = safe_read_csv(f, engine='python')
                    if df is None:
                        continue
                        
                    # 预处理数据
                    processed_df = preprocess_fund_data(df, fund_code)
                    if processed_df is not None:
                        processed_df['季度'] = quarter
                        df_list.append(processed_df)
                        
                except KeyError as e:
                    logging.error(f"读取文件 {f} 时出错：缺少关键列 {e}")
                    continue
                except Exception as e:
                    logging.error(f"读取文件 {f} 时出错：{e}")
                    continue
        
        if not df_list:
            logging.warning(f"基金 {fund_code} 没有有效数据，跳过")
            continue
            
        # 合并数据
        combined_df = pd.concat(df_list, ignore_index=True)
        
        # 标准化季度格式
        combined_df['季度'] = combined_df['季度'].str.replace('年', '-Q')
        combined_df['季度'] = combined_df['季度'].str.replace('第', '').str.replace('季度', 'Q')
        
        # 提取年份和季度编号
        combined_df['年份'] = combined_df['季度'].str.extract(r'(\d{4})').astype('Int64')
        combined_df['季度编号'] = combined_df['季度'].str.extract(r'Q([1-4])').astype('Int64')
        combined_df.sort_values(by=['年份', '季度编号'], inplace=True)
        combined_df.reset_index(drop=True, inplace=True)

        # 生成单个基金报告
        fund_report = generate_fund_report(fund_code, combined_df, stock_categories, 
                                         sector_mapping, use_detailed_categories, config)
        report.extend(fund_report)
        report.append("\n" + "="*80 + "\n")

    # 写入报告
    try:
        with open(config.output_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report))
        logging.info(f"分析报告已保存至: {config.output_file}")
    except Exception as e:
        logging.error(f"保存报告失败: {e}")

def generate_fund_report(fund_code: str, combined_df: pd.DataFrame, 
                        stock_categories: Dict, sector_mapping: Dict,
                        use_detailed_categories: bool, config: AnalysisConfig) -> List[str]:
    """生成单个基金的详细分析报告"""
    
    report = []
    report.append(f"\n## 基金代码: {fund_code} 持仓分析报告")
    report.append(f"**分析日期**: {datetime.now().strftime('%Y-%m-%d')}")
    report.append("---")
    
    # 基本信息统计
    total_quarters = combined_df['季度'].nunique()
    total_stocks = combined_df['股票代码'].nunique()
    total_weight = combined_df['占净值比例'].sum()
    avg_quarter_weight = total_weight / total_quarters
    
    report.append(f"**基本信息**:")
    report.append(f"- 分析期间: {total_quarters} 个季度")
    report.append(f"- 涉及股票: {total_stocks} 只")
    report.append(f"- 总持仓权重: {total_weight:.1f}%")
    report.append(f"- 平均每季度权重: {avg_quarter_weight:.1f}%")
    report.append("")
    
    # 添加行业分类
    combined_df['行业'] = combined_df['股票代码'].apply(
        lambda x: get_stock_industry(x, stock_categories, sector_mapping)
    )
    
    # 未分类股票处理
    unclassified_stocks = combined_df[combined_df['行业'] == '其他']
    if not unclassified_stocks.empty:
        report.append("\n### 🚨 未能匹配到行业分类的股票列表")
        report.append("---")
        unclassified_summary = unclassified_stocks.groupby('季度')['占净值比例'].sum().round(2)
        
        for quarter, group in unclassified_stocks.groupby('季度'):
            quarter_weight = unclassified_summary.get(quarter, 0)
            report.append(f"#### {quarter} (权重: {quarter_weight:.2f}%)")
            # 只显示权重超过1%的未分类股票
            significant_stocks = group[group['占净值比例'] > 1].sort_values('占净值比例', ascending=False)
            for index, row in significant_stocks.iterrows():
                report.append(f"- **{row['股票名称']}** ({row['股票代码']}): {row['占净值比例']:.2f}%")
            if len(significant_stocks) < len(group):
                report.append(f"- ... 还有 {len(group) - len(significant_stocks)} 只权重较低的股票")
        report.append("---\n")

    # 1. 重仓股变动分析
    report.extend(analyze_top_holdings_detailed(combined_df, config.top_n_holdings))
    
    # 2. 行业偏好和持仓集中度分析
    report.extend(analyze_sector_and_concentration(combined_df))
    
    # 3. 趋势总结和投资建议
    report.extend(generate_trend_insights_detailed(combined_df, use_detailed_categories))
    
    return report

def analyze_top_holdings_detailed(df: pd.DataFrame, top_n: int = 10) -> List[str]:
    """详细的重仓股变动分析"""
    report = []
    report.append("\n### 1. 重仓股变动")
    report.append("---")
    
    quarters = sorted(df['季度'].unique())
    if len(quarters) < 2:
        report.append("**注意**: 数据不足以分析持仓变动（少于2个季度）")
        return report
    
    for i in range(len(quarters) - 1):
        current_q = quarters[i]
        next_q = quarters[i+1]
        
        current_holdings = set(df[df['季度'] == current_q]['股票代码'])
        next_holdings = set(df[df['季度'] == next_q]['股票代码'])
        
        new_additions = next_holdings - current_holdings
        removed = current_holdings - next_holdings
        common = current_holdings & next_holdings
        
        report.append(f"#### 从 {current_q} 到 {next_q} 的变动")
        report.append(f"- **新增股票**: {len(new_additions)} 只")
        report.append(f"- **移除股票**: {len(removed)} 只") 
        report.append(f"- **保持持仓**: {len(common)} 只")
        
        # 详细列出变动
        if new_additions:
            new_add_stocks = (df[(df['季度'] == next_q) & 
                               (df['股票代码'].isin(new_additions))]
                            .nlargest(5, '占净值比例')[['股票名称', '股票代码', '占净值比例', '行业']])
            report.append("\n**新增重仓股** (前5名):")
            for _, row in new_add_stocks.iterrows():
                report.append(f"  - **{row['股票名称']}** ({row['股票代码']}, {row['行业']}): {row['占净值比例']:.2f}%")
            if len(new_additions) > 5:
                report.append(f"  - ... 还有 {len(new_additions) - 5} 只新增股票")
        
        if removed:
            removed_stocks = (df[(df['季度'] == current_q) & 
                               (df['股票代码'].isin(removed))]
                            .nlargest(5, '占净值比例')[['股票名称', '股票代码', '占净值比例', '行业']])
            report.append("\n**移除重仓股** (前5名):")
            for _, row in removed_stocks.iterrows():
                report.append(f"  - **{row['股票名称']}** ({row['股票代码']}, {row['行业']}): {row['占净值比例']:.2f}%")
            if len(removed) > 5:
                report.append(f"  - ... 还有 {len(removed) - 5} 只移除股票")
        
        report.append("")
    
    # 各季度前N大重仓股
    report.append(f"#### 各季度前{top_n}大重仓股")
    for quarter in quarters:
        quarter_data = df[df['季度'] == quarter].nlargest(top_n, '占净值比例')
        total_weight = quarter_data['占净值比例'].sum()
        
        report.append(f"\n**{quarter} 前{top_n}大持仓** (总权重: {total_weight:.1f}%):")
        for _, row in quarter_data.iterrows():
            report.append(f"  - **{row['股票名称']}** ({row['股票代码']}, {row['行业']}): {row['占净值比例']:.2f}%")
    
    return report

def analyze_sector_and_concentration(df: pd.DataFrame) -> List[str]:
    """行业偏好和持仓集中度分析"""
    report = []
    report.append("\n### 2. 行业偏好和持仓集中度")
    report.append("---")
    
    # 行业分析
    report.append("#### 2.1 行业偏好（占净值比例之和）")
    
    # 按年份和行业汇总
    sector_summary = df.groupby(['年份', '行业'])['占净值比例'].sum().unstack(fill_value=0)
    
    if not sector_summary.empty:
        sector_summary = sector_summary.astype(float)
        
        # 格式化表格
        formatted_sector_summary = sector_summary.map(lambda x: f"{x:.2f}%" if x > 0 else "")
        report.append(formatted_sector_summary.to_markdown())
        
        # 行业平均权重
        avg_sector_weights = sector_summary.mean().sort_values(ascending=False)
        top_sectors = avg_sector_weights[avg_sector_weights > 2]  # 平均权重>2%
        
        if len(top_sectors) > 0:
            report.append(f"\n**主要行业配置** (平均权重，前{len(top_sectors)}名):")
            for sector, weight in top_sectors.items():
                max_weight = sector_summary[sector].max()
                min_weight = sector_summary[sector].min()
                report.append(f"- **{sector}**: 平均 {weight:.1f}%，范围 [{min_weight:.1f}% - {max_weight:.1f}%]")
    else:
        report.append("**数据不足**: 无法生成行业分析表格")
    
    report.append("")
    
    # 集中度分析
    report.append("#### 2.2 前十大持仓集中度（占净值比例之和）")
    concentration_summary = df.groupby('季度')['占净值比例'].sum()
    
    if not concentration_summary.empty:
        formatted_concentration_summary = pd.DataFrame({
            '季度': concentration_summary.index,
            '前十大持仓集中度': concentration_summary.map(lambda x: f"{x:.2f}%")
        })
        report.append(formatted_concentration_summary.to_markdown(index=False))
        
        # 各季度前10大持仓明细
        report.append(f"\n**各季度前10大持仓明细**:")
        for quarter in sorted(df['季度'].unique()):
            quarter_top10 = (df[df['季度'] == quarter]
                           .nlargest(10, '占净值比例')[['股票名称', '股票代码', '占净值比例', '行业']]
                           .round({'占净值比例': 2}))
            total_weight = quarter_top10['占净值比例'].sum()
            
            report.append(f"\n**{quarter}** (总权重: {total_weight:.1f}%):")
            for _, row in quarter_top10.iterrows():
                report.append(f"  - **{row['股票名称']}** ({row['股票代码']}, {row['行业']}): {row['占净值比例']:.2f}%")
    else:
        report.append("**数据不足**: 无法生成集中度分析")
    
    report.append("")
    return report

def generate_trend_insights_detailed(df: pd.DataFrame, use_detailed_categories: bool) -> List[str]:
    """生成详细的趋势总结和投资建议"""
    report = []
    report.append("\n### 3. 趋势总结和投资建议")
    report.append("---")
    
    report.append("> **免责声明**：本报告基于历史持仓数据进行分析，不构成任何投资建议。投资有风险，入市需谨慎。")
    report.append("")
    
    concentration_summary = df.groupby('季度')['占净值比例'].sum()
    
    if len(concentration_summary) > 1:
        first_concentration = concentration_summary.iloc[0]
        last_concentration = concentration_summary.iloc[-1]
        concentration_diff = last_concentration - first_concentration
        
        if concentration_diff > 10:
            report.append("- **🔴 持仓集中度**：在分析期内，该基金的持仓集中度显著**上升**（+{:.1f}%），显示出更强的选股信心。".format(concentration_diff))
        elif concentration_diff < -10:
            report.append(f"- **🟢 持仓集中度**：在分析期内，该基金的持仓集中度显著**下降**（{concentration_diff:.1f}%），可能在分散风险。")
        else:
            report.append(f"- **🟡 持仓集中度**：该基金的持仓集中度在分析期内相对**稳定**（变化 {concentration_diff:.1f}%）。")
    else:
        report.append("- **持仓集中度**：数据不足，无法分析集中度变化趋势。")
    
    # 行业偏好分析
    sector_summary = df.groupby(['年份', '行业'])['占净值比例'].sum().unstack(fill_value=0)
    
    if len(sector_summary) > 1 and not sector_summary.empty:
        first_year_summary = sector_summary.iloc[0]
        last_year_summary = sector_summary.iloc[-1]
        
        try:
            first_dominant_sector = first_year_summary.idxmax()
            last_dominant_sector = last_year_summary.idxmax()
            
            if first_dominant_sector != last_dominant_sector:
                report.append(f"- **🔄 行业偏好**：基金的投资偏好发生了明显变化，从**{first_dominant_sector}**（{first_year_summary[first_dominant_sector]:.1f}%）转向了**{last_dominant_sector}**（{last_year_summary[last_dominant_sector]:.1f}%）。")
            else:
                report.append(f"- **🎯 行业偏好**：该基金在分析期内主要偏向于**{first_dominant_sector}**行业（平均权重 {first_year_summary[first_dominant_sector]:.1f}%）。")
        except ValueError:
            report.append("- **行业偏好**：由于数据不足，无法分析行业偏好变化。")
    elif not sector_summary.empty:
        dominant_sector = sector_summary.iloc[-1].idxmax()
        report.append(f"- **🎯 行业偏好**：该基金主要配置在**{dominant_sector}**行业（权重 {sector_summary.iloc[-1][dominant_sector]:.1f}%）。")
    else:
        report.append("- **行业偏好**：数据不足，无法分析行业配置情况。")
    
    # 持仓稳定性分析
    quarters = sorted(df['季度'].unique())
    if len(quarters) >= 2:
        first_quarter_holdings = set(df[df['季度'] == quarters[0]]['股票代码'])
        last_quarter_holdings = set(df[df['季度'] == quarters[-1]]['股票代码'])
        
        retention_rate = len(first_quarter_holdings & last_quarter_holdings) / len(first_quarter_holdings) if first_quarter_holdings else 0
        
        if retention_rate > 0.7:
            report.append(f"- **🛡️ 持仓稳定性**：该基金持仓相对**稳定**，首尾两期重叠率 {retention_rate:.0f}%，显示出较强的长期持股风格。")
        elif retention_rate > 0.3:
            report.append(f"- **⚖️ 持仓稳定性**：该基金持仓**中等调整**，首尾两期重叠率 {retention_rate:.0f}%，显示适度的动态调整。")
        else:
            report.append(f"- **🔀 持仓稳定性**：该基金持仓**频繁调整**，首尾两期重叠率仅 {retention_rate:.0f}%，显示较强的交易活跃度。")
    
    # 总结与建议
    report.append("\n**📋 投资总结与建议：**")
    report.append("1. **投资风格**：根据持仓集中度和变动频率判断该基金的投资风格（集中型/分散型，稳定型/活跃型）")
    report.append("2. **行业匹配**：评估基金的主要行业配置是否与您的投资偏好和风险承受能力匹配")
    report.append("3. **风险管理**：关注持仓集中度变化对组合风险的影响")
    report.append("4. **综合考量**：建议结合基金的历史业绩、基金经理经验、费率结构等因素进行综合评估")
    report.append("5. **动态跟踪**：定期关注基金持仓调整，及时了解投资策略的变化")
    
    return report

if __name__ == "__main__":
    try:
        analyze_holdings()
        print(f"\n✅ 分析完成！")
        print(f"📄 详细报告已生成：analysis_report.md")
        print(f"📋 日志文件：fund_analysis_{datetime.now().strftime('%Y%m%d')}.log")
        print("\n" + "="*50)
        print("📝 数据准备建议：")
        print("• 确保 fund_data 目录中的CSV文件命名格式为：季度_基金代码_描述.csv")
        print("• 分类表目录中的Excel文件应包含'股票代码'和'股票名称'列")
        print("\n⚙️ 配置优化建议：")
        print("• 可创建 config.yaml 文件来自定义分析参数")
        print("• 可添加更多自定义的行业分类规则")
        print("\n🚀 扩展功能建议：")
        print("• 可以添加业绩对比分析")
        print("• 支持更多的数据可视化输出") 
        print("• 增加基金经理风格分析")
        print("="*50)
    except KeyboardInterrupt:
        print("\n⚠️  用户中断分析过程")
    except Exception as e:
        logging.error(f"程序执行出错: {e}")
        print(f"\n❌ 分析过程中发生错误: {e}")
        print("请查看日志文件获取详细信息")

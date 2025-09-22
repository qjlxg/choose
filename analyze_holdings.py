import pandas as pd
import glob
import os
import requests
import re
from datetime import datetime

# --- 数据下载功能 ---
def get_public_dates(code: str) -> list:
    '''
    获取基金持仓的公开日期
    -
    参数
    -
        code 基金代码
    返回
        公开持仓的日期列表
    '''
    headers = {
        'Connection': 'keep-alive',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36 Edg/87.0.664.75',
        'Accept': '*/*',
        'Referer': 'http://fund.eastmoney.com/data/fundranking.html',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
    }
    params = (
        ('FCODE', code),
        ('MobileKey', '3EA024C2-7F22-408B-95E4-383D38160FB3'),
        ('OSVersion', '14.3'),
        ('appVersion', '6.3.8'),
        ('cToken', 'a6hdhrfejje88ruaeduau1rdufna1e--.6'),
        ('deviceid', '3EA024C2-7F22-408B-95E4-383D38160FB3'),
        ('passportid', '3061335960830820'),
        ('plat', 'Iphone'),
        ('product', 'EFund'),
        ('serverVersion', '6.3.6'),
        ('version', '6.3.8'),
    )

    try:
        json_response = requests.get(
            'https://fundmobapi.eastmoney.com/FundMNewApi/FundMNIVInfoMultiple', 
            headers=headers, 
            params=params,
            timeout=10
        ).json()
        
        dates = []
        if json_response.get('Datas'):
            for data in json_response['Datas']:
                report_date_str = data.get('ReportDate', '')
                if report_date_str:
                    dates.append(report_date_str)
        return dates
    except Exception as e:
        print(f"获取公开日期时出错: {e}")
        return []


def get_inverst_postion(code: str, date=None) -> pd.DataFrame:
    '''
    根据基金代码跟日期获取基金持仓信息
    -
    参数

        code 基金代码
        date 公布日期 形如 '2020-09-31' 默认为 None，得到最新公布的数据
    返回

        持仓信息表格

    '''
    EastmoneyFundHeaders = {
        'User-Agent': 'EMProjJijin/6.2.8 (iPhone; iOS 13.6; Scale/2.00)',
        'GTOKEN': '98B423068C1F4DEF9842F82ADF08C5db',
        'clientInfo': 'ttjj-iPhone10,1-iOS-iOS13.6',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Host': 'fundmobapi.eastmoney.com',
        'Referer': 'https://mpservice.com/516939c37bdb4ba2b1138c50cf69a2e1/release/pages/FundHistoryNetWorth',
    }
    params = [
        ('FCODE', code),
        ('MobileKey', '3EA024C2-7F22-408B-95E4-383D38160FB3'),
        ('OSVersion', '14.3'),
        ('appType', 'ttjj'),
        ('appVersion', '6.2.8'),
        ('deviceid', '3EA024C2-7F22-408B-95E4-383D38160FB3'),
        ('plat', 'Iphone'),
        ('product', 'EFund'),
        ('serverVersion', '6.2.8'),
        ('version', '6.2.8'),
    ]
    if date is not None:
        params.append(('DATE', date))
    params = tuple(params)

    response = requests.get('https://fundmobapi.eastmoney.com/FundMNewApi/FundMNInverstPosition',
                            headers=EastmoneyFundHeaders, params=params, timeout=10)
    
    rows = []
    stocks = response.json().get('Datas', {}).get('fundStocks')

    columns = {
        'GPDM': '股票代码',
        'GPJC': '股票简称',
        'JZBL': '持仓占比(%)',
        'PCTNVCHG': '较上期变化(%)',
    }
    if stocks is None:
        return pd.DataFrame(rows, columns=columns.values())

    df = pd.DataFrame(stocks)
    df = df[list(columns.keys())].rename(columns=columns)
    return df

def download_fund_data(fund_code):
    """
    下载指定基金代码的历史持仓数据并保存到 fund_data 目录。
    这是一个独立的辅助函数。
    """
    base_path = 'fund_data'
    os.makedirs(base_path, exist_ok=True)
    
    public_dates = get_public_dates(fund_code)
    if not public_dates:
        print(f"未找到基金 {fund_code} 的有效持仓公开日期，下载终止。")
        return
    
    print(f"开始下载基金 {fund_code} 的历史持仓数据...")
    for date in public_dates:
        output_filename = os.path.join(base_path, f"fund_{fund_code}_{date}.csv")
        # 检查文件是否已存在，如果存在则跳过，避免重复下载
        if os.path.exists(output_filename):
            print(f"文件 {output_filename} 已存在，跳过。")
            continue
            
        print(f"正在下载 {date} 的持仓数据...")
        df = get_inverst_postion(fund_code, date=date)
        
        if not df.empty:
            try:
                # 确保保存的 CSV 文件包含所有必要信息
                df.to_csv(output_filename, index=False, encoding='utf-8')
                print(f"数据已成功保存到 {output_filename}")
            except Exception as e:
                print(f"保存文件 {output_filename} 时出错：{e}")
        else:
            print(f"下载 {date} 的数据失败，跳过。")

# --- 本地数据分析功能 (保持不变) ---
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
                
                if '季度' not in df.columns:
                    print(f"文件 {f} 缺少 '季度' 列，跳过。")
                    continue

                if '最新价' in df.columns:
                    df = df.loc[:, ['序号', '股票代码', '股票名称', '相关资讯', '占净值比例', '持股数 （万股）', '持仓市值', '季度']]
                
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

        # 核心修复：强制将 '占净值比例' 列转换为数字
        combined_df['占净值比例'] = pd.to_numeric(combined_df['占净值比例'], errors='coerce')
        
        # 移除转换失败（非数字）的行，因为这些数据无法用于计算
        combined_df.dropna(subset=['占净值比例'], inplace=True)
        
        combined_df['季度'] = combined_df['季度'].str.replace('年', '-').str.replace('季度', '')
        combined_df['年份'] = combined_df['季度'].str.split('-').str[0].astype(int)
        combined_df['季度编号'] = combined_df['季度'].str.split('-').str[1]
        combined_df.sort_values(by=['年份', '季度编号'], inplace=True)
        combined_df.drop_duplicates(subset=['股票代码', '季度'], keep='first', inplace=True)

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
        
        combined_df['股票代码'] = combined_df['股票代码'].astype(str).str.zfill(6)
        combined_df['板块'] = combined_df['股票代码'].str[:3].map(sector_mapping).fillna('其他')

        sector_summary = combined_df.groupby(['季度', '板块'])['占净值比例'].sum().unstack().fillna(0)
        
        report.append("#### 板块偏好（占净值比例之和）")
        formatted_sector_summary = sector_summary.map(lambda x: f"{x:.2f}%" if x > 0 else "")
        report.append(formatted_sector_summary.to_markdown())

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
            first_year_summary = sector_summary.iloc[0].sort_values(ascending=False)
            last_year_summary = sector_summary.iloc[-1].sort_values(ascending=False)
            
            first_dominant_sector = first_year_summary.index[0]
            last_dominant_sector = last_year_summary.index[0]
            
            if first_dominant_sector != last_dominant_sector:
                report.append(f"- **板块偏好**：基金的投资偏好在分析期内发生了明显变化，从最初主要集中在**{first_dominant_sector}**转向了**{last_dominant_sector}**。这可能反映了基金经理对市场热点或宏观经济的最新判断。")
            else:
                report.append(f"- **板块偏好**：该基金在分析期内保持了相对稳定的投资风格，主要偏向于**{first_dominant_sector}**板块。")
        
        # 增加通用建议
        report.append("\n**总结与建议：**")
        report.append("  在考虑投资该基金时，建议将上述分析结果与其他因素结合考量，例如基金的过往业绩、基金经理的管理经验、基金规模以及费率等。")


    with open('analysis_report.md', 'w', encoding='utf-8') as f:
        f.write('\n'.join(report))

    print("分析报告已生成：analysis_report.md")


if __name__ == "__main__":
    # --- 脚本使用说明 ---
    print("--- 基金持仓分析脚本 ---")
    print("1. 如果需要下载新的基金数据，请调用 download_fund_data(fund_code)。")
    print("2. 确保 'fund_data' 目录有数据后，再调用 analyze_holdings() 进行分析。")
    print("-------------------------")
    
    # 示例用法：先下载数据，再进行分析
    # fund_code_to_download = '510050'
    # print(f"开始下载基金 {fund_code_to_download} 的数据...")
    # download_fund_data(fund_code_to_download)

    # 如果 fund_data 目录有数据，直接调用 analyze_holdings()
    analyze_holdings()

import pandas as pd
import os
import subprocess
import datetime

# 获取当前日期，并计算过去一个月的起始日期
today = datetime.date.today()
end_date = today.strftime('%Y-%m-%d')
start_date = (today - datetime.timedelta(days=30)).strftime('%Y-%m-%d')  # 默认过去30天，可调整

# 运行 fund-rank.py
subprocess.run(['python', 'fund-rank.py', start_date, end_date])

# 读取结果文件
result_file = f'result_{start_date}_{end_date}_C类.txt'
if not os.path.exists(result_file):
    print(f"错误：{result_file} 未生成！检查 fund-rank.py 是否运行成功。")
    exit(1)

# 提取 Top 50 基金代码和名称
df = pd.read_csv(result_file, sep='\t', encoding='utf-8')
df = df[['编码', '名称']].head(50)  # 取 Top 50，可调整为 head(20) 等
df.columns = ['代码', '名称']  # 适配 fund_analyzer.py 的输入格式

# 保存为 recommended_cn_funds.csv（备份原有文件）
backup_file = 'recommended_cn_funds_backup.csv'
if os.path.exists('recommended_cn_funds.csv'):
    os.rename('recommended_cn_funds.csv', backup_file)
    print("已备份原有 recommended_cn_funds.csv 为 recommended_cn_funds_backup.csv")

df.to_csv('recommended_cn_funds.csv', index=False, encoding='utf-8-sig')
print(f"已生成 recommended_cn_funds.csv，使用 Top 50 高增长 C 类基金作为输入。")

# 运行原有流程
subprocess.run(['python', 'download_index_data.py'])
subprocess.run(['python', 'fund_analyzer.py'])
subprocess.run(['python', 'market_monitor.py'])
subprocess.run(['python', 'fund_crawler.py'])

# 恢复备份（可选，如果想保持原样）
# os.rename(backup_file, 'recommended_cn_funds.csv')

print("整合完成！最终信号在 data/买入信号基金_*.csv")
print(f"分析日期范围：{start_date} 到 {end_date}")

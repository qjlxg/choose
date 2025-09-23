import pandas as pd
import os
import subprocess
import sys
import datetime
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('fund_rank_integrate.log', encoding='utf-8'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def check_dependencies():
    """检查必要依赖"""
    required_modules = ['pandas', 'requests', 'akshare', 'selenium', 'tenacity', 'bs4', 'lxml']
    for module in required_modules:
        try:
            __import__(module)
        except ImportError as e:
            logger.error(f"依赖缺失: {module}. 请运行 'pip install {module}'")
            sys.exit(1)
    
    # 检查 ChromeDriver（仅针对 fund_analyzer.py）
    try:
        from selenium import webdriver
        driver = webdriver.Chrome()
        driver.quit()
        logger.info("ChromeDriver 检测通过")
    except Exception as e:
        logger.error(f"ChromeDriver 缺失或配置错误: {e}")
        sys.exit(1)

def run_script(script_name):
    """运行脚本并捕获错误"""
    logger.info(f"运行脚本: {script_name}")
    try:
        result = subprocess.run(['python', script_name], check=True, capture_output=True, text=True)
        logger.info(f"{script_name} 运行成功: {result.stdout}")
    except subprocess.CalledProcessError as e:
        logger.error(f"{script_name} 运行失败: {e.stderr}")
        sys.exit(1)

def main(start_date=None, end_date=None):
    """主函数，整合 fund-rank.py 和原有流程"""
    check_dependencies()

    # 默认日期：过去30天
    today = datetime.date.today()
    end_date = end_date or today.strftime('%Y-%m-%d')
    start_date = start_date or (today - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
    logger.info(f"分析日期范围: {start_date} 到 {end_date}")

    # 运行 fund-rank.py
    logger.info("运行 fund-rank.py")
    run_script(f'fund-rank.py {start_date} {end_date}')

    # 读取 fund-rank.py 输出
    result_file = f'result_{start_date}_{end_date}_C类.txt'
    if not os.path.exists(result_file):
        logger.error(f"错误: {result_file} 未生成！检查 fund-rank.py 是否运行成功。")
        sys.exit(1)

    # 转换输出为 fund_analyzer.py 输入
    df = pd.read_csv(result_file, sep='\t', encoding='utf-8')
    df = df[['编码', '名称']].head(50)  # 取 Top 50
    df.columns = ['代码', '名称']  # 适配 fund_analyzer.py
    logger.info(f"从 {result_file} 提取 {len(df)} 只基金")

    # 备份原有 recommended_cn_funds.csv
    backup_file = 'recommended_cn_funds_backup.csv'
    if os.path.exists('recommended_cn_funds.csv'):
        os.rename('recommended_cn_funds.csv', backup_file)
        logger.info(f"已备份 recommended_cn_funds.csv 为 {backup_file}")

    # 保存新输入文件
    df.to_csv('recommended_cn_funds.csv', index=False, encoding='utf-8-sig')
    logger.info("已生成 recommended_cn_funds.csv")

    # 运行原有流程
    scripts = ['download_index_data.py', 'fund_analyzer.py', 'market_monitor.py', 'fund_crawler.py']
    for script in scripts:
        run_script(script)

    # 恢复备份（可选）
    # if os.path.exists(backup_file):
    #     os.rename(backup_file, 'recommended_cn_funds.csv')
    #     logger.info("已恢复原始 recommended_cn_funds.csv")

    logger.info(f"整合完成！最终信号在 data/买入信号基金_{end_date}.csv")

if __name__ == "__main__":
    start_date = sys.argv[1] if len(sys.argv) > 1 else None
    end_date = sys.argv[2] if len(sys.argv) > 2 else None
    main(start_date, end_date)

import subprocess
import sys
import logging
import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('fund_rank_integrate.log', encoding='utf-8'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def run_script(script_name, args=None):
    """运行脚本并捕获错误"""
    cmd = ['python', script_name] + (args or [])
    logger.info(f"运行脚本: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        logger.info(f"{script_name} 运行成功: {result.stdout}")
    except subprocess.CalledProcessError as e:
        logger.error(f"{script_name} 运行失败: {e.stderr}")
        sys.exit(1)

def main(start_date=None, end_date=None):
    """串联运行所有脚本"""
    # 默认日期：过去30天
    today = datetime.date.today()
    end_date = end_date or today.strftime('%Y-%m-%d')
    start_date = start_date or (today - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
    logger.info(f"分析日期范围: {start_date} 到 {end_date}")

    # 按顺序运行脚本
    scripts = [
        ('fund-rank.py', [start_date, end_date]),
        ('download_index_data.py', []),
        ('fund_analyzer.py', []),
        ('market_monitor.py', []),
        ('fund_crawler.py', [])
    ]
    for script, args in scripts:
        run_script(script, args)

    logger.info(f"所有脚本运行完成！最终信号在 data/买入信号基金_{end_date}.csv")

if __name__ == "__main__":
    start_date = sys.argv[1] if len(sys.argv) > 1 else None
    end_date = sys.argv[2] if len(sys.argv) > 2 else None
    main(start_date, end_date)

import pandas as pd
import numpy as np
import re
import os
import logging
from datetime import datetime, timedelta, time
import random
from io import StringIO
import requests
import tenacity
import concurrent.futures
import time as time_module

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('market_monitor.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 定义本地数据存储目录
DATA_DIR = 'fund_data'
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

class FundAllocator:
    """资金分配建议类"""
    
    @staticmethod
    def calculate_allocation_score(row):
        """计算基金的分配权重得分（0-100分）"""
        score = 0
        
        # 1. 行动信号权重 (40分)
        action_map = {
            "强烈强买入": 40, "强买入": 35, "弱买入": 25,
            "持有/观察": 15, "弱卖出/规避": 5, "强卖出/规避": 0, "N/A": 0
        }
        score += action_map.get(row['行动信号'], 0)
        
        # 2. RSI得分 (20分) - 越低越好（超卖机会）
        rsi = pd.to_numeric(row['RSI'], errors='coerce')
        if not pd.isna(rsi):
            if rsi < 30:
                score += 20
            elif rsi < 40:
                score += 15
            elif rsi < 50:
                score += 10
            else:
                score += 5
        
        # 3. 净值/MA50得分 (15分) - 越接近1越好（在均线附近）
        ma_ratio = pd.to_numeric(row['净值/MA50'], errors='coerce')
        if not pd.isna(ma_ratio):
            if 0.95 <= ma_ratio <= 1.05:
                score += 15
            elif 0.9 <= ma_ratio <= 1.1:
                score += 10
            elif 0.85 <= ma_ratio <= 1.15:
                score += 5
        
        # 4. MACD信号 (15分)
        if '金叉' in str(row['MACD信号']):
            score += 15
        elif '死叉' in str(row['MACD信号']):
            score -= 5  # 死叉扣分
        
        # 5. 布林带位置 (10分) - 下轨附近机会更大
        bb_pos = str(row['布林带位置'])
        if '下轨' in bb_pos:
            score += 10
        elif '中轨' in bb_pos:
            score += 5
        
        # 确保得分在0-100之间
        return max(0, min(100, score))
    
    @staticmethod
    def suggest_portfolio(filtered_df, total_budget=5000, max_positions=3):
        """建议投资组合"""
        # 计算分配得分
        filtered_df['分配得分'] = filtered_df.apply(FundAllocator.calculate_allocation_score, axis=1)
        
        # 按得分排序
        scored_df = filtered_df.sort_values('分配得分', ascending=False)
        
        # 选择前N个（考虑资金限制）
        n_positions = min(max_positions, len(scored_df))
        selected_funds = scored_df.head(n_positions).copy()
        
        # 计算每只基金的建议金额
        total_score = selected_funds['分配得分'].sum()
        if total_score > 0:
            selected_funds['建议权重'] = (selected_funds['分配得分'] / total_score * 100).round(1)
            selected_funds['建议金额'] = (selected_funds['建议权重'] / 100 * total_budget).round(0)
        else:
            selected_funds['建议权重'] = 0
            selected_funds['建议金额'] = 0
        
        return scored_df, selected_funds
    
    @staticmethod
    def generate_allocation_report(selected_funds, total_budget):
        """生成资金分配报告"""
        report_lines = []
        report_lines.append("\n## 💰 资金分配建议")
        report_lines.append(f"**总投资预算**: {total_budget:,} 元")
        report_lines.append(f"**建议仓位数**: {len(selected_funds)} 只基金")
        report_lines.append("\n| 基金代码 | 分配得分 | 建议权重 | 建议金额 | 行动信号 | 投资理由 |")
        report_lines.append("|----|----|----|----|----|----|")
        
        for _, row in selected_funds.iterrows():
            amount = f"{row['建议金额']:,}"
            weight = f"{row['建议权重']}%"
            score = f"{row['分配得分']:.0f}"
            signal = row['行动信号']
            reason = FundAllocator._get_investment_reason(row)
            
            report_lines.append(f"| {row['基金代码']} | {score} | {weight} | {amount}元 | {signal} | {reason} |")
        
        # 总计验证
        total_amount = selected_funds['建议金额'].sum()
        report_lines.append(f"\n**总计**: {total_amount:,.0f}元 ({(total_amount/total_budget*100):.1f}% 预算使用率)")
        
        if total_amount < total_budget * 0.9:
            report_lines.append("\n**💡 建议**: 当前建议仓位使用率较低，可考虑:")
            report_lines.append("- 增加仓位数量（分散风险）")
            report_lines.append("- 等待更多买入信号")
            report_lines.append("- 调整投资预算")
        elif total_amount > total_budget * 1.1:
            report_lines.append("\n**⚠️ 提醒**: 建议金额超出预算，建议按比例缩减")
        
        return report_lines
    
    @staticmethod
    def _get_investment_reason(row):
        """生成投资理由摘要"""
        reasons = []
        
        # RSI理由
        rsi = pd.to_numeric(row['RSI'], errors='coerce')
        if not pd.isna(rsi):
            if rsi < 30:
                reasons.append("RSI超卖")
            elif rsi < 40:
                reasons.append("RSI低位")
        
        # 均线理由
        ma_ratio = pd.to_numeric(row['净值/MA50'], errors='coerce')
        if not pd.isna(ma_ratio) and 0.95 <= ma_ratio <= 1.05:
            reasons.append("贴近均线")
        
        # MACD理由
        if '金叉' in str(row['MACD信号']):
            reasons.append("MACD金叉")
        
        # 布林带理由
        if '下轨' in str(row['布林带位置']):
            reasons.append("布林下轨")
        elif '中轨' in str(row['布林带位置']):
            reasons.append("布林中轨")
        
        return "、".join(reasons) if reasons else "综合技术指标"

class MarketMonitor:
    def __init__(self, report_file='analysis_report.md', output_file='market_monitor_report.md', filter_mode='all', rsi_threshold=None, holdings=None):
        self.report_file = report_file
        self.output_file = output_file
        self.filter_mode = filter_mode  # 'all', 'strong_buy', 'low_rsi_buy'
        self.rsi_threshold = rsi_threshold  # e.g., 40, only for low_rsi_buy
        self.holdings = holdings or []  # List of held fund codes, for prioritization
        self.fund_codes = []
        self.fund_data = {}
        self.index_data = pd.DataFrame()  # 大盘数据
        self.index_indicators = None  # 大盘指标
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
        }

    def _load_index_data(self):
        """加载大盘数据"""
        index_file = os.path.join('index_data', '000300.csv')
        if os.path.exists(index_file):
            try:
                self.index_data = pd.read_csv(index_file, parse_dates=['date'])
                self.index_data = self.index_data.sort_values(by='date', ascending=True).reset_index(drop=True)
                logger.info("大盘数据加载成功，共 %d 行，最新日期: %s", len(self.index_data), self.index_data['date'].max().date())
                # 计算大盘指标
                self.index_indicators = self._calculate_indicators(self.index_data)
                if self.index_indicators is not None:
                    logger.info("大盘指标计算完成")
                else:
                    logger.warning("大盘数据不足，无法计算指标")
            except Exception as e:
                logger.error("加载大盘数据失败: %s", e)
                self.index_data = pd.DataFrame()
        else:
            logger.warning("大盘数据文件不存在: %s", index_file)
            self.index_data = pd.DataFrame()

    def _get_index_market_trend(self):
        """获取大盘趋势信号"""
        if self.index_indicators is None or self.index_indicators.empty:
            return "中性"
        
        latest_index = self.index_indicators.iloc[-1]
        ma_ratio = latest_index['ma_ratio']
        macd_diff = latest_index['macd'] - latest_index['signal']
        rsi = latest_index['rsi']
        
        if not np.isnan(ma_ratio) and ma_ratio > 1 and not np.isnan(macd_diff) and macd_diff > 0 and not np.isnan(rsi) and rsi < 70:
            return "强势"
        elif not np.isnan(ma_ratio) and ma_ratio < 0.95 or not np.isnan(macd_diff) and macd_diff < 0 or not np.isnan(rsi) and rsi > 70:
            return "弱势"
        else:
            return "中性"

    def _get_expected_latest_date(self):
        """根据当前时间确定期望的最新数据日期"""
        now = datetime.now()
        # 假设净值更新时间为晚上21:00
        update_time = time(21, 0)
        if now.time() < update_time:
            # 如果当前时间早于21:00，则期望最新日期为昨天
            expected_date = now.date() - timedelta(days=1)
        else:
            # 否则，期望最新日期为今天
            expected_date = now.date()
        logger.info("当前时间: %s, 期望最新数据日期: %s", now.strftime('%Y-%m-%d %H:%M:%S'), expected_date)
        return expected_date

    def _parse_report(self, report_path='analysis_report.md'):
        """从 analysis_report.md 提取推荐基金代码"""
        logger.info("正在解析 %s 获取推荐基金代码...", report_path)
        if not os.path.exists(report_path):
            logger.error("报告文件 %s 不存在", report_path)
            # 如果分析报告不存在，尝试加载你的买入信号CSV
            return self._parse_buy_signals_csv()
        
        try:
            with open(report_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            pattern = re.compile(r'(?:^\| +(\d{6})|### 基金 (\d{6}))', re.M)
            matches = pattern.findall(content)

            extracted_codes = set()
            for match in matches:
                code = match[0] if match[0] else match[1]
                extracted_codes.add(code)
            
            sorted_codes = sorted(list(extracted_codes))
            self.fund_codes = sorted_codes[:1000]
            
            if not self.fund_codes:
                logger.warning("未提取到任何有效基金代码，请检查 analysis_report.md")
                # 尝试加载CSV作为备选
                self._parse_buy_signals_csv()
            else:
                logger.info("提取到 %d 个基金（测试限制前1000个）: %s", len(self.fund_codes), self.fund_codes[:5])
            
        except Exception as e:
            logger.error("解析报告文件失败: %s", e)
            # 备选方案：尝试CSV
            self._parse_buy_signals_csv()

    def _parse_buy_signals_csv(self, csv_file="买入信号基金_20250922.csv"):
        """从买入信号CSV文件解析基金代码"""
        logger.info("尝试从买入信号CSV文件解析基金代码: %s", csv_file)
        if not os.path.exists(csv_file):
            logger.error("买入信号文件 %s 不存在", csv_file)
            return False
        
        try:
            df = pd.read_csv(csv_file)
            if 'fund_code' in df.columns:
                self.fund_codes = df['fund_code'].astype(str).str.zfill(6).tolist()
                logger.info("从CSV文件提取到 %d 个强买入基金: %s", len(self.fund_codes), self.fund_codes)
                return True
            else:
                logger.warning("CSV文件缺少 'fund_code' 列")
                return False
        except Exception as e:
            logger.error("解析CSV文件失败: %s", e)
            return False

    def _read_local_data(self, fund_code):
        """读取本地文件，如果存在则返回DataFrame"""
        file_path = os.path.join(DATA_DIR, f"{fund_code}.csv")
        if os.path.exists(file_path):
            try:
                df = pd.read_csv(file_path, parse_dates=['date'])
                if not df.empty and 'date' in df.columns and 'net_value' in df.columns:
                    df = df.sort_values(by='date', ascending=True).reset_index(drop=True)
                    logger.info("本地已存在基金 %s 数据，共 %d 行，最新日期为: %s", fund_code, len(df), df['date'].max().date())
                    return df
            except Exception as e:
                logger.warning("读取本地文件 %s 失败: %s", file_path, e)
        return pd.DataFrame()

    def _save_to_local_file(self, fund_code, df):
        """将DataFrame保存到本地文件，覆盖旧文件"""
        file_path = os.path.join(DATA_DIR, f"{fund_code}.csv")
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        df.to_csv(file_path, index=False)
        logger.info("基金 %s 数据已成功保存到本地文件: %s", fund_code, file_path)

    def _clean_fund_html_content(self, raw_content_html):
        """清理基金HTML内容，移除特殊标记符号"""
        if not raw_content_html:
            return raw_content_html
        
        # 移除日期后面的特殊标记符号（如 *），通常用于标记周末或节假日双倍收益
        # 模式：匹配日期后面紧跟的 * 符号，并将其移除
        cleaned_content = re.sub(r'(\d{4}-\d{2}-\d{2})\*', r'\1', raw_content_html)
        
        # 移除可能存在的其他特殊字符，确保表格格式正确
        cleaned_content = re.sub(r'[^\w\s%\-–—\d\.\,/:]', ' ', cleaned_content)
        
        logger.debug("原始内容长度: %d, 清理后长度: %d", len(raw_content_html), len(cleaned_content))
        return cleaned_content

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(5),
        wait=tenacity.wait_fixed(10),
        retry=tenacity.retry_if_exception_type((requests.exceptions.RequestException, ValueError)),
        before_sleep=lambda retry_state: logger.info(f"重试基金 {retry_state.args[0]}，第 {retry_state.attempt_number} 次")
    )
    def _fetch_fund_data(self, fund_code, latest_local_date=None):
        """
        从网络获取基金数据，实现真正的增量更新。
        如果 latest_local_date 不为空，则只获取其之后的数据。
        """
        all_new_data = []
        page_index = 1
        has_new_data = False
        
        while True:
            url = f"http://fundf10.eastmoney.com/F10DataApi.aspx?type=lsjz&code={fund_code}&page={page_index}&per=20"
            logger.info("正在获取基金 %s 的第 %d 页数据...", fund_code, page_index)
            
            try:
                response = requests.get(url, headers=self.headers, timeout=30)
                response.raise_for_status()
                
                content_match = re.search(r'content:"(.*?)"', response.text, re.S)
                pages_match = re.search(r'pages:(\d+)', response.text)
                
                if not content_match or not pages_match:
                    logger.error("基金 %s API返回内容格式不正确，可能已无数据或接口变更", fund_code)
                    break

                raw_content_html = content_match.group(1).replace('\\"', '"')
                total_pages = int(pages_match.group(1))
                
                # 新增：清理HTML内容，移除特殊标记符号
                cleaned_content_html = self._clean_fund_html_content(raw_content_html)
                
                tables = pd.read_html(StringIO(cleaned_content_html))
                
                if not tables:
                    logger.warning("基金 %s 在第 %d 页未找到数据表格，爬取结束", fund_code, page_index)
                    break
                
                df_page = tables[0]
                
                # 动态检测列数并调整列名
                if len(df_page.columns) == 6:
                    # 货币基金格式：净值日期、每万份收益、7日年化收益率、申购状态、赎回状态、分红送配
                    df_page.columns = ['date', 'net_value', 'annualized_return', 'purchase_status', 'redemption_status', 'dividend']
                    df_page = df_page[['date', 'net_value']].copy()
                elif len(df_page.columns) == 7:
                    # 股票/混合基金格式：净值日期、日增长率、累计净值、申购状态、赎回状态、分红送配
                    df_page.columns = ['date', 'daily_growth_rate', 'cumulative_net_value', 'purchase_status', 'redemption_status', 'dividend', 'extra']
                    df_page = df_page[['date', 'daily_growth_rate']].copy()
                    df_page.rename(columns={'daily_growth_rate': 'net_value'}, inplace=True)
                else:
                    logger.warning("基金 %s 表格列数异常: %d 列，跳过此页", fund_code, len(df_page.columns))
                    break
                
                df_page['date'] = pd.to_datetime(df_page['date'], errors='coerce')
                df_page['net_value'] = pd.to_numeric(df_page['net_value'], errors='coerce')
                df_page = df_page.dropna(subset=['date', 'net_value'])
                
                # 如果是增量更新模式，检查是否已获取到本地最新数据之前的数据
                if latest_local_date:
                    new_df_page = df_page[df_page['date'].dt.date > latest_local_date]
                    if new_df_page.empty:
                        # 如果当前页没有新数据，且之前已经发现过新数据，则停止爬取
                        if has_new_data:
                            logger.info("基金 %s 已获取所有新数据，爬取结束。", fund_code)
                            break
                        # 如果当前页没有新数据，且是第一页，则说明没有新数据
                        elif page_index == 1:
                            logger.info("基金 %s 无新数据，爬取结束。", fund_code)
                            break
                    else:
                        has_new_data = True
                        all_new_data.append(new_df_page)
                        logger.info("第 %d 页: 发现 %d 行新数据", page_index, len(new_df_page))
                else:
                    # 如果是首次下载，则获取所有数据
                    all_new_data.append(df_page)

                logger.info("基金 %s 总页数: %d, 当前页: %d, 当前页行数: %d", fund_code, total_pages, page_index, len(df_page))
                
                # 如果是增量更新模式，且当前页数据比最新数据日期早，则结束循环
                if latest_local_date and (df_page['date'].dt.date <= latest_local_date).any():
                    logger.info("基金 %s 已追溯到本地数据，增量爬取结束。", fund_code)
                    break

                if page_index >= total_pages:
                    logger.info("基金 %s 已获取所有历史数据，共 %d 页，爬取结束", fund_code, total_pages)
                    break
                
                page_index += 1
                time_module.sleep(random.uniform(1, 2))  # 延长sleep到1-2秒，减少限速风险
                
            except requests.exceptions.RequestException as e:
                logger.error("基金 %s API请求失败: %s", fund_code, str(e))
                raise
            except Exception as e:
                logger.error("基金 %s API数据解析失败: %s", fund_code, str(e))
                # 尝试使用原始内容进行调试
                if 'raw_content_html' in locals():
                    logger.debug("原始HTML内容片段: %s", raw_content_html[:500])
                    logger.debug("清理后HTML内容片段: %s", self._clean_fund_html_content(raw_content_html)[:500])
                raise

        # 合并新数据并返回
        if all_new_data:
            new_combined_df = pd.concat(all_new_data, ignore_index=True)
            return new_combined_df[['date', 'net_value']]
        else:
            return pd.DataFrame()

    def _calculate_indicators(self, df):
        """计算技术指标并生成结果字典"""
        if df is None or df.empty or len(df) < 26:
            return None

        df = df.sort_values(by='date', ascending=True)
        
        # MACD
        exp12 = df['net_value'].ewm(span=12, adjust=False).mean()
        exp26 = df['net_value'].ewm(span=26, adjust=False).mean()
        df['macd'] = exp12 - exp26
        df['signal'] = df['macd'].ewm(span=9, adjust=False).mean()

        # 布林带
        window = 20
        df['bb_mid'] = df['net_value'].rolling(window=window, min_periods=1).mean()
        df['bb_std'] = df['net_value'].rolling(window=window, min_periods=1).std()
        df['bb_upper'] = df['bb_mid'] + (df['bb_std'] * 2)
        df['bb_lower'] = df['bb_mid'] - (df['bb_std'] * 2)
        
        # RSI
        delta = df['net_value'].diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        avg_gain = gain.rolling(window=14, min_periods=1).mean()
        avg_loss = loss.rolling(window=14, min_periods=1).mean()
        
        rs = avg_gain / avg_loss.replace(0, np.nan)
        df['rsi'] = 100 - (100 / (1 + rs))

        # MA50
        df['ma50'] = df['net_value'].rolling(window=min(50, len(df)), min_periods=1).mean()
        df['ma_ratio'] = df['net_value'] / df['ma50']

        return df

    def _get_latest_signals(self, fund_code, df):
        """根据最新数据计算信号，结合大盘趋势调整"""
        try:
            processed_df = self._calculate_indicators(df)
            if processed_df is None:
                logger.warning("基金 %s 数据不足，跳过计算", fund_code)
                return {
                    'fund_code': fund_code, 'latest_net_value': "数据获取失败", 'rsi': np.nan, 'ma_ratio': np.nan,
                    'macd_diff': np.nan, 'bb_upper': np.nan, 'bb_lower': np.nan, 'advice': "观察", 'action_signal': 'N/A'
                }
            
            latest_data = processed_df.iloc[-1]
            latest_net_value = latest_data['net_value']
            latest_rsi = latest_data['rsi']
            latest_ma50_ratio = latest_data['ma_ratio']
            latest_macd_diff = latest_data['macd'] - latest_data['signal']
            latest_bb_upper = latest_data['bb_upper']
            latest_bb_lower = latest_data['bb_lower']

            # 获取大盘趋势
            market_trend = self._get_index_market_trend()

            advice = "观察"
            if (not np.isnan(latest_rsi) and latest_rsi > 70) or \
               (not np.isnan(latest_bb_upper) and latest_net_value > latest_bb_upper) or \
               (not np.isnan(latest_ma50_ratio) and latest_ma50_ratio > 1.2):
                advice = "等待回调"
                # 如果大盘弱势，进一步确认卖出
                if market_trend == "弱势":
                    advice = "强烈等待回调"
            elif (not np.isnan(latest_rsi) and latest_rsi < 30) or \
                 (not np.isnan(latest_bb_lower) and latest_net_value < latest_bb_lower) or \
                 (not np.isnan(latest_ma50_ratio) and latest_ma50_ratio < 0.8):
                advice = "可分批买入"
                # 如果大盘强势，加强买入
                if market_trend == "强势":
                    advice = "强烈分批买入"
            elif (not np.isnan(latest_ma50_ratio) and latest_ma50_ratio > 1) and \
                 (not np.isnan(latest_macd_diff) and latest_macd_diff > 0):
                advice = "可分批买入"
                if market_trend == "强势":
                    advice = "强烈分批买入"
            elif (not np.isnan(latest_ma50_ratio) and latest_ma50_ratio < 1) and \
                 (not np.isnan(latest_macd_diff) and latest_macd_diff < 0):
                advice = "等待回调"
                if market_trend == "弱势":
                    advice = "强烈等待回调"

            action_signal = "持有/观察"
            if not np.isnan(latest_ma50_ratio) and latest_ma50_ratio < 0.95:
                action_signal = "强卖出/规避"
                if market_trend == "弱势":
                    action_signal = "强烈强卖出/规避"
            elif (not np.isnan(latest_rsi) and latest_rsi > 70) and \
                 (not np.isnan(latest_ma50_ratio) and latest_ma50_ratio > 1.2) and \
                 (not np.isnan(latest_macd_diff) and latest_macd_diff < 0):
                action_signal = "强卖出/规避"
                if market_trend == "弱势":
                    action_signal = "强烈强卖出/规避"
            elif (not np.isnan(latest_rsi) and latest_rsi > 65) or \
                 (not np.isnan(latest_bb_upper) and latest_net_value > latest_bb_upper) or \
                 (not np.isnan(latest_ma50_ratio) and latest_ma50_ratio > 1.2):
                action_signal = "弱卖出/规避"
                if market_trend == "弱势":
                    action_signal = "强卖出/规避"
            elif (not np.isnan(latest_rsi) and latest_rsi < 35) and \
                 (not np.isnan(latest_ma50_ratio) and latest_ma50_ratio < 0.9) and \
                 (not np.isnan(latest_macd_diff) and latest_macd_diff > 0):
                action_signal = "强买入"
                if market_trend == "强势":
                    action_signal = "强烈强买入"
            elif (not np.isnan(latest_rsi) and latest_rsi < 45) or \
                 (not np.isnan(latest_bb_lower) and latest_net_value < latest_bb_lower) or \
                 (not np.isnan(latest_ma50_ratio) and latest_ma50_ratio < 1):
                action_signal = "弱买入"
                if market_trend == "强势":
                    action_signal = "强买入"
            
            # 在结果中添加大盘趋势
            return {
                'fund_code': fund_code,
                'latest_net_value': latest_net_value,
                'rsi': latest_rsi,
                'ma_ratio': latest_ma50_ratio,
                'macd_diff': latest_macd_diff,
                'bb_upper': latest_bb_upper,
                'bb_lower': latest_bb_lower,
                'advice': advice,
                'action_signal': action_signal,
                'market_trend': market_trend
            }
        except Exception as e:
            logger.error("处理基金 %s 时发生异常: %s", fund_code, str(e))
            return {
                'fund_code': fund_code,
                'latest_net_value': "数据获取失败",
                'rsi': np.nan,
                'ma_ratio': np.nan,
                'macd_diff': np.nan,
                'bb_upper': np.nan,
                'bb_lower': np.nan,
                'advice': "观察",
                'action_signal': 'N/A',
                'market_trend': self._get_index_market_trend()
            }

    def get_fund_data(self):
        """主控函数：优先从本地加载，仅在数据非最新或不完整时下载"""
        # 加载大盘数据
        self._load_index_data()
        
        # 步骤1: 解析推荐基金代码
        self._parse_report()
        if not self.fund_codes:
            logger.error("没有提取到任何基金代码，无法继续处理")
            return

        # 步骤2: 预加载本地数据并检查是否需要下载
        logger.info("开始预加载本地缓存数据...")
        fund_codes_to_fetch = []
        expected_latest_date = self._get_expected_latest_date()
        min_data_points = 26  # 确保有足够数据计算技术指标

        for fund_code in self.fund_codes:
            local_df = self._read_local_data(fund_code)
            
            if not local_df.empty:
                latest_local_date = local_df['date'].max().date()
                data_points = len(local_df)
                
                # 检查数据是否最新且完整
                if latest_local_date >= expected_latest_date and data_points >= min_data_points:
                    logger.info("基金 %s 的本地数据已是最新 (%s, 期望: %s) 且数据量足够 (%d 行)，直接加载。",
                                 fund_code, latest_local_date, expected_latest_date, data_points)
                    self.fund_data[fund_code] = self._get_latest_signals(fund_code, local_df.tail(100))
                    continue
                else:
                    if latest_local_date < expected_latest_date:
                        logger.info("基金 %s 本地数据已过时（最新日期为 %s，期望 %s），需要从网络获取新数据。",
                                     fund_code, latest_local_date, expected_latest_date)
                    if data_points < min_data_points:
                        logger.info("基金 %s 本地数据量不足（仅 %d 行，需至少 %d 行），需要从网络获取。",
                                     fund_code, data_points, min_data_points)
            else:
                logger.info("基金 %s 本地数据不存在，需要从网络获取。", fund_code)
            
            fund_codes_to_fetch.append(fund_code)

        # 步骤3: 多线程网络下载和处理
        if fund_codes_to_fetch:
            logger.info("开始使用多线程获取 %d 个基金的新数据...", len(fund_codes_to_fetch))
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                future_to_code = {executor.submit(self._process_single_fund, code): code for code in fund_codes_to_fetch}
                for future in concurrent.futures.as_completed(future_to_code):
                    fund_code = future_to_code[future]
                    try:
                        result = future.result()
                        if result:
                            self.fund_data[fund_code] = result
                    except Exception as e:
                        logger.error("处理基金 %s 数据时出错: %s", fund_code, str(e))
                        self.fund_data[fund_code] = {
                            'fund_code': fund_code, 'latest_net_value': "数据获取失败", 'rsi': np.nan,
                            'ma_ratio': np.nan, 'macd_diff': np.nan, 'bb_upper': np.nan, 'bb_lower': np.nan, 'advice': "观察", 'action_signal': 'N/A',
                            'market_trend': self._get_index_market_trend()
                        }
        else:
            logger.info("所有基金数据均来自本地缓存，无需网络下载。")
        
        # 额外处理：如果是从CSV加载的，直接使用CSV数据
        if not fund_codes_to_fetch and not any(fund_code in self.fund_data for fund_code in self.fund_codes):
            self._load_csv_data_directly()
        
        if len(self.fund_data) > 0:
            logger.info("所有基金数据处理完成。")
        else:
            logger.error("所有基金数据均获取失败。")

    def _load_csv_data_directly(self):
        """直接从CSV加载数据作为备选方案"""
        csv_file = "买入信号基金_20250922.csv"
        if os.path.exists(csv_file):
            try:
                df_csv = pd.read_csv(csv_file)
                logger.info("成功加载买入信号CSV，包含 %d 只基金", len(df_csv))
                
                # 将CSV数据转换为我们的格式
                for _, row in df_csv.iterrows():
                    fund_code = str(row['fund_code']).zfill(6)
                    self.fund_data[fund_code] = {
                        'fund_code': fund_code,
                        'latest_net_value': row['最新净值'],
                        'rsi': row['RSI'],
                        'ma_ratio': row['净值/MA50'],
                        'macd_diff': 0 if row['MACD信号'] == '死叉' else 1,  # 简化处理
                        'bb_upper': row['最新净值'] * 1.05 if row['布林带位置'] == '上轨上方' else None,
                        'bb_lower': row['最新净值'] * 0.95 if row['布林带位置'] == '下轨下方' else None,
                        'advice': row['投资建议'],
                        'action_signal': row['行动信号'],
                        'market_trend': self._get_index_market_trend()
                    }
                
                self.fund_codes = df_csv['fund_code'].astype(str).str.zfill(6).tolist()
                logger.info("CSV数据转换完成，共 %d 只基金", len(self.fund_codes))
                
            except Exception as e:
                logger.error("直接加载CSV数据失败: %s", e)

    def _process_single_fund(self, fund_code):
        """处理单个基金数据：读取本地，下载增量，合并，保存，并计算信号"""
        local_df = self._read_local_data(fund_code)
        latest_local_date = local_df['date'].max().date() if not local_df.empty else None

        new_df = self._fetch_fund_data(fund_code, latest_local_date)
        
        if not new_df.empty:
            df_final = pd.concat([local_df, new_df]).drop_duplicates(subset=['date'], keep='last').sort_values(by='date', ascending=True)
            self._save_to_local_file(fund_code, df_final)
            return self._get_latest_signals(fund_code, df_final.tail(100))
        elif not local_df.empty:
            # 如果没有新数据，且本地有数据，则使用本地数据计算信号
            logger.info("基金 %s 无新数据，使用本地历史数据进行分析", fund_code)
            return self._get_latest_signals(fund_code, local_df.tail(100))
        else:
            # 如果既没有新数据，本地又没有数据，则返回失败
            logger.error("基金 %s 未获取到任何有效数据，且本地无缓存", fund_code)
            return None

    def generate_report(self):
        """生成市场情绪与技术指标监控报告"""
        logger.info("正在生成市场监控报告...")
        report_df_list = []
        market_trend = self._get_index_market_trend()
        
        for fund_code in self.fund_codes:
            data = self.fund_data.get(fund_code)
            if data is not None:
                latest_net_value_str = f"{data['latest_net_value']:.4f}" if isinstance(data['latest_net_value'], (float, int)) else str(data['latest_net_value'])
                rsi_str = f"{data['rsi']:.2f}" if isinstance(data['rsi'], (float, int)) and not np.isnan(data['rsi']) else "N/A"
                ma_ratio_str = f"{data['ma_ratio']:.2f}" if isinstance(data['ma_ratio'], (float, int)) and not np.isnan(data['ma_ratio']) else "N/A"
                
                macd_signal = "N/A"
                if isinstance(data['macd_diff'], (float, int)) and not np.isnan(data['macd_diff']):
                    macd_signal = "金叉" if data['macd_diff'] > 0 else "死叉"
                
                bollinger_pos = "中轨"  # 默认中轨
                if isinstance(data['latest_net_value'], (float, int)):
                    if isinstance(data['bb_upper'], (float, int)) and not np.isnan(data['bb_upper']) and data['latest_net_value'] > data['bb_upper']:
                        bollinger_pos = "上轨上方"
                    elif isinstance(data['bb_lower'], (float, int)) and not np.isnan(data['bb_lower']) and data['latest_net_value'] < data['bb_lower']:
                        bollinger_pos = "下轨下方"
                else:
                    bollinger_pos = "N/A"
                
                report_df_list.append({
                    "基金代码": fund_code,
                    "最新净值": latest_net_value_str,
                    "RSI": rsi_str,
                    "净值/MA50": ma_ratio_str,
                    "MACD信号": macd_signal,
                    "布林带位置": bollinger_pos,
                    "投资建议": data['advice'],
                    "行动信号": data['action_signal']
                })
            else:
                report_df_list.append({
                    "基金代码": fund_code,
                    "最新净值": "数据获取失败",
                    "RSI": "N/A",
                    "净值/MA50": "N/A",
                    "MACD信号": "N/A",
                    "布林带位置": "N/A",
                    "投资建议": "观察",
                    "行动信号": "N/A"
                })

        report_df = pd.DataFrame(report_df_list)

        # 新增：根据持仓优先排序（持仓基金排前）
        if self.holdings:
            report_df['is_holding'] = report_df['基金代码'].isin(self.holdings).astype(int)
            report_df = report_df.sort_values(by='is_holding', ascending=False).drop(columns=['is_holding'])

        # 新增：根据filter_mode过滤
        filtered_df = report_df.copy()
        if self.filter_mode == 'strong_buy':
            filtered_df = filtered_df[filtered_df['行动信号'].str.contains('强买入', na=False)]
        elif self.filter_mode == 'low_rsi_buy' and self.rsi_threshold:
            # 转换为数值
            filtered_df['RSI_num'] = pd.to_numeric(filtered_df['RSI'], errors='coerce')
            buy_signals = filtered_df['行动信号'].str.contains('买入', na=False)
            filtered_df = filtered_df[(buy_signals) & (filtered_df['RSI_num'] < self.rsi_threshold)].drop(columns=['RSI_num'])
        # 'all' 不过滤

        # 定义排序优先级
        order_map_action = {
            "强烈强买入": 1,
            "强买入": 1,
            "弱买入": 2,
            "持有/观察": 3,
            "弱卖出/规避": 4,
            "强卖出/规避": 5,
            "强烈强卖出/规避": 5,
            "N/A": 6
        }
        order_map_advice = {
            "强烈分批买入": 1,
            "可分批买入": 1,
            "观察": 2,
            "等待回调": 3,
            "强烈等待回调": 3,
            "N/A": 4
        }
        
        filtered_df['sort_order_action'] = filtered_df['行动信号'].map(order_map_action)
        filtered_df['sort_order_advice'] = filtered_df['投资建议'].map(order_map_advice)
        
        # 将 NaN 替换为 N/A 并对净值等数据类型进行处理
        filtered_df['最新净值'] = pd.to_numeric(filtered_df['最新净值'], errors='coerce')
        filtered_df['RSI'] = pd.to_numeric(filtered_df['RSI'], errors='coerce')
        filtered_df['净值/MA50'] = pd.to_numeric(filtered_df['净值/MA50'], errors='coerce')

        # 按照您的新排序规则进行排序
        filtered_df = filtered_df.sort_values(
            by=['sort_order_action', 'sort_order_advice', 'RSI'],
            ascending=[True, True, True] # 优先按行动信号、其次按投资建议、最后按RSI从低到高排序
        ).drop(columns=['sort_order_action', 'sort_order_advice'])

        # 将浮点数格式化为字符串，方便Markdown输出
        filtered_df['最新净值'] = filtered_df['最新净值'].apply(lambda x: f"{x:.4f}" if not pd.isna(x) else "N/A")
        filtered_df['RSI'] = filtered_df['RSI'].apply(lambda x: f"{x:.2f}" if not pd.isna(x) else "N/A")
        filtered_df['净值/MA50'] = filtered_df['净值/MA50'].apply(lambda x: f"{x:.2f}" if not pd.isna(x) else "N/A")

        # 新增：生成资金分配建议
        total_budget = 5000  # 固定为5000元预算
        max_positions = 3    # 固定为3只基金
        scored_df, selected_funds = FundAllocator.suggest_portfolio(filtered_df, total_budget, max_positions)
        allocation_report = FundAllocator.generate_allocation_report(selected_funds, total_budget)

        # 将上述排序后的 DataFrame 转换为 Markdown
        markdown_table = filtered_df.to_markdown(index=False)
        
        with open(self.output_file, 'w', encoding='utf-8') as f:
            f.write(f"# 市场情绪与技术指标监控报告\n\n")
            f.write(f"生成日期: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"## 大盘趋势分析\n")
            f.write(f"大盘（沪深300）当前趋势: **{market_trend}**\n")
            f.write(f"**说明：** 决策已结合大盘趋势调整，例如大盘强势时加强买入信号。\n\n")
            if self.holdings:
                f.write(f"**持仓基金优先显示**：{', '.join(self.holdings)}\n\n")
            if self.filter_mode != 'all':
                f.write(f"**过滤模式**：{self.filter_mode} (RSI阈值: {self.rsi_threshold if self.rsi_threshold else 'N/A'})\n\n")
            f.write(f"## 推荐基金技术指标 (处理基金数: {len(filtered_df)} / 原始{len(report_df)})\n")
            f.write("此表格已按**行动信号优先级**排序，'强买入'基金将排在最前面。\n")
            f.write("**注意：** 当'行动信号'和'投资建议'冲突时，请以**行动信号**为准，其条件更严格，更适合机械化决策。\n\n")
            f.write(markdown_table)
            
            # 添加资金分配建议
            f.write("\n".join(allocation_report))
            f.write("\n\n")
            
            f.write("## 📋 操作建议\n")
            f.write("""
### 1. **立即行动** (高优先级)
- 按建议金额分批建仓前3只基金
- 建议单笔投资不超过总预算的30%

### 2. **风险控制**
- 设置止损线：买入后跌破MA50时减仓
- 动态调整：每周复盘技术信号变化

### 3. **资金管理**
- 保持现金储备20-30%用于后续机会
- 避免追高，关注RSI<40的低位买入机会

**⚠️ 免责声明**: 本报告仅供参考，不构成投资建议。投资有风险，入市需谨慎。
            """)
        
        logger.info("报告生成完成: %s (过滤后基金数: %d, 建议仓位: %d只)", self.output_file, len(filtered_df), len(selected_funds))


if __name__ == "__main__":
    try:
        logger.info("脚本启动")
        # 示例：使用过滤模式，只显示强买入，预算5000元，最多3只仓位
        monitor = MarketMonitor(
            filter_mode='strong_buy', 
            holdings=['005118']  # 如果你已有持仓
        )
        monitor.get_fund_data()
        monitor.generate_report()
        logger.info("脚本执行完成")
        print(f"\n💰 基于5000元预算，建议投资3只基金")
        print("📄 详细分配方案请查看 market_monitor_report.md")
    except Exception as e:
        logger.error("脚本运行失败: %s", e, exc_info=True)
        raise

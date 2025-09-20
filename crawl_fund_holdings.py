import re
import pandas as pd
from datetime import datetime
import logging

class FundHoldingParser:
    """
    专门解析天天基金API返回的持仓数据格式
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def parse_apidata_content(self, content, fund_code, year, quarter=None):
        """
        解析 var apidata 中的 content 字符串
        
        Args:
            content: 原始的content字符串
            fund_code: 基金代码
            year: 年份
            quarter: 季度（1-4），None表示获取最新季度
            
        Returns:
            list: 解析后的持仓数据列表
        """
        holdings = []
        
        # 清理数据：移除多余的换行和空格
        content = content.replace('\r', '').replace('\n\n', '\n').strip()
        
        # 按季度分割数据
        quarter_patterns = [
            f'{year}年1季度股票投资明细',
            f'{year}年2季度股票投资明细', 
            f'{year}年3季度股票投资明细',
            f'{year}年4季度股票投资明细'
        ]
        
        # 查找所有季度数据
        quarter_sections = []
        for q_pattern in quarter_patterns:
            matches = re.finditer(q_pattern, content)
            for match in matches:
                start_pos = match.end()
                # 找到下一个季度或结束位置
                end_pos = len(content)
                for next_pattern in quarter_patterns:
                    next_match = re.search(next_pattern, content[start_pos:])
                    if next_match:
                        end_pos = start_pos + next_match.start()
                        break
                quarter_sections.append({
                    'quarter': q_pattern[-1],  # 提取季度数字
                    'content': content[start_pos:end_pos].strip()
                })
        
        # 如果没有指定季度，取最新季度
        if not quarter:
            if quarter_sections:
                # 按季度排序，取最新
                latest_section = max(quarter_sections, key=lambda x: int(x['quarter']))
                target_section = latest_section['content']
                target_quarter = latest_section['quarter']
            else:
                # 尝试按日期分割
                date_pattern = r'截止至：(\d{4}-\d{2}-\d{2})'
                dates = re.findall(date_pattern, content)
                if dates:
                    latest_date = max(dates, key=lambda x: datetime.strptime(x, '%Y-%m-%d'))
                    # 按日期分割
                    split_pos = content.find(latest_date)
                    target_section = content[split_pos:]
                    target_quarter = '最新'
                else:
                    target_section = content
                    target_quarter = '未知'
        else:
            # 指定季度
            target_section = None
            target_quarter = str(quarter)
            for section in quarter_sections:
                if section['quarter'] == target_quarter:
                    target_section = section['content']
                    break
            if not target_section:
                target_section = content
                self.logger.warning(f"未找到{year}年第{quarter}季度数据，使用全部数据")
        
        # 解析目标季度的数据
        holdings.extend(self._parse_quarter_holdings(
            target_section, fund_code, year, target_quarter
        ))
        
        return holdings
    
    def _parse_quarter_holdings(self, section, fund_code, year, quarter):
        """
        解析单季度的持仓数据
        """
        holdings = []
        
        # 按行分割
        lines = section.split('\n')
        
        # 找到数据开始位置（序号1开始）
        data_start = False
        data_lines = []
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            # 检测是否为数据行（以数字开头，包含制表符分隔的字段）
            if re.match(r'^\d+\t\d{5,6}\t', line):
                data_start = True
                data_lines.append(line)
            elif data_start:
                # 遇到非数据行，停止
                break
            else:
                # 还在表头部分
                continue
        
        # 解析每一行数据
        for line in data_lines:
            holding = self._parse_holding_line(line, fund_code, year, quarter)
            if holding:
                holdings.append(holding)
        
        return holdings
    
    def _parse_holding_line(self, line, fund_code, year, quarter):
        """
        解析单行持仓数据
        """
        try:
            # 使用制表符分割
            fields = line.split('\t')
            
            if len(fields) < 7:
                return None
            
            # 字段映射（根据实际数据格式）
            # 序号 股票代码 股票名称 [最新价] [涨跌幅] [相关资讯] 占净值比例 持股数 持仓市值
            rank = int(fields[0].strip())
            stock_code = fields[1].strip()
            stock_name = fields[2].strip()
            
            # 根据是否有"最新价"和"涨跌幅"字段，调整索引
            if re.match(r'^\d+\.\d+$', fields[3].strip()) or fields[3].strip() == '':
                # 有最新价字段的情况（Q2格式）
                hold_ratio = fields[6].strip()  # 第7个字段
                hold_shares = fields[7].strip()  # 第8个字段
                hold_value = fields[8].strip()   # 第9个字段
            else:
                # 没有最新价字段的情况（Q1格式）
                hold_ratio = fields[3].strip()  # 第4个字段
                hold_shares = fields[4].strip()  # 第5个字段
                hold_value = fields[5].strip()   # 第6个字段
            
            # 数据清洗
            hold_ratio_clean = re.sub(r'[^\d.]', '', hold_ratio)
            hold_shares_clean = re.sub(r'[^\d.]', '', hold_shares)
            hold_value_clean = re.sub(r'[^\d.]', '', hold_value)
            
            holding = {
                'fund_code': fund_code,
                'year': year,
                'quarter': quarter,
                'rank': rank,
                'stock_code': stock_code,
                'stock_name': stock_name,
                'hold_ratio': float(hold_ratio_clean) if hold_ratio_clean else 0.0,
                'hold_shares': float(hold_shares_clean) if hold_shares_clean else 0.0,
                'hold_value': float(hold_value_clean) if hold_value_clean else 0.0,
                'raw_line': line  # 保留原始行用于调试
            }
            
            return holding
            
        except Exception as e:
            self.logger.debug(f"解析行失败: {line}, 错误: {e}")
            return None
    
    def extract_apidata_from_response(self, response_text):
        """
        从HTTP响应中提取 apidata 对象
        
        Args:
            response_text: 完整的HTTP响应文本
            
        Returns:
            dict: 解析后的apidata对象
        """
        # 匹配 var apidata=...;
        match = re.search(r'var apidata=\{(.*?)\};', response_text, re.DOTALL)
        if not match:
            self.logger.error("未找到 apidata 对象")
            return None
        
        apidata_str = match.group(1)
        
        try:
            # 提取 content 字段
            content_match = re.search(r'content:"(.*?)"', apidata_str, re.DOTALL)
            if not content_match:
                self.logger.error("未找到 content 字段")
                return None
            
            content = content_match.group(1)
            
            # 提取 arryear 字段
            arryear_match = re.search(r'arryear:\[(.*?)\]', apidata_str)
            arryear = []
            if arryear_match:
                years_str = arryear_match.group(1)
                arryear = [int(y.strip()) for y in years_str.split(',') if y.strip().isdigit()]
            
            # 提取 curyear 字段
            curyear_match = re.search(r'curyear:(\d+)', apidata_str)
            curyear = int(curyear_match.group(1)) if curyear_match else datetime.now().year
            
            return {
                'content': content,
                'arryear': arryear,
                'curyear': curyear
            }
            
        except Exception as e:
            self.logger.error(f"解析 apidata 失败: {e}")
            return None


# 集成到原来的爬虫类中
class FundDataCrawler:
    def __init__(self, output_dir='fund_data'):
        self.session = requests.Session()
        self.ua = UserAgent()
        self.output_dir = output_dir
        self.parser = FundHoldingParser()  # 添加解析器
        self.setup_session()
        self.ensure_output_directory()
    
    # ... 其他方法保持不变 ...
    
    def get_fund_holdings_from_api(self, fund_code, years=None, quarter=None):
        """
        通过API链接爬取基金持仓数据 - 精确版本
        """
        if years is None:
            years = [datetime.now().year]
        
        all_holdings = []
        
        for year in years:
            try:
                logging.info(f"正在通过API精确解析基金 {fund_code} {year}年持仓...")
                
                # 构建API链接
                url = f"https://fundf10.eastmoney.com/FundArchivesDatas.aspx?type=jjcc&code={fund_code}&topline=10&year={year}"
                response = self.session.get(url, timeout=10)
                response.raise_for_status()
                
                # 提取 apidata 对象
                apidata = self.parser.extract_apidata_from_response(response.text)
                if not apidata:
                    logging.warning(f"无法解析基金 {fund_code} {year}年的 apidata")
                    continue
                
                # 解析持仓数据
                holdings = self.parser.parse_apidata_content(
                    apidata['content'], 
                    fund_code, 
                    year, 
                    quarter
                )
                
                all_holdings.extend(holdings)
                logging.info(f"基金 {fund_code} {year}年解析到 {len(holdings)} 条持仓记录")
                
                time.sleep(1)  # 避免请求过快
                
            except Exception as e:
                logging.error(f"爬取基金 {fund_code} {year}年持仓失败: {e}")
                continue
        
        if all_holdings:
            df = pd.DataFrame(all_holdings)
            
            # 数据质量检查
            logging.info(f"数据质量检查:")
            logging.info(f"  - 总记录数: {len(df)}")
            logging.info(f"  - 平均持仓比例: {df['hold_ratio'].mean():.2f}%")
            logging.info(f"  - 总持仓市值: {df['hold_value'].sum():,.2f}万元")
            
            return df
        else:
            logging.warning(f"基金 {fund_code} 未获取到任何持仓数据")
            return pd.DataFrame()
    
    def test_parser_with_sample_data(self):
        """
        使用你提供的示例数据测试解析器
        """
        sample_data = '''var apidata={ content:"
广发先进制造股票发起式C  2025年2季度股票投资明细    来源：天天基金    截止至：2025-06-30
序号	股票代码	股票名称	最新价	涨跌幅	相关资讯	占净值
比例	持股数
（万股）	持仓市值
（万元）
1	09992	泡泡玛特			变动详情股吧行情	8.76%	16.44	3,996.99
2	603986	兆易创新			变动详情股吧行情	7.42%	26.78	3,388.47
3	603119	浙江荣泰			变动详情股吧行情	7.04%	69.54	3,215.53
4	300502	新易盛			变动详情股吧行情	6.09%	21.88	2,779.45
5	00981	中芯国际			变动详情股吧行情	5.35%	59.95	2,443.81
6	300476	胜宏科技			变动详情股吧行情	4.85%	16.47	2,213.24
7	688385	复旦微电			变动详情股吧行情	4.49%	41.65	2,051.89
8	002130	沃尔核材			变动详情股吧行情	4.40%	84.29	2,007.79
9	002463	沪电股份			变动详情股吧行情	4.25%	45.58	1,940.80
10	688200	华峰测控			变动详情股吧行情	4.18%	13.24	1,909.48
116.09992,1.603986,1.603119,0.300502,116.00981,0.300476,1.688385,0.002130,0.002463,1.688200,
显示全部持仓明细>>
广发先进制造股票发起式C  2025年1季度股票投资明细    来源：天天基金    截止至：2025-03-31
序号	股票代码	股票名称	相关资讯	占净值
比例	持股数
（万股）	持仓市值
（万元）
1	002600	领益智造	股吧行情	7.07%	397.25	3,595.11
2	300953	震裕科技	股吧行情	6.95%	22.14	3,536.42
3	688608	恒玄科技	股吧行情	6.75%	8.46	3,436.28
4	601100	恒立液压	股吧行情	6.23%	39.88	3,172.06
5	603986	兆易创新	股吧行情	6.15%	26.78	3,130.05
6	300502	新易盛	股吧行情	5.98%	31.02	3,044.15
7	002896	中大力德	股吧行情	5.47%	31.99	2,781.21
8	603119	浙江荣泰	股吧行情	5.37%	69.54	2,731.53
9	300433	蓝思科技	股吧行情	5.17%	103.94	2,632.80
10	00981	中芯国际	股吧行情	5.01%	59.95	2,550.42
",arryear:[2025,2024,2023,2022],curyear:2025};'''
        
        # 测试解析
        apidata = self.parser.extract_apidata_from_response(sample_data)
        if apidata:
            print("✅ 成功提取 apidata:")
            print(f"   - 可用年份: {apidata['arryear']}")
            print(f"   - 当前年份: {apidata['curyear']}")
            print(f"   - 内容长度: {len(apidata['content'])} 字符")
            
            # 解析2025年第2季度数据
            holdings_q2 = self.parser.parse_apidata_content(
                apidata['content'], '014192', 2025, quarter=2
            )
            print(f"\n📊 2025年第2季度解析结果: {len(holdings_q2)} 条记录")
            
            if holdings_q2:
                df_q2 = pd.DataFrame(holdings_q2)
                print("\n前5条记录:")
                print(df_q2[['rank', 'stock_code', 'stock_name', 'hold_ratio', 'hold_value']].head())
                
                print(f"\n统计信息:")
                print(f"  - 总持仓比例: {df_q2['hold_ratio'].sum():.2f}%")
                print(f"  - 总持仓市值: {df_q2['hold_value'].sum():,.2f}万元")
                print(f"  - 平均单股市值: {df_q2['hold_value'].mean():,.2f}万元")
            
            # 解析2025年第1季度数据
            holdings_q1 = self.parser.parse_apidata_content(
                apidata['content'], '014192', 2025, quarter=1
            )
            print(f"\n📊 2025年第1季度解析结果: {len(holdings_q1)} 条记录")
            
            if holdings_q1:
                df_q1 = pd.DataFrame(holdings_q1)
                print("\n前5条记录:")
                print(df_q1[['rank', 'stock_code', 'stock_name', 'hold_ratio', 'hold_value']].head())
        else:
            print("❌ 解析 apidata 失败")


# 测试代码
def test_fund_parser():
    """测试解析器"""
    logging.basicConfig(level=logging.INFO)
    
    # 创建爬虫实例
    crawler = FundDataCrawler()
    
    # 测试示例数据
    print("🔍 测试解析器 - 使用示例数据")
    crawler.test_parser_with_sample_data()
    
    # 测试真实API
    print("\n🔍 测试真实API - 基金014192 (2025年)")
    holdings = crawler.get_fund_holdings_from_api('014192', years=[2025], quarter=2)
    
    if not holdings.empty:
        print(f"\n✅ 真实API测试成功!")
        print(f"获取到 {len(holdings)} 条2025年第2季度持仓记录")
        print("\n前10大持仓:")
        print(holdings[['rank', 'stock_code', 'stock_name', 'hold_ratio', 'hold_shares', 'hold_value']].head(10).to_string(index=False))
    else:
        print("❌ 真实API测试失败")


if __name__ == "__main__":
    test_fund_parser()

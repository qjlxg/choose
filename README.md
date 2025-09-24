# choose

download_index_data.py (下载大盘数据)

fund_analyzer.py (获取基金数据并生成分析报告 analysis_report.md)

market_monitor.py (基于分析报告和市场数据生成监控报告 market_monitor_report.md)

fund_crawler.py (从监控报告中提取买入信号)

fund-rank.py：这是一个独立的基金净值增长率排名工具。您可以指定一个日期范围，来计算某只基金或所有基金在该时间段内的净值增长情况并进行排名。

analyze_holdings.py：这个脚本用于分析基金的持仓情况。它会加载本地的股票分类表（.xlsx 文件），然后为单个或多个基金生成详细的持仓分析报告，包括重仓股变动、行业偏好等。





1. 数据准备 (下载指数数据)
首先，您需要运行 download_index_data.py。这个脚本会从网络上抓取沪深300指数（代码：000300）的历史数据，并将其保存到本地的 index_data/000300.csv 文件中。这个数据是您后续分析的基础，特别是对于 market_monitor.py 脚本，因为它需要用大盘数据来评估市场趋势。

2. 基金分析 (核心分析与评分)
接下来，运行 fund_analyzer.py。这是整个流程的核心。该脚本的主要功能是：

根据一个远程的 CSV 文件（位于 https://raw.githubusercontent.com/qjlxg/rep/main/recommended_cn_funds.csv）获取推荐的基金代码列表。

使用 akshare 或 Selenium 来抓取这些基金的净值、基金经理和持仓数据。

计算每个基金的夏普比率和最大回撤等指标。

最终，生成一个名为 analysis_report.md 的报告文件，其中包含了对基金的“评分”和“投资决策”建议。

3. 市场监控与买入信号生成
然后，您可以运行 market_monitor.py。这个脚本的功能是：

加载您在第一步中下载的大盘指数数据。

解析 analysis_report.md 文件，从中提取出 fund_analyzer.py 推荐的基金代码。

结合大盘趋势，为这些基金计算技术指标，并生成一份名为 market_monitor_report.md 的市场监控报告。这个报告会明确给出“强买入”或“弱买入”等“行动信号”。

4. 基金买入信号筛选
现在，您可以使用 fund_crawler.py。这个脚本并不会抓取任何新数据，而是作为上一个脚本的后续处理工具。它的作用是：

读取 market_monitor_report.md 文件。

从中解析出所有带有“买入”信号的基金。

将这些带有买入信号的基金信息整理成一个 CSV 文件，方便您进行后续操作。

# 数据源升级指南

本项目已升级为使用 **Tushare Pro** 作为主要数据源，并实现了完整的本地数据库缓存机制。

## 架构升级

### 1. 数据源解耦
- **DataSource 抽象基类**: 定义统一的数据获取接口
- **TushareDataSource**: Tushare Pro 数据源实现
- **AkShareDataSource**: AkShare 数据源实现（备用方案）
- **UnifiedDataSource**: 统一数据源，优先从本地数据库读取

### 2. 本地数据库缓存
- **SQLite 数据库**: 存储股票行情、新闻、基本信息、财务指标
- **智能缓存**: 优先从数据库读取，数据库没有时才调用 API
- **增量更新**: 避免重复获取已有数据

### 3. 自动数据更新
- **data_updater.py**: 独立脚本，每天凌晨或收盘后运行
- **批量更新**: 自动更新 WATCHLIST 股票的所有数据

## 快速开始

### 1. 注册 Tushare Pro 账号

访问 [Tushare Pro](https://tushare.pro/) 注册账号并获取 API Token。

### 2. 安装依赖

```bash
pip install tushare
```

### 3. 配置环境变量

```bash
# 设置 Tushare Token（必需）
export TUSHARE_TOKEN='your_tushare_token_here'

# 设置 DeepSeek API Key（可选）
export DEEPSEEK_API_KEY='your_deepseek_key_here'

# 设置数据源类型（可选，默认 tushare）
export DATA_SOURCE='tushare'

# 设置数据库路径（可选，默认 data/stock_data.db）
export DB_PATH='data/stock_data.db'
```

### 4. 初始化数据库

```bash
# 创建 data 目录
mkdir -p data

# 运行数据更新脚本（首次运行会初始化数据库）
python data_updater.py --token $TUSHARE_TOKEN
```

### 5. 测试数据获取

```bash
# 测试所有 WATCHLIST 股票
python test_data_source.py

# 测试单只股票
python test_data_source.py --stock 600519.SH
```

## 使用说明

### data_updater.py - 数据更新脚本

**功能**: 从 Tushare 获取最新数据并更新到本地数据库

**使用方法**:

```bash
# 更新所有 WATCHLIST 股票
python data_updater.py --token $TUSHARE_TOKEN

# 更新单只股票
python data_updater.py --token $TUSHARE_TOKEN --stock 600519.SH

# 仅更新日线行情
python data_updater.py --token $TUSHARE_TOKEN --prices-only

# 仅更新新闻
python data_updater.py --token $TUSHARE_TOKEN --news-only

# 仅更新基本信息
python data_updater.py --token $TUSHARE_TOKEN --info-only
```

**定时任务设置** (Linux/macOS):

```bash
# 编辑 crontab
crontab -e

# 每天凌晨 2 点运行
0 2 * * * cd /path/to/supreme-barnacle && python data_updater.py --token $TUSHARE_TOKEN >> logs/cron.log 2>&1
```

### test_data_source.py - 测试脚本

**功能**: 测试 WATCHLIST 股票的数据获取

**使用方法**:

```bash
# 测试所有股票
python test_data_source.py

# 测试单只股票
python test_data_source.py --stock 600519.SH
```

### 在应用中使用统一数据源

```python
from crawlers.unified_data_source import get_data_source

# 获取数据源实例
data_source = get_data_source()

# 获取日线行情（优先从数据库）
prices_result = data_source.get_daily_prices(
    stock_code='600519.SH',
    start_date='20240101',
    end_date='20241231'
)
print(f"获取 {len(prices_result['data'])} 条记录，来源: {prices_result['source']}")

# 获取新闻（优先从数据库）
news_result = data_source.get_news(
    stock_code='600519.SH',
    days=30
)
print(f"获取 {len(news_result['data'])} 条记录，来源: {news_result['source']}")

# 获取基本信息（优先从数据库）
info_result = data_source.get_stock_info(stock_code='600519.SH')
print(f"股票名称: {info_result['data']['name']}，来源: {info_result['source']}")

# 获取财务指标（优先从数据库）
indicators_result = data_source.get_financial_indicator(stock_code='600519.SH')
print(f"ROE: {indicators_result['data']['roe']}%，来源: {indicators_result['source']}")

# 强制刷新数据（忽略数据库缓存）
data_source.refresh_stock_data(stock_code='600519.SH')
```

## 数据库结构

### daily_prices 表
- 存储股票日线行情数据
- 列: stock_code, date, open, high, low, close, volume, amount
- 索引: (stock_code, date)

### news 表
- 存储股票新闻数据
- 列: stock_code, date, title, content, source
- 索引: (stock_code, date, title)

### stock_info 表
- 存储股票基本信息
- 列: stock_code, name, industry, list_date, ts_code
- 索引: stock_code

### financial_indicators 表
- 存储财务指标数据
- 列: stock_code, roe, gross_margin, debt_ratio
- 索引: stock_code

## WATCHLIST

当前目标股票池：

| 代码 | 名称 |
|------|------|
| 600519.SH | 贵州茅台 |
| 300750.SZ | 宁德时代 |
| 002594.SZ | 比亚迪 |
| 601318.SH | 中国平安 |
| 000858.SZ | 五粮液 |
| 601888.SH | 中国中免 |
| 000333.SZ | 美的集团 |

## 优势

### 1. 稳定性
- Tushare Pro 提供真正的 API，而非爬虫
- 数据质量远超 AkShare
- 接口稳定，维护及时

### 2. 成本
- 本地数据库缓存，减少 API 调用
- 避免重复获取已有数据
- 节省 API 积分和金钱

### 3. 性能
- 数据库查询速度远超 API 调用
- 支持批量更新
- 智能缓存策略

### 4. 可维护性
- 数据源解耦，易于切换
- 统一接口，简化上层代码
- 清晰的架构设计

## 故障排查

### 问题: Tushare Token 未设置

**错误信息**:
```
ValueError: Tushare 数据源需要 token 参数
```

**解决方法**:
```bash
export TUSHARE_TOKEN='your_token_here'
```

### 问题: 数据库文件不存在

**错误信息**:
```
sqlite3.OperationalError: unable to open database file
```

**解决方法**:
```bash
mkdir -p data
```

### 问题: Tushare API 调用失败

**错误信息**:
```
Tushare 接口初始化失败: ...
```

**解决方法**:
1. 检查 Token 是否正确
2. 检查网络连接
3. 检查 Tushare 服务状态

## 下一步

1. **修改 app.py**: 将所有 AkShare 调用替换为 UnifiedDataSource 接口
2. **设置定时任务**: 配置 crontab 每天自动更新数据
3. **监控日志**: 定期检查 data_updater.log 确保数据正常更新
4. **优化查询**: 根据实际使用情况优化数据库查询和缓存策略

## 联系

如有问题，请提交 Issue 或 Pull Request。

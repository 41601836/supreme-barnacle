import akshare as ak
import pandas as pd
import logging
import random
import time
from datetime import datetime, timedelta

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StockDataCrawler:
    """
    股票数据爬虫类，用于从AkShare获取A股数据
    """
    
    def __init__(self, max_retries=3, retry_delay=1):
        """
        初始化爬虫
        :param max_retries: 最大重试次数
        :param retry_delay: 重试延迟（秒）
        """
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.stock_list_cache = None
        self.cache_time = None
        self.cache_expiry = 3600  # 缓存过期时间（秒）
        self.stock_info_cache = {}  # 股票基本信息缓存
        self.stock_info_expiry = 86400  # 股票基本信息缓存过期时间（秒）
    
    def _with_retry(self, func, *args, **kwargs):
        """
        带重试机制的函数调用
        :param func: 要调用的函数
        :param args: 函数参数
        :param kwargs: 函数关键字参数
        :return: 函数返回值
        """
        for i in range(self.max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.warning(f"尝试 {i+1}/{self.max_retries} 失败: {e}")
                if i < self.max_retries - 1:
                    wait_time = self.retry_delay + random.uniform(0, 1)  # 增加随机延迟避免被封
                    logger.info(f"等待 {wait_time:.2f} 秒后重试...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"达到最大重试次数，获取数据失败")
                    raise
    
    def get_stock_info(self, stock_code):
        """
        获取股票基本信息
        :param stock_code: 股票代码，如 "600519"
        :return: 股票基本信息字典
        """
        try:
            # 检查缓存是否有效
            current_time = time.time()
            if stock_code in self.stock_info_cache:
                cached_info = self.stock_info_cache[stock_code]
                if current_time - cached_info['timestamp'] < self.stock_info_expiry:
                    logger.info(f"使用缓存的股票 {stock_code} 基本信息")
                    return cached_info['data']
            
            logger.info(f"获取股票 {stock_code} 基本信息")
            stock_info = self._with_retry(ak.stock_individual_info_em, symbol=stock_code)
            stock_info_dict = stock_info.to_dict('records')[0]
            
            # 更新缓存
            self.stock_info_cache[stock_code] = {
                'data': stock_info_dict,
                'timestamp': current_time
            }
            
            return stock_info_dict
        except Exception as e:
            logger.error(f"获取股票 {stock_code} 基本信息失败: {e}")
            # 如果缓存存在，返回缓存数据
            if stock_code in self.stock_info_cache:
                logger.warning(f"返回缓存的股票 {stock_code} 基本信息")
                return self.stock_info_cache[stock_code]['data']
            return {}
    
    def get_stock_basic_info(self, stock_code):
        """
        获取股票基本信息（名称、行业等）
        :param stock_code: 股票代码，如 "600519"
        :return: DataFrame包含股票基本信息
        """
        try:
            logger.info(f"获取股票 {stock_code} 基本信息")
            
            stock_info = self._with_retry(ak.stock_individual_info_em, symbol=stock_code)
            
            if stock_info.empty:
                logger.warning(f"stock_individual_info_em 返回空数据，尝试备用方案")
                return self._get_stock_info_fallback(stock_code)
            
            info_dict = dict(zip(stock_info['item'], stock_info['value']))
            
            stock_name = info_dict.get('股票名称')
            industry = info_dict.get('行业')
            
            if not stock_name:
                logger.warning(f"无法从info_dict获取股票名称，尝试备用方案")
                return self._get_stock_info_fallback(stock_code)
            
            result_df = pd.DataFrame({
                'name': [stock_name],
                'industry': [industry] if industry else ['未知']
            })
            
            logger.info(f"成功获取股票 {stock_code} 基本信息: 名称={stock_name}, 行业={industry}")
            return result_df
            
        except Exception as e:
            logger.error(f"获取股票 {stock_code} 基本信息失败: {e}")
            return self._get_stock_info_fallback(stock_code)
    
    def _get_stock_info_fallback(self, stock_code):
        """
        备用方案：通过ak.stock_info_a_code_name获取股票名称
        :param stock_code: 股票代码
        :return: DataFrame包含股票基本信息
        """
        try:
            logger.info(f"使用备用方案获取股票 {stock_code} 名称")
            stock_list = self._with_retry(ak.stock_info_a_code_name)
            
            if stock_list.empty:
                logger.warning(f"stock_info_a_code_name 返回空数据")
                return pd.DataFrame()
            
            stock_name = stock_list[stock_list['code'] == stock_code]['name'].values
            
            if len(stock_name) > 0:
                result_df = pd.DataFrame({
                    'name': [stock_name[0]],
                    'industry': ['未知']
                })
                logger.info(f"备用方案成功获取股票 {stock_code} 名称: {stock_name[0]}")
                return result_df
            else:
                logger.warning(f"在stock_info_a_code_name中未找到股票 {stock_code}")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"备用方案获取股票 {stock_code} 信息失败: {e}")
            return pd.DataFrame()

    def get_stock_price(self, stock_code, start_date=None, end_date=None):
        """
        获取股票历史行情数据
        :param stock_code: 股票代码，如 "600519"
        :param start_date: 开始日期，格式 "YYYYMMDD"
        :param end_date: 结束日期，格式 "YYYYMMDD"
        :return: 股票行情数据DataFrame
        """
        try:
            logger.info(f"获取股票 {stock_code} 价格数据，时间范围: {start_date} 至 {end_date}")
            
            # 如果没有指定日期，默认获取最近一年的数据
            if not end_date:
                end_date = datetime.now().strftime("%Y%m%d")
            if not start_date:
                start_date = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")
            
            df = self._with_retry(
                ak.stock_zh_a_hist,
                symbol=stock_code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"  # 前复权
            )
            
            logger.debug(f"原始列名: {df.columns.tolist()}")
            
            # 列名映射：确保关键列名一致
            column_mapping = {
                '收盘': '收盘价',
                ' 成交量': '成交量',  # 处理带空格的列名
                'open': '开盘',
                'close': '收盘价',
                'high': '最高',
                'low': '最低',
                'volume': '成交量',
                'amount': '成交额',
                'amplitude': '振幅',
                'pct_chg': '涨跌幅',
                'change': '涨跌额',
                'turnover': '换手率'
            }
            
            # 重命名列
            df = df.rename(columns=column_mapping)
            
            logger.debug(f"重命名后列名: {df.columns.tolist()}")
            
            # 确保必要的列存在
            required_columns = ['日期', '开盘', '收盘价', '最高', '最低', '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率']
            for col in required_columns:
                if col not in df.columns:
                    logger.warning(f"列 {col} 不存在，将使用空值填充")
                    df[col] = None
            
            # 转换日期格式
            df['日期'] = pd.to_datetime(df['日期'])
            df = df.sort_values('日期')
            
            logger.info(f"成功获取 {len(df)} 条股票 {stock_code} 的价格数据")
            return df
        except Exception as e:
            logger.error(f"获取股票 {stock_code} 价格数据失败: {e}")
            return pd.DataFrame()
    
    def get_stock_daily_data(self, stock_code, start_date=None, end_date=None):
        """
        获取股票每日行情数据（get_stock_price的别名，保持向后兼容）
        :param stock_code: 股票代码，如 "600519"
        :param start_date: 开始日期，格式 "YYYYMMDD"
        :param end_date: 结束日期，格式 "YYYYMMDD"
        :return: 股票行情数据DataFrame
        """
        return self.get_stock_price(stock_code, start_date, end_date)

    def get_stock_list(self, refresh=False):
        """
        获取A股股票列表
        :param refresh: 是否刷新缓存
        :return: 股票列表DataFrame，只包含代码和名称
        """
        try:
            # 检查缓存是否有效
            current_time = time.time()
            if not refresh and self.stock_list_cache is not None and current_time - self.cache_time < self.cache_expiry:
                logger.info("使用缓存的股票列表")
                return self.stock_list_cache
            
            logger.info("获取A股股票列表")
            stock_list = self._with_retry(ak.stock_zh_a_spot_em)
            
            # 只保留需要的列
            stock_list = stock_list[['代码', '名称']]
            
            # 更新缓存
            self.stock_list_cache = stock_list
            self.cache_time = current_time
            
            logger.info(f"成功获取 {len(stock_list)} 只股票列表")
            return stock_list
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            # 如果缓存存在，返回缓存数据
            if self.stock_list_cache is not None:
                logger.warning("返回缓存的股票列表")
                return self.stock_list_cache
            return pd.DataFrame()
    
    def company_to_stock_code(self, company_name):
        """
        将公司名映射为股票代码
        :param company_name: 公司名称，如 "贵州茅台"
        :return: 股票代码，如 "600519"，如果未找到返回None
        """
        try:
            logger.info(f"将公司名 '{company_name}' 映射为股票代码")
            stock_list = self.get_stock_list()
            
            if stock_list.empty:
                logger.warning("股票列表为空，无法进行映射")
                return None
            
            # 查找包含公司名的股票
            matches = stock_list[stock_list['名称'].str.contains(company_name, case=False, na=False)]
            
            if matches.empty:
                logger.warning(f"未找到公司 '{company_name}' 对应的股票")
                return None
            
            # 如果有多个匹配结果，返回第一个
            stock_code = matches.iloc[0]['代码']
            logger.info(f"公司 '{company_name}' 映射到股票代码 '{stock_code}'")
            return stock_code
        except Exception as e:
            logger.error(f"公司名映射失败: {e}")
            return None
    
    def get_stock_news(self, stock_code, days=7, use_mock_data=False):
        """
        获取指定股票最近N天的股吧新闻
        :param stock_code: 股票代码，如 "600519"
        :param days: 要获取的天数，默认7天
        :param use_mock_data: 是否使用模拟数据（用于测试）
        :return: 包含新闻信息的DataFrame，列包括：date, title, content, stock_code
        """
        try:
            if use_mock_data:
                logger.info(f"使用模拟数据获取股票 {stock_code} 最近 {days} 天的股吧新闻")
                from .mock_news import get_mock_stock_news
                return get_mock_stock_news(stock_code, days)
            
            logger.info(f"调用新闻爬虫获取股票 {stock_code} 最近 {days} 天的股吧新闻")
            from .news_crawler import news_crawler
            df_news = news_crawler.get_stock_news(stock_code, days)
            
            if df_news.empty:
                logger.warning(f"新闻爬虫未获取到数据，尝试使用模拟数据")
                from .mock_news import get_mock_stock_news
                return get_mock_stock_news(stock_code, days)
            
            return df_news
        except Exception as e:
            logger.error(f"获取股票新闻失败: {e}")
            logger.info(f"使用模拟数据作为备用")
            try:
                from .mock_news import get_mock_stock_news
                return get_mock_stock_news(stock_code, days)
            except Exception as mock_error:
                logger.error(f"获取模拟数据也失败: {mock_error}")
                return pd.DataFrame(columns=["date", "title", "content", "stock_code"])

def get_stock_news(symbol, days=90):
    """
    使用AKShare接口获取指定股票的真实新闻和评论数据
    :param symbol: 股票代码，如 "600519"
    :param days: 要获取的天数，默认90天
    :return: 包含新闻信息的DataFrame，列包括：date, title, content
    """
    try:
        logger.info(f"使用AKShare接口获取股票 {symbol} 的真实新闻数据（目标：{days}天）")
        
        all_news = []
        
        # 1. 尝试获取个股新闻
        try:
            df_news = ak.stock_news_em(symbol=symbol)
            if not df_news.empty:
                df_news = df_news[['发布时间', '新闻标题', '新闻内容']].copy()
                df_news.rename(columns={'发布时间': 'date', '新闻标题': 'title', '新闻内容': 'content'}, inplace=True)
                df_news['source'] = 'stock_news_em'
                all_news.append(df_news)
                logger.info(f"stock_news_em 获取 {len(df_news)} 条新闻")
        except Exception as e:
            logger.warning(f"stock_news_em 获取失败: {e}")
        
        # 2. 如果新闻数据不足，尝试获取股吧评论
        if all_news:
            total_news = sum(len(df) for df in all_news)
            if total_news < 10:
                logger.info(f"新闻数据不足（{total_news}条），尝试获取股吧评论补充")
                try:
                    df_comment = ak.stock_comment_em(symbol=symbol)
                    if not df_comment.empty:
                        df_comment = df_comment[['发布时间', '评论标题', '评论内容']].copy()
                        df_comment.rename(columns={'发布时间': 'date', '评论标题': 'title', '评论内容': 'content'}, inplace=True)
                        df_comment['source'] = 'stock_comment_em'
                        all_news.append(df_comment)
                        logger.info(f"stock_comment_em 获取 {len(df_comment)} 条股吧评论")
                except Exception as e:
                    logger.warning(f"stock_comment_em 获取失败: {e}")
        else:
            logger.info(f"未获取到新闻数据，尝试获取股吧评论")
            try:
                df_comment = ak.stock_comment_em(symbol=symbol)
                if not df_comment.empty:
                    df_comment = df_comment[['发布时间', '评论标题', '评论内容']].copy()
                    df_comment.rename(columns={'发布时间': 'date', '评论标题': 'title', '评论内容': 'content'}, inplace=True)
                    df_comment['source'] = 'stock_comment_em'
                    all_news.append(df_comment)
                    logger.info(f"stock_comment_em 获取 {len(df_comment)} 条股吧评论")
            except Exception as e:
                logger.warning(f"stock_comment_em 获取失败: {e}")
        
        # 3. 如果新闻和评论数据都不足，尝试获取公告数据作为补充
        total_news = sum(len(df) for df in all_news)
        if total_news < 10:
            logger.info(f"新闻和评论数据仍不足（{total_news}条），尝试获取公告数据补充")
            try:
                df_notice = ak.stock_notice_report_em(symbol=symbol)
                if not df_notice.empty:
                    df_notice = df_notice[['发布时间', '公告标题', '公告内容']].copy()
                    df_notice.rename(columns={'发布时间': 'date', '公告标题': 'title', '公告内容': 'content'}, inplace=True)
                    df_notice['source'] = 'stock_notice_report_em'
                    all_news.append(df_notice)
                    logger.info(f"stock_notice_report_em 获取 {len(df_notice)} 条公告")
            except Exception as e:
                logger.warning(f"stock_notice_report_em 获取失败: {e}")
        
        if not all_news:
            logger.warning(f"所有接口均未获取到股票 {symbol} 的新闻或评论数据")
            return pd.DataFrame(columns=['date', 'title', 'content'])
        
        df = pd.concat(all_news, ignore_index=True)
        
        # 处理日期格式
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df = df.dropna(subset=['date'])
        
        # 去重
        df = df.drop_duplicates(subset=['date', 'title'], keep='first')
        
        # 过滤日期范围
        end_date = pd.to_datetime(datetime.now())
        start_date = end_date - timedelta(days=days)
        df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
        
        logger.info(f"成功获取 {len(df)} 条股票 {symbol} 的新闻/评论数据（融合 {len(all_news)} 个数据源）")
        return df
    except Exception as e:
        logger.error(f"使用AKShare获取股票 {symbol} 新闻失败: {e}")
        return pd.DataFrame(columns=['date', 'title', 'content'])

# 导入新闻爬虫
from .news_crawler import NewsCrawler

# 创建全局实例供外部使用
stock_crawler = StockDataCrawler()
news_crawler = NewsCrawler()

# 向后兼容的包装函数
def get_stock_price(stock_code, start_date=None, end_date=None):
    """获取股票历史行情数据（包装函数）"""
    return stock_crawler.get_stock_price(stock_code, start_date, end_date)

def get_stock_list(refresh=False):
    """获取A股股票列表（包装函数）"""
    return stock_crawler.get_stock_list(refresh)

def get_stock_info(stock_code):
    """获取股票基本信息（包装函数）"""
    return stock_crawler.get_stock_info(stock_code)

def _normalize_stock_code(stock_code):
    """
    标准化股票代码，自动添加交易所后缀
    :param stock_code: 股票代码，如 "600519"
    :return: 标准化后的股票代码，如 "600519.SH"
    """
    if '.' in stock_code:
        return stock_code
    
    if stock_code.startswith('6'):
        return f"{stock_code}.SH"
    elif stock_code.startswith('0') or stock_code.startswith('3'):
        return f"{stock_code}.SZ"
    else:
        return stock_code

def fetch_comprehensive_data(code):
    """
    健壮的数据引擎：获取股票的全面数据（基本面+行情+舆情）
    :param code: 股票代码，如 "600519"
    :return: 包含所有数据的字典 {
        'basic_info': 基本面信息DataFrame,
        'price_data': 价格数据DataFrame,
        'news_data': 舆情数据DataFrame,
        'success': 是否成功获取数据
    }
    """
    result = {
        'basic_info': pd.DataFrame(),
        'price_data': pd.DataFrame(),
        'news_data': pd.DataFrame(),
        'success': False
    }
    
    try:
        logger.info(f"========== 开始获取股票 {code} 的全面数据 ==========")
        
        # 1. 获取基本面数据（强制转换为字典，确保 name 和 industry 永远有值）
        try:
            logger.info(f"1. 获取股票 {code} 的基本面数据")
            normalized_code = _normalize_stock_code(code)
            
            df_info = ak.stock_individual_info_em(symbol=normalized_code)
            
            if df_info.empty:
                logger.warning(f"stock_individual_info_em 返回空数据")
                # 使用备用方案
                stock_list = ak.stock_info_a_code_name()
                if not stock_list.empty:
                    match = stock_list[stock_list['code'] == code]
                    if not match.empty:
                        stock_name = match.iloc[0]['name']
                        result['basic_info'] = pd.DataFrame({
                            'name': [str(stock_name)],
                            'industry': ['未知']
                        })
                        logger.info(f"备用方案获取基本面: 名称={stock_name}")
                    else:
                        # 备用方案也失败，使用默认值
                        result['basic_info'] = pd.DataFrame({
                            'name': [str(f'股票{code}')],
                            'industry': ['未知']
                        })
                        logger.warning(f"备用方案也失败，使用默认值")
                else:
                    # stock_list 为空，使用默认值
                    result['basic_info'] = pd.DataFrame({
                        'name': [str(f'股票{code}')],
                        'industry': ['未知']
                    })
                    logger.warning(f"stock_list 为空，使用默认值")
            else:
                info_dict = dict(zip(df_info['item'], df_info['value']))
                stock_name = info_dict.get('股票名称', '未知')
                industry = info_dict.get('行业', '未知')
                
                result['basic_info'] = pd.DataFrame({
                    'name': [str(stock_name)],
                    'industry': [str(industry)]
                })
                logger.info(f"成功获取基本面: 名称={stock_name}, 行业={industry}")
                
        except Exception as e:
            logger.error(f"获取基本面数据失败: {e}")
            result['basic_info'] = pd.DataFrame({
                'name': [str(f'股票{code}')],
                'industry': ['未知']
            })
        
        # 2. 获取价格数据
        try:
            logger.info(f"2. 获取股票 {code} 的价格数据")
            end_date = datetime.now().strftime("%Y%m%d")
            start_date = (datetime.now() - timedelta(days=90)).strftime("%Y%m%d")
            
            df_price = ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"
            )
            
            if not df_price.empty:
                df_price['date'] = pd.to_datetime(df_price['日期'])
                result['price_data'] = df_price
                logger.info(f"成功获取 {len(df_price)} 条价格数据")
            else:
                logger.warning(f"价格数据为空")
                
        except Exception as e:
            logger.error(f"获取价格数据失败: {e}")
        
        # 3. 舆情数据（瀑布流）
        try:
            logger.info(f"3. 获取股票 {code} 的舆情数据（瀑布流）")
            all_news = []
            
            # 3.1 先尝试 stock_news_em
            try:
                df_news = ak.stock_news_em(symbol=code)
                if not df_news.empty:
                    df_news = df_news[['发布时间', '新闻标题', '新闻内容']].copy()
                    df_news.rename(columns={'发布时间': 'date', '新闻标题': 'title', '新闻内容': 'content'}, inplace=True)
                    df_news['source'] = 'stock_news_em'
                    all_news.append(df_news)
                    logger.info(f"stock_news_em 获取 {len(df_news)} 条新闻")
            except Exception as e:
                logger.warning(f"stock_news_em 获取失败: {e}")
            
            # 3.2 若为空，自动尝试 stock_notice_report_em
            if not all_news:
                try:
                    df_notice = ak.stock_notice_report_em(symbol=code)
                    if not df_notice.empty:
                        df_notice = df_notice[['发布时间', '公告标题', '公告内容']].copy()
                        df_notice.rename(columns={'发布时间': 'date', '公告标题': 'title', '公告内容': 'content'}, inplace=True)
                        df_notice['source'] = 'stock_notice_report_em'
                        all_news.append(df_notice)
                        logger.info(f"stock_notice_report_em 获取 {len(df_notice)} 条公告")
                except Exception as e:
                    logger.warning(f"stock_notice_report_em 获取失败: {e}")
            
            # 3.3 合并舆情数据
            if all_news:
                df_news = pd.concat(all_news, ignore_index=True)
                df_news['date'] = pd.to_datetime(df_news['date'], errors='coerce')
                df_news = df_news.dropna(subset=['date'])
                df_news = df_news.drop_duplicates(subset=['date', 'title'], keep='first')
                result['news_data'] = df_news
                logger.info(f"舆情数据总计: {len(df_news)} 条")
            else:
                logger.warning(f"所有舆情接口均未获取到数据")
                
        except Exception as e:
            logger.error(f"获取舆情数据失败: {e}")
        
        # 判断是否成功
        result['success'] = (
            not result['basic_info'].empty or 
            not result['price_data'].empty or 
            not result['news_data'].empty
        )
        
        logger.info(f"========== 股票 {code} 数据获取完成 ==========")
        return result
        
    except Exception as e:
        logger.error(f"fetch_comprehensive_data 整体失败: {e}")
        import traceback
        traceback.print_exc()
        return result

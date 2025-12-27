"""
统一数据接口封装
提供与 crawlers.stock_data 相同的接口，但使用新的 DataSource 架构
"""

import pandas as pd
import logging
from datetime import datetime, timedelta
from crawlers.unified_data_source import get_data_source

logger = logging.getLogger(__name__)


def fetch_comprehensive_data(code):
    """
    健壮的数据引擎：获取股票的全面数据（基本面+行情+舆情）
    使用新的 DataSource 架构
    
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
        
        # 标准化股票代码
        stock_code = _normalize_stock_code(code)
        
        # 获取数据源实例
        data_source = get_data_source()
        
        # 1. 获取基本面数据
        try:
            logger.info(f"1. 获取股票 {code} 的基本面数据")
            info_result = data_source.get_stock_info(stock_code)
            
            if info_result['data']:
                result['basic_info'] = pd.DataFrame({
                    'name': [info_result['data']['name']],
                    'industry': [info_result['data']['industry']]
                })
                logger.info(f"成功获取基本面: 名称={info_result['data']['name']}, 行业={info_result['data']['industry']}")
            else:
                result['basic_info'] = pd.DataFrame({
                    'name': [str(f'股票{code}')],
                    'industry': ['未知']
                })
                logger.warning(f"基本面数据获取失败，使用默认值")
                
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
            
            prices_result = data_source.get_daily_prices(stock_code, start_date, end_date)
            
            if not prices_result['data'].empty:
                df_price = prices_result['data']
                df_price = df_price.rename(columns={
                    'date': '日期',
                    'open': '开盘',
                    'high': '最高',
                    'low': '最低',
                    'close': '收盘',
                    'volume': '成交量',
                    'amount': '成交额'
                })
                result['price_data'] = df_price
                logger.info(f"成功获取 {len(df_price)} 条价格数据")
            else:
                logger.warning(f"价格数据为空")
                
        except Exception as e:
            logger.error(f"获取价格数据失败: {e}")
        
        # 3. 获取舆情数据
        try:
            logger.info(f"3. 获取股票 {code} 的舆情数据")
            news_result = data_source.get_news(stock_code, days=30)
            
            if not news_result['data'].empty:
                df_news = news_result['data']
                # 保持与原始 stock_data.py 相同的列名格式
                df_news = df_news[['date', 'title', 'content']].copy()
                df_news.rename(columns={'date': '发布时间', 'title': '新闻标题', 'content': '新闻内容'}, inplace=True)
                result['news_data'] = df_news
                logger.info(f"成功获取 {len(df_news)} 条新闻数据")
            else:
                logger.warning(f"新闻数据为空")
                
        except Exception as e:
            logger.error(f"获取舆情数据失败: {e}")
        
        # 判断是否成功
        result['success'] = (
            not result['basic_info'].empty or 
            not result['price_data'].empty or 
            not result['news_data'].empty
        )
        
        logger.info(f"========== 获取股票 {code} 的全面数据完成，成功={result['success']} ==========")
        return result
        
    except Exception as e:
        logger.error(f"获取股票 {code} 的全面数据失败: {e}")
        result['success'] = False
        return result


def get_stock_news(stock_code, days=30):
    """
    获取股票新闻数据
    
    :param stock_code: 股票代码
    :param days: 获取最近N天的新闻
    :return: 新闻DataFrame（列名：发布时间, 新闻标题, 新闻内容）
    """
    try:
        normalized_code = _normalize_stock_code(stock_code)
        data_source = get_data_source()
        news_result = data_source.get_news(normalized_code, days=days)
        
        if not news_result['data'].empty:
            df_news = news_result['data']
            # 重命名列以匹配原始 stock_data.py 的格式
            df_news = df_news[['date', 'title', 'content']].copy()
            df_news.rename(columns={'date': '发布时间', 'title': '新闻标题', 'content': '新闻内容'}, inplace=True)
            return df_news
        else:
            return pd.DataFrame()
            
    except Exception as e:
        logger.error(f"获取股票 {stock_code} 新闻失败: {e}")
        return pd.DataFrame()


def _normalize_stock_code(code):
    """
    标准化股票代码格式
    
    :param code: 股票代码（如 "600519" 或 "600519.SH"）
    :return: 标准化的股票代码（如 "600519.SH"）
    """
    code = str(code).strip()
    
    if '.' in code:
        return code
    
    if code.startswith('6'):
        return f"{code}.SH"
    elif code.startswith('0') or code.startswith('3'):
        return f"{code}.SZ"
    else:
        return f"{code}.SH"


class StockDataCrawler:
    """
    股票数据爬虫类（兼容旧接口）
    使用新的 DataSource 架构
    """
    
    def __init__(self, max_retries=3, retry_delay=1):
        """
        初始化爬虫
        :param max_retries: 最大重试次数（已废弃，保留兼容性）
        :param retry_delay: 重试延迟（已废弃，保留兼容性）
        """
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        logger.info("StockDataCrawler 初始化完成（使用新的 DataSource 架构）")
    
    def get_stock_price(self, stock_code, start_date, end_date):
        """
        获取股票价格数据
        
        :param stock_code: 股票代码
        :param start_date: 开始日期（格式：YYYYMMDD）
        :param end_date: 结束日期（格式：YYYYMMDD）
        :return: 价格DataFrame
        """
        try:
            normalized_code = _normalize_stock_code(stock_code)
            data_source = get_data_source()
            prices_result = data_source.get_daily_prices(normalized_code, start_date, end_date)
            
            if not prices_result['data'].empty:
                df = prices_result['data']
                df = df.rename(columns={
                    'date': '日期',
                    'open': '开盘',
                    'high': '最高',
                    'low': '最低',
                    'close': '收盘',
                    'volume': '成交量',
                    'amount': '成交额'
                })
                return df
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"获取股票 {stock_code} 价格失败: {e}")
            return pd.DataFrame()
    
    def get_stock_info(self, stock_code):
        """
        获取股票基本信息
        
        :param stock_code: 股票代码
        :return: 信息DataFrame
        """
        try:
            normalized_code = _normalize_stock_code(stock_code)
            data_source = get_data_source()
            info_result = data_source.get_stock_info(normalized_code)
            
            if info_result['data']:
                return pd.DataFrame({
                    'name': [info_result['data']['name']],
                    'industry': [info_result['data']['industry']]
                })
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"获取股票 {stock_code} 信息失败: {e}")
            return pd.DataFrame()
    
    def get_stock_basic_info(self, stock_code):
        """
        获取股票基本信息（兼容旧接口）
        
        :param stock_code: 股票代码
        :return: 信息DataFrame
        """
        return self.get_stock_info(stock_code)
    
    def get_stock_daily_data(self, stock_code, start_date, end_date):
        """
        获取股票日线数据（兼容旧接口）
        
        :param stock_code: 股票代码
        :param start_date: 开始日期（格式：YYYYMMDD）
        :param end_date: 结束日期（格式：YYYYMMDD）
        :return: 价格DataFrame
        """
        return self.get_stock_price(stock_code, start_date, end_date)
    
    def get_stock_list(self):
        """
        获取股票列表（兼容旧接口）
        
        :return: 股票列表DataFrame
        """
        try:
            import akshare as ak
            df = ak.stock_info_a_code_name()
            if not df.empty:
                df = df.rename(columns={'code': '代码', 'name': '名称'})
            return df
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            return pd.DataFrame()
    
    def get_news(self, stock_code, days=30, use_mock_data=False):
        """
        获取股票新闻数据（兼容旧接口）
        
        :param stock_code: 股票代码
        :param days: 获取最近N天的新闻
        :param use_mock_data: 是否使用模拟数据（已废弃）
        :return: 新闻DataFrame
        """
        return get_stock_news(stock_code, days)

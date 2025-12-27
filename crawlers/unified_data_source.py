"""
统一数据接口封装器
优先从本地数据库读取数据，数据库没有时才调用 API
"""

import logging
from typing import Optional
from datetime import datetime, timedelta

from crawlers.data_source import DataSource, create_data_source
from crawlers.local_database import LocalDatabase
from config import Config

logger = logging.getLogger(__name__)


class UnifiedDataSource:
    """
    统一数据源
    优先从本地数据库读取，数据库没有时才调用 API
    """
    
    def __init__(self, source_type: str = None, token: str = None, db_path: str = None):
        """
        初始化统一数据源
        
        :param source_type: 数据源类型（'tushare' 或 'akshare'），默认从配置读取
        :param token: API Token，默认从配置读取
        :param db_path: 数据库路径，默认从配置读取
        """
        # 确定数据源类型
        if source_type is None:
            source_type = Config.DATA_SOURCE
        
        # 确定 Token
        if token is None and source_type == 'tushare':
            token = Config.TUSHARE_TOKEN
        
        # 确定数据库路径
        if db_path is None:
            db_path = Config.DB_PATH
        
        # 初始化数据源
        self.data_source = create_data_source(source_type, token=token)
        
        # 初始化本地数据库
        self.db = LocalDatabase(db_path)
        
        logger.info(f"UnifiedDataSource 初始化完成: source={source_type}, db={db_path}")
    
    def get_daily_prices(self, stock_code: str, start_date: str, end_date: str, force_refresh: bool = False) -> dict:
        """
        获取股票日线行情数据（优先从数据库）
        
        :param stock_code: 股票代码
        :param start_date: 开始日期（格式：YYYYMMDD）
        :param end_date: 结束日期（格式：YYYYMMDD）
        :param force_refresh: 是否强制刷新（忽略数据库）
        :return: {'data': DataFrame, 'source': 'db' or 'api'}
        """
        # 如果不强制刷新，先尝试从数据库读取
        if not force_refresh:
            df_db = self.db.get_daily_prices(stock_code, start_date, end_date)
            if not df_db.empty:
                logger.info(f"从数据库获取 {stock_code} 日线数据: {len(df_db)} 条")
                return {
                    'data': df_db,
                    'source': 'db'
                }
        
        # 数据库没有或强制刷新，调用 API
        logger.info(f"从 API 获取 {stock_code} 日线数据")
        df_api = self.data_source.get_daily_prices(stock_code, start_date, end_date)
        
        if not df_api.empty:
            # 保存到数据库
            self.db.save_daily_prices(stock_code, df_api)
            logger.info(f"保存 {stock_code} 日线数据到数据库: {len(df_api)} 条")
        
        return {
            'data': df_api,
            'source': 'api'
        }
    
    def get_news(self, stock_code: str, days: int = 30, force_refresh: bool = False) -> dict:
        """
        获取股票新闻数据（优先从数据库）
        
        :param stock_code: 股票代码
        :param days: 获取最近N天的新闻
        :param force_refresh: 是否强制刷新（忽略数据库）
        :return: {'data': DataFrame, 'source': 'db' or 'api'}
        """
        # 如果不强制刷新，先尝试从数据库读取
        if not force_refresh:
            df_db = self.db.get_news(stock_code, days)
            if not df_db.empty:
                logger.info(f"从数据库获取 {stock_code} 新闻数据: {len(df_db)} 条")
                return {
                    'data': df_db,
                    'source': 'db'
                }
        
        # 数据库没有或强制刷新，调用 API
        logger.info(f"从 API 获取 {stock_code} 新闻数据")
        df_api = self.data_source.get_news(stock_code, days)
        
        if not df_api.empty:
            # 保存到数据库
            self.db.save_news(stock_code, df_api)
            logger.info(f"保存 {stock_code} 新闻数据到数据库: {len(df_api)} 条")
        
        return {
            'data': df_api,
            'source': 'api'
        }
    
    def get_stock_info(self, stock_code: str, force_refresh: bool = False) -> dict:
        """
        获取股票基本信息（优先从数据库）
        
        :param stock_code: 股票代码
        :param force_refresh: 是否强制刷新（忽略数据库）
        :return: {'data': dict, 'source': 'db' or 'api'}
        """
        # 如果不强制刷新，先尝试从数据库读取
        if not force_refresh:
            info_db = self.db.get_stock_info(stock_code)
            if info_db:
                logger.info(f"从数据库获取 {stock_code} 基本信息")
                return {
                    'data': info_db,
                    'source': 'db'
                }
        
        # 数据库没有或强制刷新，调用 API
        logger.info(f"从 API 获取 {stock_code} 基本信息")
        info_api = self.data_source.get_stock_info(stock_code)
        
        # 保存到数据库
        self.db.save_stock_info(stock_code, info_api)
        logger.info(f"保存 {stock_code} 基本信息到数据库")
        
        return {
            'data': info_api,
            'source': 'api'
        }
    
    def get_financial_indicator(self, stock_code: str, force_refresh: bool = False) -> dict:
        """
        获取财务指标数据（优先从数据库）
        
        :param stock_code: 股票代码
        :param force_refresh: 是否强制刷新（忽略数据库）
        :return: {'data': dict, 'source': 'db' or 'api'}
        """
        # 如果不强制刷新，先尝试从数据库读取
        if not force_refresh:
            indicators_db = self.db.get_financial_indicator(stock_code)
            if indicators_db:
                logger.info(f"从数据库获取 {stock_code} 财务指标")
                return {
                    'data': indicators_db,
                    'source': 'db'
                }
        
        # 数据库没有或强制刷新，调用 API
        logger.info(f"从 API 获取 {stock_code} 财务指标")
        indicators_api = self.data_source.get_financial_indicator(stock_code)
        
        # 保存到数据库
        self.db.save_financial_indicator(stock_code, indicators_api)
        logger.info(f"保存 {stock_code} 财务指标到数据库")
        
        return {
            'data': indicators_api,
            'source': 'api'
        }
    
    def refresh_stock_data(self, stock_code: str) -> dict:
        """
        强制刷新单只股票的所有数据
        
        :param stock_code: 股票代码
        :return: 刷新结果
        """
        logger.info(f"开始强制刷新 {stock_code} 的所有数据")
        
        results = {
            'stock_code': stock_code,
            'prices': None,
            'news': None,
            'info': None,
            'indicators': None
        }
        
        # 刷新日线行情
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=90)).strftime('%Y%m%d')
        prices_result = self.get_daily_prices(stock_code, start_date, end_date, force_refresh=True)
        results['prices'] = prices_result['source']
        
        # 刷新新闻
        news_result = self.get_news(stock_code, days=30, force_refresh=True)
        results['news'] = news_result['source']
        
        # 刷新基本信息
        info_result = self.get_stock_info(stock_code, force_refresh=True)
        results['info'] = info_result['source']
        
        # 刷新财务指标
        indicators_result = self.get_financial_indicator(stock_code, force_refresh=True)
        results['indicators'] = indicators_result['source']
        
        logger.info(f"{stock_code} 刷新完成")
        return results
    
    def get_db_stats(self) -> dict:
        """
        获取数据库统计信息
        
        :return: 统计信息字典
        """
        stock_list = self.db.get_stock_list()
        
        stats = {
            'total_stocks': len(stock_list),
            'stock_list': stock_list
        }
        
        # 统计每只股票的数据情况
        for stock_code in stock_list:
            stats[stock_code] = {
                'has_prices': self.db.has_data(stock_code, 'prices'),
                'has_news': self.db.has_data(stock_code, 'news'),
                'has_info': self.db.has_data(stock_code, 'info'),
                'has_indicators': self.db.has_data(stock_code, 'indicators')
            }
        
        return stats


# 创建全局实例
_data_source_instance: Optional[UnifiedDataSource] = None


def get_data_source() -> UnifiedDataSource:
    """
    获取全局数据源实例（单例模式）
    
    :return: UnifiedDataSource 实例
    """
    global _data_source_instance
    
    if _data_source_instance is None:
        # 验证配置
        if not Config.validate():
            raise ValueError("配置无效，请检查环境变量")
        
        # 创建实例
        _data_source_instance = UnifiedDataSource()
        
        # 打印配置
        Config.print_config()
    
    return _data_source_instance

"""
数据源抽象基类和实现
用于解耦数据获取逻辑，支持多种数据源（Tushare、AkShare等）
"""

import pandas as pd
import logging
from abc import ABC, abstractmethod
from typing import Optional, Dict, List
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class DataSource(ABC):
    """
    数据源抽象基类
    定义统一的数据获取接口，实现数据源解耦
    """
    
    @abstractmethod
    def get_daily_prices(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取股票日线行情数据
        
        :param stock_code: 股票代码（如 '600519.SH'）
        :param start_date: 开始日期（格式：YYYYMMDD）
        :param end_date: 结束日期（格式：YYYYMMDD）
        :return: 包含日线行情的DataFrame，列包括：trade_date, open, high, low, close, volume, amount
        """
        pass
    
    @abstractmethod
    def get_news(self, stock_code: str, days: int = 30) -> pd.DataFrame:
        """
        获取股票新闻数据
        
        :param stock_code: 股票代码（如 '600519.SH'）
        :param days: 获取最近N天的新闻
        :return: 包含新闻的DataFrame，列包括：datetime, title, content, source
        """
        pass
    
    @abstractmethod
    def get_stock_info(self, stock_code: str) -> Dict:
        """
        获取股票基本信息
        
        :param stock_code: 股票代码（如 '600519.SH'）
        :return: 包含股票基本信息的字典，包括：name, industry, list_date, etc.
        """
        pass
    
    @abstractmethod
    def get_financial_indicator(self, stock_code: str) -> Dict:
        """
        获取财务指标数据
        
        :param stock_code: 股票代码（如 '600519.SH'）
        :return: 包含财务指标的字典，包括：roe, gross_margin, debt_ratio
        """
        pass


class TushareDataSource(DataSource):
    """
    Tushare Pro 数据源实现
    使用 Tushare Pro API 获取股票数据
    """
    
    def __init__(self, token: str):
        """
        初始化 Tushare 数据源
        
        :param token: Tushare Pro API Token
        """
        self.token = token
        self.pro = None
        self._init_pro()
        logger.info("TushareDataSource 初始化完成")
    
    def _init_pro(self):
        """初始化 Tushare Pro 接口"""
        try:
            import tushare as ts
            self.pro = ts.pro_api(self.token)
            logger.info("Tushare Pro 接口初始化成功")
        except ImportError:
            logger.error("未安装 tushare 库，请运行: pip install tushare")
            raise
        except Exception as e:
            logger.error(f"Tushare Pro 接口初始化失败: {e}")
            raise
    
    def get_daily_prices(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取股票日线行情数据
        
        :param stock_code: 股票代码（如 '600519.SH'）
        :param start_date: 开始日期（格式：YYYYMMDD）
        :param end_date: 结束日期（格式：YYYYMMDD）
        :return: 包含日线行情的DataFrame
        """
        try:
            logger.info(f"从 Tushare 获取 {stock_code} 日线行情: {start_date} - {end_date}")
            
            df = self.pro.daily(
                ts_code=stock_code,
                start_date=start_date,
                end_date=end_date
            )
            
            if df.empty:
                logger.warning(f"未获取到 {stock_code} 的日线数据")
                return pd.DataFrame()
            
            # 标准化列名
            df = df.rename(columns={
                'trade_date': 'date',
                'open': 'open',
                'high': 'high',
                'low': 'low',
                'close': 'close',
                'vol': 'volume',
                'amount': 'amount'
            })
            
            # 转换日期格式
            df['date'] = pd.to_datetime(df['date'])
            
            # 按日期排序
            df = df.sort_values('date')
            
            logger.info(f"成功获取 {len(df)} 条 {stock_code} 日线数据")
            return df
            
        except Exception as e:
            logger.error(f"获取 {stock_code} 日线行情失败: {e}")
            return pd.DataFrame()
    
    def get_news(self, stock_code: str, days: int = 30) -> pd.DataFrame:
        """
        获取股票新闻数据
        
        :param stock_code: 股票代码（如 '600519.SH'）
        :param days: 获取最近N天的新闻
        :return: 包含新闻的DataFrame
        """
        try:
            logger.info(f"从 Tushare 获取 {stock_code} 最近 {days} 天的新闻")
            
            # 计算开始日期
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            # Tushare 的新闻接口
            df = self.pro.news(
                src='sina',  # 新浪财经
                start_date=start_date.strftime('%Y-%m-%d %H:%M:%S'),
                end_date=end_date.strftime('%Y-%m-%d %H:%M:%S')
            )
            
            if df.empty:
                logger.warning(f"未获取到 {stock_code} 的新闻数据")
                return pd.DataFrame()
            
            # 标准化列名
            df = df.rename(columns={
                'datetime': 'date',
                'title': 'title',
                'content': 'content'
            })
            
            # 转换日期格式
            df['date'] = pd.to_datetime(df['date'])
            
            # 添加来源
            df['source'] = 'Tushare'
            
            # 按日期排序
            df = df.sort_values('date', ascending=False)
            
            logger.info(f"成功获取 {len(df)} 条 {stock_code} 新闻数据")
            return df
            
        except Exception as e:
            logger.error(f"获取 {stock_code} 新闻失败: {e}")
            return pd.DataFrame()
    
    def get_stock_info(self, stock_code: str) -> Dict:
        """
        获取股票基本信息
        
        :param stock_code: 股票代码（如 '600519.SH'）
        :return: 包含股票基本信息的字典
        """
        try:
            logger.info(f"从 Tushare 获取 {stock_code} 基本信息")
            
            # 获取股票基本信息
            df = self.pro.stock_basic(
                ts_code=stock_code,
                fields='ts_code,symbol,name,area,industry,list_date'
            )
            
            if df.empty:
                logger.warning(f"未获取到 {stock_code} 的基本信息")
                return {
                    'name': f'股票{stock_code}',
                    'industry': '未知',
                    'list_date': None
                }
            
            info = df.iloc[0]
            
            return {
                'name': info['name'],
                'industry': info['industry'],
                'list_date': info['list_date'],
                'ts_code': info['ts_code']
            }
            
        except Exception as e:
            logger.error(f"获取 {stock_code} 基本信息失败: {e}")
            return {
                'name': f'股票{stock_code}',
                'industry': '未知',
                'list_date': None
            }
    
    def get_financial_indicator(self, stock_code: str) -> Dict:
        """
        获取财务指标数据
        
        :param stock_code: 股票代码（如 '600519.SH'）
        :return: 包含财务指标的字典
        """
        try:
            logger.info(f"从 Tushare 获取 {stock_code} 财务指标")
            
            # 获取财务指标数据
            df = self.pro.fina_indicator(
                ts_code=stock_code,
                start_date='20240101',
                end_date=datetime.now().strftime('%Y%m%d')
            )
            
            if df.empty:
                logger.warning(f"未获取到 {stock_code} 的财务指标")
                return {
                    'roe': 0.0,
                    'gross_margin': 0.0,
                    'debt_ratio': 0.0
                }
            
            # 获取最新一期的数据
            latest = df.iloc[0]
            
            # 提取关键指标
            roe = float(latest['roe']) if pd.notna(latest['roe']) else 0.0
            gross_margin = float(latest['grossprofit_margin']) if pd.notna(latest['grossprofit_margin']) else 0.0
            debt_ratio = float(latest['debt_to_assets']) if pd.notna(latest['debt_to_assets']) else 0.0
            
            logger.info(f"成功获取 {stock_code} 财务指标: ROE={roe:.2f}%, 毛利率={gross_margin:.2f}%, 负债率={debt_ratio:.2f}%")
            
            return {
                'roe': roe,
                'gross_margin': gross_margin,
                'debt_ratio': debt_ratio
            }
            
        except Exception as e:
            logger.error(f"获取 {stock_code} 财务指标失败: {e}")
            return {
                'roe': 0.0,
                'gross_margin': 0.0,
                'debt_ratio': 0.0
            }


class AkShareDataSource(DataSource):
    """
    AkShare 数据源实现（备用方案）
    当 Tushare 不可用时使用
    """
    
    def __init__(self):
        """初始化 AkShare 数据源"""
        import akshare as ak
        self.ak = ak
        logger.info("AkShareDataSource 初始化完成")
    
    def get_daily_prices(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取股票日线行情数据
        """
        try:
            logger.info(f"从 AkShare 获取 {stock_code} 日线行情: {start_date} - {end_date}")
            
            # 转换股票代码格式（600519.SH -> 600519）
            code = stock_code.split('.')[0]
            
            df = self.ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"
            )
            
            if df.empty:
                logger.warning(f"未获取到 {stock_code} 的日线数据")
                return pd.DataFrame()
            
            # 标准化列名
            df = df.rename(columns={
                '日期': 'date',
                '开盘': 'open',
                '最高': 'high',
                '最低': 'low',
                '收盘': 'close',
                '成交量': 'volume',
                '成交额': 'amount'
            })
            
            # 转换日期格式
            df['date'] = pd.to_datetime(df['date'])
            
            # 按日期排序
            df = df.sort_values('date')
            
            logger.info(f"成功获取 {len(df)} 条 {stock_code} 日线数据")
            return df
            
        except Exception as e:
            logger.error(f"获取 {stock_code} 日线行情失败: {e}")
            return pd.DataFrame()
    
    def get_news(self, stock_code: str, days: int = 30) -> pd.DataFrame:
        """
        获取股票新闻数据
        """
        try:
            logger.info(f"从 AkShare 获取 {stock_code} 最近 {days} 天的新闻")
            
            # 转换股票代码格式
            code = stock_code.split('.')[0]
            
            df = self.ak.stock_news_em(symbol=code)
            
            if df.empty:
                logger.warning(f"未获取到 {stock_code} 的新闻数据")
                return pd.DataFrame()
            
            # 标准化列名
            df = df.rename(columns={
                '发布时间': 'date',
                '新闻标题': 'title',
                '新闻内容': 'content',
                '文章来源': 'source'
            })
            
            # 转换日期格式
            df['date'] = pd.to_datetime(df['date'])
            
            # 按日期排序
            df = df.sort_values('date', ascending=False)
            
            logger.info(f"成功获取 {len(df)} 条 {stock_code} 新闻数据")
            return df
            
        except Exception as e:
            logger.error(f"获取 {stock_code} 新闻失败: {e}")
            return pd.DataFrame()
    
    def get_stock_info(self, stock_code: str) -> Dict:
        """
        获取股票基本信息
        """
        try:
            logger.info(f"从 AkShare 获取 {stock_code} 基本信息")
            
            # 转换股票代码格式
            code = stock_code.split('.')[0]
            
            df = self.ak.stock_individual_info_em(symbol=code)
            
            if df.empty:
                logger.warning(f"未获取到 {stock_code} 的基本信息")
                return {
                    'name': f'股票{stock_code}',
                    'industry': '未知',
                    'list_date': None
                }
            
            info_dict = dict(zip(df['item'], df['value']))
            
            return {
                'name': info_dict.get('股票名称', f'股票{stock_code}'),
                'industry': info_dict.get('行业', '未知'),
                'list_date': info_dict.get('上市日期', None)
            }
            
        except Exception as e:
            logger.error(f"获取 {stock_code} 基本信息失败: {e}")
            return {
                'name': f'股票{stock_code}',
                'industry': '未知',
                'list_date': None
            }
    
    def get_financial_indicator(self, stock_code: str) -> Dict:
        """
        获取财务指标数据
        """
        try:
            logger.info(f"从 AkShare 获取 {stock_code} 财务指标")
            
            # 转换股票代码格式
            code = stock_code.split('.')[0]
            
            df = self.ak.stock_financial_abstract_new_ths(symbol=code)
            
            if df.empty:
                logger.warning(f"未获取到 {stock_code} 的财务指标")
                return {
                    'roe': 0.0,
                    'gross_margin': 0.0,
                    'debt_ratio': 0.0
                }
            
            # 获取最新一期的数据
            latest_report = df['report_date'].max()
            latest_df = df[df['report_date'] == latest_report]
            
            roe = 0.0
            gross_margin = 0.0
            debt_ratio = 0.0
            
            # 提取 ROE
            roe_row = latest_df[latest_df['metric_name'] == 'index_weighted_avg_roe']
            if not roe_row.empty:
                roe_value = roe_row['value'].iloc[0]
                roe = float(roe_value) if pd.notna(roe_value) else 0.0
            
            # 提取毛利率
            gross_margin_row = latest_df[latest_df['metric_name'] == 'sale_gross_margin']
            if not gross_margin_row.empty:
                gross_margin_value = gross_margin_row['value'].iloc[0]
                gross_margin = float(gross_margin_value) if pd.notna(gross_margin_value) else 0.0
            
            # 提取资产负债率
            debt_ratio_row = latest_df[latest_df['metric_name'] == 'assets_debt_ratio']
            if not debt_ratio_row.empty:
                debt_ratio_value = debt_ratio_row['value'].iloc[0]
                debt_ratio = float(debt_ratio_value) if pd.notna(debt_ratio_value) else 0.0
            
            logger.info(f"成功获取 {stock_code} 财务指标: ROE={roe:.2f}%, 毛利率={gross_margin:.2f}%, 负债率={debt_ratio:.2f}%")
            
            return {
                'roe': roe,
                'gross_margin': gross_margin,
                'debt_ratio': debt_ratio
            }
            
        except Exception as e:
            logger.error(f"获取 {stock_code} 财务指标失败: {e}")
            return {
                'roe': 0.0,
                'gross_margin': 0.0,
                'debt_ratio': 0.0
            }


def create_data_source(source_type: str = 'tushare', **kwargs) -> DataSource:
    """
    工厂函数：创建数据源实例
    
    :param source_type: 数据源类型 ('tushare' 或 'akshare')
    :param kwargs: 数据源初始化参数（如 token）
    :return: 数据源实例
    """
    if source_type == 'tushare':
        if 'token' not in kwargs:
            raise ValueError("Tushare 数据源需要 token 参数")
        return TushareDataSource(token=kwargs['token'])
    elif source_type == 'akshare':
        return AkShareDataSource()
    else:
        raise ValueError(f"不支持的数据源类型: {source_type}")

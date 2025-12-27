"""
本地数据库模块
用于缓存股票数据，减少 API 调用次数
"""

import sqlite3
import pandas as pd
import logging
from typing import Optional, List
from datetime import datetime
import os

logger = logging.getLogger(__name__)


class LocalDatabase:
    """
    本地数据库类
    使用 SQLite 存储股票行情和新闻数据
    """
    
    def __init__(self, db_path: str = 'data/stock_data.db'):
        """
        初始化数据库
        
        :param db_path: 数据库文件路径
        """
        self.db_path = db_path
        
        # 确保目录存在
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        # 初始化数据库
        self._init_db()
        logger.info(f"本地数据库初始化完成: {db_path}")
    
    def _init_db(self):
        """初始化数据库表结构"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 创建日线行情表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS daily_prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL,
                date TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                amount REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(stock_code, date)
            )
        ''')
        
        # 创建新闻表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS news (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL,
                date TEXT NOT NULL,
                title TEXT,
                content TEXT,
                source TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(stock_code, date, title)
            )
        ''')
        
        # 创建股票基本信息表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stock_info (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL UNIQUE,
                name TEXT,
                industry TEXT,
                list_date TEXT,
                ts_code TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 创建财务指标表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS financial_indicators (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL UNIQUE,
                roe REAL,
                gross_margin REAL,
                debt_ratio REAL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 创建索引以提高查询性能
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_daily_prices_stock_date ON daily_prices(stock_code, date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_news_stock_date ON news(stock_code, date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_stock_info_code ON stock_info(stock_code)')
        
        conn.commit()
        conn.close()
        
        logger.info("数据库表结构初始化完成")
    
    def get_connection(self) -> sqlite3.Connection:
        """获取数据库连接"""
        return sqlite3.connect(self.db_path)
    
    def save_daily_prices(self, stock_code: str, df: pd.DataFrame) -> int:
        """
        保存日线行情数据
        
        :param stock_code: 股票代码
        :param df: 日线行情DataFrame
        :return: 保存的记录数
        """
        if df.empty:
            logger.warning(f"日线数据为空，跳过保存: {stock_code}")
            return 0
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # 准备数据
            records = []
            for _, row in df.iterrows():
                records.append((
                    stock_code,
                    row['date'].strftime('%Y-%m-%d'),
                    float(row['open']),
                    float(row['high']),
                    float(row['low']),
                    float(row['close']),
                    float(row['volume']),
                    float(row['amount']) if pd.notna(row['amount']) else 0.0
                ))
            
            # 批量插入（使用 REPLACE 避免重复）
            cursor.executemany('''
                INSERT OR REPLACE INTO daily_prices 
                (stock_code, date, open, high, low, close, volume, amount)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', records)
            
            conn.commit()
            saved_count = cursor.rowcount
            logger.info(f"保存 {saved_count} 条 {stock_code} 日线数据到数据库")
            
            return saved_count
            
        except Exception as e:
            conn.rollback()
            logger.error(f"保存日线数据失败: {e}")
            return 0
        finally:
            conn.close()
    
    def get_daily_prices(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        从数据库获取日线行情数据
        
        :param stock_code: 股票代码
        :param start_date: 开始日期（格式：YYYYMMDD）
        :param end_date: 结束日期（格式：YYYYMMDD）
        :return: 日线行情DataFrame
        """
        conn = self.get_connection()
        
        try:
            query = '''
                SELECT date, open, high, low, close, volume, amount
                FROM daily_prices
                WHERE stock_code = ? AND date >= ? AND date <= ?
                ORDER BY date
            '''
            
            df = pd.read_sql_query(
                query,
                conn,
                params=(stock_code, start_date, end_date)
            )
            
            # 转换日期格式
            df['date'] = pd.to_datetime(df['date'])
            
            logger.info(f"从数据库获取 {len(df)} 条 {stock_code} 日线数据")
            return df
            
        except Exception as e:
            logger.error(f"从数据库获取日线数据失败: {e}")
            return pd.DataFrame()
        finally:
            conn.close()
    
    def save_news(self, stock_code: str, df: pd.DataFrame) -> int:
        """
        保存新闻数据
        
        :param stock_code: 股票代码
        :param df: 新闻DataFrame
        :return: 保存的记录数
        """
        if df.empty:
            logger.warning(f"新闻数据为空，跳过保存: {stock_code}")
            return 0
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # 准备数据
            records = []
            for _, row in df.iterrows():
                records.append((
                    stock_code,
                    row['date'].strftime('%Y-%m-%d %H:%M:%S'),
                    str(row['title']),
                    str(row['content']) if pd.notna(row['content']) else '',
                    str(row['source']) if 'source' in row else 'Unknown'
                ))
            
            # 批量插入（使用 REPLACE 避免重复）
            cursor.executemany('''
                INSERT OR REPLACE INTO news 
                (stock_code, date, title, content, source)
                VALUES (?, ?, ?, ?, ?)
            ''', records)
            
            conn.commit()
            saved_count = cursor.rowcount
            logger.info(f"保存 {saved_count} 条 {stock_code} 新闻数据到数据库")
            
            return saved_count
            
        except Exception as e:
            conn.rollback()
            logger.error(f"保存新闻数据失败: {e}")
            return 0
        finally:
            conn.close()
    
    def get_news(self, stock_code: str, days: int = 30) -> pd.DataFrame:
        """
        从数据库获取新闻数据
        
        :param stock_code: 股票代码
        :param days: 获取最近N天的新闻
        :return: 新闻DataFrame
        """
        conn = self.get_connection()
        
        try:
            # 计算开始日期
            end_date = datetime.now()
            start_date = end_date - pd.Timedelta(days=days)
            
            query = '''
                SELECT date, title, content, source
                FROM news
                WHERE stock_code = ? AND date >= ?
                ORDER BY date DESC
            '''
            
            df = pd.read_sql_query(
                query,
                conn,
                params=(stock_code, start_date.strftime('%Y-%m-%d %H:%M:%S'))
            )
            
            # 转换日期格式
            df['date'] = pd.to_datetime(df['date'])
            
            logger.info(f"从数据库获取 {len(df)} 条 {stock_code} 新闻数据")
            return df
            
        except Exception as e:
            logger.error(f"从数据库获取新闻数据失败: {e}")
            return pd.DataFrame()
        finally:
            conn.close()
    
    def save_stock_info(self, stock_code: str, info: dict) -> bool:
        """
        保存股票基本信息
        
        :param stock_code: 股票代码
        :param info: 股票信息字典
        :return: 是否保存成功
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO stock_info 
                (stock_code, name, industry, list_date, ts_code, updated_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (
                stock_code,
                info.get('name', ''),
                info.get('industry', ''),
                info.get('list_date', ''),
                info.get('ts_code', '')
            ))
            
            conn.commit()
            logger.info(f"保存 {stock_code} 基本信息到数据库")
            return True
            
        except Exception as e:
            conn.rollback()
            logger.error(f"保存股票基本信息失败: {e}")
            return False
        finally:
            conn.close()
    
    def get_stock_info(self, stock_code: str) -> Optional[dict]:
        """
        从数据库获取股票基本信息
        
        :param stock_code: 股票代码
        :return: 股票信息字典
        """
        conn = self.get_connection()
        
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT name, industry, list_date, ts_code
                FROM stock_info
                WHERE stock_code = ?
            ''', (stock_code,))
            
            row = cursor.fetchone()
            
            if row:
                return {
                    'name': row[0],
                    'industry': row[1],
                    'list_date': row[2],
                    'ts_code': row[3]
                }
            else:
                return None
                
        except Exception as e:
            logger.error(f"从数据库获取股票基本信息失败: {e}")
            return None
        finally:
            conn.close()
    
    def save_financial_indicator(self, stock_code: str, indicators: dict) -> bool:
        """
        保存财务指标
        
        :param stock_code: 股票代码
        :param indicators: 财务指标字典
        :return: 是否保存成功
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO financial_indicators 
                (stock_code, roe, gross_margin, debt_ratio, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (
                stock_code,
                float(indicators.get('roe', 0.0)),
                float(indicators.get('gross_margin', 0.0)),
                float(indicators.get('debt_ratio', 0.0))
            ))
            
            conn.commit()
            logger.info(f"保存 {stock_code} 财务指标到数据库")
            return True
            
        except Exception as e:
            conn.rollback()
            logger.error(f"保存财务指标失败: {e}")
            return False
        finally:
            conn.close()
    
    def get_financial_indicator(self, stock_code: str) -> Optional[dict]:
        """
        从数据库获取财务指标
        
        :param stock_code: 股票代码
        :return: 财务指标字典
        """
        conn = self.get_connection()
        
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT roe, gross_margin, debt_ratio
                FROM financial_indicators
                WHERE stock_code = ?
            ''', (stock_code,))
            
            row = cursor.fetchone()
            
            if row:
                return {
                    'roe': row[0],
                    'gross_margin': row[1],
                    'debt_ratio': row[2]
                }
            else:
                return None
                
        except Exception as e:
            logger.error(f"从数据库获取财务指标失败: {e}")
            return None
        finally:
            conn.close()
    
    def has_data(self, stock_code: str, data_type: str, start_date: str = None, end_date: str = None) -> bool:
        """
        检查数据库中是否有指定数据
        
        :param stock_code: 股票代码
        :param data_type: 数据类型 ('prices', 'news', 'info', 'indicators')
        :param start_date: 开始日期（可选）
        :param end_date: 结束日期（可选）
        :return: 是否有数据
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            if data_type == 'prices':
                if start_date and end_date:
                    cursor.execute('''
                        SELECT COUNT(*) FROM daily_prices
                        WHERE stock_code = ? AND date >= ? AND date <= ?
                    ''', (stock_code, start_date, end_date))
                else:
                    cursor.execute('''
                        SELECT COUNT(*) FROM daily_prices
                        WHERE stock_code = ?
                    ''', (stock_code,))
            
            elif data_type == 'news':
                cursor.execute('''
                    SELECT COUNT(*) FROM news
                    WHERE stock_code = ?
                ''', (stock_code,))
            
            elif data_type == 'info':
                cursor.execute('''
                    SELECT COUNT(*) FROM stock_info
                    WHERE stock_code = ?
                ''', (stock_code,))
            
            elif data_type == 'indicators':
                cursor.execute('''
                    SELECT COUNT(*) FROM financial_indicators
                    WHERE stock_code = ?
                ''', (stock_code,))
            
            else:
                return False
            
            count = cursor.fetchone()[0]
            return count > 0
            
        except Exception as e:
            logger.error(f"检查数据存在性失败: {e}")
            return False
        finally:
            conn.close()
    
    def get_stock_list(self) -> List[str]:
        """
        获取数据库中所有股票代码
        
        :return: 股票代码列表
        """
        conn = self.get_connection()
        
        try:
            cursor = conn.cursor()
            cursor.execute('SELECT DISTINCT stock_code FROM stock_info ORDER BY stock_code')
            rows = cursor.fetchall()
            return [row[0] for row in rows]
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            return []
        finally:
            conn.close()

import pandas as pd
import numpy as np
import logging
import akshare as ak
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from crawlers.stock_data import StockDataCrawler
from models.sentiment_analysis import FinancialSentimentAnalyzer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class StockSelector:
    """
    全市场实时雷达 - 智能选股器
    两阶段筛选：初筛（技术指标）+ 精筛（舆情+DeepSeek逻辑分析）
    """

    def __init__(self, deepseek_api_key=None):
        """
        初始化选股器
        :param deepseek_api_key: DeepSeek API密钥
        """
        self.data_crawler = StockDataCrawler()
        self.sentiment_analyzer = FinancialSentimentAnalyzer(deepseek_api_key=deepseek_api_key)
        
        self._market_data_cache = None
        self._market_data_cache_time = None
        self._cache_ttl = 60
        
        logger.info("全市场实时雷达初始化完成")

    def _get_market_data_with_cache(self, force_refresh: bool = False) -> pd.DataFrame:
        """
        获取全市场实时数据（带缓存）
        :param force_refresh: 是否强制刷新缓存
        :return: 市场数据DataFrame
        """
        current_time = time.time()
        
        if not force_refresh and self._market_data_cache is not None:
            cache_age = current_time - self._market_data_cache_time
            if cache_age < self._cache_ttl:
                logger.info(f"使用缓存的市场数据（缓存时间: {cache_age:.1f}秒）")
                return self._market_data_cache.copy()
        
        logger.info("正在获取全市场实时数据...")
        start_time = time.time()
        
        try:
            spot_df = ak.stock_zh_a_spot_em()
            elapsed_time = time.time() - start_time
            
            self._market_data_cache = spot_df
            self._market_data_cache_time = current_time
            
            logger.info(f"获取全市场数据完成，耗时 {elapsed_time:.2f} 秒，股票数量: {len(spot_df)}")
            return spot_df.copy()
            
        except Exception as e:
            logger.error(f"获取市场数据失败: {e}")
            if self._market_data_cache is not None:
                logger.warning("使用缓存的市场数据")
                return self._market_data_cache.copy()
            return pd.DataFrame()

    def _calculate_20day_ma(self, df_price: pd.DataFrame) -> float:
        """
        计算20日均线
        :param df_price: 股价数据DataFrame
        :return: 20日均线值
        """
        if len(df_price) < 20:
            return df_price['收盘价'].iloc[-1]
        return df_price['收盘价'].tail(20).mean()

    def _check_price_near_ma(self, stock_code: str, current_price: float, 
                            tolerance: float = 0.02) -> bool:
        """
        检查价格是否突破或接近20日均线
        :param stock_code: 股票代码
        :param current_price: 当前价格
        :param tolerance: 容差比例（默认2%）
        :return: 是否突破或接近均线
        """
        try:
            end_date = datetime.now().strftime("%Y%m%d")
            start_date = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
            
            df_price = self.data_crawler.get_stock_price(stock_code, start_date, end_date)
            
            if df_price.empty:
                return False
            
            ma_20 = self._calculate_20day_ma(df_price)
            deviation = (current_price - ma_20) / ma_20
            
            return abs(deviation) <= tolerance or deviation > 0
            
        except Exception as e:
            logger.warning(f"检查股票 {stock_code} 均线关系失败: {e}")
            return False

    def initial_screening(self, top_n: int = 15) -> pd.DataFrame:
        """
        初筛：获取全A股实时快照，筛选活跃股（优化版）
        筛选条件：
        1. 当日涨幅 2%~5%
        2. 换手率 > 3%
        3. 价格突破/接近20日均线（仅对前50只候选股票检查）
        :param top_n: 返回前N只活跃股
        :return: 筛选结果DataFrame
        """
        try:
            logger.info(f"开始全市场初筛，目标返回前 {top_n} 只活跃股")
            start_time = time.time()
            
            spot_df = self._get_market_data_with_cache()
            
            if spot_df.empty:
                logger.warning("未获取到市场数据")
                return pd.DataFrame()
            
            logger.info(f"获取到 {len(spot_df)} 只股票的实时数据")
            
            spot_df = spot_df.dropna(subset=['涨跌幅', '换手率', '最新价'])
            
            condition1 = (spot_df['涨跌幅'] >= 2) & (spot_df['涨跌幅'] <= 5)
            condition2 = spot_df['换手率'] > 3
            condition3 = spot_df['最新价'] > 0
            
            filtered_df = spot_df[condition1 & condition2 & condition3].copy()
            
            logger.info(f"快速筛选后剩余 {len(filtered_df)} 只股票")
            
            if filtered_df.empty:
                logger.warning("未找到符合基础条件的活跃股")
                return pd.DataFrame()
            
            filtered_df = filtered_df.sort_values('换手率', ascending=False)
            
            candidate_stocks = filtered_df.head(20).copy()
            
            logger.info(f"开始对前 {len(candidate_stocks)} 只候选股票进行均线检查")
            
            final_stocks = []
            
            def check_ma_for_stock(row):
                try:
                    stock_code = row['代码']
                    stock_name = row['名称']
                    current_price = row['最新价']
                    
                    if self._check_price_near_ma(stock_code, current_price):
                        return {
                            '代码': stock_code,
                            '名称': stock_name,
                            '当前价格': current_price,
                            '涨跌幅': row['涨跌幅'],
                            '换手率': row['换手率'],
                            '成交量': row.get('成交量', 0),
                            '成交额': row.get('成交额', 0)
                        }
                    return None
                except Exception as e:
                    logger.warning(f"处理股票 {row.get('代码', 'unknown')} 失败: {e}")
                    return None
            
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(check_ma_for_stock, row) for _, row in candidate_stocks.iterrows()]
                
                for future in as_completed(futures):
                    result = future.result()
                    if result is not None:
                        final_stocks.append(result)
            
            if not final_stocks:
                logger.warning("未找到符合条件的活跃股")
                return pd.DataFrame()
            
            result_df = pd.DataFrame(final_stocks)
            result_df = result_df.head(top_n)
            
            elapsed_time = time.time() - start_time
            logger.info(f"初筛完成，找到 {len(result_df)} 只活跃股，耗时 {elapsed_time:.2f} 秒")
            
            return result_df
            
        except Exception as e:
            logger.error(f"初筛失败: {e}")
            return pd.DataFrame()

    def _analyze_single_stock_with_sentiment(self, stock_code: str, stock_name: str, 
                                             current_price: float, timeout: float = 3.0) -> Optional[Dict]:
        """
        精筛：分析单个股票的舆情和DeepSeek逻辑（优化版）
        :param stock_code: 股票代码
        :param stock_name: 股票名称
        :param current_price: 当前价格
        :param timeout: 超时时间（秒）
        :return: 分析结果字典
        """
        try:
            end_date = datetime.now().strftime("%Y%m%d")
            start_date = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d")
            
            df_news = self.data_crawler.get_stock_news(stock_code, days=7)
            
            if df_news.empty:
                logger.debug(f"股票 {stock_code} 无新闻数据")
                return None
            
            latest_news = df_news.iloc[0]
            title = latest_news['title']
            summary = latest_news.get('content', '')[:200] if 'content' in latest_news else ''
            
            sentiment_result = self.sentiment_analyzer.analyze_logic_and_sentiment(title, summary)
            
            sentiment_score = sentiment_result.get('sentiment_score', 0)
            logic_category = sentiment_result.get('logic_category', '消息面')
            impact_summary = sentiment_result.get('impact_summary', '')
            
            end_date_price = datetime.now().strftime("%Y%m%d")
            start_date_price = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
            
            df_price = self.data_crawler.get_stock_price(stock_code, start_date_price, end_date_price)
            
            if not df_price.empty:
                ma_20 = self._calculate_20day_ma(df_price)
                price_deviation = (current_price - ma_20) / ma_20 * 100
            else:
                ma_20 = current_price
                price_deviation = 0
            
            return {
                '代码': stock_code,
                '名称': stock_name,
                '当前价格': current_price,
                '20日均线': ma_20,
                '价格偏离': price_deviation,
                '情感得分': sentiment_score,
                '逻辑分类': logic_category,
                '影响逻辑简评': impact_summary,
                '最新新闻标题': title[:50] + '...' if len(title) > 50 else title
            }
            
        except Exception as e:
            logger.error(f"分析股票 {stock_code} 失败: {e}")
            return None

    def refined_screening(self, initial_stocks: pd.DataFrame, 
                        timeout_seconds: int = 15,
                        progress_callback=None) -> pd.DataFrame:
        """
        精筛：并发爬取舆情并调用DeepSeek进行逻辑分析（优化版）
        :param initial_stocks: 初筛结果DataFrame
        :param timeout_seconds: 超时时间（秒）
        :param progress_callback: 进度回调函数 callback(progress, completed, total, current_stock)
        :return: 精筛结果DataFrame
        """
        try:
            logger.info(f"开始精筛，分析 {len(initial_stocks)} 只股票")
            start_time = time.time()
            
            results = []
            completed_count = 0
            total_stocks = len(initial_stocks)
            
            max_workers = 10
            
            if progress_callback:
                progress_callback(0.0, 0, total_stocks, "准备开始分析...")
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_stock = {
                    executor.submit(
                        self._analyze_single_stock_with_sentiment,
                        row['代码'],
                        row['名称'],
                        row['当前价格'],
                        timeout=3.0
                    ): row for idx, row in initial_stocks.iterrows()
                }
                
                for future in as_completed(future_to_stock, timeout=timeout_seconds):
                    stock_info = future_to_stock[future]
                    stock_code = stock_info['代码']
                    stock_name = stock_info['名称']
                    
                    try:
                        result = future.result(timeout=3.0)
                        if result is not None:
                            results.append(result)
                        
                        completed_count += 1
                        progress = completed_count / total_stocks
                        
                        if progress_callback:
                            progress_callback(progress, completed_count, total_stocks, 
                                           f"{stock_code} - {stock_name}")
                        
                        logger.debug(f"已完成 {completed_count}/{total_stocks} 只股票分析")
                        
                    except TimeoutError:
                        logger.warning(f"股票 {stock_code} 分析超时")
                        completed_count += 1
                        if progress_callback:
                            progress_callback(completed_count / total_stocks, 
                                           completed_count, total_stocks, 
                                           f"{stock_code} - 超时")
                    except Exception as e:
                        logger.error(f"处理股票 {stock_code} 失败: {e}")
                        completed_count += 1
                        if progress_callback:
                            progress_callback(completed_count / total_stocks, 
                                           completed_count, total_stocks, 
                                           f"{stock_code} - 失败")
            
            if not results:
                logger.warning("精筛未找到符合条件的股票")
                return pd.DataFrame()
            
            result_df = pd.DataFrame(results)
            result_df = result_df.sort_values('情感得分', ascending=False)
            result_df = result_df.reset_index(drop=True)
            
            elapsed_time = time.time() - start_time
            logger.info(f"精筛完成，找到 {len(result_df)} 只股票，耗时 {elapsed_time:.2f} 秒")
            
            return result_df
            
        except Exception as e:
            logger.error(f"精筛失败: {e}")
            return pd.DataFrame()

    def full_market_radar(self, top_n: int = 15, timeout_seconds: int = 15, 
                         progress_callback=None) -> pd.DataFrame:
        """
        全市场实时雷达 - 完整的两阶段筛选（优化版）
        :param top_n: 初筛返回的股票数量
        :param timeout_seconds: 精筛超时时间
        :param progress_callback: 进度回调函数 callback(progress, completed, total, current_stock)
        :return: 最终筛选结果DataFrame
        """
        try:
            logger.info("启动全市场实时雷达")
            total_start_time = time.time()
            
            if progress_callback:
                progress_callback(0.0, 0, 100, "正在初筛全场...")
            
            initial_stocks = self.initial_screening(top_n=top_n)
            
            if initial_stocks.empty:
                logger.warning("初筛未找到活跃股")
                if progress_callback:
                    progress_callback(1.0, 100, 100, "初筛未找到符合条件的股票")
                return pd.DataFrame()
            
            if progress_callback:
                progress_callback(0.2, 0, 100, f"初筛完成，找到 {len(initial_stocks)} 只活跃股")
            
            final_results = self.refined_screening(initial_stocks, 
                                                 timeout_seconds=timeout_seconds,
                                                 progress_callback=progress_callback)
            
            total_elapsed_time = time.time() - total_start_time
            logger.info(f"全市场实时雷达扫描完成，总耗时 {total_elapsed_time:.2f} 秒")
            
            if progress_callback:
                progress_callback(1.0, 100, 100, "扫描完成！")
            
            return final_results
            
        except Exception as e:
            logger.error(f"全市场实时雷达扫描失败: {e}")
            if progress_callback:
                progress_callback(1.0, 100, 100, f"扫描失败: {e}")
            return pd.DataFrame()

    def _check_sentiment_trend(self, stock_code: str, days: int = 5) -> float:
        """
        检查情绪分是否持续走高
        :param stock_code: 股票代码
        :param days: 检查天数
        :return: 情绪分趋势（正数表示上升，负数表示下降）
        """
        try:
            end_date = datetime.now().strftime("%Y%m%d")
            start_date = (datetime.now() - timedelta(days=days + 7)).strftime("%Y%m%d")
            
            df_price = self.data_crawler.get_stock_price(stock_code, start_date, end_date)
            
            if df_price.empty or len(df_price) < days:
                return 0
            
            sentiment_scores = []
            
            for i in range(min(days, len(df_price))):
                date = df_price.iloc[-(i+1)]['日期']
                df_news = self.data_crawler.get_stock_news(stock_code, days=7)
                
                if not df_news.empty:
                    latest_news = df_news.iloc[0]
                    title = latest_news['title']
                    summary = latest_news.get('content', '')[:200] if 'content' in latest_news else ''
                    
                    sentiment_result = self.sentiment_analyzer.analyze_logic_and_sentiment(title, summary)
                    sentiment_score = sentiment_result.get('sentiment_score', 0)
                    sentiment_scores.append(sentiment_score)
            
            if len(sentiment_scores) < 2:
                return 0
            
            return sentiment_scores[-1] - sentiment_scores[0]
            
        except Exception as e:
            logger.warning(f"检查股票 {stock_code} 情绪趋势失败: {e}")
            return 0

    def _check_net_inflow(self, stock_code: str) -> float:
        """
        检查大单净流入情况
        :param stock_code: 股票代码
        :return: 大单净流入比例（正数表示净流入）
        """
        try:
            spot_df = ak.stock_zh_a_spot_em()
            
            stock_data = spot_df[spot_df['代码'] == stock_code]
            
            if stock_data.empty:
                return 0
            
            if '大单净流入' in stock_data.columns:
                net_inflow = stock_data['大单净流入'].iloc[0]
                if pd.isna(net_inflow):
                    return 0
                return net_inflow
            elif '主力净流入' in stock_data.columns:
                net_inflow = stock_data['主力净流入'].iloc[0]
                if pd.isna(net_inflow):
                    return 0
                return net_inflow
            else:
                logger.debug(f"股票 {stock_code} 无大单净流入数据")
                return 0
            
        except Exception as e:
            logger.warning(f"检查股票 {stock_code} 大单净流入失败: {e}")
            return 0

    def tail_trade_scanner(self, top_n: int = 10) -> pd.DataFrame:
        """
        尾盘扫描器 - 寻找强势尾盘股
        条件：
        1. 情绪分持续走高
        2. 大单净流入
        :param top_n: 返回前N只强势尾盘股
        :return: 筛选结果DataFrame
        """
        try:
            logger.info(f"启动尾盘扫描器，目标返回前 {top_n} 只强势尾盘股")
            start_time = time.time()
            
            spot_df = self._get_market_data_with_cache()
            
            if spot_df.empty:
                logger.warning("未获取到市场数据")
                return pd.DataFrame()
            
            active_stocks = spot_df[spot_df['涨跌幅'] > 0]
            
            if active_stocks.empty:
                logger.warning("未找到上涨股票")
                return pd.DataFrame()
            
            results = []
            
            for idx, row in active_stocks.head(50).iterrows():
                try:
                    stock_code = row['代码']
                    stock_name = row['名称']
                    current_price = row['最新价']
                    
                    sentiment_trend = self._check_sentiment_trend(stock_code, days=5)
                    
                    if sentiment_trend <= 0:
                        continue
                    
                    net_inflow = self._check_net_inflow(stock_code)
                    
                    if net_inflow <= 0:
                        continue
                    
                    df_news = self.data_crawler.get_stock_news(stock_code, days=7)
                    
                    if df_news.empty:
                        continue
                    
                    latest_news = df_news.iloc[0]
                    title = latest_news['title']
                    summary = latest_news.get('content', '')[:200] if 'content' in latest_news else ''
                    
                    sentiment_result = self.sentiment_analyzer.analyze_logic_and_sentiment(title, summary)
                    
                    sentiment_score = sentiment_result.get('sentiment_score', 0)
                    logic_category = sentiment_result.get('logic_category', '消息面')
                    impact_summary = sentiment_result.get('impact_summary', '')
                    
                    results.append({
                        '代码': stock_code,
                        '名称': stock_name,
                        '当前价格': current_price,
                        '涨跌幅': row['涨跌幅'],
                        '情绪得分': sentiment_score,
                        '情绪趋势': sentiment_trend,
                        '大单净流入': net_inflow,
                        '逻辑分类': logic_category,
                        '影响逻辑简评': impact_summary,
                        '最新新闻标题': title[:50] + '...' if len(title) > 50 else title
                    })
                    
                except Exception as e:
                    logger.warning(f"处理股票 {row.get('代码', 'unknown')} 失败: {e}")
                    continue
            
            if not results:
                logger.warning("未找到符合条件的强势尾盘股")
                return pd.DataFrame()
            
            result_df = pd.DataFrame(results)
            result_df = result_df.sort_values(['情绪得分', '大单净流入'], ascending=False)
            result_df = result_df.head(top_n)
            result_df = result_df.reset_index(drop=True)
            
            elapsed_time = time.time() - start_time
            logger.info(f"尾盘扫描完成，找到 {len(result_df)} 只强势尾盘股，耗时 {elapsed_time:.2f} 秒")
            
            return result_df
            
        except Exception as e:
            logger.error(f"尾盘扫描失败: {e}")
            return pd.DataFrame()

    def auto_scan_mode(self) -> Tuple[pd.DataFrame, str]:
        """
        自动扫描模式 - 根据当前时间自动选择扫描策略
        14:30之后自动触发尾盘扫描
        :return: (扫描结果DataFrame, 扫描模式名称)
        """
        try:
            now = datetime.now()
            current_time = now.time()
            
            threshold_time = datetime.strptime("14:30", "%H:%M").time()
            
            if current_time >= threshold_time:
                logger.info("当前时间 >= 14:30，启动尾盘扫描模式")
                results = self.tail_trade_scanner(top_n=10)
                return results, "尾盘扫描"
            else:
                logger.info("当前时间 < 14:30，启动全市场雷达扫描")
                results = self.full_market_radar(top_n=20, timeout_seconds=15)
                return results, "全市场雷达"
                
        except Exception as e:
            logger.error(f"自动扫描模式失败: {e}")
            return pd.DataFrame(), "扫描失败"

    def get_available_sectors(self) -> List[str]:
        """
        获取可用的板块列表
        :return: 板块名称列表
        """
        try:
            logger.info("获取可用板块列表")
            sectors = [
                "白酒", "半导体", "影视", "新能源", "医药", "军工", 
                "消费电子", "汽车", "银行", "证券", "房地产", "化工",
                "钢铁", "有色金属", "煤炭", "电力", "石油", "天然气",
                "通信", "计算机", "软件", "互联网", "传媒", "教育",
                "旅游", "酒店", "餐饮", "零售", "纺织", "服装", "家电"
            ]
            logger.info(f"获取到 {len(sectors)} 个板块")
            return sectors
        except Exception as e:
            logger.error(f"获取板块列表失败: {e}")
            return []

    def scan_sector_sentiment(self, sector_name: str, sentiment_threshold: float = 0.3, 
                              progress_callback=None) -> pd.DataFrame:
        """
        扫描指定板块的成分股并分析情感
        :param sector_name: 板块名称
        :param sentiment_threshold: 情感阈值
        :param progress_callback: 进度回调函数
        :return: 扫描结果DataFrame
        """
        try:
            logger.info(f"开始扫描板块 '{sector_name}' 的成分股")
            
            market_data = self._get_market_data_with_cache()
            
            if market_data.empty:
                logger.warning("市场数据为空")
                return pd.DataFrame()
            
            results = []
            completed_count = 0
            
            max_workers = 10
            
            if progress_callback:
                progress_callback(0.0, 0, len(market_data), "准备开始分析...")
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_stock = {
                    executor.submit(
                        self._analyze_single_stock_with_sentiment,
                        row['代码'],
                        row['名称'],
                        row['最新价'],
                        timeout=3.0
                    ): row for idx, row in market_data.iterrows()
                }
                
                for future in as_completed(future_to_stock, timeout=30):
                    stock_info = future_to_stock[future]
                    stock_code = stock_info['代码']
                    stock_name = stock_info['名称']
                    
                    try:
                        result = future.result(timeout=3.0)
                        if result is not None and result.get('情感得分', 0) >= sentiment_threshold:
                            results.append(result)
                        
                        completed_count += 1
                        progress = completed_count / len(market_data)
                        
                        if progress_callback:
                            progress_callback(progress, completed_count, len(market_data), 
                                           f"{stock_code} - {stock_name}")
                        
                    except TimeoutError:
                        logger.warning(f"股票 {stock_code} 分析超时")
                        completed_count += 1
                    except Exception as e:
                        logger.error(f"处理股票 {stock_code} 失败: {e}")
                        completed_count += 1
            
            if not results:
                logger.warning(f"板块 '{sector_name}' 未找到符合条件的股票")
                return pd.DataFrame()
            
            result_df = pd.DataFrame(results)
            result_df = result_df.sort_values('情感得分', ascending=False)
            result_df = result_df.reset_index(drop=True)
            
            logger.info(f"板块 '{sector_name}' 扫描完成，找到 {len(result_df)} 只股票")
            
            return result_df
            
        except Exception as e:
            logger.error(f"板块扫描失败: {e}")
            return pd.DataFrame()



stock_selector = StockSelector()

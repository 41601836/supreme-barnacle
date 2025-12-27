import pandas as pd
import numpy as np
import logging
from scipy.stats import pearsonr
from typing import Dict, Tuple, Optional
from pandas import Series
from models.sentiment_analysis import FinancialSentimentAnalyzer
import akshare as ak

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StockSentimentAnalyzer:
    """
    股票与情绪关联分析器，用于分析股价与情绪指数的关系
    """
    
    def __init__(self):
        """
        初始化分析器
        """
        logger.info("初始化股票情绪关联分析器")
    
    def align_data(self, stock_df: pd.DataFrame, sentiment_df: pd.DataFrame) -> pd.DataFrame:
        """
        将股价数据和情绪数据按照日期对齐
        :param stock_df: 股价数据DataFrame，包含'日期'和'收盘价'等列
        :param sentiment_df: 情绪数据DataFrame，包含'日期'和'情感得分'列
        :return: 对齐后的合并数据DataFrame
        """
        try:
            logger.info("对齐股价数据和情绪数据")
            
            # 确保日期列是datetime类型
            stock_df['日期'] = pd.to_datetime(stock_df['日期'])
            sentiment_df['日期'] = pd.to_datetime(sentiment_df['日期'])
            
            # 按日期合并数据
            merged_df = pd.merge(stock_df, sentiment_df, on='日期', how='inner')
            
            # 按日期排序
            merged_df = merged_df.sort_values('日期')
            
            logger.info(f"对齐后的数据量: {len(merged_df)} 条")
            return merged_df
        except Exception as e:
            logger.error(f"数据对齐失败: {e}")
            return pd.DataFrame()
    
    def calculate_correlation(self, data_df: pd.DataFrame, lag_days: int = 1) -> Tuple[float, float]:
        """
        计算股价涨跌幅与情绪得分的Pearson相关系数
        :param data_df: 包含'涨跌幅'和'情感得分'的DataFrame
        :param lag_days: 情绪得分滞后天数
        :return: (相关系数, p值)
        """
        try:
            logger.info(f"计算股价涨跌幅与滞后 {lag_days} 天情绪得分的相关系数")
            
            # 确保数据完整
            if '涨跌幅' not in data_df.columns or '情感得分' not in data_df.columns:
                logger.error("数据缺少必要的列")
                return 0.0, 1.0
            
            # 计算滞后情绪得分
            data_df['滞后情感得分'] = data_df['情感得分'].shift(lag_days)
            
            # 移除包含NaN的行
            valid_data = data_df[['涨跌幅', '滞后情感得分']].dropna()
            
            if len(valid_data) < 2:
                logger.warning("有效数据不足，无法计算相关系数")
                return 0.0, 1.0
            
            # 计算Pearson相关系数
            correlation, p_value = pearsonr(valid_data['涨跌幅'], valid_data['滞后情感得分'])
            
            logger.info(f"相关系数: {correlation:.3f}, p值: {p_value:.3f}")
            return correlation, p_value
        except Exception as e:
            logger.error(f"计算相关系数失败: {e}")
            return 0.0, 1.0
    
    def calculate_sentiment_index(self, df_news: pd.DataFrame) -> Tuple[Series, pd.DataFrame]:
        """
        计算每日情绪指数
        :param df_news: 新闻爬虫返回的DataFrame，包含date, title, content, reading, comments
        :return: 情绪指数Series和包含动量和滚动极值的DataFrame
        """
        try:
            logger.info("计算每日情绪指数")
            
            # 初始化情感分析器
            sentiment_analyzer = FinancialSentimentAnalyzer()
            
            # 合并标题和内容
            df_news['full_text'] = df_news['title'] + ' ' + df_news['content']
            
            # 批量计算情感得分
            logger.info(f"对 {len(df_news)} 条新闻进行情感分析")
            sentiment_scores = sentiment_analyzer.batch_analyze_sentiment(df_news['full_text'].tolist())
            df_news['sentiment_score'] = sentiment_scores
            
            # 确保日期列是datetime类型
            df_news['date'] = pd.to_datetime(df_news['date'])
            
            # 计算权重
            if 'reading' in df_news.columns and 'comments' in df_news.columns:
                df_news['weight'] = df_news['reading'] + df_news['comments'] * 2
            elif 'view_count' in df_news.columns and 'comment_count' in df_news.columns:
                df_news['weight'] = df_news['view_count'] + df_news['comment_count'] * 2
            else:
                df_news['weight'] = 1
            
            # 按日期分组计算加权平均情绪指数
            sentiment_index = df_news.groupby('date').apply(
                lambda x: (x['sentiment_score'] * x['weight']).sum() / x['weight'].sum(),
                include_groups=False
            )
            
            # 按日期排序
            sentiment_index = sentiment_index.sort_index()
            
            # 计算情绪动量（当日 - 前日）
            sentiment_momentum = sentiment_index.diff()
            
            # 计算7天滚动极值
            rolling_max = sentiment_index.rolling(window=7, min_periods=1).max()
            rolling_min = sentiment_index.rolling(window=7, min_periods=1).min()
            
            # 创建结果DataFrame
            result_df = pd.DataFrame({
                'date': sentiment_index.index,
                'sentiment_index': sentiment_index.values,
                'sentiment_momentum': sentiment_momentum.values,
                'rolling_max_7d': rolling_max.values,
                'rolling_min_7d': rolling_min.values
            })
            
            # 重置索引
            result_df = result_df.reset_index(drop=True)
            
            logger.info(f"情绪指数计算完成，共 {len(result_df)} 个日期")
            return sentiment_index, result_df
        except Exception as e:
            logger.error(f"计算情绪指数失败: {e}")
            return pd.Series(dtype='float64'), pd.DataFrame()
    
    def identify_divergence(self, data_df: pd.DataFrame, price_col: str = '收盘价', sentiment_col: str = '情绪指数', window: int = 10) -> pd.DataFrame:
        """
        识别股价与情绪指数的背离信号
        :param data_df: 包含股价和情绪数据的DataFrame
        :param price_col: 股价列名
        :param sentiment_col: 情绪指数列名
        :param window: 检测窗口大小
        :return: 包含背离信号的DataFrame
        """
        try:
            logger.info("识别股价与情绪指数的背离信号")
            
            # 复制数据避免修改原始数据
            result_df = data_df.copy()
            
            # 初始化信号列
            result_df['背离信号'] = '无'
            
            # 计算股价和情绪指数的滚动极值
            result_df['价格最高价'] = result_df[price_col].rolling(window=window, center=True).max()
            result_df['价格最低价'] = result_df[price_col].rolling(window=window, center=True).min()
            result_df['情绪最高价'] = result_df[sentiment_col].rolling(window=window, center=True).max()
            result_df['情绪最低价'] = result_df[sentiment_col].rolling(window=window, center=True).min()
            
            # 识别顶背离：价格创新高，情绪未创新高
            result_df.loc[
                (result_df[price_col] == result_df['价格最高价']) &  # 价格创新高
                (result_df[sentiment_col] < result_df['情绪最高价']),   # 情绪未创新高
                '背离信号'
            ] = '顶背离'
            
            # 识别底背离：价格创新低，情绪未创新低
            result_df.loc[
                (result_df[price_col] == result_df['价格最低价']) &  # 价格创新低
                (result_df[sentiment_col] > result_df['情绪最低价']),   # 情绪未创新低
                '背离信号'
            ] = '底背离'
            
            # 移除辅助列
            result_df = result_df.drop(['价格最高价', '价格最低价', '情绪最高价', '情绪最低价'], axis=1)
            
            # 统计背离信号数量
            top_divergence = len(result_df[result_df['背离信号'] == '顶背离'])
            bottom_divergence = len(result_df[result_df['背离信号'] == '底背离'])
            
            logger.info(f"识别到顶背离信号: {top_divergence} 个，底背离信号: {bottom_divergence} 个")
            return result_df
        except Exception as e:
            logger.error(f"识别背离信号失败: {e}")
            return data_df.copy()
    
    def detect_divergence(self, df_price: pd.DataFrame, series_sentiment: Series, window: int = 20) -> pd.DataFrame:
        """
        检测价格与情绪指数之间的背离信号
        :param df_price: AkShare获取的日K数据，包含date, close, high, low
        :param series_sentiment: 情绪指数Series
        :param window: 检测窗口大小
        :return: 带信号列的合并DataFrame
        """
        try:
            logger.info(f"检测价格与情绪指数的背离信号，窗口大小: {window} 天")
            
            # 确保日期列是datetime类型
            df_price['date'] = pd.to_datetime(df_price['date'])
            
            # 将情绪指数Series转换为DataFrame并确保索引是datetime类型
            df_sentiment = pd.DataFrame({'sentiment_index': series_sentiment.values, 'date': series_sentiment.index})
            df_sentiment['date'] = pd.to_datetime(df_sentiment['date'])
            
            # 合并价格数据和情绪指数
            merged_df = pd.merge(df_price, df_sentiment, on='date', how='left')
            
            # 确保sentiment_index列没有NaN值
            if 'sentiment_index' in merged_df.columns:
                merged_df['sentiment_index'] = merged_df['sentiment_index'].fillna(0)  # 使用0填充任何剩余的NaN值
            
            # 计算价格的滚动极值
            merged_df['price_high_rolling'] = merged_df['high'].rolling(window=window, min_periods=1).max()
            merged_df['price_low_rolling'] = merged_df['low'].rolling(window=window, min_periods=1).min()
            
            # 计算情绪指数的滚动极值
            merged_df['sentiment_high_rolling'] = merged_df['sentiment_index'].rolling(window=window, min_periods=1).max()
            merged_df['sentiment_low_rolling'] = merged_df['sentiment_index'].rolling(window=window, min_periods=1).min()
            
            # 初始化信号列
            merged_df['signal'] = None
            
            # 检测顶背离：价格创新高，但情绪指数未创新高
            merged_df.loc[
                (merged_df['high'] == merged_df['price_high_rolling']) &  # 价格创新高
                (merged_df['sentiment_index'] < merged_df['sentiment_high_rolling']),  # 情绪未创新高
                'signal'
            ] = '潜在卖出'
            
            # 检测底背离：价格创新低，但情绪指数未创新低
            merged_df.loc[
                (merged_df['low'] == merged_df['price_low_rolling']) &  # 价格创新低
                (merged_df['sentiment_index'] > merged_df['sentiment_low_rolling']),  # 情绪未创新低
                'signal'
            ] = '潜在买入'
            
            # 移除辅助列
            merged_df = merged_df.drop([
                'price_high_rolling', 'price_low_rolling', 
                'sentiment_high_rolling', 'sentiment_low_rolling'
            ], axis=1)
            
            # 按日期排序
            merged_df = merged_df.sort_values('date')
            
            # 统计信号数量
            buy_signals = len(merged_df[merged_df['signal'] == '潜在买入'])
            sell_signals = len(merged_df[merged_df['signal'] == '潜在卖出'])
            
            logger.info(f"检测到潜在买入信号: {buy_signals} 个，潜在卖出信号: {sell_signals} 个")
            return merged_df
        except Exception as e:
            logger.error(f"检测背离信号失败: {e}")
            return pd.DataFrame()
    
    def get_fundamental_metrics(self, stock_code: str) -> Dict[str, float]:
        """
        获取股票基本面指标（ROE、毛利率、负债率）
        :param stock_code: 股票代码（如 '600519'）
        :return: 包含基本面指标的字典
        """
        try:
            logger.info(f"获取股票 {stock_code} 的基本面指标")
            
            # 方案1：尝试从 stock_individual_info_em 获取基本信息
            try:
                df_info = ak.stock_individual_info_em(symbol=stock_code)
                if not df_info.empty:
                    info_dict = dict(zip(df_info['item'], df_info['value']))
                    logger.info(f"从 stock_individual_info_em 获取到 {len(info_dict)} 个基本信息项")
            except Exception as e:
                logger.warning(f"stock_individual_info_em 获取失败: {e}")
                info_dict = {}
            
            # 方案2：尝试从 stock_financial_indicator_ths 获取财务指标
            roe = 0.0
            gross_margin = 0.0
            debt_ratio = 0.0
            
            try:
                df_indicator = ak.stock_financial_indicator_ths(symbol=f"{stock_code}.sh" if stock_code.startswith('6') else f"{stock_code}.sz")
                if df_indicator is not None and not df_indicator.empty:
                    logger.info(f"从 stock_financial_indicator_ths 获取到 {len(df_indicator)} 条财务指标数据")
                    latest_indicator = df_indicator.iloc[0]
                    
                    # 尝试获取 ROE
                    if 'ROE(%)' in latest_indicator:
                        roe = float(latest_indicator['ROE(%)']) if pd.notna(latest_indicator['ROE(%)']) else 0.0
                        logger.info(f"从财务指标获取 ROE: {roe:.2f}%")
                    elif '净资产收益率' in latest_indicator:
                        roe = float(latest_indicator['净资产收益率']) if pd.notna(latest_indicator['净资产收益率']) else 0.0
                        logger.info(f"从财务指标获取 ROE: {roe:.2f}%")
                    
                    # 尝试获取毛利率
                    if '销售毛利率' in latest_indicator:
                        gross_margin = float(latest_indicator['销售毛利率']) if pd.notna(latest_indicator['销售毛利率']) else 0.0
                        logger.info(f"从财务指标获取毛利率: {gross_margin:.2f}%")
                    elif '毛利率' in latest_indicator:
                        gross_margin = float(latest_indicator['毛利率']) if pd.notna(latest_indicator['毛利率']) else 0.0
                        logger.info(f"从财务指标获取毛利率: {gross_margin:.2f}%")
                    
                    # 尝试获取负债率
                    if '资产负债率' in latest_indicator:
                        debt_ratio = float(latest_indicator['资产负债率']) if pd.notna(latest_indicator['资产负债率']) else 0.0
                        logger.info(f"从财务指标获取负债率: {debt_ratio:.2f}%")
                else:
                    logger.warning(f"财务指标数据为空或返回None")
            except Exception as e:
                logger.warning(f"从财务指标获取数据失败: {e}")
            
            # 方案3：如果方案2失败，尝试从利润表和资产负债表获取
            if roe == 0.0 or gross_margin == 0.0 or debt_ratio == 0.0:
                try:
                    df_profit = ak.stock_profit_sheet_by_report_em(symbol=stock_code)
                    if df_profit is not None and not df_profit.empty:
                        latest_profit = df_profit.iloc[0]
                        
                        # 计算 ROE = 净利润 / 股东权益
                        if roe == 0.0 and '净利润' in latest_profit and '股东权益合计' in latest_profit:
                            net_income = float(latest_profit['净利润']) if pd.notna(latest_profit['净利润']) else 0
                            equity = float(latest_profit['股东权益合计']) if pd.notna(latest_profit['股东权益合计']) else 0
                            if equity != 0:
                                roe = (net_income / equity) * 100
                                logger.info(f"从利润表计算 ROE: {roe:.2f}%")
                        
                        # 计算毛利率 = (营业收入 - 营业成本) / 营业收入
                        if gross_margin == 0.0 and '营业收入' in latest_profit and '营业成本' in latest_profit:
                            revenue = float(latest_profit['营业收入']) if pd.notna(latest_profit['营业收入']) else 0
                            cost = float(latest_profit['营业成本']) if pd.notna(latest_profit['营业成本']) else 0
                            if revenue != 0:
                                gross_margin = ((revenue - cost) / revenue) * 100
                                logger.info(f"从利润表计算毛利率: {gross_margin:.2f}%")
                    else:
                        logger.warning(f"利润表数据为空或返回None")
                except Exception as e:
                    logger.warning(f"从利润表获取数据失败: {e}")
                
                try:
                    df_balance = ak.stock_balance_sheet_by_report_em(symbol=stock_code)
                    if df_balance is not None and not df_balance.empty:
                        latest_balance = df_balance.iloc[0]
                        
                        # 计算负债率 = 总负债 / 总资产
                        if debt_ratio == 0.0 and '负债合计' in latest_balance and '资产总计' in latest_balance:
                            total_liability = float(latest_balance['负债合计']) if pd.notna(latest_balance['负债合计']) else 0
                            total_assets = float(latest_balance['资产总计']) if pd.notna(latest_balance['资产总计']) else 0
                            if total_assets != 0:
                                debt_ratio = (total_liability / total_assets) * 100
                                logger.info(f"从资产负债表计算负债率: {debt_ratio:.2f}%")
                    else:
                        logger.warning(f"资产负债表数据为空或返回None")
                except Exception as e:
                    logger.warning(f"从资产负债表获取数据失败: {e}")
            
            # 判断基本面是否健康（ROE > 8% 且 毛利率 > 20% 且 负债率 < 70%）
            is_fundamentally_healthy = (roe > 8.0) and (gross_margin > 20.0) and (debt_ratio < 70.0)
            
            logger.info(f"基本面指标汇总: ROE={roe:.2f}%, 毛利率={gross_margin:.2f}%, 负债率={debt_ratio:.2f}%, 健康状态={is_fundamentally_healthy}")
            
            return {
                'roe': roe,
                'gross_margin': gross_margin,
                'debt_ratio': debt_ratio,
                'is_fundamentally_healthy': is_fundamentally_healthy
            }
                
        except Exception as e:
            logger.error(f"获取股票 {stock_code} 基本面指标失败: {e}")
            import traceback
            traceback.print_exc()
            return {
                'roe': 0.0,
                'gross_margin': 0.0,
                'debt_ratio': 0.0,
                'is_fundamentally_healthy': False
            }
    
    def apply_fundamental_weighting(self, sentiment_score: float, stock_code: str) -> Tuple[float, Dict]:
        """
        应用基本面加权到情感分
        :param sentiment_score: 原始情感分
        :param stock_code: 股票代码
        :return: (加权后的情感分, 基本面指标字典)
        """
        try:
            fundamental_metrics = self.get_fundamental_metrics(stock_code)
            
            if fundamental_metrics['is_fundamentally_healthy']:
                weighted_score = sentiment_score * 1.2
                logger.info(f"股票 {stock_code} 基本面健康（ROE > 8%），情感分加权 1.2 倍: {sentiment_score:.3f} -> {weighted_score:.3f}")
            else:
                weighted_score = sentiment_score
                logger.info(f"股票 {stock_code} 基本面一般，情感分保持不变: {sentiment_score:.3f}")
            
            return weighted_score, fundamental_metrics
        except Exception as e:
            logger.error(f"应用基本面加权失败: {e}")
            return sentiment_score, {'roe': 0.0, 'gross_margin': 0.0, 'debt_ratio': 0.0, 'is_fundamentally_healthy': False}
    
    def detect_extreme_sentiment(self, sentiment_series: Series, window: int = 20, threshold: float = 2.0) -> pd.DataFrame:
        """
        使用 Z-Score 检测极端情绪信号（特征降噪）
        :param sentiment_series: 情绪得分Series
        :param window: 回溯窗口天数（默认20天）
        :param threshold: Z-Score阈值（默认2.0，即2个标准差）
        :return: 包含异动信号的DataFrame
        """
        try:
            logger.info(f"检测极端情绪信号，窗口: {window} 天，Z-Score阈值: {threshold}")
            
            df = pd.DataFrame({
                'date': sentiment_series.index,
                'sentiment_score': sentiment_series.values
            })
            
            rolling_mean = df['sentiment_score'].rolling(window=window, min_periods=2).mean()
            rolling_std = df['sentiment_score'].rolling(window=window, min_periods=2).std()
            
            df['sentiment_mean'] = rolling_mean
            df['sentiment_std'] = rolling_std
            df['z_score'] = (df['sentiment_score'] - rolling_mean) / rolling_std
            
            df['extreme_signal'] = '无'
            df.loc[df['z_score'] > threshold, 'extreme_signal'] = '异动信号（正向）'
            df.loc[df['z_score'] < -threshold, 'extreme_signal'] = '异动信号（负向）'
            
            positive_signals = len(df[df['extreme_signal'] == '异动信号（正向）'])
            negative_signals = len(df[df['extreme_signal'] == '异动信号（负向）'])
            
            logger.info(f"检测到正向异动信号: {positive_signals} 个，负向异动信号: {negative_signals} 个")
            
            return df
        except Exception as e:
            logger.error(f"检测极端情绪信号失败: {e}")
            return pd.DataFrame()
    
    def calculate_sentiment_price_correlation(self, sentiment_series: Series, price_series: Series, window: int = 60) -> Dict:
        """
        计算情绪-股价相关性（相关性校验）
        :param sentiment_series: 情绪得分Series
        :param price_series: 股价Series
        :param window: 回溯窗口天数（默认60天）
        :return: 包含相关性信息和警告的字典
        """
        try:
            logger.info(f"计算情绪-股价相关性，窗口: {window} 天")
            
            df = pd.DataFrame({
                'sentiment': sentiment_series.values,
                'price': price_series.values
            }).dropna()
            
            if len(df) < 10:
                return {
                    'correlation': 0.0,
                    'p_value': 1.0,
                    'is_significant': False,
                    'warning': '数据不足，无法计算相关性'
                }
            
            recent_df = df.tail(window)
            
            correlation, p_value = pearsonr(recent_df['sentiment'], recent_df['price'])
            
            is_significant = p_value < 0.05
            
            warning = None
            if abs(correlation) < 0.1:
                warning = '该股受舆情驱动极弱，谨慎参考'
            elif not is_significant:
                warning = '相关性不显著，参考价值有限'
            
            logger.info(f"情绪-股价相关性: {correlation:.3f}, p值: {p_value:.3f}")
            
            return {
                'correlation': float(correlation),
                'p_value': float(p_value),
                'is_significant': is_significant,
                'warning': warning
            }
        except Exception as e:
            logger.error(f"计算情绪-股价相关性失败: {e}")
            return {
                'correlation': 0.0,
                'p_value': 1.0,
                'is_significant': False,
                'warning': '计算失败'
            }
    
    def calculate_statistics(self, data_df: pd.DataFrame) -> Dict[str, float]:
        """
        计算统计指标
        :param data_df: 包含股价和情绪数据的DataFrame
        :return: 统计指标字典
        """
        try:
            logger.info("计算统计指标")
            
            stats = {}
            
            # 基本统计
            stats['总数据量'] = len(data_df)
            
            # 股价统计
            if '涨跌幅' in data_df.columns:
                stats['平均日涨跌幅(%)'] = data_df['涨跌幅'].mean()
                stats['日涨跌幅标准差(%)'] = data_df['涨跌幅'].std()
                stats['最大日涨幅(%)'] = data_df['涨跌幅'].max()
                stats['最大日跌幅(%)'] = data_df['涨跌幅'].min()
            
            # 情绪统计
            if '情感得分' in data_df.columns:
                stats['平均情感得分'] = data_df['情感得分'].mean()
                stats['情感得分标准差'] = data_df['情感得分'].std()
                stats['最高情感得分'] = data_df['情感得分'].max()
                stats['最低情感得分'] = data_df['情感得分'].min()
                
                # 正向/负向情绪比例
                positive_days = len(data_df[data_df['情感得分'] > 0])
                negative_days = len(data_df[data_df['情感得分'] < 0])
                neutral_days = len(data_df[data_df['情感得分'] == 0])
                
                stats['正向情绪比例(%)'] = (positive_days / len(data_df)) * 100 if len(data_df) > 0 else 0
                stats['负向情绪比例(%)'] = (negative_days / len(data_df)) * 100 if len(data_df) > 0 else 0
                stats['中性情绪比例(%)'] = (neutral_days / len(data_df)) * 100 if len(data_df) > 0 else 0
            
            logger.info(f"统计指标计算完成: {len(stats)} 个指标")
            return stats
        except Exception as e:
            logger.error(f"计算统计指标失败: {e}")
            return {}
    
    def detect_sentiment_events(self, sentiment_df: pd.DataFrame, threshold: float = 0.5) -> pd.DataFrame:
        """
        检测极端情绪事件
        :param sentiment_df: 情绪数据DataFrame，包含'日期'和'情感得分'列
        :param threshold: 极端情绪阈值
        :return: 标记了极端情绪事件的DataFrame
        """
        try:
            logger.info(f"检测极端情绪事件，阈值: {threshold}")
            
            result_df = sentiment_df.copy()
            
            result_df['情绪事件'] = '无'
            result_df.loc[result_df['情感得分'] > threshold, '情绪事件'] = '极端正面'
            result_df.loc[result_df['情感得分'] < -threshold, '情绪事件'] = '极端负面'
            
            extreme_positive = len(result_df[result_df['情绪事件'] == '极端正面'])
            extreme_negative = len(result_df[result_df['情绪事件'] == '极端负面'])
            
            logger.info(f"检测到极端正面情绪事件: {extreme_positive} 个，极端负面情绪事件: {extreme_negative} 个")
            return result_df
        except Exception as e:
            logger.error(f"检测情绪事件失败: {e}")
            return sentiment_df.copy()
    
    def calculate_sentiment_sma(self, sentiment_df: pd.DataFrame, window: int = 5) -> pd.DataFrame:
        """
        计算情感得分的移动平均线，减少噪音
        :param sentiment_df: 情绪数据DataFrame，包含'日期'和'情感得分'列
        :param window: 移动平均窗口大小，默认5日
        :return: 包含移动平均线的DataFrame
        """
        try:
            logger.info(f"计算情感得分 {window} 日移动平均线")
            
            result_df = sentiment_df.copy()
            
            result_df['情感得分_SMA'] = result_df['情感得分'].rolling(window=window, min_periods=1).mean()
            
            logger.info(f"情感得分移动平均线计算完成")
            return result_df
        except Exception as e:
            logger.error(f"计算情感得分移动平均线失败: {e}")
            return sentiment_df.copy()
    
    def calculate_sentiment_percentile(self, sentiment_df: pd.DataFrame, lookback_days: int = 252) -> Dict:
        """
        计算当前情感得分的历史百分位和情绪状态
        :param sentiment_df: 情绪数据DataFrame，包含'日期'和'情感得分'列
        :param lookback_days: 回溯天数，默认252天（约一年）
        :return: 包含百分位、情绪状态等信息的字典
        """
        try:
            logger.info(f"计算情感得分历史百分位，回溯 {lookback_days} 天")
            
            if len(sentiment_df) < 2:
                return {'current_sentiment': 0, 'percentile': 0, 'status': '数据不足', 'status_color': 'gray', 'lookback_days': lookback_days}
            
            recent_df = sentiment_df.tail(lookback_days)
            current_sentiment = recent_df['情感得分'].iloc[-1]
            
            percentile = (recent_df['情感得分'] <= current_sentiment).sum() / len(recent_df) * 100
            
            if percentile >= 90:
                status = '极度乐观'
                status_color = 'green'
            elif percentile >= 70:
                status = '乐观'
                status_color = 'lightgreen'
            elif percentile >= 30:
                status = '中性'
                status_color = 'yellow'
            elif percentile >= 10:
                status = '悲观'
                status_color = 'orange'
            else:
                status = '极度悲观'
                status_color = 'red'
            
            result = {
                'current_sentiment': current_sentiment,
                'percentile': percentile,
                'status': status,
                'status_color': status_color,
                'lookback_days': lookback_days
            }
            
            logger.info(f"情感得分历史百分位: {percentile:.1f}%, 状态: {status}")
            return result
        except Exception as e:
            logger.error(f"计算情感得分历史百分位失败: {e}")
            return {'current_sentiment': 0, 'percentile': 0, 'status': '计算失败', 'status_color': 'gray', 'lookback_days': lookback_days}
    
    def run_complete_analysis(self, stock_df: pd.DataFrame, sentiment_df: pd.DataFrame, lag_days: int = 1, divergence_window: int = 5) -> Dict:
        """
        运行完整的关联分析
        :param stock_df: 股价数据DataFrame
        :param sentiment_df: 情绪数据DataFrame
        :param lag_days: 情绪得分滞后天数
        :param divergence_window: 背离分析窗口大小
        :return: 分析结果字典
        """
        try:
            logger.info("运行完整的股票情绪关联分析")
            
            # 1. 数据对齐
            aligned_df = self.align_data(stock_df, sentiment_df)
            if aligned_df.empty:
                logger.error("数据对齐后为空，无法进行分析")
                return {}
            
            # 2. 计算情绪指数
            sentiment_with_index = self.calculate_sentiment_index(sentiment_df, divergence_window)
            
            # 3. 再次对齐包含情绪指数的数据
            aligned_df = self.align_data(stock_df, sentiment_with_index)
            
            # 4. 计算相关系数
            correlation, p_value = self.calculate_correlation(aligned_df, lag_days)
            
            # 5. 识别背离信号
            data_with_divergence = self.identify_divergence(aligned_df, price_col='收盘价', sentiment_col='情绪指数', window=divergence_window)
            
            # 6. 检测极端情绪事件
            data_with_events = self.detect_sentiment_events(data_with_divergence)
            
            # 7. 计算统计指标
            statistics = self.calculate_statistics(data_with_events)
            
            # 统计背离信号数量
            top_divergence_count = len(data_with_events[data_with_events['背离信号'] == '顶背离'])
            bottom_divergence_count = len(data_with_events[data_with_events['背离信号'] == '底背离'])
            
            # 整理分析结果
            analysis_result = {
                'merged_data': data_with_events,
                'correlation': {
                    'correlation_coefficient': correlation,
                    'p_value': p_value,
                    'lag_days': lag_days
                },
                'divergence_signals': {
                    'top_divergence_count': top_divergence_count,
                    'bottom_divergence_count': bottom_divergence_count,
                    'window_size': divergence_window
                },
                'statistics': statistics
            }
            
            logger.info("完整分析完成")
            return analysis_result
        except Exception as e:
            logger.error(f"完整分析失败: {e}")
            return {}

# 创建全局实例供外部使用
stock_sentiment_analyzer = StockSentimentAnalyzer()

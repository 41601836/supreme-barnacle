"""
数据更新脚本
每天凌晨或收盘后运行，从 Tushare 获取最新数据并更新到本地数据库
"""

import sys
import os
import logging
from datetime import datetime, timedelta
from typing import List
import time

# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from crawlers.data_source import create_data_source
from crawlers.local_database import LocalDatabase

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/data_updater.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# WATCHLIST - 目标股票池
WATCHLIST = [
    "600519.SH",  # 贵州茅台
    "300750.SZ",  # 宁德时代
    "002594.SZ",  # 比亚迪
    "601318.SH",  # 中国平安
    "000858.SZ",  # 五粮液
    "601888.SH",  # 中国中免
    "000333.SZ",  # 美的集团
    "002769.SZ",  # 招商积余
    "002759.SZ",  # 天际股份
    "002856.SZ",  # 美芝股份
    "000659.SZ",  # 珠海中富
    "002347.SZ",  # 泰尔股份
    "603660.SH",  # 苏州科达
    "000523.SZ",  # 广州浪奇
    "002136.SZ",  # 安纳达
    "301117.SZ",  # 佳缘科技
]


class DataUpdater:
    """
    数据更新器
    负责从 Tushare 获取数据并更新到本地数据库
    """
    
    def __init__(self, tushare_token: str, db_path: str = 'data/stock_data.db'):
        """
        初始化数据更新器
        
        :param tushare_token: Tushare Pro API Token
        :param db_path: 数据库文件路径
        """
        self.data_source = create_data_source('tushare', token=tushare_token)
        self.db = LocalDatabase(db_path)
        logger.info("DataUpdater 初始化完成")
    
    def update_stock_prices(self, stock_code: str, days: int = 90) -> bool:
        """
        更新股票日线行情数据
        
        :param stock_code: 股票代码
        :param days: 获取最近N天的数据
        :return: 是否更新成功
        """
        try:
            # 计算日期范围
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            start_date_str = start_date.strftime('%Y%m%d')
            end_date_str = end_date.strftime('%Y%m%d')
            
            logger.info(f"开始更新 {stock_code} 日线行情: {start_date_str} - {end_date_str}")
            
            # 从 Tushare 获取数据
            df_prices = self.data_source.get_daily_prices(
                stock_code=stock_code,
                start_date=start_date_str,
                end_date=end_date_str
            )
            
            if df_prices.empty:
                logger.warning(f"未获取到 {stock_code} 日线数据，跳过")
                return False
            
            # 保存到数据库
            saved_count = self.db.save_daily_prices(stock_code, df_prices)
            
            logger.info(f"✓ {stock_code} 日线行情更新完成，保存 {saved_count} 条记录")
            return True
            
        except Exception as e:
            logger.error(f"✗ {stock_code} 日线行情更新失败: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def update_stock_news(self, stock_code: str, days: int = 30) -> bool:
        """
        更新股票新闻数据
        
        :param stock_code: 股票代码
        :param days: 获取最近N天的新闻
        :return: 是否更新成功
        """
        try:
            logger.info(f"开始更新 {stock_code} 新闻数据（最近 {days} 天）")
            
            # 从 Tushare 获取数据
            df_news = self.data_source.get_news(
                stock_code=stock_code,
                days=days
            )
            
            if df_news.empty:
                logger.warning(f"未获取到 {stock_code} 新闻数据，跳过")
                return False
            
            # 保存到数据库
            saved_count = self.db.save_news(stock_code, df_news)
            
            logger.info(f"✓ {stock_code} 新闻数据更新完成，保存 {saved_count} 条记录")
            return True
            
        except Exception as e:
            logger.error(f"✗ {stock_code} 新闻数据更新失败: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def update_stock_info(self, stock_code: str) -> bool:
        """
        更新股票基本信息
        
        :param stock_code: 股票代码
        :return: 是否更新成功
        """
        try:
            logger.info(f"开始更新 {stock_code} 基本信息")
            
            # 从 Tushare 获取数据
            info = self.data_source.get_stock_info(stock_code)
            
            # 保存到数据库
            success = self.db.save_stock_info(stock_code, info)
            
            if success:
                logger.info(f"✓ {stock_code} 基本信息更新完成: {info.get('name', 'Unknown')}")
            return True
            
        except Exception as e:
            logger.error(f"✗ {stock_code} 基本信息更新失败: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def update_financial_indicators(self, stock_code: str) -> bool:
        """
        更新财务指标数据
        
        :param stock_code: 股票代码
        :return: 是否更新成功
        """
        try:
            logger.info(f"开始更新 {stock_code} 财务指标")
            
            # 从 Tushare 获取数据
            indicators = self.data_source.get_financial_indicator(stock_code)
            
            # 保存到数据库
            success = self.db.save_financial_indicator(stock_code, indicators)
            
            if success:
                logger.info(f"✓ {stock_code} 财务指标更新完成: ROE={indicators.get('roe', 0):.2f}%")
            return True
            
        except Exception as e:
            logger.error(f"✗ {stock_code} 财务指标更新失败: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def update_stock(self, stock_code: str) -> dict:
        """
        更新单只股票的所有数据
        
        :param stock_code: 股票代码
        :return: 更新结果统计
        """
        results = {
            'stock_code': stock_code,
            'prices': False,
            'news': False,
            'info': False,
            'indicators': False
        }
        
        # 更新日线行情
        results['prices'] = self.update_stock_prices(stock_code, days=90)
        time.sleep(0.5)  # 避免请求过快
        
        # 更新新闻
        results['news'] = self.update_stock_news(stock_code, days=30)
        time.sleep(0.5)
        
        # 更新基本信息
        results['info'] = self.update_stock_info(stock_code)
        time.sleep(0.5)
        
        # 更新财务指标
        results['indicators'] = self.update_financial_indicators(stock_code)
        
        return results
    
    def update_all_stocks(self, stock_list: List[str] = None) -> dict:
        """
        更新所有股票的数据
        
        :param stock_list: 股票代码列表，默认使用 WATCHLIST
        :return: 更新结果统计
        """
        if stock_list is None:
            stock_list = WATCHLIST
        
        logger.info("=" * 60)
        logger.info(f"开始更新 {len(stock_list)} 只股票的数据")
        logger.info("=" * 60)
        
        all_results = []
        success_count = 0
        
        for i, stock_code in enumerate(stock_list, 1):
            logger.info(f"\n[{i}/{len(stock_list)}] 处理 {stock_code}")
            
            # 更新单只股票
            results = self.update_stock(stock_code)
            all_results.append(results)
            
            # 统计成功数
            if all([results['prices'], results['news'], results['info'], results['indicators']]):
                success_count += 1
            
            # 添加延迟，避免请求过快
            time.sleep(1)
        
        # 输出汇总统计
        logger.info("\n" + "=" * 60)
        logger.info("更新完成！汇总统计：")
        logger.info("=" * 60)
        logger.info(f"总股票数: {len(stock_list)}")
        logger.info(f"完全成功: {success_count} 只")
        logger.info(f"部分成功: {len(all_results) - success_count} 只")
        logger.info(f"完全失败: {0} 只")
        logger.info("=" * 60)
        
        return {
            'total': len(stock_list),
            'success': success_count,
            'partial': len(all_results) - success_count,
            'results': all_results
        }


def main():
    """
    主函数
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='股票数据更新脚本')
    parser.add_argument('--token', type=str, required=True, help='Tushare Pro API Token')
    parser.add_argument('--db-path', type=str, default='data/stock_data.db', help='数据库文件路径')
    parser.add_argument('--stock', type=str, help='指定股票代码（如 600519.SH），不指定则更新全部')
    parser.add_argument('--prices-only', action='store_true', help='仅更新日线行情')
    parser.add_argument('--news-only', action='store_true', help='仅更新新闻')
    parser.add_argument('--info-only', action='store_true', help='仅更新基本信息')
    
    args = parser.parse_args()
    
    # 初始化更新器
    updater = DataUpdater(
        tushare_token=args.token,
        db_path=args.db_path
    )
    
    # 执行更新
    if args.stock:
        # 更新单只股票
        stock_code = args.stock
        logger.info(f"更新单只股票: {stock_code}")
        
        results = updater.update_stock(stock_code)
        logger.info(f"\n更新结果: {results}")
    else:
        # 更新全部股票
        summary = updater.update_all_stocks()
        logger.info(f"\n总体统计: 成功 {summary['success']}/{summary['total']}")


if __name__ == '__main__':
    main()

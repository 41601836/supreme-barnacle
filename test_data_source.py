"""
测试脚本
测试 WATCHLIST 股票的日线行情和新闻数据获取
"""

import sys
import os
import logging
from datetime import datetime, timedelta

# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from crawlers.unified_data_source import get_data_source

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# WATCHLIST - 目标股票池
WATCHLIST = [
    "600519.SH",  # 贵州茅台
    "300750.SZ",  # 宁德时代
    "002594.SZ",  # 比亚迪
    "601318.SH",  # 中国平安
]


def test_single_stock(stock_code: str):
    """
    测试单只股票的数据获取
    
    :param stock_code: 股票代码
    """
    logger.info("=" * 60)
    logger.info(f"测试股票: {stock_code}")
    logger.info("=" * 60)
    
    try:
        # 获取数据源实例
        data_source = get_data_source()
        
        # 计算日期范围
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        start_date_str = start_date.strftime('%Y%m%d')
        end_date_str = end_date.strftime('%Y%m%d')
        
        # 测试 1: 获取日线行情
        logger.info(f"\n[测试 1] 获取日线行情: {start_date_str} - {end_date_str}")
        prices_result = data_source.get_daily_prices(
            stock_code=stock_code,
            start_date=start_date_str,
            end_date=end_date_str
        )
        
        if not prices_result['data'].empty:
            logger.info(f"✓ 日线行情获取成功: {len(prices_result['data'])} 条记录，来源: {prices_result['source']}")
            logger.info(f"  最新日期: {prices_result['data']['date'].max()}")
            logger.info(f"  最新收盘价: {prices_result['data']['close'].iloc[-1]:.2f}")
        else:
            logger.warning(f"✗ 日线行情获取失败")
        
        # 测试 2: 获取新闻数据
        logger.info(f"\n[测试 2] 获取最近 30 天新闻")
        news_result = data_source.get_news(
            stock_code=stock_code,
            days=30
        )
        
        if not news_result['data'].empty:
            logger.info(f"✓ 新闻数据获取成功: {len(news_result['data'])} 条记录，来源: {news_result['source']}")
            if len(news_result['data']) > 0:
                logger.info(f"  最新新闻: {news_result['data']['title'].iloc[0]}")
        else:
            logger.warning(f"✗ 新闻数据获取失败")
        
        # 测试 3: 获取基本信息
        logger.info(f"\n[测试 3] 获取基本信息")
        info_result = data_source.get_stock_info(stock_code=stock_code)
        
        if info_result['data']:
            logger.info(f"✓ 基本信息获取成功，来源: {info_result['source']}")
            logger.info(f"  股票名称: {info_result['data']['name']}")
            logger.info(f"  所属行业: {info_result['data']['industry']}")
        else:
            logger.warning(f"✗ 基本信息获取失败")
        
        # 测试 4: 获取财务指标
        logger.info(f"\n[测试 4] 获取财务指标")
        indicators_result = data_source.get_financial_indicator(stock_code=stock_code)
        
        if indicators_result['data']:
            logger.info(f"✓ 财务指标获取成功，来源: {indicators_result['source']}")
            logger.info(f"  ROE: {indicators_result['data']['roe']:.2f}%")
            logger.info(f"  毛利率: {indicators_result['data']['gross_margin']:.2f}%")
            logger.info(f"  负债率: {indicators_result['data']['debt_ratio']:.2f}%")
        else:
            logger.warning(f"✗ 财务指标获取失败")
        
        logger.info(f"\n✓ {stock_code} 所有测试完成")
        return True
        
    except Exception as e:
        logger.error(f"\n✗ {stock_code} 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_all_stocks():
    """
    测试所有 WATCHLIST 股票
    """
    logger.info("\n" + "=" * 60)
    logger.info("开始测试 WATCHLIST 股票数据获取")
    logger.info("=" * 60)
    
    results = {
        'total': len(WATCHLIST),
        'success': 0,
        'failed': 0,
        'details': []
    }
    
    for i, stock_code in enumerate(WATCHLIST, 1):
        logger.info(f"\n[{i}/{len(WATCHLIST)}] 测试 {stock_code}")
        
        success = test_single_stock(stock_code)
        
        if success:
            results['success'] += 1
        else:
            results['failed'] += 1
        
        results['details'].append({
            'stock_code': stock_code,
            'success': success
        })
    
    # 输出汇总
    logger.info("\n" + "=" * 60)
    logger.info("测试完成！汇总统计：")
    logger.info("=" * 60)
    logger.info(f"总股票数: {results['total']}")
    logger.info(f"成功: {results['success']}")
    logger.info(f"失败: {results['failed']}")
    logger.info(f"成功率: {results['success'] / results['total'] * 100:.1f}%")
    logger.info("=" * 60)
    
    return results


def main():
    """
    主函数
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='测试股票数据获取')
    parser.add_argument('--stock', type=str, help='指定股票代码（如 600519.SH），不指定则测试全部')
    
    args = parser.parse_args()
    
    # 执行测试
    if args.stock:
        # 测试单只股票
        logger.info(f"测试单只股票: {args.stock}")
        test_single_stock(args.stock)
    else:
        # 测试全部股票
        test_all_stocks()


if __name__ == '__main__':
    main()

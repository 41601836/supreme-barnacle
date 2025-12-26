import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

def generate_mock_news(stock_code: str = "600519", days: int = 30, posts_per_day: int = 5) -> pd.DataFrame:
    """
    生成模拟的股吧新闻数据用于测试
    
    Args:
        stock_code: 股票代码
        days: 生成天数
        posts_per_day: 每天生成的帖子数量
    
    Returns:
        包含模拟新闻数据的DataFrame
    """
    logger.info(f"生成股票 {stock_code} 的模拟新闻数据，共 {days} 天")
    
    # 模拟的标题模板（包含正面、负面和中性情绪）
    positive_titles = [
        "贵州茅台业绩超预期，股价创新高",
        "茅台股价强势上涨，机构看好后市",
        "贵州茅台发布利好消息，市场反应积极",
        "茅台销量大增，营收创新纪录",
        "贵州茅台获得多家机构增持评级",
        "茅台品牌价值持续提升，投资者信心增强",
        "贵州茅台分红方案超预期，股东收益可观",
        "茅台股价突破关键阻力位，技术面转好",
        "贵州茅台海外市场拓展顺利，国际化加速",
        "茅台股价稳步上涨，长期投资价值凸显"
    ]
    
    negative_titles = [
        "贵州茅台股价下跌，市场担忧加剧",
        "茅台业绩不及预期，股价承压",
        "贵州茅台面临政策风险，投资者观望",
        "茅台销量下滑，库存压力增加",
        "贵州茅台被机构下调评级",
        "茅台股价跌破重要支撑位，技术面转弱",
        "贵州茅台遭遇负面舆情，品牌受损",
        "茅台行业竞争加剧，市场份额受挑战",
        "贵州茅台成本上升，利润率下降",
        "茅台股价持续低迷，投资者信心不足"
    ]
    
    neutral_titles = [
        "贵州茅台今日股价小幅波动",
        "茅台召开股东大会，讨论未来发展",
        "贵州茅台发布例行公告",
        "茅台行业分析报告出炉",
        "贵州茅台管理层调整公告",
        "茅台参加行业展会",
        "贵州茅台发布月度经营数据",
        "茅台股价横盘整理，等待方向",
        "贵州茅台发布社会责任报告",
        "茅台股价震荡，多空博弈激烈"
    ]
    
    # 模拟的帖子内容
    positive_contents = [
        "茅台基本面强劲，长期看好。业绩持续增长，品牌价值不断提升，是优质的投资标的。",
        "贵州茅台作为行业龙头，具有强大的护城河。当前估值合理，值得长期持有。",
        "茅台的盈利能力突出，现金流充沛。随着消费升级，未来发展空间广阔。",
        "贵州茅台的渠道改革成效显著，直销比例提升，利润率有望进一步改善。",
        "茅台的品牌影响力无可替代，在高端白酒市场具有绝对优势。"
    ]
    
    negative_contents = [
        "茅台估值过高，存在回调风险。当前价格已经透支了未来几年的增长预期。",
        "贵州茅台面临政策不确定性，消费税改革可能对利润产生影响。",
        "茅台的销量增长放缓，市场竞争加剧。年轻消费者对白酒的偏好下降。",
        "贵州茅台的库存问题值得关注，去库存压力可能影响短期业绩。",
        "茅台股价技术面走弱，短期建议观望。"
    ]
    
    neutral_contents = [
        "贵州茅台今日股价小幅震荡，成交量温和。市场多空双方博弈激烈。",
        "茅台发布最新公告，公司经营情况正常。投资者需关注后续发展。",
        "贵州茅台股价在关键位置整理，等待方向选择。建议关注成交量变化。",
        "茅台行业整体表现平稳，公司基本面无明显变化。投资者需理性分析。",
        "贵州茅台股价波动符合市场预期，短期走势需关注宏观环境。"
    ]
    
    news_data = []
    end_date = datetime.now()
    
    # 生成每天的帖子
    for day in range(days):
        current_date = end_date - timedelta(days=day)
        
        # 根据日期生成情绪趋势（模拟市场波动）
        day_factor = np.sin(day / 5)  # 周期性波动
        
        for post in range(posts_per_day):
            # 根据日期因子决定情绪倾向
            rand = np.random.random()
            if day_factor > 0.3:
                # 正面情绪较多
                if rand < 0.5:
                    title = np.random.choice(positive_titles)
                    content = np.random.choice(positive_contents)
                    sentiment = 0.8
                elif rand < 0.8:
                    title = np.random.choice(neutral_titles)
                    content = np.random.choice(neutral_contents)
                    sentiment = 0.1
                else:
                    title = np.random.choice(negative_titles)
                    content = np.random.choice(negative_contents)
                    sentiment = -0.6
            elif day_factor < -0.3:
                # 负面情绪较多
                if rand < 0.5:
                    title = np.random.choice(negative_titles)
                    content = np.random.choice(negative_contents)
                    sentiment = -0.8
                elif rand < 0.8:
                    title = np.random.choice(neutral_titles)
                    content = np.random.choice(neutral_contents)
                    sentiment = 0.1
                else:
                    title = np.random.choice(positive_titles)
                    content = np.random.choice(positive_contents)
                    sentiment = 0.6
            else:
                # 中性情绪较多
                if rand < 0.4:
                    title = np.random.choice(neutral_titles)
                    content = np.random.choice(neutral_contents)
                    sentiment = 0.1
                elif rand < 0.7:
                    title = np.random.choice(positive_titles)
                    content = np.random.choice(positive_contents)
                    sentiment = 0.5
                else:
                    title = np.random.choice(negative_titles)
                    content = np.random.choice(negative_contents)
                    sentiment = -0.5
            
            # 添加随机噪声
            sentiment += np.random.uniform(-0.2, 0.2)
            sentiment = max(-1, min(1, sentiment))
            
            # 模拟阅读量和评论数
            view_count = int(np.random.exponential(500))
            comment_count = int(np.random.exponential(20))
            
            news_data.append({
                "date": current_date,
                "title": title,
                "content": content,
                "stock_code": stock_code,
                "view_count": view_count,
                "comment_count": comment_count
            })
    
    df = pd.DataFrame(news_data)
    df = df.sort_values("date", ascending=False)
    
    logger.info(f"成功生成 {len(df)} 条模拟新闻数据")
    return df

def get_mock_stock_news(stock_code: str = "600519", days: int = 30) -> pd.DataFrame:
    """
    获取模拟的股票新闻数据（用于测试）
    
    Args:
        stock_code: 股票代码
        days: 获取天数
    
    Returns:
        包含新闻数据的DataFrame
    """
    return generate_mock_news(stock_code, days)

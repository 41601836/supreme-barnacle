import numpy as np
import pandas as pd
import logging
import re
import random
import time
import hashlib
import json
import requests
import os
from typing import List, Dict, Optional, Tuple

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FinancialSentimentAnalyzer:
    """
    金融情感分析器，使用 DeepSeek-R1 API 进行逻辑推理和情感分析，
    不依赖本地模型，纯 API 调用模式
    """
    
    def __init__(self, deepseek_api_key=None):
        """
        初始化金融情感分析器
        :param deepseek_api_key: DeepSeek API密钥（可选，未提供则从环境变量读取）
        """
        if deepseek_api_key is None:
            deepseek_api_key = os.getenv('DEEPSEEK_API_KEY')
        
        self.use_deepseek = deepseek_api_key is not None
        self.deepseek_api_key = deepseek_api_key
        self.deepseek_api_url = os.getenv('DEEPSEEK_API_BASE', 'https://api.deepseek.com/v1/chat/completions')
        
        # 情感分析结果缓存（使用哈希值作为键，支持长文本）
        self.sentiment_cache = {}  # 文本哈希 -> 情感得分
        self.cache_max_size = 50000  # 最大缓存条数
        
        # 缓存统计
        self.cache_hits = 0
        self.cache_misses = 0
        
        # 金融黑话词典 - 极端词汇直接赋予最高权重
        self.extreme_terms_weights = {
            # 极端正面词汇
            "涨停": 1.0,
            "封板": 1.0,
            "连板": 0.9,
            "一字涨停": 1.0,
            "地天板": 1.0,
            "大单封板": 0.95,
            "主力封板": 0.95,
            
            # 极端负面词汇
            "跌停": -1.0,
            "一字跌停": -1.0,
            "天地板": -1.0,
            "闪崩": -0.95,
            "崩盘": -0.95,
            "跌穿": -0.9,
            "破位": -0.85,
        }
        
        # 金融特定术语权重调整（非极端词汇）
        self.financial_terms_weights = {
            # 正面术语
            "打板": 0.7,
            "利好": 0.6,
            "超预期": 0.5,
            "增持": 0.4,
            "回购": 0.4,
            "创新高": 0.5,
            "业绩增长": 0.6,
            "政策支持": 0.5,
            "缩量上涨": 0.3,
            "底背离": 0.5,
            
            # 负面术语
            "割韭菜": -0.7,
            "利空": -0.6,
            "低于预期": -0.5,
            "减持": -0.4,
            "创新低": -0.5,
            "业绩下滑": -0.6,
            "政策收紧": -0.5,
            "高位放量": -0.6,
            "顶背离": -0.5,
            "放量下跌": -0.7,
        }
        
        # 广告/垃圾信息关键词
        self.spam_keywords = [
            "推荐股票",
            "牛股推荐",
            "免费领取",
            "加群",
            "微信",
            "QQ群",
            "电话",
            "老师指导",
            "内幕消息",
            "必涨",
            "稳赚",
            "翻倍",
        ]
        
        logger.info("金融情感分析器初始化完成（纯API模式，无本地模型）")
    
    def _get_text_hash(self, text: str) -> str:
        """
        计算文本的哈希值，用于缓存键
        :param text: 输入文本
        :return: MD5哈希字符串
        """
        return hashlib.md5(text.encode('utf-8')).hexdigest()
    
    def _manage_cache_size(self, cache_dict: dict, max_size: int):
        """
        管理缓存大小，当超过最大值时移除最早的条目
        :param cache_dict: 缓存字典
        :param max_size: 最大缓存大小
        """
        if len(cache_dict) >= max_size:
            # 移除最早的 10% 条目，避免频繁删除
            num_to_remove = max(1, int(max_size * 0.1))
            keys_to_remove = list(cache_dict.keys())[:num_to_remove]
            for key in keys_to_remove:
                del cache_dict[key]
    
    def get_cache_stats(self) -> Dict[str, int]:
        """
        获取缓存统计信息
        :return: 缓存统计字典
        """
        return {
            'sentiment_cache_size': len(self.sentiment_cache),
            'batch_cache_size': getattr(self, 'batch_cache_size', 0),
            'cache_hits': self.cache_hits,
            'cache_misses': self.cache_misses,
            'cache_hit_rate': self.cache_hits / (self.cache_hits + self.cache_misses) if (self.cache_hits + self.cache_misses) > 0 else 0,
            'batch_cache_hit_rate': getattr(self, 'batch_cache_hit_rate', 0.0)
        }
    
    def clear_cache(self):
        """
        清空所有缓存
        """
        self.sentiment_cache.clear()
        self.cache_hits = 0
        self.cache_misses = 0
        logger.info("缓存已清空")
    
    def _is_spam(self, text: str) -> bool:
        """
        检查文本是否为垃圾广告信息
        :param text: 要检查的文本
        :return: 是否为垃圾信息
        """
        text_lower = text.lower()
        for keyword in self.spam_keywords:
            if keyword.lower() in text_lower:
                logger.debug(f"检测到垃圾信息关键词: {keyword}，文本: {text[:50]}...")
                return True
        return False
    
    def _check_extreme_terms(self, text: str) -> Tuple[float, Optional[str]]:
        """
        检查文本中是否包含极端金融词汇
        :param text: 要检查的文本
        :return: (权重, 匹配的词汇) 如果没有匹配返回 (0, None)
        """
        # 预编译极端词汇的正则表达式
        if not hasattr(self, '_extreme_terms_regex'):
            sorted_terms = sorted(self.extreme_terms_weights.keys(), key=len, reverse=True)
            terms_pattern = '|'.join(re.escape(term) for term in sorted_terms)
            self._extreme_terms_regex = re.compile(terms_pattern)
        
        # 查找匹配的极端词汇
        matched_terms = self._extreme_terms_regex.findall(text)
        
        if matched_terms:
            # 返回第一个匹配的极端词汇及其权重
            term = matched_terms[0]
            weight = self.extreme_terms_weights[term]
            logger.debug(f"检测到极端金融词汇: {term}，直接赋予权重: {weight}")
            return weight, term
        
        return 0.0, None
    
    def _call_deepseek_api(self, title: str, summary: str) -> Dict:
        """
        调用 DeepSeek-R1 API 进行逻辑推理和情感分析
        :param title: 新闻标题
        :param summary: 新闻摘要
        :return: 包含情感分、逻辑分类、影响逻辑简评的字典
        """
        if not self.deepseek_api_key:
            raise ValueError("DeepSeek API密钥未配置")
        
        prompt = f"""请分析以下金融新闻的情感和逻辑：

标题：{title}
摘要：{summary}

请以JSON格式返回分析结果，包含以下字段：
- sentiment_score: 情感分（-1到1之间的浮点数，负值表示负面，正值表示正面，0表示中性）
- logic_category: 逻辑分类（只能是以下之一：基本面、资金面、消息面）
- impact_summary: 影响逻辑简评（50字以内的简短说明）

只返回JSON，不要有其他内容。"""
        
        try:
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.deepseek_api_key}"
            }
            
            payload = {
                "model": "deepseek-reasoner",
                "messages": [
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                "temperature": 0.3,
                "max_tokens": 500
            }
            
            response = requests.post(
                self.deepseek_api_url,
                headers=headers,
                json=payload,
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                content = result['choices'][0]['message']['content']
                
                # 提取JSON内容
                json_match = re.search(r'\{.*\}', content, re.DOTALL)
                if json_match:
                    json_str = json_match.group(0)
                    analysis_result = json.loads(json_str)
                    
                    # 验证返回的JSON格式
                    if 'sentiment_score' in analysis_result and 'logic_category' in analysis_result and 'impact_summary' in analysis_result:
                        logger.info(f"DeepSeek API调用成功: {title[:30]}...")
                        return analysis_result
                    else:
                        logger.warning(f"DeepSeek API返回的JSON格式不正确: {json_str}")
                        raise ValueError("JSON格式不正确")
                else:
                    logger.warning(f"DeepSeek API返回的内容中未找到JSON: {content}")
                    raise ValueError("未找到JSON内容")
            else:
                error_msg = f"DeepSeek API调用失败，状态码: {response.status_code}, 响应: {response.text}"
                logger.error(error_msg)
                raise Exception(error_msg)
                
        except requests.exceptions.Timeout:
            logger.error("DeepSeek API调用超时")
            raise Exception("API调用超时")
        except requests.exceptions.RequestException as e:
            logger.error(f"DeepSeek API请求异常: {e}")
            raise Exception(f"API请求异常: {e}")
        except json.JSONDecodeError as e:
            logger.error(f"DeepSeek API返回的JSON解析失败: {e}")
            raise Exception(f"JSON解析失败: {e}")
    
    def analyze_logic_and_sentiment(self, title: str, summary: str = "") -> Dict:
        """
        使用 DeepSeek-R1 API 进行逻辑推理和情感分析
        :param title: 新闻标题
        :param summary: 新闻摘要（可选）
        :return: 包含情感分、逻辑分类、影响逻辑简评的字典
        """
        # 检查是否为垃圾信息
        if self._is_spam(title) or self._is_spam(summary):
            logger.warning(f"文本被识别为垃圾信息，跳过分析: {title[:50]}...")
            return {
                'sentiment_score': 0.0,
                'logic_category': '消息面',
                'impact_summary': '垃圾信息，忽略'
            }
        
        # 预处理层：检查极端金融词汇
        extreme_weight, extreme_term = self._check_extreme_terms(title + ' ' + summary)
        if extreme_term:
            # 如果包含极端词汇，直接返回结果，不调用API
            sentiment_score = extreme_weight
            logic_category = '资金面' if extreme_weight > 0 else '消息面'
            impact_summary = f"检测到{extreme_term}，{('强烈利好' if extreme_weight > 0 else '强烈利空')}"
            
            logger.info(f"极端词汇匹配，直接返回结果: {extreme_term}")
            return {
                'sentiment_score': sentiment_score,
                'logic_category': logic_category,
                'impact_summary': impact_summary
            }
        
        # 调用 DeepSeek-R1 API
        if self.use_deepseek:
            try:
                result = self._call_deepseek_api(title, summary)
                return result
            except Exception as e:
                logger.warning(f"DeepSeek API调用失败，降级到关键词匹配: {e}")
                # 降级到关键词匹配
                return self._fallback_to_keywords(title, summary)
        else:
            logger.info("未配置DeepSeek API密钥，使用关键词匹配")
            return self._fallback_to_keywords(title, summary)
    
    def _fallback_to_keywords(self, title: str, summary: str) -> Dict:
        """
        降级到关键词匹配进行情感分析
        :param title: 新闻标题
        :param summary: 新闻摘要
        :return: 包含情感分、逻辑分类、影响逻辑简评的字典
        """
        # 合并标题和摘要进行分析
        text = title + ' ' + summary if summary else title
        
        # 使用关键词匹配计算情感分
        sentiment_score = self.analyze_sentiment(text)
        
        # 根据关键词推断逻辑分类
        logic_category = self._infer_logic_category(text)
        
        # 生成影响逻辑简评
        impact_summary = self._generate_impact_summary(sentiment_score, logic_category)
        
        return {
            'sentiment_score': sentiment_score,
            'logic_category': logic_category,
            'impact_summary': impact_summary
        }
    
    def _infer_logic_category(self, text: str) -> str:
        """
        根据文本内容推断逻辑分类
        :param text: 文本内容
        :return: 逻辑分类（基本面/资金面/消息面）
        """
        # 基本面关键词
        fundamental_keywords = ['业绩', '营收', '利润', '财报', '盈利', '亏损', '增长', '下滑', '营收', '毛利率', '净利率']
        # 资金面关键词
        capital_keywords = ['涨停', '跌停', '封板', '放量', '缩量', '资金', '主力', '机构', '外资', '北向', '流入', '流出', '成交', '换手']
        # 消息面关键词
        news_keywords = ['政策', '公告', '消息', '传闻', '报道', '新闻', '公告', '通知', '声明', '发布']
        
        fundamental_count = sum(1 for kw in fundamental_keywords if kw in text)
        capital_count = sum(1 for kw in capital_keywords if kw in text)
        news_count = sum(1 for kw in news_keywords if kw in text)
        
        if fundamental_count >= capital_count and fundamental_count >= news_count:
            return '基本面'
        elif capital_count >= fundamental_count and capital_count >= news_count:
            return '资金面'
        else:
            return '消息面'
    
    def _generate_impact_summary(self, sentiment_score: float, logic_category: str) -> str:
        """
        生成影响逻辑简评
        :param sentiment_score: 情感得分
        :param logic_category: 逻辑分类
        :return: 影响逻辑简评（50字以内）
        """
        if sentiment_score > 0.5:
            impact = "强烈利好"
        elif sentiment_score > 0.2:
            impact = "利好"
        elif sentiment_score > -0.2:
            impact = "中性"
        elif sentiment_score > -0.5:
            impact = "利空"
        else:
            impact = "强烈利空"
        
        category_map = {
            '基本面': '基本面驱动',
            '资金面': '资金面驱动',
            '消息面': '消息面驱动'
        }
        
        summary = f"{category_map.get(logic_category, '消息面驱动')}，{impact}"
        
        # 确保不超过50字
        if len(summary) > 50:
            summary = summary[:50]
        
        return summary
    
    def _adjust_for_financial_terms(self, base_score: float, text: str) -> float:
        """
        根据金融特定术语调整情感得分（非极端词汇）
        :param base_score: 基础情感得分
        :param text: 分析的文本
        :return: 调整后的情感得分
        """
        adjusted_score = base_score
        
        # 使用更高效的方式检测术语
        if not hasattr(self, '_terms_regex'):
            # 将长术语放在前面，避免部分匹配
            sorted_terms = sorted(self.financial_terms_weights.keys(), key=len, reverse=True)
            terms_pattern = '|'.join(re.escape(term) for term in sorted_terms)
            self._terms_regex = re.compile(terms_pattern)
        
        # 一次性查找所有匹配的术语
        matched_terms = self._terms_regex.findall(text)
        
        for term in matched_terms:
            weight = self.financial_terms_weights[term]
            logger.debug(f"检测到金融术语: {term}，权重: {weight}")
            adjusted_score += weight
        
        # 确保得分在-1到1之间
        adjusted_score = max(-1.0, min(1.0, adjusted_score))
        return adjusted_score
    
    def analyze_sentiment(self, text: str) -> float:
        """
        分析文本的金融情感（基于关键词匹配）
        :param text: 要分析的文本
        :return: 情感得分（-1到1之间，负值表示负面，正值表示正面）
        """
        # 检查是否为垃圾信息
        if self._is_spam(text):
            logger.warning(f"文本被识别为垃圾信息，跳过情感分析: {text[:50]}...")
            return 0.0  # 垃圾信息返回中性得分
        
        # 预处理层：检查极端金融词汇
        extreme_weight, extreme_term = self._check_extreme_terms(text)
        if extreme_term:
            return extreme_weight
        
        # 检查缓存
        text_hash = self._get_text_hash(text)
        if text_hash in self.sentiment_cache:
            self.cache_hits += 1
            logger.debug(f"缓存命中: {text[:30]}...")
            return self.sentiment_cache[text_hash]
        
        self.cache_misses += 1
        
        # 基于关键词匹配的情感分析
        positive_keywords = ['利好', '增长', '上涨', '突破', '创新高', '业绩', '盈利', '增持', '回购', '政策支持', '超预期', '打板', '封板', '涨停']
        negative_keywords = ['利空', '下跌', '下滑', '创新低', '亏损', '减持', '政策收紧', '低于预期', '跌停', '崩盘', '闪崩', '破位']
        
        # 计算情感得分
        sentiment_score = 0.0
        
        # 检查正面关键词
        for keyword in positive_keywords:
            if keyword in text:
                sentiment_score += 0.3
        
        # 检查负面关键词
        for keyword in negative_keywords:
            if keyword in text:
                sentiment_score -= 0.3
        
        # 根据金融术语调整得分
        sentiment_score = self._adjust_for_financial_terms(sentiment_score, text)
        
        # 确保得分在-1到1之间
        sentiment_score = max(-1.0, min(1.0, sentiment_score))
        
        # 缓存结果
        self._manage_cache_size(self.sentiment_cache, self.cache_max_size)
        self.sentiment_cache[text_hash] = sentiment_score
        
        logger.debug(f"情感分析完成: {text[:30]}... 得分: {sentiment_score:.3f}")
        return sentiment_score
    
    def analyze_batch(self, texts: List[str]) -> List[float]:
        """
        批量分析文本的金融情感
        :param texts: 要分析的文本列表
        :return: 情感得分列表
        """
        results = []
        for text in texts:
            score = self.analyze_sentiment(text)
            results.append(score)
        return results
    
    def generate_news_data(self, dates, stock_code=None):
        """
        获取真实的新闻数据和情感得分
        :param dates: 日期序列
        :param stock_code: 股票代码，如 "600519"
        :return: 包含日期、新闻标题和情感得分的DataFrame
        """
        if stock_code is None:
            logger.warning("未提供股票代码，无法获取真实新闻数据")
            return pd.DataFrame(columns=["日期", "新闻标题", "情感得分"])
        
        try:
            from crawlers.stock_data import get_stock_news
            
            logger.info(f"获取股票 {stock_code} 的真实新闻数据")
            
            df_news = get_stock_news(symbol=stock_code, days=90)
            
            if df_news.empty:
                logger.warning(f"未获取到股票 {stock_code} 的新闻数据")
                return pd.DataFrame(columns=["日期", "新闻标题", "情感得分"])
            
            df_news = df_news.rename(columns={'date': '日期', 'title': '新闻标题'})
            
            if 'content' in df_news.columns:
                texts = df_news['content'].fillna('').tolist()
            else:
                texts = df_news['新闻标题'].fillna('').tolist()
            
            scores = [self.analyze_sentiment(text) for text in texts]
            df_news['情感得分'] = scores
            
            df_news['日期'] = pd.to_datetime(df_news['日期']).dt.normalize()
            
            logger.info(f"成功获取 {len(df_news)} 条真实新闻数据")
            return df_news
            
        except Exception as e:
            logger.error(f"获取真实新闻数据失败: {e}")
            return pd.DataFrame(columns=["日期", "新闻标题", "情感得分"])

# 创建全局实例供外部使用
sentiment_analyzer = FinancialSentimentAnalyzer()

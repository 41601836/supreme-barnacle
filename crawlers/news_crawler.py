import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
import random
import time
from datetime import datetime, timedelta
from typing import Optional

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class NewsCrawler:
    """
    东方财富股吧爬虫类，用于获取指定股票的热门帖子和评论
    """
    
    def __init__(self, max_retries=3, retry_delay=2):
        """
        初始化爬虫
        :param max_retries: 最大重试次数
        :param retry_delay: 重试基础延迟（秒）
        """
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        
        # 创建session并强制禁用代理
        self.session = requests.Session()
        self.session.proxies.update({'http': None, 'https': None})
        
        # 添加Cookie支持
        self.session.cookies.set_cookie(
            requests.cookies.create_cookie(
                domain='eastmoney.com',
                name='em_cookie',
                value='test'
            )
        )
        
        logger.info("============================================================")
        logger.info("东方财富股吧爬虫初始化")
        logger.info("============================================================")
        logger.info("代理设置: 已强制禁用 (http=None, https=None)")
        logger.info("说明: 即使开启全局代理，也不会影响国内网站爬取")
        logger.info("============================================================")
        
        # User-Agent列表，用于反爬
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/89.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/90.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Safari/605.1.15",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        ]
        
        # 东方财富股吧URL模板
        self.board_url_template = "http://guba.eastmoney.com/list,{}{}_1.html"
        self.post_url_template = "http://guba.eastmoney.com/news,{}{},{}.html"
    
    def _get_random_headers(self):
        """
        获取随机User-Agent的请求头
        :return: 请求头字典
        """
        return {
            "User-Agent": random.choice(self.user_agents),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Cache-Control": "max-age=0",
            "Referer": "http://guba.eastmoney.com/"
        }
    
    def _with_retry(self, func, *args, **kwargs):
        """
        带重试机制的函数调用
        :param func: 要调用的函数
        :param args: 函数参数
        :param kwargs: 函数关键字参数
        :return: 函数返回值
        """
        for i in range(self.max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.warning(f"尝试 {i+1}/{self.max_retries} 失败: {e}")
                if i < self.max_retries - 1:
                    wait_time = self.retry_delay + random.uniform(0, 2)  # 增加随机延迟避免被封
                    logger.info(f"等待 {wait_time:.2f} 秒后重试...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"达到最大重试次数，获取数据失败")
                    raise
    
    def _parse_post_date(self, date_str):
        """
        解析帖子发布时间
        :param date_str: 时间字符串，如 "12-23 15:30" 或 "2023-12-23 15:30"
        :return: datetime对象
        """
        try:
            # 尝试解析带年份的格式
            return datetime.strptime(date_str, "%Y-%m-%d %H:%M")
        except ValueError:
            try:
                # 尝试解析不带年份的格式，加上当前年份
                current_year = datetime.now().year
                date_with_year = f"{current_year}-{date_str}"
                return datetime.strptime(date_with_year, "%Y-%m-%d %H:%M")
            except ValueError:
                # 尝试解析其他格式
                try:
                    return datetime.strptime(date_str, "%Y-%m-%d")
                except ValueError:
                    logger.error(f"无法解析日期格式: {date_str}")
                    return datetime.now()
    
    def _get_board_id(self, stock_code):
        """
        根据股票代码获取股吧板块ID
        :param stock_code: 股票代码
        :return: 板块ID字符串
        """
        # 根据股票代码前缀判断市场
        if stock_code.startswith("6"):
            # 沪市A股
            return f"{stock_code},1"
        elif stock_code.startswith("0") or stock_code.startswith("3"):
            # 深市A股或创业板
            return f"{stock_code},2"
        else:
            # 默认返回沪市
            return f"{stock_code},1"
    
    def _extract_json_from_page(self, text):
        """
        从页面中提取JSON数据，尝试多种可能的模式
        :param text: 页面文本
        :return: 提取到的JSON数据，如果找不到返回None
        """
        import json
        import re
        
        # 打印页面前500字符用于调试
        logger.info(f"页面前500字符:\n{text[:500]}")
        
        # 尝试多种可能的JavaScript变量名和模式
        patterns = [
            # 常见的变量名模式
            (r'var\s+article_list\s*=\s*(\{.*?\});', 'article_list'),
            (r'var\s+articleList\s*=\s*(\{.*?\});', 'articleList'),
            (r'var\s+post_list\s*=\s*(\{.*?\});', 'post_list'),
            (r'var\s+postList\s*=\s*(\{.*?\});', 'postList'),
            (r'var\s+data\s*=\s*(\{.*?\});', 'data'),
            (r'window\.__data__\s*=\s*(\{.*?\});', '__data__'),
            (r'window\.articleList\s*=\s*(\{.*?\});', 'window.articleList'),
            
            # 尝试匹配包含 "re" 或 "list" 的JSON对象
            (r'"re"\s*:\s*\[', 're_array'),
            (r'"list"\s*:\s*\[', 'list_array'),
            (r'"data"\s*:\s*\[', 'data_array'),
            (r'"items"\s*:\s*\[', 'items_array'),
        ]
        
        for pattern, name in patterns:
            try:
                if name in ['re_array', 'list_array', 'data_array', 'items_array']:
                    # 对于数组模式，尝试提取整个JSON对象
                    match = re.search(pattern, text, re.DOTALL)
                    if match:
                        # 向前查找JSON对象的开始
                        start_pos = match.start()
                        # 向后查找JSON对象的结束
                        brace_count = 0
                        in_string = False
                        escape = False
                        for i in range(start_pos, -1, -1):
                            char = text[i]
                            if escape:
                                escape = False
                                continue
                            if char == '\\':
                                escape = True
                                continue
                            if char == '"':
                                in_string = not in_string
                                continue
                            if not in_string:
                                if char == '}':
                                    brace_count += 1
                                elif char == '{':
                                    brace_count -= 1
                                    if brace_count < 0:
                                        json_start = i
                                        break
                        
                        # 向后查找JSON对象的结束
                        brace_count = 1
                        in_string = False
                        escape = False
                        for i in range(start_pos + match.end() - start_pos, len(text)):
                            char = text[i]
                            if escape:
                                escape = False
                                continue
                            if char == '\\':
                                escape = True
                                continue
                            if char == '"':
                                in_string = not in_string
                                continue
                            if not in_string:
                                if char == '{':
                                    brace_count += 1
                                elif char == '}':
                                    brace_count -= 1
                                    if brace_count == 0:
                                        json_end = i + 1
                                        break
                        
                        json_str = text[json_start:json_end]
                        logger.info(f"找到模式 '{name}'，尝试解析JSON...")
                        return json.loads(json_str)
                else:
                    # 对于变量名模式，直接匹配
                    match = re.search(pattern, text, re.DOTALL)
                    if match:
                        json_str = match.group(1)
                        logger.info(f"找到变量 '{name}'，尝试解析JSON...")
                        return json.loads(json_str)
            except Exception as e:
                logger.debug(f"模式 '{name}' 解析失败: {e}")
                continue
        
        logger.warning("所有JSON提取模式均失败")
        return None
    
    def _parse_posts_from_html(self, soup, stock_code):
        """
        从HTML中解析帖子列表（fallback方法）
        :param soup: BeautifulSoup对象
        :param stock_code: 股票代码
        :return: 帖子列表
        """
        posts = []
        
        # 尝试多种可能的CSS选择器
        selectors = [
            'div.note-item',
            'div.post-item',
            'div.article-item',
            'li.normal_post',
            'div[class*="note"]',
            'div[class*="post"]',
            'div[class*="article"]',
        ]
        
        for selector in selectors:
            items = soup.select(selector)
            if items:
                logger.info(f"使用选择器 '{selector}' 找到 {len(items)} 个帖子元素")
                break
        else:
            logger.warning("未找到任何帖子元素")
            return posts
        
        market_prefix = "1" if stock_code.startswith("6") else "2"
        
        for item in items:
            try:
                # 提取标题和链接
                title_elem = item.find('a')
                if not title_elem:
                    continue
                
                title = title_elem.text.strip()
                link = title_elem.get('href', '')
                
                # 提取帖子ID
                import re
                post_id_match = re.search(r'(\d+)\.html', link)
                if not post_id_match:
                    continue
                post_id = post_id_match.group(1)
                
                # 构建完整链接
                if not link.startswith('http'):
                    link = f"http://guba.eastmoney.com/news,{stock_code},{market_prefix},{post_id}.html"
                
                # 提取发布时间
                time_elem = item.find('span', class_='time') or item.find('span', class_='date')
                time_str = time_elem.text.strip() if time_elem else ''
                post_date = self._parse_post_date(time_str) if time_str else datetime.now()
                
                # 提取阅读量
                view_elem = item.find('span', class_='view') or item.find('span', class_='read')
                view_count = int(view_elem.text.strip()) if view_elem else 0
                
                # 提取评论数
                comment_elem = item.find('span', class_='comment') or item.find('span', class_='reply')
                comment_count = int(comment_elem.text.strip()) if comment_elem else 0
                
                posts.append({
                    "post_id": post_id,
                    "title": title,
                    "date": post_date,
                    "view_count": view_count,
                    "comment_count": comment_count,
                    "link": link
                })
                
            except Exception as e:
                logger.error(f"解析HTML帖子元素失败: {e}")
                continue
        
        return posts
    
    def _is_flash_verification_page(self, text):
        """
        检测是否为Flash验证页面
        :param text: 页面文本
        :return: True如果是验证页面，False否则
        """
        flash_indicators = [
            'flashcookie.swf',
            '1.1.1.3:89',
            'setTimeout("location.replace',
            'clsid:d27cdb6e-ae6d-11cf-96b8-444553540000'
        ]
        
        for indicator in flash_indicators:
            if indicator in text:
                return True
        return False
    
    def _get_posts_list(self, stock_code, days=7):
        """
        获取帖子列表
        :param stock_code: 股票代码
        :param days: 要获取的天数
        :return: 帖子列表，每个元素包含帖子ID、标题、发布时间
        """
        # 修正URL构建，使用正确的板块ID格式
        market_type = "1" if stock_code.startswith("6") else "2"
        board_url = f"http://guba.eastmoney.com/list,{stock_code},{market_type}.html"
        
        posts = []
        end_date = datetime.now() - timedelta(days=days)
        page = 1
        max_pages = 1  # 最多爬取1页（优化性能）
        max_posts = 3  # 最多获取3条帖子（优化性能）
        
        logger.info(f"开始爬取股票 {stock_code} 的股吧帖子列表")
        logger.info(f"使用URL: {board_url}")
        
        while page <= max_pages:
            try:
                current_url = board_url.replace(".html", f"_{page}.html")
                logger.info(f"正在爬取第 {page} 页: {current_url}")
                logger.info(f"代理设置: 已禁用 (proxies=None)")
                
                # 发送请求获取页面内容，强制禁用代理，使用session
                response = self._with_retry(
                    self.session.get,
                    current_url,
                    headers=self._get_random_headers(),
                    timeout=10
                )
                response.encoding = "utf-8"
                
                logger.info(f"页面状态码: {response.status_code}")
                logger.info(f"页面大小: {len(response.text)} 字符")
                
                # 检测是否为Flash验证页面
                if self._is_flash_verification_page(response.text):
                    logger.warning("检测到Flash验证页面，尝试重新获取...")
                    # 等待重定向
                    import time
                    time.sleep(2)
                    continue
                
                # 尝试从JavaScript变量中提取JSON数据
                json_data = self._extract_json_from_page(response.text)
                
                if json_data:
                    # 成功提取JSON数据
                    import json
                    
                    # 尝试多种可能的键名
                    post_list = None
                    for key in ['re', 'list', 'data', 'items', 'article_list', 'postList']:
                        if key in json_data:
                            post_list = json_data[key]
                            logger.info(f"从JSON键 '{key}' 中找到帖子列表")
                            break
                    
                    if post_list and isinstance(post_list, list):
                        logger.info(f"提取到 {len(post_list)} 条帖子数据")
                        
                        for post_data in post_list:
                            try:
                                post_id = str(post_data.get('post_id', post_data.get('id', '')))
                                title = post_data.get('post_title', post_data.get('title', '')).strip()
                                
                                if not post_id or not title:
                                    continue
                                
                                # 获取发布时间
                                publish_time_str = post_data.get('post_publish_time', post_data.get('time', post_data.get('date', '')))
                                if not publish_time_str:
                                    continue
                                
                                post_date = self._parse_post_date(publish_time_str)
                                
                                # 检查是否超过时间范围
                                if post_date < end_date:
                                    logger.info(f"已获取到 {days} 天前的帖子，停止爬取")
                                    return posts
                                
                                # 获取阅读量
                                view_count = post_data.get('post_click_count', post_data.get('view_count', post_data.get('click_count', 0)))
                                
                                # 获取评论数
                                comment_count = post_data.get('post_comment_count', post_data.get('comment_count', post_data.get('reply_count', 0)))
                                
                                # 构建帖子链接
                                market_prefix = "1" if stock_code.startswith("6") else "2"
                                link = f"http://guba.eastmoney.com/news,{stock_code},{market_prefix},{post_id}.html"
                                
                                # 添加到帖子列表
                                posts.append({
                                    "post_id": post_id,
                                    "title": title,
                                    "date": post_date,
                                    "view_count": view_count,
                                    "comment_count": comment_count,
                                    "link": link
                                })
                                
                                # 检查是否已达到最大帖子数量
                                if len(posts) >= max_posts:
                                    logger.info(f"已获取到 {max_posts} 条帖子，停止爬取")
                                    return posts
                                
                            except Exception as e:
                                logger.error(f"解析帖子数据失败: {e}")
                                continue
                    else:
                        logger.warning("JSON数据中未找到帖子列表数组")
                else:
                    # JSON提取失败，尝试解析HTML
                    logger.info("JSON提取失败，尝试从HTML解析...")
                    soup = BeautifulSoup(response.text, "html.parser")
                    html_posts = self._parse_posts_from_html(soup, stock_code)
                    posts.extend(html_posts)
                    
                    if not html_posts:
                        logger.warning("HTML解析也未找到帖子")
                        break
                
                logger.info(f"已爬取第 {page} 页，获取 {len(posts)} 条帖子")
                page += 1
                
                # 添加随机延迟 2-4 秒（优化性能）
                delay = random.uniform(2, 4)
                logger.info(f"等待 {delay:.2f} 秒后继续...")
                time.sleep(delay)
                
            except Exception as e:
                logger.error(f"爬取帖子列表第 {page} 页失败: {e}")
                import traceback
                traceback.print_exc()
                page += 1
                continue
        
        logger.info(f"爬取完成，共获取 {len(posts)} 条帖子")
        return posts
    
    def _get_post_content(self, post_link):
        """
        获取帖子内容
        :param post_link: 帖子链接
        :return: 帖子内容字符串
        """
        try:
            logger.info(f"正在获取帖子内容: {post_link}")
            logger.info(f"代理设置: 已禁用 (proxies=None)")
            
            response = self._with_retry(
                requests.get,
                post_link,
                headers=self._get_random_headers(),
                timeout=10,
                proxies=None
            )
            response.encoding = "utf-8"
            
            logger.info(f"帖子页状态码: {response.status_code}")
            logger.info(f"帖子页大小: {len(response.text)} 字符")
            
            soup = BeautifulSoup(response.text, "html.parser")
            
            # 尝试多种可能的内容选择器
            content_selectors = [
                'div.stockcodec',
                'div.article-content',
                'div.post-content',
                'div.note-content',
                'div[class*="content"]',
                'div[id*="content"]',
            ]
            
            content = ""
            for selector in content_selectors:
                content_div = soup.select_one(selector)
                if content_div:
                    logger.info(f"使用选择器 '{selector}' 找到内容区域")
                    
                    # 移除无关标签
                    for tag in content_div.find_all(["script", "style", "iframe", "noscript"]):
                        tag.decompose()
                    
                    content = content_div.text.strip()
                    
                    # 清理多余空白
                    import re
                    content = re.sub(r'\s+', ' ', content)
                    content = content.strip()
                    
                    break
            else:
                logger.warning("未找到帖子内容区域")
            
            logger.info(f"帖子内容长度: {len(content)} 字符")
            
            # 添加随机延迟 5-10 秒
            delay = random.uniform(5, 10)
            logger.info(f"等待 {delay:.2f} 秒后继续...")
            time.sleep(delay)
            
            return content
        except Exception as e:
            logger.error(f"获取帖子内容失败 {post_link}: {e}")
            import traceback
            traceback.print_exc()
            return ""
    
    def get_stock_news(self, stock_code: str, days: int = 7) -> pd.DataFrame:
        """
        获取指定股票最近N天的股吧帖子
        :param stock_code: 股票代码，如 "600519"
        :param days: 要获取的天数，默认7天
        :return: 包含帖子信息的DataFrame，列包括：date, title, content, stock_code
        """
        try:
            logger.info(f"开始获取股票 {stock_code} 最近 {days} 天的股吧新闻")
            
            # 获取帖子列表
            posts = self._get_posts_list(stock_code, days)
            
            if not posts:
                logger.info(f"未获取到股票 {stock_code} 的股吧帖子")
                return pd.DataFrame(columns=["date", "title", "content", "stock_code"])
            
            # 获取每个帖子的内容
            news_data = []
            for i, post in enumerate(posts):
                logger.info(f"正在获取第 {i+1}/{len(posts)} 条帖子内容")
                content = self._get_post_content(post["link"])
                
                news_data.append({
                    "date": post["date"],
                    "title": post["title"],
                    "content": content,
                    "stock_code": stock_code
                })
            
            # 创建DataFrame
            df = pd.DataFrame(news_data)
            
            # 按日期排序
            df = df.sort_values("date", ascending=False)
            
            logger.info(f"成功获取股票 {stock_code} 的 {len(df)} 条股吧新闻")
            return df
        
        except Exception as e:
            logger.error(f"获取股票 {stock_code} 的股吧新闻失败: {e}")
            # 返回空DataFrame
            return pd.DataFrame(columns=["date", "title", "content", "stock_code"])

# 创建全局实例供外部使用
news_crawler = NewsCrawler()
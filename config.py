"""
配置文件
存储 API 密钥和配置信息
"""

import os
from typing import Optional
from dotenv import load_dotenv

# 加载 .env 文件
load_dotenv()


class Config:
    """
    配置类
    从环境变量或配置文件读取配置
    """
    
    # Tushare Pro Token
    TUSHARE_TOKEN: Optional[str] = os.getenv('TUSHARE_TOKEN')
    
    # DeepSeek API Key
    DEEPSEEK_API_KEY: Optional[str] = os.getenv('DEEPSEEK_API_KEY')
    
    # 数据源类型 ('tushare' 或 'akshare')
    DATA_SOURCE: str = os.getenv('DATA_SOURCE', 'tushare')
    
    # 数据库路径
    DB_PATH: str = os.getenv('DB_PATH', 'data/stock_data.db')
    
    # 日志级别
    LOG_LEVEL: str = os.getenv('LOG_LEVEL', 'INFO')
    
    @classmethod
    def validate(cls) -> bool:
        """
        验证配置是否有效
        
        :return: 是否有效
        """
        if cls.DATA_SOURCE == 'tushare' and not cls.TUSHARE_TOKEN:
            print("错误: 使用 Tushare 数据源需要设置 TUSHARE_TOKEN 环境变量")
            print("请运行: export TUSHARE_TOKEN='your_token_here'")
            return False
        
        return True
    
    @classmethod
    def print_config(cls):
        """打印当前配置"""
        print("=" * 60)
        print("当前配置:")
        print("=" * 60)
        print(f"数据源: {cls.DATA_SOURCE}")
        print(f"Tushare Token: {'已设置' if cls.TUSHARE_TOKEN else '未设置'}")
        print(f"DeepSeek API Key: {'已设置' if cls.DEEPSEEK_API_KEY else '未设置'}")
        print(f"数据库路径: {cls.DB_PATH}")
        print(f"日志级别: {cls.LOG_LEVEL}")
        print("=" * 60)

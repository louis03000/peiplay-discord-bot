"""
Discord Bot 配置管理
集中管理所有配置項，支持環境變數覆蓋
"""
import os
from typing import Dict, List, Any
from dotenv import load_dotenv

# 載入環境變數
load_dotenv()

class Config:
    """配置類，統一管理所有設定"""
    
    # Discord 基本設定
    DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')
    GUILD_ID = int(os.getenv('DISCORD_GUILD_ID', '0'))
    ADMIN_CHANNEL_ID = int(os.getenv('ADMIN_CHANNEL_ID', '0'))
    ADMIN_USER_ID = int(os.getenv('ADMIN_USER_ID', '0'))
    
    # 資料庫設定
    DATABASE_URL = os.getenv('DATABASE_URL')
    
    # 頻道設定
    BOOKING_CATEGORY_NAME = os.getenv('BOOKING_CATEGORY_NAME', '預約頻道')
    INSTANT_BOOKING_CATEGORY_NAME = os.getenv('INSTANT_BOOKING_CATEGORY_NAME', '即時預約')
    
    # 任務頻率設定（秒）
    CHECK_BOOKINGS_INTERVAL = int(os.getenv('CHECK_BOOKINGS_INTERVAL', '180'))  # 3分鐘
    CLEANUP_CHANNELS_INTERVAL = int(os.getenv('CLEANUP_CHANNELS_INTERVAL', '300'))  # 5分鐘
    AUTO_CLOSE_AVAILABLE_INTERVAL = int(os.getenv('AUTO_CLOSE_AVAILABLE_INTERVAL', '600'))  # 10分鐘
    
    # 重試設定
    MAX_RETRY_ATTEMPTS = int(os.getenv('MAX_RETRY_ATTEMPTS', '3'))
    RETRY_DELAY_BASE = int(os.getenv('RETRY_DELAY_BASE', '30'))  # 基礎延遲秒數
    
    # 評價系統設定
    RATING_DIMENSIONS = [
        {
            'key': 'overall',
            'name': '整體滿意度',
            'type': 'star',
            'required': True
        },
        {
            'key': 'communication',
            'name': '溝通流暢度',
            'type': 'star',
            'required': False
        },
        {
            'key': 'skill',
            'name': '技術水平',
            'type': 'star',
            'required': False
        }
    ]
    
    # 時區設定
    TIMEZONE_OFFSET = int(os.getenv('TIMEZONE_OFFSET', '8'))  # UTC+8 (台灣時間)
    
    # 頻道命名模板
    CHANNEL_NAME_TEMPLATES = {
        'booking': '📅{date} {time}-{end_time} {partner_name}',
        'instant': '⚡即時{date} {time}-{end_time} {partner_name}',
        'text': '📅{date} {time}-{end_time} {partner_name}'
    }
    
    # 通知設定
    NOTIFICATION_SETTINGS = {
        'enable_admin_alerts': True,
        'enable_user_dm': True,
        'enable_voice_channel_alerts': True
    }
    
    @classmethod
    def validate(cls) -> bool:
        """驗證必要配置項"""
        required_configs = [
            ('DISCORD_TOKEN', cls.DISCORD_TOKEN),
            ('DISCORD_GUILD_ID', cls.GUILD_ID),
            ('ADMIN_CHANNEL_ID', cls.ADMIN_CHANNEL_ID),
            ('DATABASE_URL', cls.DATABASE_URL)
        ]
        
        missing_configs = []
        for name, value in required_configs:
            if not value or value == 0:
                missing_configs.append(name)
        
        if missing_configs:
            print(f"❌ 錯誤：缺少必要配置項: {', '.join(missing_configs)}")
            return False
        
        return True
    
    @classmethod
    def get_rating_dimensions(cls) -> List[Dict[str, Any]]:
        """獲取評價維度配置"""
        return cls.RATING_DIMENSIONS
    
    @classmethod
    def get_channel_name_template(cls, channel_type: str) -> str:
        """獲取頻道命名模板"""
        return cls.CHANNEL_NAME_TEMPLATES.get(channel_type, cls.CHANNEL_NAME_TEMPLATES['booking'])
    
    @classmethod
    def is_notification_enabled(cls, notification_type: str) -> bool:
        """檢查通知類型是否啟用"""
        return cls.NOTIFICATION_SETTINGS.get(notification_type, False)

# 全局配置實例
config = Config()

import os 
import asyncio
import random
import time
import uuid
import discord
from discord.ext import commands, tasks
from discord import app_commands
from discord.ui import View, Button, Modal, TextInput
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, Boolean, Float, text
from sqlalchemy.orm import declarative_base, sessionmaker, relationship, joinedload
from datetime import datetime, timedelta, timezone
from flask import Flask, request, jsonify
import threading
import io
import requests

# --- 環境與資料庫設定 ---
load_dotenv()
TOKEN = os.getenv("DISCORD_BOT_TOKEN")
GUILD_ID = int(os.getenv("DISCORD_GUILD_ID", "0"))
POSTGRES_CONN = os.getenv("POSTGRES_CONN")
ADMIN_CHANNEL_ID = int(os.getenv("ADMIN_CHANNEL_ID", "1419601068110778450"))

# 調試資訊（已隱藏，保持終端乾淨）
# print("環境變數檢查:")
# print(f"   ADMIN_CHANNEL_ID: {ADMIN_CHANNEL_ID}")

# 檢查必要的環境變數
if not TOKEN:
    print("❌ 錯誤：未設定 DISCORD_BOT_TOKEN 環境變數")
    print("請在 .env 檔案中設定您的 Discord bot token")
    exit(1)

if not POSTGRES_CONN:
    print("❌ 錯誤：未設定 POSTGRES_CONN 環境變數")
    print("請在 .env 檔案中設定資料庫連線字串")
    exit(1)
CHANNEL_CREATION_CHANNEL_ID = int(os.getenv("CHANNEL_CREATION_CHANNEL_ID", "1410318589348810923"))  # 創建頻道通知頻道
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "30"))  # 檢查間隔（秒）

# --- 標準化 Discord 用戶名的函數（去除尾隨空格、下劃線和點號）---
def normalize_discord_username(username: str) -> str:
    """標準化 Discord 用戶名，去除尾隨空格、下劃線和點號"""
    if not username:
        return ""
    # 去除尾隨空格、下劃線和點號
    return username.rstrip().rstrip('_').rstrip('.')

Base = declarative_base()

# 資料庫連接初始化函數
def create_db_engine():
    """創建資料庫引擎，使用適合 Supabase 的連接池配置"""
    return create_engine(
    POSTGRES_CONN,
        pool_size=3,           # 減少連接數，避免 Supabase 連接限制
        max_overflow=5,        # 減少溢出連接
        pool_pre_ping=True,    # 自動重連，在每次使用前檢查連接
        pool_recycle=300,      # 5分鐘後回收連接（Supabase 通常會在10分鐘後關閉閒置連接）
        pool_timeout=20,       # 連接超時20秒
        connect_args={
            "connect_timeout": 10,  # 連接超時10秒
            "keepalives": 1,        # 啟用 TCP keepalive
            "keepalives_idle": 30,  # 30秒後開始發送 keepalive
            "keepalives_interval": 10,  # 每10秒發送一次 keepalive
            "keepalives_count": 3,  # 最多3次 keepalive 失敗後關閉連接
        },
    echo=False
)

# 初始化資料庫連接
engine = create_db_engine()
Session = sessionmaker(bind=engine)
session = Session()

def reconnect_database():
    """重新建立資料庫連接"""
    global engine, Session, session, db_connection_error_reported
    try:
        # 關閉舊連接
        if engine:
            engine.dispose()
        # 重新創建引擎和 Session
        engine = create_db_engine()
        Session = sessionmaker(bind=engine)
        session = Session()
        # 🔥 連接成功時重置錯誤報告標誌
        db_connection_error_reported = False
        return True
    except Exception as e:
        # 🔥 連接失敗時不輸出錯誤（由調用者處理）
        return False

# --- 統一的資料庫連線管理 ---
def is_db_connection_error(error):
    """
    判斷是否為資料庫連線相關錯誤
    """
    import psycopg2
    from sqlalchemy.exc import OperationalError, DisconnectionError, TimeoutError as SQLTimeoutError
    
    # 檢查錯誤類型
    if isinstance(error, (psycopg2.OperationalError, psycopg2.InterfaceError, 
                          psycopg2.DatabaseError, OperationalError, DisconnectionError,
                          SQLTimeoutError, ConnectionError, TimeoutError)):
        error_msg = str(error).lower()
        # 檢查是否為連線相關錯誤
        return any(keyword in error_msg for keyword in [
            'connection', 'dns', 'timeout', 'closed', 'broken', 
            'network', 'unreachable', 'refused', 'reset', 'lost',
            'server closed', 'connection pool', 'could not connect'
        ])
    return False

def safe_db_execute(operation_func, *args, **kwargs):
    """
    統一的資料庫操作包裝函數，安全處理所有連線錯誤
    發生錯誤時安全跳過，不讓 bot 崩潰
    """
    max_retries = 1  # 只重試一次，避免無限重試
    
    for attempt in range(max_retries + 1):
        try:
            return operation_func(*args, **kwargs)
        except Exception as e:
            if is_db_connection_error(e):
                if attempt < max_retries:
                    # 嘗試重新連接
                    try:
                        reconnect_database()
                        time.sleep(0.5)
                        continue
                    except:
                        pass
                # 達到最大重試次數或重連失敗，安全跳過該輪檢查
                return None
            else:
                # 非連線錯誤，也安全跳過
                return None
    
    return None

# --- 資料庫模型（對應 Prisma schema）---
class User(Base):
    __tablename__ = 'User'
    id = Column(String, primary_key=True)
    email = Column(String)
    name = Column(String)
    discord = Column(String)  # 已經在註冊時設定
    role = Column(String)
    createdAt = Column(DateTime)
    updatedAt = Column(DateTime)

class Partner(Base):
    __tablename__ = 'Partner'
    id = Column(String, primary_key=True)
    name = Column(String)
    userId = Column(String, ForeignKey('User.id'))
    user = relationship("User")
    createdAt = Column(DateTime)
    updatedAt = Column(DateTime)

class Customer(Base):
    __tablename__ = 'Customer'
    id = Column(String, primary_key=True)
    name = Column(String)
    userId = Column(String, ForeignKey('User.id'))
    user = relationship("User")
    createdAt = Column(DateTime)
    updatedAt = Column(DateTime)

class Schedule(Base):
    __tablename__ = 'Schedule'
    id = Column(String, primary_key=True)
    partnerId = Column(String, ForeignKey('Partner.id'))
    date = Column(DateTime)
    startTime = Column(DateTime)
    endTime = Column(DateTime)
    isAvailable = Column(Boolean, default=True)
    partner = relationship("Partner")
    createdAt = Column(DateTime)
    updatedAt = Column(DateTime)

class Booking(Base):
    __tablename__ = 'Booking'
    id = Column(String, primary_key=True)
    customerId = Column(String, ForeignKey('Customer.id'))
    scheduleId = Column(String, ForeignKey('Schedule.id'))
    status = Column(String)  # BookingStatus
    orderNumber = Column(String, nullable=True)  # 可選欄位
    paymentInfo = Column(String, nullable=True)  # JSON string
    createdAt = Column(DateTime, default=datetime.utcnow)
    updatedAt = Column(DateTime, default=datetime.utcnow)
    finalAmount = Column(Float, nullable=True)
    # 新增欄位
    isInstantBooking = Column(Boolean, default=False)
    partnerResponseDeadline = Column(DateTime, nullable=True)
    isWaitingPartnerResponse = Column(Boolean, default=False)
    serviceType = Column(String, default="GAMING")
    groupBookingId = Column(String, nullable=True)
    tenMinuteReminderShown = Column(Boolean, default=False)
    extensionButtonShown = Column(Boolean, default=False)
    ratingCompleted = Column(Boolean, default=False)
    textChannelCleaned = Column(Boolean, default=False)
    discordTextChannelId = Column(String, nullable=True)
    discordVoiceChannelId = Column(String, nullable=True)
    customer = relationship("Customer")
    schedule = relationship("Schedule")

class PairingRecord(Base):
    __tablename__ = 'PairingRecord'
    id = Column(String, primary_key=True)  # 改為 String 類型，對應 Prisma 的 cuid
    user1Id = Column('user1Id', String)
    user2Id = Column('user2Id', String)
    timestamp = Column(DateTime, default=datetime.utcnow)
    extendedTimes = Column('extendedTimes', Integer, default=0)
    duration = Column(Integer, default=0)
    rating = Column(Integer, nullable=True)
    comment = Column(String, nullable=True)
    animalName = Column('animalName', String)
    bookingId = Column('bookingId', String, nullable=True)  # 關聯到預約ID
    createdAt = Column('createdAt', DateTime, default=datetime.utcnow)
    updatedAt = Column('updatedAt', DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class GroupBooking(Base):
    __tablename__ = "GroupBooking"
    
    id = Column(String, primary_key=True)
    type = Column(String)  # USER_INITIATED, PARTNER_INITIATED
    title = Column(String)
    description = Column(String)
    date = Column(DateTime)
    startTime = Column(DateTime)
    endTime = Column(DateTime)
    maxParticipants = Column(Integer, default=10)
    currentParticipants = Column(Integer, default=0)
    pricePerPerson = Column(Float)
    totalPrice = Column(Float)
    status = Column(String, default='ACTIVE')  # ACTIVE, COMPLETED, CANCELLED, FULL
    createdAt = Column(DateTime, default=datetime.utcnow)
    updatedAt = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    initiatorId = Column(String)
    initiatorType = Column(String)  # USER, PARTNER

class GroupBookingParticipant(Base):
    __tablename__ = "GroupBookingParticipant"
    
    id = Column(String, primary_key=True)
    groupBookingId = Column(String)
    customerId = Column(String)
    partnerId = Column(String)
    status = Column(String, default='ACTIVE')  # ACTIVE, CANCELLED, COMPLETED
    joinedAt = Column(DateTime, default=datetime.utcnow)

class GroupBookingReview(Base):
    __tablename__ = "GroupBookingReview"
    
    id = Column(String, primary_key=True)
    groupBookingId = Column(String)
    reviewerId = Column(String)
    rating = Column(Integer)
    comment = Column(String)
    createdAt = Column(DateTime, default=datetime.utcnow)
    isApproved = Column(Boolean, default=False)

class BlockRecord(Base):
    __tablename__ = 'block_records'
    id = Column(Integer, primary_key=True)
    blocker_id = Column(String)
    blocked_id = Column(String)

# 不自動創建表，因為我們使用的是現有的 Prisma 資料庫
# Base.metadata.create_all(engine)

intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True
intents.members = True
intents.voice_states = True

bot = commands.Bot(command_prefix="!", intents=intents)
active_voice_channels = {}
evaluated_records = set()
pending_ratings = {}
processed_bookings = set()  # 記錄已處理的預約
processed_text_channels = set()  # 記錄已創建文字頻道的預約
rating_sent_bookings = set()  # 追蹤已發送評價系統的預約
rating_submitted_users = {}  # 追蹤每個記錄的已提交評價用戶 {record_id: set(user_ids)}
active_countdown_tasks = set()  # 追蹤已啟動的倒數計時任務 {booking_id}
active_voice_channel_tasks = set()  # 追蹤已啟動的語音頻道創建任務 {booking_id}
rating_text_channels = {}  # 追蹤每個記錄的文字頻道 {record_id: text_channel}
rating_channel_created_time = {}  # 追蹤每個記錄的文字頻道創建時間 {record_id: timestamp}
group_rating_text_channels = {}  # 追蹤群組預約評價的文字頻道 {group_booking_id: text_channel}
group_rating_channel_created_time = {}  # 追蹤群組預約評價的文字頻道創建時間 {group_booking_id: timestamp}
db_connection_error_reported = False  # 追蹤是否已報告資料庫連接錯誤（避免重複輸出）
sent_reminders = set()  # 追蹤已發送的提醒，防止重複發送 {(booking_id, reminder_type)}

# 可愛的動物和物品列表
CUTE_ITEMS = ["🦊 狐狸", "🐱 貓咪", "🐶 小狗", "🐻 熊熊", "🐼 貓熊", "🐯 老虎", "🦁 獅子", "🐸 青蛙", "🐵 猴子", "🐰 兔子", "🦄 獨角獸", "🐙 章魚", "🦋 蝴蝶", "🌸 櫻花", "⭐ 星星", "🌈 彩虹", "🍀 幸運草", "🎀 蝴蝶結", "🍭 棒棒糖", "🎈 氣球"]
TW_TZ = timezone(timedelta(hours=8))

# --- 成員搜尋函數 ---
def find_member_by_discord_name(guild, discord_name):
    """根據 Discord 名稱搜尋成員（支持多種匹配方式）"""
    if not discord_name:
        return None
    
    # 🔥 驗證 Discord ID 類型：必須為 str 或 int，不能為 float 或 None
    if isinstance(discord_name, float):
        print(f"❌ 錯誤：Discord ID 類型錯誤，收到 float 類型: {discord_name}")
        return None
    
    if not isinstance(discord_name, (str, int)):
        print(f"❌ 錯誤：Discord ID 類型錯誤，必須為 str 或 int，收到: {type(discord_name).__name__} = {discord_name}")
        return None
    
    # 🔥 改進：支持多種匹配方式
    discord_name_lower = discord_name.lower().strip() if isinstance(discord_name, str) else str(discord_name).lower().strip()
    
    # 1. 先嘗試精確匹配（名稱或顯示名稱，大小寫不敏感）
    for member in guild.members:
        if member.name.lower() == discord_name_lower or (member.display_name and member.display_name.lower() == discord_name_lower):
            return member
    
    # 1.5. 嘗試精確匹配（原始大小寫，處理特殊情況如 "0.08377"）
    for member in guild.members:
        if member.name == discord_name or (member.display_name and member.display_name == discord_name):
            return member
    
    # 1.6. 🔥 新增：移除下劃線和點號後匹配（處理如 "Louis0088" 匹配 "louis0088_" 的情況）
    discord_name_clean = discord_name_lower.replace('_', '').replace('.', '').replace('-', '')
    for member in guild.members:
        member_name_clean = member.name.lower().replace('_', '').replace('.', '').replace('-', '')
        member_display_clean = (member.display_name.lower() if member.display_name else "").replace('_', '').replace('.', '').replace('-', '')
        if (member_name_clean == discord_name_clean or member_display_clean == discord_name_clean):
            print(f"✅ 通過清理特殊字符匹配找到成員: {member.name} (查詢: {discord_name})")
            return member
    
    # 2. 🔥 優先匹配前綴（處理 Discord 名稱後綴，如 louis0099._03864 匹配 Louis0099）
    # 提取查詢名稱的字母數字部分（去除特殊字符，但保留小數點和數字）
    # 🔥 對於包含小數點的用戶名（如 "0.08377"），不要移除小數點，直接使用原始名稱匹配
    discord_name_alphanumeric = ''.join(c for c in discord_name_lower if c.isalnum())
    # 🔥 如果原始名稱包含小數點且看起來像用戶名（不是 ID），也嘗試直接匹配
    if '.' in discord_name and len(discord_name.replace('.', '').replace('-', '')) < 17:
        # 這是包含小數點的用戶名（如 "0.08377"），嘗試直接匹配
        for member in guild.members:
            if member.name == discord_name or (member.display_name and member.display_name == discord_name):
                return member
    
    if discord_name_alphanumeric and len(discord_name_alphanumeric) >= 3:
        for member in guild.members:
            # 提取成員名稱的字母數字部分（大小寫不敏感）
            member_name_alphanumeric = ''.join(c for c in member.name.lower() if c.isalnum())
            member_display_alphanumeric = ''.join(c for c in (member.display_name.lower() if member.display_name else "") if c.isalnum())
            
            # 🔥 改進前綴匹配：雙向匹配，處理各種情況
            # 情況1：查詢名稱是成員名稱的前綴（如 "louis0099" 匹配 "louis0099._03864"）
            # 情況2：成員名稱是查詢名稱的前綴（如 "louis" 匹配 "louis0099"）
            # 情況3：兩者完全相同（字母數字部分）
            # 情況4：查詢名稱包含在成員名稱中（如 "louis0099" 在 "louis0099._03864" 中）
            if (member_name_alphanumeric.startswith(discord_name_alphanumeric) or
                member_display_alphanumeric.startswith(discord_name_alphanumeric) or
                discord_name_alphanumeric.startswith(member_name_alphanumeric) or
                discord_name_alphanumeric.startswith(member_display_alphanumeric) or
                member_name_alphanumeric == discord_name_alphanumeric or
                member_display_alphanumeric == discord_name_alphanumeric or
                discord_name_alphanumeric in member_name_alphanumeric or
                discord_name_alphanumeric in member_display_alphanumeric):
                print(f"✅ 通過前綴匹配找到成員: {member.name} (查詢: {discord_name})")
                return member
    
    # 2.5. 🔥 新增：使用清理後的名稱進行前綴匹配（處理下劃線和點號）
    if discord_name_clean and len(discord_name_clean) >= 3:
        for member in guild.members:
            member_name_clean = member.name.lower().replace('_', '').replace('.', '').replace('-', '')
            member_display_clean = (member.display_name.lower() if member.display_name else "").replace('_', '').replace('.', '').replace('-', '')
            
            # 雙向前綴匹配
            if (member_name_clean.startswith(discord_name_clean) or
                member_display_clean.startswith(discord_name_clean) or
                discord_name_clean.startswith(member_name_clean) or
                discord_name_clean.startswith(member_display_clean) or
                member_name_clean == discord_name_clean or
                member_display_clean == discord_name_clean):
                print(f"✅ 通過清理後前綴匹配找到成員: {member.name} (查詢: {discord_name})")
                return member
    
    # 3. 嘗試部分匹配（名稱或顯示名稱包含）
    for member in guild.members:
        if discord_name_lower in member.name.lower() or (member.display_name and discord_name_lower in member.display_name.lower()):
            return member
    
    # 4. 嘗試匹配 Discord ID（如果 discord_name 是數字或包含小數點）
    # 🔥 注意：如果 discord_name 包含小數點但看起來像用戶名（如 "0.08377"），不應該當作 ID 處理
    # 只有在看起來像真正的 Discord ID（18-19 位數字）時才嘗試 ID 匹配
    try:
        # 純數字 ID（18-19 位數字，Discord ID 的標準長度）
        if discord_name.replace('-', '').isdigit() and len(discord_name.replace('-', '')) >= 17:
            member = guild.get_member(int(discord_name.replace('-', '')))
            if member:
                return member
    except (ValueError, TypeError, AttributeError):
        pass
    
    # 5. 嘗試只匹配字母和數字（移除特殊字符，更寬鬆的匹配）
    if discord_name_alphanumeric and len(discord_name_alphanumeric) >= 3:  # 至少3個字符才進行匹配
        for member in guild.members:
            member_name_alphanumeric = ''.join(c for c in member.name.lower() if c.isalnum())
            member_display_alphanumeric = ''.join(c for c in (member.display_name.lower() if member.display_name else "") if c.isalnum())
            
            # 雙向匹配：查詢名稱包含成員名稱，或成員名稱包含查詢名稱
            if (discord_name_alphanumeric in member_name_alphanumeric or 
                discord_name_alphanumeric in member_display_alphanumeric or
                member_name_alphanumeric in discord_name_alphanumeric or
                member_display_alphanumeric in discord_name_alphanumeric):
                return member
    
    # 6. 如果都找不到，記錄詳細日誌
    print(f"❌ 找不到 Discord 成員: {discord_name}")
    # 列出前10個成員作為調試信息
    member_list = [f"{m.name} (ID: {m.id})" for m in list(guild.members)[:10]]
    if member_list:
        print(f"   調試：伺服器中的部分成員: {', '.join(member_list)}")
    
    return None

# --- 429 安全創建文字頻道（僅替換創建文字頻道，不影響其他 Discord API）---
# 若 Render 因 terminal 輸出過多觸發 Cloudflare 1015，可適度減少他處 print 頻率或本函式內日誌。
async def safe_create_text_channel(guild, name, **kwargs):
    """
    創建文字頻道。遇 Discord API 429 時依 retry_after 等待後重試，其他錯誤照常拋出。
    即時預約、純聊天、多人陪玩、群組預約等皆透過此函式創建文字頻道，避免 Render 上大量創建觸發限速。
    """
    max_retries = 5
    for attempt in range(max_retries):
        try:
            return await guild.create_text_channel(name=name, **kwargs)
        except discord.HTTPException as e:
            if e.status == 429:
                wait = getattr(e, 'retry_after', 5.0)
                if not isinstance(wait, (int, float)) or wait <= 0:
                    wait = 5.0
                wait = min(float(wait), 60.0)  # 最多等 60 秒
                if attempt < max_retries - 1:
                    print(f"⚠️ Discord API 429 限速，等待 {wait:.1f} 秒後重試創建文字頻道...")
                    await asyncio.sleep(wait)
                else:
                    print(f"❌ 創建文字頻道 429，已重試 {max_retries} 次，放棄")
                    raise
            else:
                raise
    return None  # unreachable

# --- 創建預約文字頻道函數 ---
async def create_booking_text_channel(booking_id, customer_discord, partner_discord, start_time, end_time):
    """為預約創建文字頻道"""
    try:
        guild = bot.get_guild(GUILD_ID)
        if not guild:
            print("❌ 找不到 Discord 伺服器")
            return None
        
        # 查找 Discord 成員
        customer_member = None
        partner_member = None
        
        # 處理顧客 Discord ID
        if customer_discord:
            try:
                if customer_discord.replace('.', '').replace('-', '').isdigit():
                    # 如果是數字格式的 ID
                    customer_member = guild.get_member(int(float(customer_discord)))
                else:
                    # 如果是名稱格式
                    customer_member = find_member_by_discord_name(guild, customer_discord)
            except (ValueError, TypeError):
                # 靜默處理無效的 Discord ID
                customer_member = None
        
        # 處理夥伴 Discord ID
        if partner_discord:
            try:
                if partner_discord.replace('.', '').replace('-', '').isdigit():
                    # 如果是數字格式的 ID
                    partner_member = guild.get_member(int(float(partner_discord)))
                else:
                    # 如果是名稱格式
                    partner_member = find_member_by_discord_name(guild, partner_discord)
            except (ValueError, TypeError):
                # 靜默處理無效的 Discord ID
                partner_member = None
        
        if not customer_member or not partner_member:
            print(f"❌ 找不到 Discord 成員: 顧客={customer_discord}, 夥伴={partner_discord}")
            return None
        
        # 計算頻道持續時間
        duration_minutes = int((end_time - start_time).total_seconds() / 60)
        
        # 創建頻道名稱 - 使用日期和時間
        # 確保時間有時區資訊，並轉換為台灣時間
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)
        
        # 轉換為台灣時間
        tw_start_time = start_time.astimezone(TW_TZ)
        tw_end_time = end_time.astimezone(TW_TZ)
        
        # 格式化日期和時間
        date_str = tw_start_time.strftime("%m%d")  # 改為 1016 格式
        start_time_str = tw_start_time.strftime("%H:%M")
        end_time_str = tw_end_time.strftime("%H:%M")
        
        # 調試日誌
        
        # 🔥 創建統一的頻道名稱 - 使用 booking ID 來生成一致的 emoji（與語音頻道相同）
        import hashlib
        hash_obj = hashlib.md5(str(booking_id).encode())
        hash_hex = hash_obj.hexdigest()
        cute_item_full = CUTE_ITEMS[int(hash_hex[:2], 16) % len(CUTE_ITEMS)]
        # 只提取 emoji 部分（去掉後面的文字）
        cute_item = cute_item_full.split()[0] if cute_item_full else "🎀"
        channel_name = f"📅{date_str} {start_time_str}-{end_time_str} {cute_item}"
        
        # 設定權限
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            customer_member: discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True),
            partner_member: discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True),
        }
        
        # 找到分類
        category = discord.utils.get(guild.categories, name="Text Channels")
        if not category:
            category = discord.utils.get(guild.categories, name="文字頻道")
        if not category:
            category = discord.utils.get(guild.categories, name="文字")
        if not category:
            if guild.categories:
                category = guild.categories[0]
            else:
                print("❌ 找不到任何分類")
                return None
        
        # 創建文字頻道（429 安全）
        text_channel = await safe_create_text_channel(
            guild,
            name=channel_name,
            overwrites=overwrites,
            category=category
        )
        
        # 發送歡迎訊息 - 修正時區顯示
        # 確保時間有時區資訊，並轉換為台灣時間
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)
        
        # 轉換為台灣時間
        tw_start_time = start_time.astimezone(TW_TZ)
        tw_end_time = end_time.astimezone(TW_TZ)
        
        start_time_str = tw_start_time.strftime("%Y/%m/%d %H:%M")
        end_time_str = tw_end_time.strftime("%H:%M")
        
        embed = discord.Embed(
            title=f"🎮 預約頻道",
            description=f"歡迎來到預約頻道！\n\n"
                       f"📅 **預約時間**: {start_time_str} - {end_time_str}\n"
                       f"⏰ **時長**: {duration_minutes} 分鐘\n"
                       f"👤 **顧客**: {customer_member.mention}\n"
                       f"👥 **夥伴**: {partner_member.mention}\n\n"
                       f"💬 你們可以在這裡提前溝通\n"
                       f"🎤 語音頻道將在預約開始前 3 分鐘自動創建",
            color=0x00ff00
        )
        
        await text_channel.send(embed=embed)
        
        # 發送安全規範
        safety_embed = discord.Embed(
            title="🎙️ 聊天頻道使用規範與警告",
            description="為了您的安全，請務必遵守以下規範：",
            color=0xff6b6b,
            timestamp=datetime.now(timezone.utc)
        )
        
        safety_embed.add_field(
            name="📌 頻道性質",
            value="此語音頻道為【單純聊天用途】。\n僅限輕鬆互動、日常話題、遊戲閒聊使用。\n禁止任何涉及交易、暗示、或其他非聊天用途的行為。",
            inline=False
        )
        
        safety_embed.add_field(
            name="⚠️ 使用規範（請務必遵守）",
            value="• 禁止挑釁、辱罵、騷擾他人，保持禮貌尊重\n"
                  "• 禁止使用色情、暴力、血腥、歧視等不當言語或內容\n"
                  "• 不得進行金錢交易、索取或提供個資（例如 LINE、IG、電話）\n"
                  "• 不得錄音、偷拍或截圖他人對話，除非經雙方同意\n"
                  "• 禁止語音假裝、惡意模仿或干擾他人聊天\n"
                  "• 禁止使用變聲器或播放音效干擾頻道秩序",
            inline=False
        )
        
        safety_embed.add_field(
            name="🚨 警告事項",
            value="• 系統將隨機錄取部分語音內容以進行安全稽核\n"
                  "• 如被舉報違規，管理員可立即封鎖或禁言，不另行通知\n"
                  "• 為了您的安全，禁止隨意透漏個人資訊，包括(身分證、住家地址、等等......)\n"
                  "• 若你無法接受以上規範，請勿加入頻道",
            inline=False
        )
        
        await text_channel.send(embed=safety_embed)
        
        # 發送預約通知到指定頻道
        notification_channel = bot.get_channel(1419585779432423546)
        if notification_channel:
            notification_embed = discord.Embed(
                title="🎉 新預約通知",
                description="新的預約已創建！",
                color=0x00ff00,
                timestamp=datetime.now(timezone.utc)
            )
            
            # 第一行：時間和參與者
            notification_embed.add_field(
                name="📅 預約時間",
                value=f"`{start_time_str} - {end_time_str}`",
                inline=True
            )
            notification_embed.add_field(
                name="👥 參與者",
                value=f"{customer_member.mention} × {partner_member.mention}",
                inline=True
            )
            notification_embed.add_field(
                name="💬 溝通頻道",
                value=f"{text_channel.mention}",
                inline=True
            )
            
            # 第二行：時長和語音頻道
            notification_embed.add_field(
                name="⏰ 時長",
                value=f"`{duration_minutes} 分鐘`",
                inline=True
            )
            notification_embed.add_field(
                name="🎤 語音頻道",
                value="`將在預約開始前 5 分鐘自動創建`",
                inline=True
            )
            notification_embed.add_field(
                name="🆔 預約ID",
                value=f"`{booking_id}`",
                inline=True
            )
            
            await notification_channel.send(embed=notification_embed)
            # 已發送預約通知，減少日誌輸出
            
        
        # 保存頻道 ID 到資料庫
        try:
            with Session() as s:
                # 先檢查欄位是否存在
                check_column = s.execute(text("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'Booking' 
                    AND column_name = 'discordTextChannelId'
                """)).fetchone()
                
                if check_column:
                    # 更新預約記錄，保存 Discord 頻道 ID
                    result = s.execute(
                        text("UPDATE \"Booking\" SET \"discordTextChannelId\" = :channel_id WHERE id = :booking_id"),
                        {"channel_id": str(text_channel.id), "booking_id": booking_id}
                    )
                    s.commit()
                    # 已保存文字頻道ID，減少日誌輸出
                else:
                    print(f"⚠️ Discord 欄位尚未創建，跳過保存頻道 ID")
        except Exception as db_error:
            print(f"❌ 保存頻道 ID 到資料庫失敗: {db_error}")
            # 即使保存失敗，頻道仍然可以使用
        
        # 通知創建頻道頻道
        channel_creation_channel = bot.get_channel(CHANNEL_CREATION_CHANNEL_ID)
        if channel_creation_channel:
            await channel_creation_channel.send(
                f"📝 預約文字頻道已創建：\n"
                f"📋 預約ID: {booking_id}\n"
                f"👤 顧客: {customer_member.mention} ({customer_discord})\n"
                f"👥 夥伴: {partner_member.mention} ({partner_discord})\n"
                f"⏰ 時間: {start_time_str} - {end_time_str}\n"
                f"💬 頻道: {text_channel.mention}"
            )
        
        # 頻道創建成功，減少日誌輸出
        return text_channel
        
    except Exception as e:
        print(f"❌ 創建預約文字頻道時發生錯誤: {e}")
        return None

# --- 創建預約語音頻道函數 ---
async def create_group_booking_voice_channel(group_booking_id, customer_discord, partner_discords, start_time, end_time, is_multiplayer=False):
    """為群組預約或多人陪玩創建語音頻道"""
    try:
        # ✅ 統一判斷依據：根據 is_multiplayer 檢查對應的資料表
        with Session() as s:
            if is_multiplayer:
                # ✅ 多人陪玩：檢查 MultiPlayerBooking 表
                existing = s.execute(text("""
                    SELECT "discordVoiceChannelId" 
                    FROM "MultiPlayerBooking" 
                    WHERE id = :booking_id
                """), {'booking_id': group_booking_id}).fetchone()
            else:
                # 群組預約：檢查 GroupBooking 表
                existing = s.execute(text("""
                    SELECT "discordVoiceChannelId" 
                    FROM "GroupBooking" 
                    WHERE id = :group_id
                """), {'group_id': group_booking_id}).fetchone()
            
            if existing and existing[0]:
                # 檢查頻道是否真的存在
                guild = bot.get_guild(GUILD_ID)
                if guild:
                    existing_channel = guild.get_channel(int(existing[0]))
                    if existing_channel:
                        return existing_channel
        
        guild = bot.get_guild(GUILD_ID)
        if not guild:
            print("❌ 找不到 Discord 伺服器")
            return None
        
        # 查找 Discord 成員
        customer_member = find_member_by_discord_name(guild, customer_discord)
        partner_members = []
        failed_partners = []
        
        for partner_discord in partner_discords:
            partner_member = find_member_by_discord_name(guild, partner_discord)
            if partner_member:
                partner_members.append(partner_member)
            else:
                failed_partners.append(partner_discord)
        
        # 🔥 如果找不到成員，先檢查是否已經有頻道存在（使用一開始創建的頻道）
        if not customer_member or not partner_members:
            # 計算頻道名稱以查找已存在的頻道
            if start_time.tzinfo is None:
                start_time_temp = start_time.replace(tzinfo=timezone.utc)
            else:
                start_time_temp = start_time
            if end_time.tzinfo is None:
                end_time_temp = end_time.replace(tzinfo=timezone.utc)
            else:
                end_time_temp = end_time
            
            tw_start_time_temp = start_time_temp.astimezone(TW_TZ)
            tw_end_time_temp = end_time_temp.astimezone(TW_TZ)
            
            date_str_temp = tw_start_time_temp.strftime("%m%d")
            start_time_str_temp = tw_start_time_temp.strftime("%H:%M")
            end_time_str_temp = tw_end_time_temp.strftime("%H:%M")
            
            import hashlib
            hash_obj = hashlib.md5(str(group_booking_id).encode())
            hash_hex = hash_obj.hexdigest()
            cute_item_temp = CUTE_ITEMS[int(hash_hex[:2], 16) % len(CUTE_ITEMS)]
            
            if is_multiplayer:
                channel_name_temp = f"👥多人陪玩{date_str_temp} {start_time_str_temp}-{end_time_str_temp} {cute_item_temp}"
            else:
                channel_name_temp = f"👥群組預約{date_str_temp} {start_time_str_temp}-{end_time_str_temp} {cute_item_temp}"
            
            # 檢查是否已存在相同名稱的頻道
            existing_channels = [ch for ch in guild.voice_channels if ch.name == channel_name_temp]
            if existing_channels:
                return existing_channels[0]
            
            # 如果找不到成員且沒有已存在的頻道，不創建新頻道
            return None
        
        # 🔥 如果部分夥伴找不到，仍然創建頻道，但記錄警告
        if failed_partners:
            print(f"⚠️ 部分夥伴找不到 Discord 成員: {failed_partners}")
        
        # 計算頻道持續時間
        duration_minutes = int((end_time - start_time).total_seconds() / 60)
        
        # 創建頻道名稱
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)
        
        tw_start_time = start_time.astimezone(TW_TZ)
        tw_end_time = end_time.astimezone(TW_TZ)
        
        date_str = tw_start_time.strftime("%m%d")
        start_time_str = tw_start_time.strftime("%H:%M")
        end_time_str = tw_end_time.strftime("%H:%M")
        
        # 🔥 使用 group_booking_id 的 hash 來確定性地選擇動物，確保文字和語音頻道使用相同的動物
        import hashlib
        hash_obj = hashlib.md5(str(group_booking_id).encode())
        hash_hex = hash_obj.hexdigest()
        cute_item = CUTE_ITEMS[int(hash_hex[:2], 16) % len(CUTE_ITEMS)]
        # ✅ 頻道命名修正：多人陪玩使用「多人陪玩」，群組預約使用「群組預約」
        if is_multiplayer:
            channel_name = f"👥多人陪玩{date_str} {start_time_str}-{end_time_str} {cute_item}"
        else:
            channel_name = f"👥群組預約{date_str} {start_time_str}-{end_time_str} {cute_item}"
        
        # ✅ 再次檢查資料庫（防止在檢查和創建之間有其他進程創建了頻道）
        with Session() as s:
            if is_multiplayer:
                # ✅ 多人陪玩：檢查 MultiPlayerBooking 表
                existing_check = s.execute(text("""
                    SELECT "discordVoiceChannelId" 
                    FROM "MultiPlayerBooking" 
                    WHERE id = :booking_id
                """), {'booking_id': group_booking_id}).fetchone()
            else:
                # 群組預約：檢查 GroupBooking 表
                existing_check = s.execute(text("""
                    SELECT "discordVoiceChannelId" 
                    FROM "GroupBooking" 
                    WHERE id = :group_id
                """), {'group_id': group_booking_id}).fetchone()
            
            if existing_check and existing_check[0]:
                existing_channel = guild.get_channel(int(existing_check[0]))
                if existing_channel:
                    # 只在第一次檢查時打印，避免重複日誌
                    return existing_channel
        
        # ✅ 檢查是否已存在相同名稱的語音頻道（防止重複創建）
        existing_channels = [ch for ch in guild.voice_channels if ch.name == channel_name]
        if existing_channels:
            # 如果找到相同名稱的頻道，更新資料庫
            channel_type = "多人陪玩" if is_multiplayer else "群組預約"
            print(f"⚠️ 已存在相同名稱的{channel_type}語音頻道: {channel_name}，更新資料庫並返回現有頻道")
            with Session() as s:
                if is_multiplayer:
                    # ✅ 多人陪玩：更新 MultiPlayerBooking 表
                    s.execute(text("""
                        UPDATE "MultiPlayerBooking"
                        SET "discordVoiceChannelId" = :channel_id
                        WHERE id = :booking_id
                    """), {'channel_id': str(existing_channels[0].id), 'booking_id': group_booking_id})
                else:
                    # 群組預約：更新 GroupBooking 表
                    s.execute(text("""
                        UPDATE "GroupBooking"
                        SET "discordVoiceChannelId" = :channel_id
                        WHERE id = :group_id
                    """), {'channel_id': str(existing_channels[0].id), 'group_id': group_booking_id})
                s.commit()
            return existing_channels[0]
        
        # 設置權限 - 包含顧客和所有夥伴
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            customer_member: discord.PermissionOverwrite(view_channel=True, connect=True, speak=True),
        }
        
        for partner_member in partner_members:
            overwrites[partner_member] = discord.PermissionOverwrite(view_channel=True, connect=True, speak=True)
        
        category = discord.utils.get(guild.categories, name="Voice Channels")
        if not category:
            category = discord.utils.get(guild.categories, name="語音頻道")
        if not category:
            category = discord.utils.get(guild.categories, name="語音")
        if not category:
            category = guild.categories[0] if guild.categories else None
        
        # 創建語音頻道
        vc = await guild.create_voice_channel(
            name=channel_name, 
            overwrites=overwrites, 
            user_limit=len(partner_members) + 1,  # 顧客 + 夥伴數量
            category=category
        )
        
        # ✅ 創建配對記錄（檢查是否已存在）- 群組預約和多人陪玩都需要
        user1_id = str(customer_member.id)
        # ✅ 多人陪玩：使用第一個夥伴作為 user2_id（用於配對記錄）
        user2_id = str(partner_members[0].id) if partner_members else None
        
        record_id = None
        if user2_id:
            with Session() as s:
                try:
                    # 🔥 先檢查是否已經有配對記錄
                    existing_record = s.execute(text("""
                        SELECT id 
                        FROM "PairingRecord" 
                        WHERE "bookingId" = :booking_id
                    """), {'booking_id': group_booking_id}).fetchone()
                    
                    if existing_record:
                        record_id = existing_record[0]
                        print(f"⚠️ 配對記錄已存在: {record_id}，跳過創建")
                    else:
                        import uuid
                        record_id = f"group_{uuid.uuid4().hex[:12]}"
                        # ✅ 多人陪玩使用「多人陪玩」作為 animalName，群組預約使用「群組預約」
                        animal_name = "多人陪玩" if is_multiplayer else "群組預約"
                        record = PairingRecord(
                            id=record_id,
                            user1Id=user1_id,
                            user2Id=user2_id,
                            duration=duration_minutes * 60,
                            animalName=animal_name,
                            bookingId=group_booking_id
                        )
                        s.add(record)
                        s.commit()
                        created_at = record.createdAt
                        print(f"✅ 創建配對記錄: {record_id} ({animal_name})")
                except Exception as e:
                    print(f"❌ 創建配對記錄失敗: {e}")
                    try:
                        record_id = "temp_" + str(int(time.time()))
                    except:
                        record_id = None
        
        # 記錄活躍語音頻道
        active_voice_channels[vc.id] = {
            'remaining': duration_minutes * 60,
            'start_time': start_time,
            'end_time': end_time,
            'members': [customer_member] + partner_members,
            'record_id': record_id,
            'booking_id': group_booking_id,
            'extended': 0,
            'is_group_booking': True,
            'partner_count': len(partner_members)
        }
        
        # 發送通知
        channel_creation_channel = bot.get_channel(CHANNEL_CREATION_CHANNEL_ID)
        if channel_creation_channel:
            group_embed = discord.Embed(
                title="👥 多人陪玩語音頻道已創建" if is_multiplayer else "👥 群組預約語音頻道已創建",
                color=0x9b59b6,
                timestamp=datetime.now(timezone.utc)
            )
            
            group_embed.add_field(
                name="🆔 群組預約ID",
                value=f"`{group_booking_id}`",
                inline=True
            )
            
            group_embed.add_field(
                name="👤 顧客",
                value=f"{customer_member.mention}\n`{customer_discord}`",
                inline=True
            )
            
            partner_mentions = [partner.mention for partner in partner_members]
            group_embed.add_field(
                name="👥 夥伴們",
                value="\n".join(partner_mentions),
                inline=False
            )
            
            group_embed.add_field(
                name="⏰ 開始時間",
                value=f"`{tw_start_time.strftime('%Y/%m/%d %H:%M')}`",
                inline=True
            )
            
            group_embed.add_field(
                name="⏱️ 時長",
                value=f"`{duration_minutes} 分鐘`",
                inline=True
            )
            
            group_embed.add_field(
                name="🎮 頻道",
                value=f"{vc.mention}",
                inline=True
            )
            
            group_embed.add_field(
                name="👥 人數上限",
                value=f"`{len(partner_members) + 1} 人`",
                inline=False
            )
            
            await channel_creation_channel.send(embed=group_embed)
        
        # ✅ 更新資料庫中的語音頻道ID（根據 is_multiplayer 更新對應的資料表）
        with Session() as s:
            try:
                if is_multiplayer:
                    # ✅ 多人陪玩：更新 MultiPlayerBooking 表
                    s.execute(text("""
                        UPDATE "MultiPlayerBooking" 
                        SET "discordVoiceChannelId" = :channel_id
                        WHERE id = :booking_id
                    """), {
                        'channel_id': str(vc.id),
                        'booking_id': group_booking_id
                    })
                else:
                    # 群組預約：更新 GroupBooking 表
                    s.execute(text("""
                        UPDATE "GroupBooking" 
                        SET "discordVoiceChannelId" = :channel_id
                        WHERE id = :group_id
                    """), {
                        'channel_id': str(vc.id),
                        'group_id': group_booking_id
                    })
                s.commit()
            except Exception as e:
                channel_type = "多人陪玩" if is_multiplayer else "群組預約"
                print(f"⚠️ 更新{channel_type}語音頻道ID失敗: {e}")
                s.rollback()
        
        return vc
        
    except Exception as e:
        print(f"❌ 創建群組預約語音頻道失敗: {e}")
        return None

async def create_group_booking_text_channel(group_booking_id, customer_discords, partner_discords, start_time, end_time, is_multiplayer=False):
    """為群組預約或多人陪玩創建文字頻道
    
    Args:
        group_booking_id: 群組預約ID或多人陪玩ID
        customer_discords: 顧客 Discord ID 列表（有付費記錄的人）
        partner_discords: 夥伴 Discord ID 列表（提供服務的人）
        start_time: 開始時間
        end_time: 結束時間
        is_multiplayer: 是否為多人陪玩（用於區分命名和資料表）
    """
    try:
        # ✅ 統一判斷依據：根據 is_multiplayer 檢查對應的資料表
        with Session() as s:
            if is_multiplayer:
                # ✅ 多人陪玩：檢查 MultiPlayerBooking 表
                existing = s.execute(text("""
                    SELECT "discordTextChannelId" 
                    FROM "MultiPlayerBooking" 
                    WHERE id = :booking_id
                """), {'booking_id': group_booking_id}).fetchone()
            else:
                # 群組預約：檢查 GroupBooking 表
                existing = s.execute(text("""
                    SELECT "discordTextChannelId" 
                    FROM "GroupBooking" 
                    WHERE id = :group_id
                """), {'group_id': group_booking_id}).fetchone()
            
            if existing and existing[0]:
                # 檢查頻道是否真的存在
                guild = bot.get_guild(GUILD_ID)
                if guild:
                    existing_channel = guild.get_channel(int(existing[0]))
                    if existing_channel:
                        channel_type = "多人陪玩" if is_multiplayer else "群組預約"
                        print(f"⚠️ {channel_type}文字頻道已存在: {existing_channel.name} (ID: {existing_channel.id})，跳過創建")
                        return existing_channel
        
        guild = bot.get_guild(GUILD_ID)
        if not guild:
            print("❌ 找不到 Discord 伺服器")
            return None
        
        # 🔥 使用 group_booking_id 的 hash 來確定性地選擇動物，確保文字和語音頻道使用相同的動物
        import hashlib
        hash_obj = hashlib.md5(str(group_booking_id).encode())
        hash_hex = hash_obj.hexdigest()
        animal = CUTE_ITEMS[int(hash_hex[:2], 16) % len(CUTE_ITEMS)]
        # ✅ 頻道命名修正：多人陪玩使用「多人陪玩聊天」，群組預約使用「群組預約聊天」
        if is_multiplayer:
            channel_name = f"👥{animal}多人陪玩聊天"
        else:
            channel_name = f"👥{animal}群組預約聊天"
        
        # 檢查是否已存在相同名稱的文字頻道（防止重複創建）
        existing_channels = [ch for ch in guild.text_channels if ch.name == channel_name]
        if existing_channels:
            channel_type = "多人陪玩" if is_multiplayer else "群組預約"
            print(f"⚠️ {channel_type}文字頻道已存在: {channel_name}，跳過創建")
            return existing_channels[0]
        
        # 查找所有顧客成員
        customer_members = []
        for customer_discord in customer_discords:
            customer_member = find_member_by_discord_name(guild, customer_discord)
            if customer_member:
                customer_members.append(customer_member)
            else:
                print(f"⚠️ 找不到顧客: {customer_discord}")
        
        # 查找所有夥伴成員
        partner_members = []
        for partner_discord in partner_discords:
            partner_member = find_member_by_discord_name(guild, partner_discord)
            if partner_member:
                partner_members.append(partner_member)
            else:
                print(f"⚠️ 找不到夥伴: {partner_discord}")
        
        # 🔥 如果找不到成員，不創建新頻道（使用一開始創建的頻道）
        if not customer_members:
            channel_type = "多人陪玩" if is_multiplayer else "群組預約"
            print(f"❌ 找不到任何顧客，且沒有已存在的{channel_type}文字頻道，跳過創建")
            return None
        
        # 即使沒有夥伴也創建文字頻道（至少可以發送評價系統）
        if not partner_members:
            print("⚠️ 找不到任何夥伴，但仍會創建文字頻道（供評價系統使用）")
        
        # 轉換為台灣時間
        # 處理 start_time 可能是字符串或 datetime 對象
        if isinstance(start_time, str):
            # 如果是字符串，解析它
            if start_time.endswith('Z'):
                start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            elif '+' in start_time or start_time.count('-') >= 3:
                # 已經包含時區信息
                start_dt = datetime.fromisoformat(start_time)
            else:
                # 假設是 UTC 時間（沒有時區信息）
                start_dt = datetime.fromisoformat(start_time + '+00:00')
        else:
            # 如果是 datetime 對象
            start_dt = start_time
            if start_dt.tzinfo is None:
                # 如果沒有時區信息，假設是 UTC
                start_dt = start_dt.replace(tzinfo=timezone.utc)
        
        # 轉換為台灣時間（UTC+8）
        tw_start_time = start_dt.astimezone(TW_TZ)
        
        # 處理結束時間
        if isinstance(end_time, str):
            if end_time.endswith('Z'):
                end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
            elif '+' in end_time or end_time.count('-') >= 3:
                end_dt = datetime.fromisoformat(end_time)
            else:
                end_dt = datetime.fromisoformat(end_time + '+00:00')
        else:
            end_dt = end_time
            if end_dt.tzinfo is None:
                end_dt = end_dt.replace(tzinfo=timezone.utc)
        
        tw_end_time = end_dt.astimezone(TW_TZ)
        
        # 創建分類
        category = discord.utils.get(guild.categories, name="Voice Channels")
        if not category:
            category = discord.utils.get(guild.categories, name="語音頻道")
        if not category:
            category = discord.utils.get(guild.categories, name="語音")
        if not category:
            if guild.categories:
                category = guild.categories[0]
            else:
                print("❌ 找不到任何分類")
                return None
        
        # 設定權限
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
        }
        
        # 為所有顧客添加權限
        for customer_member in customer_members:
            overwrites[customer_member] = discord.PermissionOverwrite(view_channel=True, send_messages=True)
        
        # 為所有夥伴添加權限
        for partner_member in partner_members:
            overwrites[partner_member] = discord.PermissionOverwrite(view_channel=True, send_messages=True)
        
        # 創建文字頻道（429 安全）
        text_channel = await safe_create_text_channel(
            guild,
            name=channel_name,
            category=category,
            overwrites=overwrites
        )
        
        # 發送歡迎訊息（根據類型切換文案：群組預約 / 多人陪玩）
        booking_type_name = "多人陪玩" if is_multiplayer else "群組預約"
        id_label = "📋 多人陪玩ID" if is_multiplayer else "📋 群組預約ID"
        welcome_embed = discord.Embed(
            title=f"🎮 {booking_type_name}聊天頻道",
            description=f"歡迎來到{booking_type_name}聊天頻道！",
            color=0x9b59b6,
            timestamp=datetime.now(timezone.utc)
        )
        
        # 顯示所有顧客
        customer_mentions = [customer.mention for customer in customer_members]
        if customer_mentions:
            welcome_embed.add_field(
                name="👤 顧客",
                value="\n".join(customer_mentions),
                inline=False
            )
        
        # 顯示所有夥伴
        partner_mentions = [partner.mention for partner in partner_members]
        if partner_mentions:
            welcome_embed.add_field(
                name="👥 夥伴們",
                value="\n".join(partner_mentions),
                inline=False
            )
        else:
            welcome_embed.add_field(
                name="👥 夥伴們",
                value="暫無其他參與者",
                inline=False
            )
        
        welcome_embed.add_field(
            name="⏰ 開始時間",
            value=f"`{tw_start_time.strftime('%Y/%m/%d %H:%M')} - {tw_end_time.strftime('%H:%M')}`",
            inline=True
        )
        
        welcome_embed.add_field(
            name=id_label,
            value=f"`{group_booking_id}`",
            inline=True
        )
        
        await text_channel.send(embed=welcome_embed)
        
        # 發送安全規範（根據類型切換文案）
        safety_title = "🎙️ 多人陪玩聊天頻道使用規範與警告" if is_multiplayer else "🎙️ 群組預約聊天頻道使用規範與警告"
        safety_channel_nature = (
            "此聊天頻道為【多人陪玩用途】。\n僅限遊戲討論、戰術交流、團隊協作使用。\n禁止任何涉及交易、暗示、或其他非遊戲用途的行為。"
            if is_multiplayer
            else "此聊天頻道為【群組預約用途】。\n僅限遊戲討論、戰術交流、團隊協作使用。\n禁止任何涉及交易、暗示、或其他非遊戲用途的行為。"
        )
        safety_embed = discord.Embed(
            title=safety_title,
            description="為了您的安全，請務必遵守以下規範：",
            color=0xff6b6b,
            timestamp=datetime.now(timezone.utc)
        )
        safety_embed.add_field(
            name="📌 頻道性質",
            value=safety_channel_nature,
            inline=False
        )
        safety_embed.add_field(
            name="⚠️ 使用規範（請務必遵守）",
            value="• 禁止挑釁、辱罵、騷擾他人，保持禮貌尊重\n"
                  "• 禁止使用色情、暴力、血腥、歧視等不當言語或內容\n"
                  "• 不得進行金錢交易、索取或提供個資（例如 LINE、IG、電話）\n"
                  "• 不得錄音、偷拍或截圖他人對話，除非經雙方同意\n"
                  "• 禁止惡意模仿或干擾他人聊天\n"
                  "• 禁止使用變聲器或播放音效干擾頻道秩序",
            inline=False
        )
        safety_embed.add_field(
            name="🚨 警告事項",
            value="• 系統將隨機錄取部分聊天內容以進行安全稽核\n"
                  "• 如被舉報違規，管理員可立即封鎖或禁言，不另行通知\n"
                  "• 為了您的安全，禁止隨意透漏個人資訊，包括(身分證、住家地址、等等......)\n"
                  "• 若你無法接受以上規範，請勿加入頻道",
            inline=False
        )
        await text_channel.send(embed=safety_embed)
        
        # 🔥 更新資料庫，保存文字頻道 ID
        try:
            with Session() as s:
                if is_multiplayer:
                    s.execute(
                        text("UPDATE \"MultiPlayerBooking\" SET \"discordTextChannelId\" = :channel_id WHERE id = :booking_id"),
                        {"channel_id": str(text_channel.id), "booking_id": group_booking_id}
                    )
                else:
                    s.execute(
                        text("UPDATE \"GroupBooking\" SET \"discordTextChannelId\" = :channel_id WHERE id = :group_id"),
                        {"channel_id": str(text_channel.id), "group_id": group_booking_id}
                    )
                s.commit()
                channel_type = "多人陪玩" if is_multiplayer else "群組預約"
                # 更新資料庫成功，終端輸出略過以減少雜訊
                # print(f"✅ 已更新{channel_type}文字頻道 ID 到資料庫: {text_channel.id}")
        except Exception as db_err:
            channel_type = "多人陪玩" if is_multiplayer else "群組預約"
            print(f"❌ 更新{channel_type}文字頻道 ID 到資料庫失敗: {db_err}")
        
        # 🔥 發送預約通知到「創建通知」頻道（與一般預約邏輯一致）
        notification_channel = bot.get_channel(1419585779432423546)
        if notification_channel:
            try:
                # 計算時長（分鐘）- 使用已轉換的台灣時間
                duration_minutes = int((tw_end_time - tw_start_time).total_seconds() / 60)
                
                notification_embed = discord.Embed(
                    title="🎉 新預約通知",
                    description="新的預約已創建！",
                    color=0x00ff00,
                    timestamp=datetime.now(timezone.utc)
                )
                
                # 第一行：時間和參與者
                # 🔥 使用已轉換的台灣時間（tw_start_time 和 tw_end_time）
                notification_embed.add_field(
                    name="📅 預約時間",
                    value=f"`{tw_start_time.strftime('%Y/%m/%d %H:%M')} - {tw_end_time.strftime('%H:%M')}`",
                    inline=True
                )
                notification_embed.add_field(
                    name="👥 參與者",
                    value=f"{customer_member.mention} × {' × '.join([p.mention for p in partner_members])}",
                    inline=True
                )
                notification_embed.add_field(
                    name="💬 溝通頻道",
                    value=f"{text_channel.mention}",
                    inline=True
                )
                
                # 第二行：時長和語音頻道
                notification_embed.add_field(
                    name="⏰ 時長",
                    value=f"`{duration_minutes} 分鐘`",
                    inline=True
                )
                notification_embed.add_field(
                    name="🎤 語音頻道",
                    value="`將在預約開始前 5 分鐘自動創建`",
                    inline=True
                )
                notification_embed.add_field(
                    name="🆔 預約ID",
                    value=f"`{group_booking_id}`",
                    inline=True
                )
                
                await notification_channel.send(embed=notification_embed)
            except Exception as e:
                print(f"⚠️ 發送群組預約通知失敗: {e}")
        else:
            print(f"⚠️ 找不到創建通知頻道 (ID: 1419585779432423546)")
        
        return text_channel
        
    except Exception as e:
        print(f"❌ 創建群組預約文字頻道失敗: {e}")
        return None

async def countdown_with_group_rating(vc_id, channel_name, text_channel, vc, members, record_id, group_booking_id, is_multiplayer=False):
    """群組預約或多人陪玩的倒數計時函數，包含評價系統
    
    Args:
        vc_id: 語音頻道 ID
        channel_name: 頻道名稱
        text_channel: 文字頻道
        vc: 語音頻道對象
        members: 參與者列表
        record_id: 配對記錄 ID
        group_booking_id: 群組預約ID或多人陪玩ID
        is_multiplayer: 是否為多人陪玩（False=群組預約，True=多人陪玩）
    """
    try:
        # 獲取 guild 對象
        guild = bot.get_guild(GUILD_ID)
        if not guild:
            print(f"❌ 找不到 Guild ID: {GUILD_ID}")
            return
        
        # 計算預約結束時間
        now = datetime.now(timezone.utc)
        
        # 🔥 根據類型從對應的資料表獲取預約開始和結束時間
        booking_type = "多人陪玩" if is_multiplayer else "群組預約"
        with Session() as s:
            if is_multiplayer:
                # 多人陪玩：從 MultiPlayerBooking 表查詢
                result = s.execute(text("""
                    SELECT mpb."startTime", mpb."endTime"
                    FROM "MultiPlayerBooking" mpb
                    WHERE mpb.id = :booking_id
                """), {"booking_id": group_booking_id}).fetchone()
            else:
                # 群組預約：從 GroupBooking 表查詢
                result = s.execute(text("""
                    SELECT gb."startTime", gb."endTime", gb."currentParticipants", gb."maxParticipants"
                    FROM "GroupBooking" gb
                    WHERE gb.id = :group_booking_id
                """), {"group_booking_id": group_booking_id}).fetchone()
            
            if not result:
                print(f"❌ 找不到{booking_type}記錄: {group_booking_id}")
                return
            
            start_time = result[0]
            end_time = result[1]
            # 群組預約才有參與者數量
            current_participants = result[2] if not is_multiplayer else None
            max_participants = result[3] if not is_multiplayer else None
        
        # 處理時區：確保時間有時區信息
        # 如果從資料庫獲取的是 naive datetime，需要轉換為 aware datetime
        if isinstance(start_time, datetime):
            if start_time.tzinfo is None:
                start_time = start_time.replace(tzinfo=timezone.utc)
        if isinstance(end_time, datetime):
            if end_time.tzinfo is None:
                end_time = end_time.replace(tzinfo=timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        
        # 計算預約總時長（秒）
        total_duration_seconds = int((end_time - start_time).total_seconds())
        total_duration_minutes = total_duration_seconds / 60
        
        # 計算剩餘時間
        remaining_seconds = int((end_time - now).total_seconds())
        
        
        if remaining_seconds <= 0:
            booking_type = "多人陪玩" if is_multiplayer else "群組預約"
            print(f"⏰ {booking_type} {group_booking_id} 已結束")
            
            # 🔥 檢查是否已經發送過評價系統（防止重複發送）
            if group_booking_id not in rating_sent_bookings:
                # 🔥 獲取參與者列表
                def get_participants(booking_id, is_mp):
                    with Session() as s:
                        if is_mp:
                            # 多人陪玩：從 Booking 表獲取參與者
                            result = s.execute(text("""
                                SELECT DISTINCT cu.discord as customer_discord, pu.discord as partner_discord
                                FROM "MultiPlayerBooking" mpb
                                JOIN "Booking" b ON b."multiPlayerBookingId" = mpb.id
                                JOIN "Customer" c ON c.id = b."customerId"
                                JOIN "User" cu ON cu.id = c."userId"
                                JOIN "Schedule" s ON s.id = b."scheduleId"
                                JOIN "Partner" p ON p.id = s."partnerId"
                                JOIN "User" pu ON pu.id = p."userId"
                                WHERE mpb.id = :booking_id
                                AND b.status IN ('CONFIRMED', 'PARTNER_ACCEPTED')
                            """), {"booking_id": booking_id}).fetchall()
                        else:
                            # 群組預約：從 GroupBooking 和 GroupBookingParticipant 獲取參與者
                            result = s.execute(text("""
                                SELECT DISTINCT cu.discord as customer_discord, pu.discord as partner_discord
                                FROM "GroupBooking" gb
                                LEFT JOIN "Booking" b ON b."groupBookingId" = gb.id
                                LEFT JOIN "Customer" c ON c.id = b."customerId"
                                LEFT JOIN "User" cu ON cu.id = c."userId"
                                LEFT JOIN "GroupBookingParticipant" gbp ON gbp."groupBookingId" = gb.id
                                LEFT JOIN "Partner" p ON p.id = gbp."partnerId"
                                LEFT JOIN "User" pu ON pu.id = p."userId"
                                WHERE gb.id = :booking_id
                            """), {"booking_id": booking_id}).fetchall()
                        
                        members = []
                        for row in result:
                            if row.customer_discord:
                                members.append(row.customer_discord)
                            if row.partner_discord:
                                members.append(row.partner_discord)
                        return list(set(members))
                
                participants = await asyncio.to_thread(get_participants, group_booking_id, is_multiplayer)
                
                # 🔥 使用 show_group_rating_system 顯示評價系統（支持多人陪玩和群組預約）
                await show_group_rating_system(text_channel, group_booking_id, participants, is_multiplayer=is_multiplayer)
                rating_sent_bookings.add(group_booking_id)
            else:
                print(f"⚠️ {booking_type} {group_booking_id} 已發送過評價系統，跳過")
            
            # 等待5分鐘讓用戶填寫評價，然後刪除文字頻道
            await asyncio.sleep(300)  # 5分鐘 = 300秒
            
            # 刪除文字頻道
            try:
                if text_channel:
                    # 🔥 使用 try-except 來檢查頻道是否已刪除，而不是檢查 deleted 屬性
                    try:
                        # 嘗試訪問頻道屬性來檢查是否還存在
                        _ = text_channel.name
                        await text_channel.delete()
                        # 清理追蹤
                        group_rating_text_channels.pop(group_booking_id, None)
                        group_rating_channel_created_time.pop(group_booking_id, None)
                    except (discord.errors.NotFound, AttributeError):
                        # 頻道已經被刪除，靜默處理
                        group_rating_text_channels.pop(group_booking_id, None)
                        group_rating_channel_created_time.pop(group_booking_id, None)
                        pass
            except Exception as e:
                print(f"❌ 刪除群組預約文字頻道失敗: {e}")
                # 即使刪除失敗，也清理追蹤
                group_rating_text_channels.pop(group_booking_id, None)
                group_rating_channel_created_time.pop(group_booking_id, None)
            return
        
        # 發送倒數提醒（只有在預約總時長超過提醒時長時才發送）
        # 10分鐘提醒：只有在總時長超過10分鐘，且剩餘時間超過10分鐘時才發送
        if total_duration_seconds > 600 and remaining_seconds > 600:  # 總時長和剩餘時間都超過10分鐘
            # 等待到結束前10分鐘
            await asyncio.sleep(remaining_seconds - 600)
            
            # 發送10分鐘提醒
            booking_type = "多人陪玩" if is_multiplayer else "群組預約"
            embed = discord.Embed(
                title=f"⏰ {booking_type}提醒",
                description=f"{booking_type}還有 10 分鐘結束，請準備結束遊戲。",
                color=0xff9900
            )
            await text_channel.send(embed=embed)
            
            # 等待剩餘的10分鐘
            remaining_seconds = 600
        
        # 5分鐘提醒：只有在總時長超過5分鐘，且剩餘時間超過5分鐘時才發送
        if total_duration_seconds > 300 and remaining_seconds > 300:  # 總時長和剩餘時間都超過5分鐘
            # 等待到結束前5分鐘
            await asyncio.sleep(remaining_seconds - 300)
            
            # 發送5分鐘提醒
            booking_type = "多人陪玩" if is_multiplayer else "群組預約"
            embed = discord.Embed(
                title=f"⏰ {booking_type}提醒",
                description=f"{booking_type}還有 5 分鐘結束，請準備結束遊戲。",
                color=0xff9900
            )
            await text_channel.send(embed=embed)
            
            # 等待剩餘的5分鐘
            remaining_seconds = 300
        
        # 1分鐘提醒：只有在總時長超過1分鐘，且剩餘時間超過1分鐘時才發送
        if total_duration_seconds > 60 and remaining_seconds > 60:  # 總時長和剩餘時間都超過1分鐘
            # 等待到結束前1分鐘
            await asyncio.sleep(remaining_seconds - 60)
            
            # 發送1分鐘提醒
            booking_type = "多人陪玩" if is_multiplayer else "群組預約"
            await text_channel.send(f"⏰ {booking_type}還有 1 分鐘結束！")
            
            # 等待剩餘的1分鐘
            remaining_seconds = 60
        
        # 等待到結束時間
        if remaining_seconds > 0:
            await asyncio.sleep(remaining_seconds)
        
        # 時間結束，顯示評價系統
        # 🔥 檢查是否已經發送過評價系統（防止重複發送）
        booking_type = "多人陪玩" if is_multiplayer else "群組預約"
        if group_booking_id not in rating_sent_bookings:
            # 🔥 獲取參與者列表
            def get_participants(booking_id, is_mp):
                with Session() as s:
                    if is_mp:
                        # 多人陪玩：從 Booking 表獲取參與者
                        result = s.execute(text("""
                            SELECT DISTINCT cu.discord as customer_discord, pu.discord as partner_discord
                            FROM "MultiPlayerBooking" mpb
                            JOIN "Booking" b ON b."multiPlayerBookingId" = mpb.id
                            JOIN "Customer" c ON c.id = b."customerId"
                            JOIN "User" cu ON cu.id = c."userId"
                            JOIN "Schedule" s ON s.id = b."scheduleId"
                            JOIN "Partner" p ON p.id = s."partnerId"
                            JOIN "User" pu ON pu.id = p."userId"
                            WHERE mpb.id = :booking_id
                            AND b.status IN ('CONFIRMED', 'PARTNER_ACCEPTED')
                        """), {"booking_id": booking_id}).fetchall()
                    else:
                        # 群組預約：從 GroupBooking 和 GroupBookingParticipant 獲取參與者
                        result = s.execute(text("""
                            SELECT DISTINCT cu.discord as customer_discord, pu.discord as partner_discord
                            FROM "GroupBooking" gb
                            LEFT JOIN "Booking" b ON b."groupBookingId" = gb.id
                            LEFT JOIN "Customer" c ON c.id = b."customerId"
                            LEFT JOIN "User" cu ON cu.id = c."userId"
                            LEFT JOIN "GroupBookingParticipant" gbp ON gbp."groupBookingId" = gb.id
                            LEFT JOIN "Partner" p ON p.id = gbp."partnerId"
                            LEFT JOIN "User" pu ON pu.id = p."userId"
                            WHERE gb.id = :booking_id
                        """), {"booking_id": booking_id}).fetchall()
                    
                    members = []
                    for row in result:
                        if row.customer_discord:
                            members.append(row.customer_discord)
                        if row.partner_discord:
                            members.append(row.partner_discord)
                    return list(set(members))
            
            participants = await asyncio.to_thread(get_participants, group_booking_id, is_multiplayer)
            
            # 🔥 使用 show_group_rating_system 顯示評價系統（支持多人陪玩和群組預約）
            await show_group_rating_system(text_channel, group_booking_id, participants, is_multiplayer=is_multiplayer)
            rating_sent_bookings.add(group_booking_id)
        else:
            print(f"⚠️ {booking_type} {group_booking_id} 已發送過評價系統，跳過")
        
        # 等待5分鐘讓用戶填寫評價，然後刪除文字頻道
        await asyncio.sleep(300)  # 5分鐘 = 300秒
        
        # 刪除文字頻道
        try:
            if text_channel:
                # 🔥 使用 try-except 來檢查頻道是否已刪除，而不是檢查 deleted 屬性
                try:
                    # 嘗試訪問頻道屬性來檢查是否還存在
                    _ = text_channel.name
                    await text_channel.delete()
                    # 清理追蹤
                    group_rating_text_channels.pop(group_booking_id, None)
                    group_rating_channel_created_time.pop(group_booking_id, None)
                except (discord.errors.NotFound, AttributeError):
                    # 頻道已經被刪除，靜默處理
                    group_rating_text_channels.pop(group_booking_id, None)
                    group_rating_channel_created_time.pop(group_booking_id, None)
                    pass
        except Exception as e:
            print(f"❌ 刪除群組預約文字頻道失敗: {e}")
            # 即使刪除失敗，也清理追蹤
            group_rating_text_channels.pop(group_booking_id, None)
            group_rating_channel_created_time.pop(group_booking_id, None)
        
    except Exception as e:
        print(f"❌ 群組預約倒數計時錯誤: {e}")
        import traceback
        traceback.print_exc()

async def show_group_rating_system(text_channel, group_booking_id, members, is_multiplayer=False):
    """顯示群組預約或多人陪玩評價系統（直接在文字頻道發送，不創建新頻道）
    
    Args:
        text_channel: 文字頻道
        group_booking_id: 群組預約ID或多人陪玩ID
        members: 參與者列表
        is_multiplayer: 是否為多人陪玩（False=群組預約，True=多人陪玩）
    """
    try:
        if not text_channel:
            print(f"❌ 文字頻道不存在，無法顯示評價系統")
            return
        
        # 嘗試訪問頻道屬性來檢查頻道是否還存在
        try:
            _ = text_channel.name
        except (AttributeError, discord.errors.NotFound):
            print(f"❌ 文字頻道已刪除，無法顯示評價系統")
            return
            print(f"❌ 文字頻道不存在或已刪除，無法顯示評價系統")
            return
        
        # 🔥 根據類型設置標題和描述
        booking_type = "多人陪玩" if is_multiplayer else "群組預約"
        id_label = "多人陪玩ID" if is_multiplayer else "群組ID"
        
        # 發送評價提示訊息
        embed = discord.Embed(
            title=f"⭐ {booking_type}結束 - 請進行整體評價",
            description=f"感謝您參與{booking_type}！請花一點時間為這次預約體驗進行評價。",
            color=0xffd700
        )
        embed.add_field(
            name="📝 評價說明",
            value="• 評分範圍：1-5 星\n• 留言為選填項目\n• 評價完全匿名\n• 評價結果會回報給管理員",
            inline=False
        )
        embed.add_field(
            name="👥 參與人數",
            value=f"`{len(members)} 人`",
            inline=True
        )
        embed.add_field(
            name=f"🆔 {id_label}",
            value=f"`{group_booking_id}`",
            inline=True
        )
        embed.set_footer(text=f"評價有助於我們提供更好的{booking_type}服務品質")
        
        await text_channel.send(embed=embed)
        await text_channel.send("📝 請點擊以下按鈕進行匿名評分：")
        
        class GroupRatingView(View):
            def __init__(self, group_booking_id):
                super().__init__(timeout=600)  # 10分鐘超時
                self.group_booking_id = group_booking_id
                self.submitted_users = set()
                self.user_ratings = {}  # {user_id: rating}

            @discord.ui.button(label="⭐ 匿名評分", style=discord.ButtonStyle.success, emoji="⭐")
            async def submit_rating(self, interaction: discord.Interaction, button: Button):
                if interaction.user.id in self.submitted_users:
                    await interaction.response.send_message("❗ 您已經提交過評價。", ephemeral=True)
                    return
                
                # 顯示星星選擇器
                await interaction.response.send_message(
                    "⭐ 請選擇您的評分（點擊星星）：",
                    view=StarRatingView(self.group_booking_id, self),
                    ephemeral=True
                )
        
        await text_channel.send(view=GroupRatingView(group_booking_id))
        
        # 記錄評價頻道和創建時間，用於5分鐘後自動刪除
        group_rating_text_channels[group_booking_id] = text_channel
        group_rating_channel_created_time[group_booking_id] = datetime.now(timezone.utc)
        
        
    except Exception as e:
        print(f"❌ 顯示群組預約評價系統失敗: {e}")

class StarRatingView(View):
    """星星評分選擇器"""
    def __init__(self, group_booking_id, parent_view):
        super().__init__(timeout=300)  # 5分鐘超時
        self.group_booking_id = group_booking_id
        self.parent_view = parent_view
        self.selected_rating = None
    
    @discord.ui.button(label="1", emoji="⭐", style=discord.ButtonStyle.secondary, row=0)
    async def star1(self, interaction: discord.Interaction, button: Button):
        await self.handle_rating(interaction, 1)
    
    @discord.ui.button(label="2", emoji="⭐", style=discord.ButtonStyle.secondary, row=0)
    async def star2(self, interaction: discord.Interaction, button: Button):
        await self.handle_rating(interaction, 2)
    
    @discord.ui.button(label="3", emoji="⭐", style=discord.ButtonStyle.secondary, row=0)
    async def star3(self, interaction: discord.Interaction, button: Button):
        await self.handle_rating(interaction, 3)
    
    @discord.ui.button(label="4", emoji="⭐", style=discord.ButtonStyle.secondary, row=0)
    async def star4(self, interaction: discord.Interaction, button: Button):
        await self.handle_rating(interaction, 4)
    
    @discord.ui.button(label="5", emoji="⭐", style=discord.ButtonStyle.secondary, row=0)
    async def star5(self, interaction: discord.Interaction, button: Button):
        await self.handle_rating(interaction, 5)
    
    async def handle_rating(self, interaction: discord.Interaction, rating: int):
        """處理評分選擇"""
        self.selected_rating = rating
        self.parent_view.user_ratings[interaction.user.id] = rating
        
        # 直接顯示 Modal 輸入評論（評分已確定）
        await interaction.response.send_modal(
            GroupRatingModal(self.group_booking_id, self.parent_view, rating)
        )

class GroupRatingModal(Modal):
    comment = TextInput(label="留下你的留言（選填）", required=False, placeholder="分享您的開團體驗...", style=discord.TextStyle.paragraph)

    def __init__(self, group_booking_id, parent_view, rating):
        # 在 title 中顯示已選擇的評分
        super().__init__(title=f"群組預約匿名評分與留言 - {'⭐' * rating}星")
        self.group_booking_id = group_booking_id
        self.parent_view = parent_view
        self.rating = rating

    async def on_submit(self, interaction: discord.Interaction):
        try:
            rating = self.rating
            
            # 保存評價到資料庫
            with Session() as s:
                # ✅ 修正用戶查找：使用 normalize_discord_username 標準化 Discord 用戶名（去除尾隨空格、下劃線、點）
                normalized_discord_name = normalize_discord_username(interaction.user.name)
                discord_id_str = str(interaction.user.id)
                
                # 🔥 只允許顧客提交評價（因為 GroupBookingReview.reviewerId 必須是 Customer.id）
                # ✅ 改進：使用多種方式匹配 Discord 用戶（顯示名稱、標準化名稱、Discord ID）
                # 注意：Discord 的 interaction.user.name 可能是顯示名稱（display name），而不是用戶名（username）
                # Discord 用戶可能有多個名稱：display_name (try1) 和 username (qaz789456)
                # 所以需要同時檢查多種變體
                customer_result = s.execute(text("""
                    SELECT c.id FROM "Customer" c
                    JOIN "User" u ON u.id = c."userId"
                    WHERE u.discord = :discord_name 
                       OR u.discord = :normalized_name 
                       OR u.discord = :discord_id
                       OR LOWER(TRIM(u.discord)) = LOWER(TRIM(:discord_name))
                       OR LOWER(TRIM(u.discord)) = LOWER(TRIM(:normalized_name))
                """), {
                    "discord_name": interaction.user.name,
                    "normalized_name": normalized_discord_name,
                    "discord_id": discord_id_str
                }).fetchone()
                
                # ✅ 如果第一次查詢失敗，嘗試使用 Discord global_name 或用戶名（如果存在）
                if not customer_result:
                    # 嘗試使用 global_name（Discord 顯示名稱）
                    global_name = getattr(interaction.user, 'global_name', None)
                    if global_name:
                        customer_result = s.execute(text("""
                            SELECT c.id FROM "Customer" c
                            JOIN "User" u ON u.id = c."userId"
                            WHERE u.discord = :global_name 
                               OR LOWER(TRIM(u.discord)) = LOWER(TRIM(:global_name))
                        """), {
                            "global_name": global_name
                        }).fetchone()
                    
                    # 如果還是找不到，嘗試模糊匹配（包含關係）
                    if not customer_result:
                        # 使用 LIKE 進行模糊匹配（嘗試匹配部分名稱）
                        customer_result = s.execute(text("""
                            SELECT c.id FROM "Customer" c
                            JOIN "User" u ON u.id = c."userId"
                            WHERE u.discord LIKE :discord_name_pattern
                               OR u.discord LIKE :normalized_name_pattern
                               OR :discord_name LIKE '%' || u.discord || '%'
                               OR :normalized_name LIKE '%' || u.discord || '%'
                        """), {
                            "discord_name_pattern": f"%{interaction.user.name}%",
                            "normalized_name_pattern": f"%{normalized_discord_name}%",
                            "discord_name": interaction.user.name,
                            "normalized_name": normalized_discord_name
                        }).fetchone()
                
                # 如果找不到顧客記錄，嘗試使用 Discord ID 查找
                if not customer_result:
                    user_result = s.execute(text("""
                        SELECT id FROM "User"
                        WHERE discord = :discord_name OR discord = :normalized_name OR discord = :discord_id
                    """), {
                        "discord_name": interaction.user.name,
                        "normalized_name": normalized_discord_name,
                        "discord_id": discord_id_str
                    }).fetchone()
                    
                    if user_result:
                        user_id = user_result[0]
                        customer_result = s.execute(text("""
                            SELECT id FROM "Customer" WHERE "userId" = :user_id
                        """), {"user_id": user_id}).fetchone()
                
                # 如果還是找不到顧客記錄，檢查是否為夥伴
                if not customer_result:
                    partner_result = s.execute(text("""
                        SELECT p.id FROM "Partner" p
                        JOIN "User" u ON u.id = p."userId"
                        WHERE u.discord = :discord_name OR u.discord = :discord_id
                    """), {
                        "discord_name": interaction.user.name,
                        "discord_id": str(interaction.user.id)
                    }).fetchone()
                    
                    # ✅ 修正用戶查找：使用標準化名稱查找夥伴
                    if not partner_result:
                        partner_result = s.execute(text("""
                            SELECT p.id FROM "Partner" p
                            JOIN "User" u ON u.id = p."userId"
                            WHERE u.discord = :normalized_name
                        """), {
                            "normalized_name": normalized_discord_name
                        }).fetchone()
                    
                    if partner_result:
                        # 夥伴不能提交評價（因為 GroupBookingReview.reviewerId 必須是 Customer.id）
                        print(f"⚠️ 夥伴嘗試提交評價: Discord名稱={interaction.user.name}, Discord ID={interaction.user.id}")
                        await interaction.response.send_message("❌ 抱歉，只有顧客可以提交評價。", ephemeral=True)
                        return
                    else:
                        # 🔥 改進錯誤信息：提供更多調試信息
                        # ✅ 檢查用戶是否存在於 User 表中（使用標準化名稱）
                        user_check = s.execute(text("""
                            SELECT id, discord, name FROM "User" 
                            WHERE discord = :discord_id OR discord = :discord_name OR discord = :normalized_name
                        """), {
                            "discord_id": discord_id_str,
                            "discord_name": interaction.user.name,
                            "normalized_name": normalized_discord_name
                        }).fetchone()
                        
                        if user_check:
                            print(f"⚠️ 用戶存在但沒有 Customer 或 Partner 記錄: Discord名稱={interaction.user.name}, Discord ID={interaction.user.id}, User ID={user_check[0]}")
                        else:
                            print(f"❌ 找不到用戶記錄: Discord名稱={interaction.user.name}, Discord ID={interaction.user.id}")
                        await interaction.response.send_message("❌ 找不到您的用戶記錄，請聯繫管理員", ephemeral=True)
                        return
                
                reviewer_id = customer_result[0]
                
                # ✅ 檢查 group_booking_id 是 GroupBooking 還是 MultiPlayerBooking
                group_booking_check = s.execute(text("""
                    SELECT id FROM "GroupBooking" WHERE id = :group_booking_id
                """), {"group_booking_id": self.group_booking_id}).fetchone()
                
                multi_player_check = s.execute(text("""
                    SELECT id FROM "MultiPlayerBooking" WHERE id = :group_booking_id
                """), {"group_booking_id": self.group_booking_id}).fetchone()
                
                is_multiplayer = bool(multi_player_check and not group_booking_check)
                
                if not group_booking_check and not multi_player_check:
                    print(f"❌ 找不到群組預約或多人陪玩記錄: {self.group_booking_id}")
                    await interaction.response.send_message("❌ 找不到預約記錄，請聯繫管理員", ephemeral=True)
                    return
                
                # ✅ 如果是多人陪玩，需要創建一個對應的 GroupBooking 記錄（如果不存在，用於評價系統）
                if is_multiplayer and not group_booking_check:
                    # 獲取多人陪玩信息
                    mpb_info = s.execute(text("""
                        SELECT "customerId", date, "startTime", "endTime", "totalAmount", status
                        FROM "MultiPlayerBooking"
                        WHERE id = :mpb_id
                    """), {"mpb_id": self.group_booking_id}).fetchone()
                    
                    if mpb_info:
                        # 創建對應的 GroupBooking 記錄（用於評價系統）
                        # 注意：GroupBooking 使用 initiatorId 和 initiatorType，而不是 customerId
                        s.execute(text("""
                            INSERT INTO "GroupBooking" (id, type, "initiatorId", "initiatorType", title, date, "startTime", "endTime", 
                                                       "maxParticipants", "currentParticipants", status, "createdAt", "updatedAt")
                            VALUES (:id, 'USER_INITIATED', :initiator_id, 'CUSTOMER', :title, :date, :start_time, :end_time, 
                                    :max_participants, :current_participants, :status, NOW(), NOW())
                        """), {
                            "id": self.group_booking_id,
                            "initiator_id": mpb_info[0],  # customerId 作為 initiatorId
                            "title": f"多人陪玩評價 - {self.group_booking_id[:8]}",
                            "date": mpb_info[1],
                            "start_time": mpb_info[2],
                            "end_time": mpb_info[3],
                            "max_participants": 10,
                            "current_participants": 0,
                            "status": "COMPLETED"
                        })
                        s.commit()
                
                # 🔥 生成唯一的 ID（使用 cuid 格式）
                import uuid
                review_id = f"gbr_{uuid.uuid4().hex[:12]}"
                
                # 創建群組預約評價記錄
                review = GroupBookingReview(
                    id=review_id,
                    groupBookingId=self.group_booking_id,
                    reviewerId=reviewer_id,
                    rating=rating,
                    comment=str(self.comment) if self.comment else None
                )
                s.add(review)
                s.commit()
            
            # ✅ 發送到管理員頻道：多人陪玩使用「多人陪玩」類型，群組預約使用「群組預約」類型
            # ✅ 多人陪玩顧客對多人陪玩的評價是對的，但本身本來就不需要分別對每一位夥伴評價，所以管理員頻道不需要回饋顧客對每一位或夥伴的評價
            if is_multiplayer:
                # ✅ 多人陪玩：使用「多人陪玩」類型，只發送一個整體評價回饋（不對每一位夥伴發送）
                await send_unified_rating_feedback(self.group_booking_id, "多人陪玩", rating, str(self.comment) if self.comment else None, interaction.user.name)
            else:
                # 群組預約：使用「群組預約」類型
                await send_group_rating_to_admin(self.group_booking_id, rating, str(self.comment) if self.comment else None, interaction.user.name)
            
            # 標記用戶已提交評價
            self.parent_view.submitted_users.add(interaction.user.id)
            
            # 確認收到評價
            await interaction.response.send_message(
                f"✅ 感謝您的評價！\n"
                f"評分：{'⭐' * rating}\n"
                f"評論：{str(self.comment) if self.comment else '無'}",
                ephemeral=True
            )
            
        except Exception as e:
            print(f"❌ 處理群組預約評價提交失敗: {e}")
            import traceback
            traceback.print_exc()
            try:
                if not interaction.response.is_done():
                    await interaction.response.send_message("❌ 處理評價時發生錯誤，請稍後再試", ephemeral=True)
                else:
                    await interaction.followup.send("❌ 處理評價時發生錯誤，請稍後再試", ephemeral=True)
            except Exception as e2:
                print(f"❌ 發送錯誤訊息失敗: {e2}")

async def send_unified_rating_feedback(booking_id: str, booking_type: str = "一般預約", rating: int = None, comment: str = None, reviewer_name: str = None):
    """統一的評價回饋函數，適用於所有類型的預約（一般預約、即時預約、純聊天、多人陪玩、群組預約）"""
    try:
        # 🔥 改善錯誤處理：避免 try/except 吃掉 SQL 錯誤，讓錯誤可以正確傳播
        admin_channel = bot.get_channel(ADMIN_CHANNEL_ID)
        if not admin_channel:
            print(f"❌ 找不到管理員頻道 (ID: {ADMIN_CHANNEL_ID})")
            return
        
        # 根據預約類型獲取資訊
        with Session() as s:
            if booking_type == "群組預約":
                # 群組預約
                result = s.execute(text("""
                    SELECT 
                        gb.title, 
                        gb."currentParticipants", 
                        gb."maxParticipants",
                        gb."startTime",
                        gb."endTime",
                        gb."initiatorId",
                        gb."initiatorType",
                        gb."discordTextChannelId",
                        gb."discordVoiceChannelId"
                    FROM "GroupBooking" gb
                    WHERE gb.id = :booking_id
                """), {"booking_id": booking_id}).fetchone()
                
                if not result:
                    print(f"❌ 找不到群組預約記錄: {booking_id}")
                    return
                
                title = result[0] or "群組預約"
                current_participants = result[1]
                max_participants = result[2]
                start_time = result[3]
                end_time = result[4]
                initiator_id = result[5]
                initiator_type = result[6]
                text_channel_id = result[7]
                voice_channel_id = result[8]
                
                # 獲取參與者資訊
                participants_info = []
                if initiator_type == 'Customer':
                    customer_result = s.execute(text("""
                        SELECT u.discord, u.name
                        FROM "Customer" c
                        JOIN "User" u ON u.id = c."userId"
                        WHERE c.id = :initiator_id
                    """), {"initiator_id": initiator_id}).fetchone()
                    if customer_result:
                        customer_discord = customer_result[0]
                        customer_name = customer_result[1] or customer_discord
                        participants_info.append(f"顧客: {customer_name} ({customer_discord})")
                
                booking_results = s.execute(text("""
                    SELECT DISTINCT u.discord, u.name
                    FROM "Booking" b
                    JOIN "Partner" p ON p.id = b."partnerId"
                    JOIN "User" u ON u.id = p."userId"
                    WHERE b."groupBookingId" = :booking_id
                """), {"booking_id": booking_id}).fetchall()
                
                for partner_result in booking_results:
                    partner_discord = partner_result[0]
                    partner_name = partner_result[1] or partner_discord
                    participants_info.append(f"夥伴: {partner_name} ({partner_discord})")
                
                participants_text = "\n".join(participants_info) if participants_info else "無"
                participant_count = f"{current_participants}/{max_participants}"
                booking_id_display = f"`{booking_id}`"
                
            elif booking_type == "多人陪玩":
                # ✅ 多人陪玩：獲取所有參與者資訊（顧客和所有夥伴），不需要分別對每一位夥伴評價
                result = s.execute(text("""
                    SELECT 
                        mp."startTime",
                        mp."endTime",
                        mp."discordTextChannelId",
                        mp."discordVoiceChannelId",
                        c.name as customer_name,
                        cu.discord as customer_discord
                    FROM "MultiPlayerBooking" mp
                    JOIN "Customer" c ON c.id = mp."customerId"
                    JOIN "User" cu ON cu.id = c."userId"
                    WHERE mp.id = :booking_id
                """), {"booking_id": booking_id}).fetchone()
                
                if not result:
                    print(f"❌ 找不到多人陪玩記錄: {booking_id}")
                    return
                
                start_time = result[0]
                end_time = result[1]
                text_channel_id = result[2]
                voice_channel_id = result[3]
                customer_name = result[4] or result[5]
                customer_discord = result[5]
                
                # ✅ 獲取所有夥伴資訊（不需要分別對每一位夥伴評價，只顯示整體資訊）
                partner_results = s.execute(text("""
                    SELECT DISTINCT p.name as partner_name, pu.discord as partner_discord
                    FROM "MultiPlayerBooking" mp
                    JOIN "Booking" b ON b."multiPlayerBookingId" = mp.id
                    JOIN "Schedule" s ON s.id = b."scheduleId"
                    JOIN "Partner" p ON p.id = s."partnerId"
                    JOIN "User" pu ON pu.id = p."userId"
                    WHERE mp.id = :booking_id
                    AND b.status IN ('CONFIRMED', 'PARTNER_ACCEPTED', 'COMPLETED')
                """), {"booking_id": booking_id}).fetchall()
                
                # ✅ 構建參與者資訊（只顯示顧客和夥伴列表，不需要分別評價）
                participants_info = [f"顧客: {customer_name} ({customer_discord})"]
                for partner_result in partner_results:
                    partner_name = partner_result[0] or partner_result[1]
                    partner_discord = partner_result[1]
                    participants_info.append(f"夥伴: {partner_name} ({partner_discord})")
                
                participants_text = "\n".join(participants_info)
                participant_count = f"1/{len(partner_results) + 1}"  # 顧客 + 夥伴數量
                booking_id_display = f"`{booking_id}`"
                title = "多人陪玩"
                
            else:
                # 一般預約、即時預約、純聊天
                # 🔥 修復：Booking 表不存在 isInstantBooking 欄位，改用 paymentInfo JSON 判斷
                result = s.execute(text("""
                    SELECT 
                        s."startTime",
                        s."endTime",
                        b."discordTextChannelId",
                        b."discordVoiceChannelId",
                        c.name as customer_name,
                        cu.discord as customer_discord,
                        p.name as partner_name,
                        pu.discord as partner_discord,
                        b."serviceType",
                        b."paymentInfo"->>'isInstantBooking' as is_instant_booking
                    FROM "Booking" b
                    JOIN "Schedule" s ON s.id = b."scheduleId"
                    JOIN "Customer" c ON c.id = b."customerId"
                    JOIN "User" cu ON cu.id = c."userId"
                    JOIN "Partner" p ON p.id = s."partnerId"
                    JOIN "User" pu ON pu.id = p."userId"
                    WHERE b.id = :booking_id
                """), {"booking_id": booking_id}).fetchone()
                
                if not result:
                    print(f"❌ 找不到預約記錄: {booking_id}")
                    return
                
                start_time = result[0]
                end_time = result[1]
                text_channel_id = result[2]
                voice_channel_id = result[3]
                customer_name = result[4] or result[5]
                customer_discord = result[5]
                partner_name = result[6] or result[7]
                partner_discord = result[7]
                service_type = result[8]
                is_instant_booking_str = result[9]
                
                # 🔥 判斷是否為即時預約（從 paymentInfo JSON 中獲取）
                is_instant = (
                    is_instant_booking_str == 'true' or 
                    is_instant_booking_str == True or
                    (is_instant_booking_str is not None and str(is_instant_booking_str).lower() == 'true')
                )
                
                participants_text = f"顧客: {customer_name} ({customer_discord})\n夥伴: {partner_name} ({partner_discord})"
                participant_count = "2/2"
                booking_id_display = f"`{booking_id}`"
                
                # 確定預約類型標題
                if service_type == "CHAT_ONLY":
                    title = "純聊天"
                elif is_instant:
                    title = "即時預約"
                else:
                    title = "一般預約"
            
            # 轉換時間為台灣時間
            if start_time and end_time:
                if start_time.tzinfo is None:
                    start_time = start_time.replace(tzinfo=timezone.utc)
                if end_time.tzinfo is None:
                    end_time = end_time.replace(tzinfo=timezone.utc)
                
                tw_start_time = start_time.astimezone(TW_TZ)
                tw_end_time = end_time.astimezone(TW_TZ)
                duration_minutes = int((end_time - start_time).total_seconds() / 60)
            else:
                tw_start_time = None
                tw_end_time = None
                duration_minutes = 0
            
            # 獲取文字頻道資訊
            text_channel_mention = "#不明"
            if text_channel_id:
                try:
                    text_channel = bot.get_channel(int(text_channel_id))
                    if text_channel:
                        text_channel_mention = text_channel.mention
                except:
                    pass
            
            # 獲取評價資訊（如果沒有提供）
            if rating is None or reviewer_name is None:
                review_result = s.execute(text("""
                    SELECT r.rating, r.comment, r."reviewerId"
                    FROM "Review" r
                    WHERE r."bookingId" = :booking_id
                    ORDER BY r."createdAt" DESC
                    LIMIT 1
                """), {"booking_id": booking_id}).fetchone()
                
                if review_result:
                    if rating is None:
                        rating = review_result[0]
                    if comment is None:
                        comment = review_result[1]
                    if reviewer_name is None:
                        reviewer_id = review_result[2]
                        # 獲取評價者名稱
                        user_result = s.execute(text("""
                            SELECT u.name, u.discord
                            FROM "User" u
                            WHERE u.id = :user_id
                        """), {"user_id": reviewer_id}).fetchone()
                        if user_result:
                            reviewer_name = user_result[0] or user_result[1] or "未知"
                        else:
                            reviewer_name = "未知"
        
        # 創建評價嵌入訊息（統一格式）
        embed = discord.Embed(
            title=f"⭐ {title}評價回饋",
            description="新的評價已提交！" if rating else "尚未收到評價",
            color=0x00ff00 if rating else 0xff9900,
            timestamp=datetime.now(timezone.utc)
        )
        
        # 第一行：預約時間和參與者
        if tw_start_time and tw_end_time:
            embed.add_field(
                name="📅 預約時間",
                value=f"`{tw_start_time.strftime('%Y/%m/%d %H:%M')} - {tw_end_time.strftime('%H:%M')}`",
                inline=True
            )
        
        embed.add_field(
            name="👥 參與者",
            value=participants_text[:1024],  # Discord 欄位限制
            inline=True
        )
        
        embed.add_field(
            name="💬 溝通頻道",
            value=text_channel_mention,
            inline=True
        )
        
        # 第二行：時長、語音頻道、評價資訊
        embed.add_field(
            name="⏰ 時長",
            value=f"`{duration_minutes} 分鐘`",
            inline=True
        )
        
        voice_channel_status = "`已創建`" if voice_channel_id else "`未創建`"
        embed.add_field(
            name="🎤 語音頻道",
            value=voice_channel_status,
            inline=True
        )
        
        embed.add_field(
            name="👤 評價者",
            value=reviewer_name or "無",
            inline=True
        )
        
        # 第三行：評價詳情
        rating_display = "⭐" * rating if rating else "無"
        embed.add_field(
            name="⭐ 評分",
            value=rating_display,
            inline=True
        )
        
        embed.add_field(
            name="👥 參與人數",
            value=participant_count,
            inline=True
        )
        
        if booking_type == "群組預約":
            embed.add_field(
                name="📋 群組預約ID",
                value=booking_id_display,
                inline=True
            )
        else:
            embed.add_field(
                name="📋 預約ID",
                value=booking_id_display,
                inline=True
            )
        
        if comment:
            embed.add_field(
                name="💬 留言",
                value=comment[:1024],  # Discord 欄位限制
                inline=False
            )
        
        embed.set_footer(text=f"PeiPlay {title}評價系統")
        
        await admin_channel.send(embed=embed)
        
    except Exception as e:
        # 🔥 改善錯誤處理：區分 SQL 錯誤和其他錯誤，SQL 錯誤應該重新拋出
        import traceback
        error_str = str(e).lower()
        is_sql_error = any(keyword in error_str for keyword in ['sql', 'database', 'column', 'table', 'syntax', 'relation does not exist'])
        
        if is_sql_error:
            print(f"❌ SQL 錯誤：發送{booking_type}評價到管理員頻道時發生資料庫錯誤: {e}")
            traceback.print_exc()
            # 🔥 SQL 錯誤應該重新拋出，不要靜默失敗
            raise
        else:
            print(f"❌ 發送{booking_type}評價到管理員頻道失敗: {e}")
            traceback.print_exc()

async def send_group_rating_to_admin(group_booking_id, rating, comment, reviewer_name):
    """發送群組預約評價結果到管理員頻道（使用統一格式）"""
    await send_unified_rating_feedback(group_booking_id, "群組預約", rating, comment, reviewer_name)

async def create_booking_voice_channel(booking_id, customer_discord, partner_discord, start_time, end_time, is_instant_booking=None, discord_delay_minutes=None):
    """為預約創建語音頻道"""
    try:
        guild = bot.get_guild(GUILD_ID)
        if not guild:
            print("❌ 找不到 Discord 伺服器")
            return None
        
        # 查找 Discord 成員
        customer_member = find_member_by_discord_name(guild, customer_discord)
        partner_member = find_member_by_discord_name(guild, partner_discord)
        
        if not customer_member or not partner_member:
            print(f"❌ 找不到 Discord 成員: 顧客={customer_discord}, 夥伴={partner_discord}")
            return None
        
        # 計算頻道持續時間
        duration_minutes = int((end_time - start_time).total_seconds() / 60)
        
        # 創建頻道名稱 - 使用日期和時間
        # 確保時間有時區資訊，並轉換為台灣時間
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)
        
        # 轉換為台灣時間
        tw_start_time = start_time.astimezone(TW_TZ)
        tw_end_time = end_time.astimezone(TW_TZ)
        
        # 格式化日期和時間
        date_str = tw_start_time.strftime("%m%d")  # 改為 1016 格式
        start_time_str = tw_start_time.strftime("%H:%M")
        end_time_str = tw_end_time.strftime("%H:%M")
        
        # 創建統一的頻道名稱（與文字頻道相同）
        cute_item = random.choice(CUTE_ITEMS)
        if is_instant_booking == 'true':
            channel_name = f"⚡即時{date_str} {start_time_str}-{end_time_str} {cute_item}"
        else:
            channel_name = f"📅{date_str} {start_time_str}-{end_time_str} {cute_item}"
        
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            customer_member: discord.PermissionOverwrite(view_channel=True, connect=True, speak=True),
            partner_member: discord.PermissionOverwrite(view_channel=True, connect=True, speak=True),
        }
        
        category = discord.utils.get(guild.categories, name="Voice Channels")
        if not category:
            category = discord.utils.get(guild.categories, name="語音頻道")
        if not category:
            category = discord.utils.get(guild.categories, name="語音")
        if not category:
            # 嘗試使用第一個可用的分類
            if guild.categories:
                category = guild.categories[0]
                print(f"⚠️ 自動檢查使用現有分類: {category.name}")
            else:
                print("❌ 找不到任何分類，跳過此預約")
                return None
        
        vc = await guild.create_voice_channel(
            name=channel_name, 
            overwrites=overwrites, 
            user_limit=2, 
            category=category
        )
        
        # 不創建文字頻道，因為 check_new_bookings 已經創建了
        # text_channel = await guild.create_text_channel(
        #     name="🔒匿名文字區", 
        #     overwrites=overwrites, 
        #     category=category
        # )
        
        # 創建配對記錄
        user1_id = str(customer_member.id)
        user2_id = str(partner_member.id)
        
        # 添加調試信息
        # 自動創建配對記錄，減少日誌輸出
        
        with Session() as s:
            try:
                # 生成唯一的 ID（類似 Prisma 的 cuid）
                import uuid
                record_id = f"pair_{uuid.uuid4().hex[:12]}"
                
                record = PairingRecord(
                    id=record_id,
                    user1Id=user1_id,
                    user2Id=user2_id,
                    duration=duration_minutes * 60,
                    animalName="預約頻道",
                    bookingId=booking_id
                )
                s.add(record)
                s.commit()
                created_at = record.createdAt
            except Exception as e:
                print(f"❌ 創建配對記錄失敗: {e}")
                # 如果表不存在，使用預設的 record_id
                if "relation \"PairingRecord\" does not exist" in str(e):
                    record_id = "temp_" + str(int(time.time()))
                    print(f"⚠️ 使用臨時 record_id: {record_id}")
                else:
                    record_id = None
        
        # 初始化頻道狀態
        active_voice_channels[vc.id] = {
            'text_channel': None,  # 文字頻道由 check_new_bookings 創建
            'remaining': duration_minutes * 60,
            'extended': 0,
            'record_id': record_id,
            'vc': vc,
            'booking_id': booking_id
        }
        
        if is_instant_booking == 'true':
            print(f"⏰ Discord 頻道將在 {discord_delay_minutes} 分鐘後自動開啟")
            
            # 通知創建頻道頻道
            channel_creation_channel = bot.get_channel(CHANNEL_CREATION_CHANNEL_ID)
            if channel_creation_channel:
                instant_embed = discord.Embed(
                    title="⚡ 即時預約語音頻道已創建",
                    color=0xff6b35,
                    timestamp=datetime.now(timezone.utc)
                )
                
                # 第一行：預約ID和顧客
                instant_embed.add_field(
                    name="🆔 預約ID",
                    value=f"`{booking_id}`",
                    inline=True
                )
                instant_embed.add_field(
                    name="👤 顧客",
                    value=f"{customer_member.mention}\n`{customer_discord}`",
                    inline=True
                )
                instant_embed.add_field(
                    name="👥 夥伴",
                    value=f"{partner_member.mention}\n`{partner_discord}`",
                    inline=True
                )
                
                # 第二行：時間和頻道
                instant_embed.add_field(
                    name="⏰ 開始時間",
                    value=f"`{tw_start_time.strftime('%Y/%m/%d %H:%M')}`",
                    inline=True
                )
                instant_embed.add_field(
                    name="⏱️ 時長",
                    value=f"`{duration_minutes} 分鐘`",
                    inline=True
                )
                instant_embed.add_field(
                    name="🎮 頻道",
                    value=f"{vc.mention}",
                    inline=True
                )
                
                # 第三行：延遲時間
                instant_embed.add_field(
                    name="⏳ 自動開啟",
                    value=f"`將在 {discord_delay_minutes} 分鐘後自動開啟`",
                    inline=False
                )
                
                await channel_creation_channel.send(embed=instant_embed)
            
            # 延遲開啟語音頻道
            async def delayed_open_voice():
                await asyncio.sleep(int(discord_delay_minutes or 3) * 60)  # 等待指定分鐘數
                try:
                    # 檢查預約狀態是否仍然是 PARTNER_ACCEPTED
                    with Session() as check_s:
                        current_booking = check_s.execute(
                            text("SELECT status FROM \"Booking\" WHERE id = :booking_id"),
                            {"booking_id": booking_id}
                        ).fetchone()
                        
                        if current_booking and current_booking.status == 'PARTNER_ACCEPTED':
                            # 開啟語音頻道
                            await vc.set_permissions(guild.default_role, view_channel=True)
                            # 文字頻道由 check_new_bookings 創建，這裡不需要處理
                            
                            # 發送開啟通知
                            embed = discord.Embed(
                                title="🎮 即時預約頻道已開啟！",
                                description=f"歡迎 {customer_member.mention} 和 {partner_member.mention} 來到 {channel_name}！",
                                color=0x00ff00,
                                timestamp=datetime.now(timezone.utc)
                            )
                            embed.add_field(name="⏰ 預約時長", value=f"{duration_minutes} 分鐘", inline=True)
                            embed.add_field(name="💰 費用", value=f"${duration_minutes * 2 * 150}", inline=True)  # 假設每半小時150元
                            
                            # 文字頻道由 check_new_bookings 創建，這裡不需要發送通知
                            # 即時預約語音頻道已開啟，減少日誌輸出
                        else:
                            print(f"⚠️ 預約 {booking_id} 狀態已改變，取消延遲開啟")
                except Exception as e:
                    print(f"❌ 延遲開啟語音頻道失敗: {e}")
            
            # 啟動延遲開啟任務
            bot.loop.create_task(delayed_open_voice())
            
        else:
            # 通知創建頻道頻道
            channel_creation_channel = bot.get_channel(CHANNEL_CREATION_CHANNEL_ID)
            if channel_creation_channel:
                await channel_creation_channel.send(
                    f"🎉 自動創建語音頻道：\n"
                    f"📋 預約ID: {booking_id}\n"
                    f"👤 顧客: {customer_member.mention} ({customer_discord})\n"
                    f"👥 夥伴: {partner_member.mention} ({partner_discord})\n"
                    f"⏰ 開始時間: {tw_start_time.strftime('%Y/%m/%d %H:%M')}\n"
                    f"⏱️ 時長: {duration_minutes} 分鐘\n"
                    f"🎮 頻道: {vc.mention}"
                )
            
            # 啟動倒數
            if record_id:
                # 文字頻道由 check_new_bookings 創建，這裡先不啟動倒數
                # bot.loop.create_task(
                #     countdown(vc.id, channel_name, text_channel, vc, None, [customer_member, partner_member], record_id)
                # )
                pass
            
            # 自動創建頻道成功，減少日誌輸出
        
        return vc
        
    except Exception as e:
        print(f"❌ 創建語音頻道失敗: {e}")
        import traceback
        traceback.print_exc()
        return None

# --- 刪除預約頻道函數 ---
async def delete_booking_channels(booking_id: str):
    """刪除預約相關的 Discord 頻道"""
    try:
        guild = bot.get_guild(GUILD_ID)
        if not guild:
            print("❌ 找不到 Discord 伺服器")
            return False
        
        # 從資料庫獲取頻道 ID
        with Session() as s:
            # 先檢查欄位是否存在
            check_columns = s.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'Booking' 
                AND column_name IN ('discordTextChannelId', 'discordVoiceChannelId')
            """)).fetchall()
            
            if len(check_columns) < 2:
                print(f"⚠️ Discord 欄位尚未創建，無法獲取頻道資訊")
                return False
            
            result = s.execute(
                text("SELECT \"discordTextChannelId\", \"discordVoiceChannelId\" FROM \"Booking\" WHERE id = :booking_id"),
                {"booking_id": booking_id}
            )
            row = result.fetchone()
            
            if not row:
                print(f"❌ 找不到預約 {booking_id} 的頻道資訊")
                return False
            
            text_channel_id = row[0]
            voice_channel_id = row[1]
        
        deleted_channels = []
        
        # 刪除文字頻道
        if text_channel_id:
            try:
                text_channel = guild.get_channel(int(text_channel_id))
                if text_channel:
                    await text_channel.delete()
                    deleted_channels.append(f"文字頻道 {text_channel.name}")
                    # 已刪除文字頻道，減少日誌輸出
                else:
                    print(f"⚠️ 文字頻道 {text_channel_id} 不存在")
            except Exception as text_error:
                print(f"❌ 刪除文字頻道失敗: {text_error}")
        
        # 刪除語音頻道
        if voice_channel_id:
            try:
                voice_channel = guild.get_channel(int(voice_channel_id))
                if voice_channel:
                    await voice_channel.delete()
                    deleted_channels.append(f"語音頻道 {voice_channel.name}")
                    # 已刪除語音頻道，減少日誌輸出
                else:
                    print(f"⚠️ 語音頻道 {voice_channel_id} 不存在")
            except Exception as voice_error:
                print(f"❌ 刪除語音頻道失敗: {voice_error}")
        
        # 清除資料庫中的頻道 ID
        try:
            with Session() as s:
                # 先檢查欄位是否存在
                check_columns = s.execute(text("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'Booking' 
                    AND column_name IN ('discordTextChannelId', 'discordVoiceChannelId')
                """)).fetchall()
                
                if len(check_columns) >= 2:
                    s.execute(
                        text("UPDATE \"Booking\" SET \"discordTextChannelId\" = NULL, \"discordVoiceChannelId\" = NULL WHERE id = :booking_id"),
                        {"booking_id": booking_id}
                    )
                    s.commit()
                    # 已清除預約的頻道ID，減少日誌輸出
                else:
                    print(f"⚠️ Discord 欄位尚未創建，跳過清除頻道 ID")
        except Exception as db_error:
            print(f"❌ 清除頻道 ID 失敗: {db_error}")
        
        # 通知管理員
        try:
            admin_channel = bot.get_channel(ADMIN_CHANNEL_ID)
            if admin_channel and deleted_channels:
                await admin_channel.send(
                    f"🗑️ **預約頻道已刪除**\n"
                    f"預約ID: `{booking_id}`\n"
                    f"已刪除頻道: {', '.join(deleted_channels)}"
                )
        except Exception as notify_error:
            print(f"❌ 發送刪除通知失敗: {notify_error}")
        
        return len(deleted_channels) > 0
        
    except Exception as error:
        print(f"❌ 刪除預約頻道失敗: {error}")
        return False

# --- 檢查新預約並創建文字頻道任務 ---
@tasks.loop(seconds=60)  # 每分鐘檢查一次
async def check_new_bookings():
    """檢查預約開始前 5 分鐘的預約並創建文字頻道"""
    await bot.wait_until_ready()
    
    try:
        # 將同步資料庫操作移到線程池，避免阻塞事件循環
        def query_bookings():
            def _query():
                # ADDED FOR TRANSACTION SAFETY: 使用 with Session() 確保自動關閉
                with Session() as s:
                    try:
                        now = datetime.now(timezone.utc)
                        five_minutes_from_now = now + timedelta(minutes=5)
                        query = """
                            SELECT 
                                b.id, b."customerId", b."scheduleId", b.status, b."createdAt", b."updatedAt",
                                c.name as customer_name, cu.discord as customer_discord,
                                p.name as partner_name, pu.discord as partner_discord,
                                s."startTime", s."endTime"
                            FROM "Booking" b
                            JOIN "Schedule" s ON s.id = b."scheduleId"
                            JOIN "Customer" c ON c.id = b."customerId"
                            JOIN "User" cu ON cu.id = c."userId"
                            JOIN "Partner" p ON p.id = s."partnerId"
                            JOIN "User" pu ON pu.id = p."userId"
                            WHERE b.status = 'CONFIRMED'
                            AND b."groupBookingId" IS NULL
                            AND b."multiPlayerBookingId" IS NULL
                            AND s."startTime" <= :five_minutes_from_now
                            AND s."startTime" > :now
                            AND s."endTime" > :now
                            AND b."discordTextChannelId" IS NULL
                        """
                        result = s.execute(text(query), {
                            "five_minutes_from_now": five_minutes_from_now,
                            "now": now
                        })
                        return list(result)  # 轉換為列表，避免在線程外訪問結果
                    except Exception as e:
                        # ADDED FOR TRANSACTION SAFETY: 確保異常時 rollback
                        s.rollback()
                        raise
            
            return safe_db_execute(_query) or []
        
        # 在線程池中執行資料庫查詢
        rows = await asyncio.to_thread(query_bookings)
        
        for row in rows:
                try:
                    # 檢查是否已經創建過文字頻道
                    if row.id in processed_text_channels:
                        print(f"⚠️ 預約 {row.id} 已在記憶體中標記為已處理，跳過")
                        continue  # 靜默跳過，不輸出日誌
                    
                    # 檢查資料庫中是否已經有文字頻道ID（在線程中執行）
                    # ADDED FOR TRANSACTION SAFETY: 每輪創建新 session，確保異常時 rollback
                    def check_existing_channel(booking_id):
                        # ADDED FOR TRANSACTION SAFETY: 使用 with Session() 確保自動關閉
                        with Session() as check_s:
                            try:
                                existing_channel = check_s.execute(
                                    text("SELECT \"discordTextChannelId\" FROM \"Booking\" WHERE id = :booking_id"),
                                    {"booking_id": booking_id}
                                ).fetchone()
                                return existing_channel[0] if existing_channel and existing_channel[0] else None
                            except Exception as e:
                                # ADDED FOR TRANSACTION SAFETY: 確保異常時 rollback
                                check_s.rollback()
                                raise
                    
                    existing_channel_id = await asyncio.to_thread(check_existing_channel, row.id)
                    if existing_channel_id:
                        # 🔥 如果已有頻道 ID，驗證頻道是否真的存在且可用
                        guild = bot.get_guild(GUILD_ID)
                        if guild:
                            try:
                                text_channel = guild.get_channel(int(existing_channel_id))
                                if text_channel:
                                    # 頻道存在且可用，標記為已處理
                                    print(f"✅ 預約 {row.id} 在資料庫中已有文字頻道ID且頻道存在，跳過")
                                    processed_text_channels.add(row.id)
                                    continue
                                else:
                                    # 頻道 ID 存在但頻道不存在，視為錯誤
                                    print(f"❌ 錯誤：預約 {row.id} 的文字頻道 ID {existing_channel_id} 在 Discord 中不存在")
                                    # 不標記為 processed，允許後續重試
                                    continue
                            except (ValueError, TypeError) as e:
                                print(f"❌ 錯誤：預約 {row.id} 的文字頻道 ID {existing_channel_id} 無效: {e}")
                                # 不標記為 processed，允許後續重試
                                continue
                        else:
                            # 無法驗證頻道，不標記為 processed
                            print(f"⚠️ 預約 {row.id} 在資料庫中已有文字頻道ID，但無法驗證頻道是否存在")
                            # 不標記為 processed，允許後續重試
                            continue
                    
                    # ⚠️ 允許在此流程建立文字頻道（一般預約預聊已完成，5 分鐘前補建）
                    if not existing_channel_id:
                        # 檢查必備的 Discord 名稱
                        if not row.customer_discord or not row.partner_discord:
                            print(f"❌ 預約 {row.id} 缺少 Discord 名稱: 顧客={row.customer_discord}, 夥伴={row.partner_discord}")
                            continue

                        # 嘗試建立文字頻道
                        try:
                            text_channel = await create_booking_text_channel(
                                row.id,
                                row.customer_discord,
                                row.partner_discord,
                                row.startTime,
                                row.endTime
                            )
                            if not text_channel:
                                # 建立失敗，保留待重試
                                continue
                        except Exception as e:
                            print(f"❌ 預約 {row.id} 建立文字頻道失敗: {e}")
                            continue

                        # 建立成功後，更新資料庫並標記 processed
                        try:
                            # ADDED FOR TRANSACTION SAFETY: 每輪創建新 session，確保異常時 rollback
                            with Session() as update_s:
                                try:
                                    update_s.execute(
                                        text("""
                                            UPDATE "Booking"
                                            SET "discordTextChannelId" = :channel_id
                                            WHERE id = :booking_id
                                        """),
                                        {"channel_id": str(text_channel.id), "booking_id": row.id}
                                    )
                                    update_s.commit()
                                except Exception as e:
                                    # ADDED FOR TRANSACTION SAFETY: 確保異常時 rollback
                                    update_s.rollback()
                                    raise
                            processed_text_channels.add(row.id)
                            print(f"✅ 預約 {row.id} 已建立文字頻道並寫回資料庫")
                            continue
                        except Exception as db_err:
                            print(f"❌ 預約 {row.id} 保存文字頻道 ID 失敗: {db_err}")
                            # 不標記 processed，允許後續重試
                            continue
                        
                except Exception as e:
                    print(f"❌ 處理新預約 {row.id} 時發生錯誤: {e}")
                    continue
                    
    except Exception as e:
        # 資料庫連線錯誤時安全跳過，不讓 bot 崩潰
        if is_db_connection_error(e):
            return  # 安全跳過該輪檢查
        print(f"❌ 檢查新預約時發生錯誤: {e}")

# --- 自動關閉「現在有空」狀態任務 ---
@tasks.loop(seconds=60)  # 每1分鐘檢查一次
async def auto_close_available_now():
    """自動關閉開啟超過30分鐘的「現在有空」狀態"""
    await bot.wait_until_ready()
    
    try:
        # 計算30分鐘前的時間
        thirty_minutes_ago = datetime.now(timezone.utc) - timedelta(minutes=30)
        
        # 將同步資料庫操作移到線程池，避免阻塞事件循環
        # ADDED FOR TRANSACTION SAFETY: 每輪創建新 session，確保異常時 rollback
        def query_and_update_expired():
            # ADDED FOR TRANSACTION SAFETY: 使用 with Session() 確保自動關閉
            with Session() as s:
                try:
                    # 查詢開啟「現在有空」超過30分鐘的夥伴
                    expired_query = """
                    SELECT id, name, "availableNowSince"
                    FROM "Partner"
                    WHERE "isAvailableNow" = true
                    AND "availableNowSince" < :thirty_minutes_ago
                    """
                    
                    expired_partners = s.execute(text(expired_query), {"thirty_minutes_ago": thirty_minutes_ago}).fetchall()
                    
                    if expired_partners:
                        # 批量關閉過期的「現在有空」狀態
                        update_query = """
                        UPDATE "Partner"
                        SET "isAvailableNow" = false, "availableNowSince" = NULL
                        WHERE "isAvailableNow" = true
                        AND "availableNowSince" < :thirty_minutes_ago
                        """
                        
                        result = s.execute(text(update_query), {"thirty_minutes_ago": thirty_minutes_ago})
                        s.commit()
                        return len(expired_partners)
                    return 0
                except Exception as e:
                    # ADDED FOR TRANSACTION SAFETY: 確保異常時 rollback
                    s.rollback()
                    raise
        
        # 在線程池中執行資料庫操作
        expired_count = await asyncio.to_thread(query_and_update_expired)
        
        if expired_count > 0:
            print(f"🕐 自動關閉了 {expired_count} 個夥伴的「現在有空」狀態")
        # 沒有需要關閉的狀態，不輸出日誌
                
    except Exception as e:
        print(f"❌ 自動關閉「現在有空」狀態時發生錯誤: {e}")

# --- 檢查即時預約並立即創建文字頻道 ---
@tasks.loop(seconds=60)  # 每60秒檢查一次，減少資料庫負載
async def check_instant_bookings_for_text_channel():
    """檢查新的即時預約並立即創建文字頻道"""
    await bot.wait_until_ready()
    
    try:
        # 將同步資料庫操作移到線程池，避免阻塞事件循環
        # ADDED FOR TRANSACTION SAFETY: 每輪創建新 session，確保異常時 rollback
        def query_instant_bookings():
            # ADDED FOR TRANSACTION SAFETY: 使用 with Session() 確保自動關閉
            with Session() as s:
                try:
                    now = datetime.now(timezone.utc)
                    query = """
                        SELECT 
                            b.id, b."customerId", b."scheduleId", b.status, b."createdAt", b."updatedAt",
                            c.name as customer_name,
                            COALESCE(b."paymentInfo"->>'customerDiscord', cu.discord) as customer_discord,
                            p.name as partner_name, pu.discord as partner_discord,
                            s."startTime", s."endTime",
                            b."paymentInfo"->>'discordDelayMinutes' as discord_delay_minutes,
                            b."serviceType" as service_type,
                            b."paymentInfo"->>'isChatOnly' as is_chat_only
                        FROM "Booking" b
                        JOIN "Schedule" s ON s.id = b."scheduleId"
                        JOIN "Customer" c ON c.id = b."customerId"
                        JOIN "User" cu ON cu.id = c."userId"
                        JOIN "Partner" p ON p.id = s."partnerId"
                        JOIN "User" pu ON pu.id = p."userId"
                        WHERE b.status = 'CONFIRMED'
                        AND b."paymentInfo"->>'isInstantBooking' = 'true'
                        AND b."discordEarlyTextChannelId" IS NULL
                        AND s."startTime" > :now
                    """
                    result = s.execute(text(query), {"now": now})
                    return result.fetchall()
                except Exception as e:
                    # ADDED FOR TRANSACTION SAFETY: 確保異常時 rollback
                    s.rollback()
                    raise
        
        # 添加連接重試機制
        max_retries = 3
        rows = []
        for attempt in range(max_retries):
            try:
                # 在線程池中執行資料庫查詢
                rows = await asyncio.to_thread(query_instant_bookings)
                break  # 成功執行，跳出重試循環
            except Exception as db_error:
                if attempt < max_retries - 1:
                    print(f"⚠️ 資料庫連接失敗，重試 {attempt + 1}/{max_retries}: {db_error}")
                    await asyncio.sleep(2 ** attempt)  # 指數退避
                else:
                    print(f"❌ 資料庫連接失敗，已重試 {max_retries} 次: {db_error}")
                    return
        
        # 🔥 過濾掉已經處理過的預約（避免重複輸出）
        filtered_rows = [row for row in rows if row.id not in processed_text_channels]
        
        # 處理找到的即時預約
        for row in filtered_rows:
            try:
                booking_id = row.id
                
                # 檢查是否已經處理過
                if booking_id in processed_text_channels:
                    continue
                
                guild = bot.get_guild(GUILD_ID)
                if not guild:
                    print("❌ 找不到 Discord 伺服器")
                    continue
                
                # 🔥 嘗試查找 Discord 成員（優先使用用戶名，因為 Discord 用戶名更可靠）
                customer_name = row.customer_name
                partner_name = row.partner_name
                customer_discord = row.customer_discord
                partner_discord = row.partner_discord
                
                # 🔥 調試信息：只在第一次處理時輸出，避免重複輸出
                if booking_id not in processed_text_channels:
                    print(f"🔍 即時預約 {booking_id} Discord 信息: 顧客名稱={customer_name}, 顧客Discord={customer_discord}, 夥伴名稱={partner_name}, 夥伴Discord={partner_discord}")
                
                customer_member = None
                partner_member = None
                
                # 🔥 優先使用 Discord 字段查找（因為這是用戶在 Discord 中的實際用戶名，最可靠）
                # 先嘗試用 Discord 字段查找
                if customer_discord:
                    try:
                        # 🔥 不管 Discord 名稱有什麼特殊符號，都嘗試查找成員
                        # 先嘗試作為 Discord ID 查找（如果是純數字且長度足夠）
                        discord_id_clean = str(customer_discord).replace('.', '').replace('-', '') if isinstance(customer_discord, str) else str(customer_discord)
                        if discord_id_clean.isdigit() and len(discord_id_clean) >= 17:
                            # 這是 Discord ID，直接查找
                            customer_member = guild.get_member(int(discord_id_clean))
                            if customer_member:
                                print(f"✅ 通過 Discord ID 找到顧客: {customer_member.name}")
                        else:
                            # 這是用戶名（可能包含特殊符號），使用 find_member_by_discord_name 查找
                            customer_member = find_member_by_discord_name(guild, str(customer_discord))
                    except (ValueError, TypeError) as e:
                        # 如果查找失敗，繼續嘗試用用戶名查找
                        customer_member = None
                
                # 如果 Discord 字段找不到，再嘗試用用戶名查找
                if not customer_member and customer_name:
                    print(f"🔍 Discord 字段找不到，嘗試用用戶名查找顧客: '{customer_name}'")
                    customer_member = find_member_by_discord_name(guild, customer_name)
                
                # 🔥 優先使用 Discord 字段查找夥伴（因為這是用戶在 Discord 中的實際用戶名）
                # 先嘗試用 Discord 字段查找（這是最可靠的）
                if partner_discord:
                    try:
                        # 🔥 不管 Discord 名稱有什麼特殊符號，都嘗試查找成員
                        # 先嘗試作為 Discord ID 查找（如果是純數字且長度足夠）
                        discord_id_clean = str(partner_discord).replace('.', '').replace('-', '') if isinstance(partner_discord, str) else str(partner_discord)
                        if discord_id_clean.isdigit() and len(discord_id_clean) >= 17:
                            # 這是 Discord ID，直接查找
                            partner_member = guild.get_member(int(discord_id_clean))
                        else:
                            # 這是用戶名（可能包含特殊符號），使用 find_member_by_discord_name 查找
                            partner_member = find_member_by_discord_name(guild, str(partner_discord))
                    except (ValueError, TypeError) as e:
                        # 如果查找失敗，繼續嘗試用用戶名查找
                        partner_member = None
                
                # 如果 Discord 字段找不到，再嘗試用用戶名查找
                if not partner_member and partner_name:
                    print(f"🔍 Discord 字段找不到，嘗試用用戶名查找夥伴: {partner_name}")
                    partner_member = find_member_by_discord_name(guild, partner_name)
                
                # 如果還是找不到，輸出警告並嘗試最後的查找方式
                if not customer_member:
                    print(f"❌ 找不到 Discord 成員: 顧客={customer_name} (Discord: {customer_discord})")
                    # 🔥 最後嘗試：直接遍歷所有成員，查找完全匹配的用戶名
                    if customer_discord:
                        for member in guild.members:
                            if member.name == customer_discord or (member.display_name and member.display_name == customer_discord):
                                customer_member = member
                                print(f"✅ 最後嘗試成功找到 Discord 成員: {member.name} (顯示名稱: {member.display_name}) 匹配 {customer_discord}")
                                break
                    # 🔥 如果 customer_discord 為 None，嘗試用 customer_name 進行更寬鬆的匹配
                    elif customer_name:
                        # 嘗試清理特殊字符後匹配
                        customer_name_clean = customer_name.lower().replace('_', '').replace('.', '').replace('-', '')
                        for member in guild.members:
                            member_name_clean = member.name.lower().replace('_', '').replace('.', '').replace('-', '')
                            member_display_clean = (member.display_name.lower() if member.display_name else "").replace('_', '').replace('.', '').replace('-', '')
                            if (member_name_clean == customer_name_clean or member_display_clean == customer_name_clean or
                                customer_name_clean in member_name_clean or customer_name_clean in member_display_clean):
                                customer_member = member
                                print(f"✅ 通過清理特殊字符匹配找到顧客: {member.name} (查詢: {customer_name})")
                                break
                
                if not partner_member:
                    print(f"❌ 找不到 Discord 成員: 夥伴={partner_name} (Discord: {partner_discord})")
                    # 🔥 最後嘗試：直接遍歷所有成員，查找完全匹配的用戶名
                    if partner_discord:
                        for member in guild.members:
                            if member.name == partner_discord or (member.display_name and member.display_name == partner_discord):
                                partner_member = member
                                print(f"✅ 最後嘗試成功找到 Discord 成員: {member.name} (顯示名稱: {member.display_name}) 匹配 {partner_discord}")
                                break
                    # 🔥 如果 partner_discord 為 None，嘗試用 partner_name 進行更寬鬆的匹配
                    elif partner_name:
                        # 嘗試清理特殊字符後匹配
                        partner_name_clean = partner_name.lower().replace('_', '').replace('.', '').replace('-', '')
                        for member in guild.members:
                            member_name_clean = member.name.lower().replace('_', '').replace('.', '').replace('-', '')
                            member_display_clean = (member.display_name.lower() if member.display_name else "").replace('_', '').replace('.', '').replace('-', '')
                            if (member_name_clean == partner_name_clean or member_display_clean == partner_name_clean or
                                partner_name_clean in member_name_clean or partner_name_clean in member_display_clean):
                                partner_member = member
                                print(f"✅ 通過清理特殊字符匹配找到夥伴: {member.name} (查詢: {partner_name})")
                                break
                
                # 🔥 即使找不到 Discord 成員，也繼續創建頻道（用戶可能尚未加入伺服器）
                if not customer_member or not partner_member:
                    missing_info = []
                    if not customer_member:
                        missing_info.append(f"顧客={customer_discord}")
                    if not partner_member:
                        missing_info.append(f"夥伴={partner_discord}")
                    print(f"⚠️ 即時預約 {booking_id} 找不到 Discord 成員: {', '.join(missing_info)}，將繼續創建頻道（用戶可能尚未加入伺服器）")
                    # 繼續創建頻道，即使找不到成員
                
                # 計算時長
                start_time = row.startTime
                end_time = row.endTime
                if start_time.tzinfo is None:
                    start_time = start_time.replace(tzinfo=timezone.utc)
                if end_time.tzinfo is None:
                    end_time = end_time.replace(tzinfo=timezone.utc)
                duration_minutes = int((end_time - start_time).total_seconds() / 60)
                
                # 轉換為台灣時間
                tw_start_time = start_time.astimezone(TW_TZ)
                tw_end_time = end_time.astimezone(TW_TZ)
                start_time_str = tw_start_time.strftime("%Y/%m/%d %H:%M")
                end_time_str = tw_end_time.strftime("%H:%M")
                
                # 🔥 判斷是否為純聊天（與群組預約邏輯一致）
                is_chat_only = (
                    row.service_type == 'CHAT_ONLY' or 
                    row.is_chat_only == 'true' or 
                    row.is_chat_only == True
                )
                
                # 🔥 使用 booking_id 的 hash 來確定性地選擇動物，確保文字和語音頻道使用相同的動物（與群組預約邏輯一致）
                import hashlib
                hash_obj = hashlib.md5(str(booking_id).encode())
                hash_hex = hash_obj.hexdigest()
                animal = CUTE_ITEMS[int(hash_hex[:2], 16) % len(CUTE_ITEMS)]
                
                # 🔥 創建頻道名稱（與群組預約格式一致）
                if is_chat_only:
                    channel_name = f"👥{animal}純聊天預約"
                else:
                    channel_name = f"👥{animal}即時預約聊天"
                
                # 🔥 檢查是否已存在相同名稱的文字頻道（防止重複創建，與群組預約邏輯一致）
                existing_channels = [ch for ch in guild.text_channels if ch.name == channel_name]
                if existing_channels:
                    # 🔥 只有在以下條件全部成立時，才允許標記為 processed：
                    # 1. 頻道存在且可用
                    # 2. Discord 成員成功取得 (customer_member 和 partner_member 都存在)
                    # 3. 至少完成一個實際 Discord 動作（如更新資料庫）
                    if customer_member and partner_member:
                        print(f"✅ 已存在相同名稱的文字頻道: {channel_name}，更新資料庫並標記為已處理")
                        with Session() as update_s:
                            update_s.execute(
                                text("UPDATE \"Booking\" SET \"discordEarlyTextChannelId\" = :channel_id WHERE id = :booking_id"),
                                {"channel_id": str(existing_channels[0].id), "booking_id": booking_id}
                            )
                            update_s.commit()
                        # 只有在成功更新資料庫且成員都存在時，才標記為 processed
                        processed_text_channels.add(booking_id)
                        continue
                    else:
                        print(f"⚠️ 已存在相同名稱的文字頻道: {channel_name}，但缺少 Discord 成員，不標記為 processed")
                        # 不標記為 processed，允許後續重試
                        continue
                
                # 🔥 找到分類（與群組預約邏輯一致）
                category = discord.utils.get(guild.categories, name="Voice Channels")
                if not category:
                    category = discord.utils.get(guild.categories, name="語音頻道")
                if not category:
                    category = discord.utils.get(guild.categories, name="語音")
                if not category:
                    if guild.categories:
                        category = guild.categories[0]
                    else:
                        print("❌ 找不到任何分類")
                        continue
                
                # 🔥 設定權限（與群組預約邏輯一致）
                overwrites = {
                    guild.default_role: discord.PermissionOverwrite(view_channel=False),
                }
                
                # 為顧客添加權限
                if customer_member:
                    overwrites[customer_member] = discord.PermissionOverwrite(view_channel=True, send_messages=True)
                
                # 為夥伴添加權限
                if partner_member:
                    overwrites[partner_member] = discord.PermissionOverwrite(view_channel=True, send_messages=True)
                
                # 允許在此流程建立文字頻道（429 安全，即時預約）
                try:
                    text_channel = await safe_create_text_channel(
                        guild,
                        name=channel_name,
                        category=category,
                        overwrites=overwrites
                    )
                except Exception as e:
                    print(f"❌ 即時預約 {booking_id} 創建文字頻道失敗: {e}")
                    continue
                
                # 建立成功後，更新資料庫的提前溝通頻道 ID
                try:
                    with Session() as s:
                        s.execute(
                            text("""
                                UPDATE "Booking"
                                SET "discordEarlyTextChannelId" = :channel_id
                                WHERE id = :booking_id
                            """),
                            {"channel_id": str(text_channel.id), "booking_id": booking_id}
                        )
                        s.commit()
                except Exception as db_err:
                    print(f"❌ 即時預約 {booking_id} 保存文字頻道 ID 失敗: {db_err}")
                    continue
                # 🔥 發送歡迎訊息（與群組預約格式一致）
                welcome_embed = discord.Embed(
                    title="🎮 即時預約聊天頻道" if not is_chat_only else "🎮 純聊天預約聊天頻道",
                    description="歡迎來到即時預約聊天頻道！" if not is_chat_only else "歡迎來到純聊天預約聊天頻道！",
                    color=0x9b59b6,
                    timestamp=datetime.now(timezone.utc)
                )
                
                # 顯示顧客（優先使用 Discord mention，如果找不到則使用 Discord 用戶名）
                if customer_member:
                    welcome_embed.add_field(
                        name="👤 顧客",
                        value=customer_member.mention,
                        inline=False
                    )
                elif customer_discord:
                    # 使用 Discord 用戶名（格式：@username），這樣才能正確抓取用戶
                    welcome_embed.add_field(
                        name="👤 顧客",
                        value=f"@{customer_discord}",
                        inline=False
                    )
                else:
                    welcome_embed.add_field(
                        name="👤 顧客",
                        value=customer_name,
                        inline=False
                    )
                
                # 顯示夥伴（優先使用 Discord mention，如果找不到則使用 Discord 用戶名）
                if partner_member:
                    welcome_embed.add_field(
                        name="👥 夥伴們",
                        value=partner_member.mention,
                        inline=False
                    )
                elif partner_discord:
                    # 使用 Discord 用戶名（格式：@username），這樣才能正確抓取用戶
                    welcome_embed.add_field(
                        name="👥 夥伴們",
                        value=f"@{partner_discord}",
                        inline=False
                    )
                else:
                    welcome_embed.add_field(
                        name="👥 夥伴們",
                        value=partner_name,
                        inline=False
                    )
                
                welcome_embed.add_field(
                    name="⏰ 開始時間",
                    value=f"`{start_time_str} - {end_time_str}`",
                    inline=True
                )
                
                welcome_embed.add_field(
                    name="📋 預約ID",
                    value=f"`{booking_id}`",
                    inline=True
                )
                
                await text_channel.send(embed=welcome_embed)
                
                # 🔥 如果找不到成員，在頻道中 @ 提及用戶名（即使用戶尚未加入伺服器）
                if not customer_member and customer_discord:
                    try:
                        await text_channel.send(f"👤 顧客：@{customer_discord}")
                    except:
                        pass
                if not partner_member and partner_discord:
                    try:
                        await text_channel.send(f"👥 夥伴：@{partner_discord}")
                    except:
                        pass
                
                # 🔥 發送安全規範（與群組預約格式一致）
                safety_embed = discord.Embed(
                    title="🎙️ 即時預約聊天頻道使用規範與警告" if not is_chat_only else "🎙️ 純聊天預約聊天頻道使用規範與警告",
                    description="為了您的安全，請務必遵守以下規範：",
                    color=0xff6b6b,
                    timestamp=datetime.now(timezone.utc)
                )
                
                if is_chat_only:
                    safety_embed.add_field(
                        name="📌 頻道性質",
                        value="此聊天頻道為【純聊天預約用途】。\n僅限輕鬆互動、日常話題、遊戲閒聊使用。\n禁止任何涉及交易、暗示、或其他非聊天用途的行為。",
                        inline=False
                    )
                else:
                    safety_embed.add_field(
                        name="📌 頻道性質",
                        value="此聊天頻道為【即時預約用途】。\n僅限遊戲討論、戰術交流、團隊協作使用。\n禁止任何涉及交易、暗示、或其他非遊戲用途的行為。",
                        inline=False
                    )
                
                safety_embed.add_field(
                    name="⚠️ 使用規範（請務必遵守）",
                    value="• 禁止挑釁、辱罵、騷擾他人，保持禮貌尊重\n"
                          "• 禁止使用色情、暴力、血腥、歧視等不當言語或內容\n"
                          "• 不得進行金錢交易、索取或提供個資（例如 LINE、IG、電話）\n"
                          "• 不得錄音、偷拍或截圖他人對話，除非經雙方同意\n"
                          "• 禁止惡意模仿或干擾他人聊天\n"
                          "• 禁止使用變聲器或播放音效干擾頻道秩序",
                    inline=False
                )
                
                safety_embed.add_field(
                    name="🚨 警告事項",
                    value="• 系統將隨機錄取部分聊天內容以進行安全稽核\n"
                          "• 如被舉報違規，管理員可立即封鎖或禁言，不另行通知\n"
                          "• 為了您的安全，禁止隨意透漏個人資訊，包括(身分證、住家地址、等等......)\n"
                          "• 若你無法接受以上規範，請勿加入頻道",
                    inline=False
                )
                
                await text_channel.send(embed=safety_embed)
                
                # 🔥 語音頻道將在預約開始前 5 分鐘創建（不在這裡創建）
                # 更新資料庫，保存文字頻道 ID（用於倒數計時和評價系統）
                with Session() as update_s:
                    update_s.execute(
                        text("UPDATE \"Booking\" SET \"discordTextChannelId\" = :text_channel_id WHERE id = :booking_id"),
                        {
                            "text_channel_id": str(text_channel.id),
                            "booking_id": booking_id
                        }
                    )
                    update_s.commit()
                
                # 🔥 創建語音頻道的任務（在預約開始前 5 分鐘執行）
                async def create_voice_channel_5min_before():
                    try:
                        # 獲取當前時間
                        current_now = datetime.now(timezone.utc)
                        
                        # 計算等待時間：預約開始時間 - 3 分鐘 - 現在時間
                        wait_seconds = (start_time - current_now).total_seconds() - 180  # 減去 3 分鐘（180 秒）
                        
                        # 🔥 只在第一次啟動時輸出日誌，避免重複輸出
                        if wait_seconds > 0:
                            # 只在等待時間較長時輸出一次日誌
                            if wait_seconds > 300:  # 只在大於5分鐘時輸出
                                print(f"⏰ 語音頻道將在 {wait_seconds/60:.1f} 分鐘後創建: 預約 {booking_id}")
                            await asyncio.sleep(wait_seconds)
                        else:
                            print(f"⚡ 立即創建語音頻道（已超過開始前 3 分鐘）: 預約 {booking_id}")
                        
                        # 檢查預約狀態是否仍然是 CONFIRMED，以及是否已經創建過語音頻道
                        with Session() as check_s:
                            current_booking = check_s.execute(
                                text("SELECT status, \"discordVoiceChannelId\" FROM \"Booking\" WHERE id = :booking_id"),
                                {"booking_id": booking_id}
                            ).fetchone()
                            
                            if not current_booking or current_booking.status != 'CONFIRMED':
                                print(f"⚠️ 預約 {booking_id} 狀態已改變，取消創建語音頻道")
                                return
                            
                            # 🔥 檢查是否已經創建過語音頻道，避免重複創建
                            if current_booking.discordVoiceChannelId:
                                print(f"✅ 預約 {booking_id} 的語音頻道已存在，跳過創建")
                                return
                        
                        # 重新查找 Discord 成員（可能現在已經在伺服器中了）
                        customer_member_vc = None
                        partner_member_vc = None
                        
                        if customer_discord:
                            try:
                                if customer_discord.replace('.', '').replace('-', '').isdigit():
                                    customer_member_vc = guild.get_member(int(float(customer_discord)))
                                else:
                                    customer_member_vc = find_member_by_discord_name(guild, customer_discord)
                            except (ValueError, TypeError):
                                customer_member_vc = None
                        
                        if partner_discord:
                            try:
                                if partner_discord.replace('.', '').replace('-', '').isdigit():
                                    partner_member_vc = guild.get_member(int(float(partner_discord)))
                                else:
                                    partner_member_vc = find_member_by_discord_name(guild, partner_discord)
                            except (ValueError, TypeError):
                                partner_member_vc = None
                        
                        # 如果找不到成員，嘗試使用用戶名查找
                        if not customer_member_vc and customer_name:
                            customer_member_vc = find_member_by_discord_name(guild, customer_name)
                        
                        if not partner_member_vc and partner_name:
                            partner_member_vc = find_member_by_discord_name(guild, partner_name)
                        
                        # 🔥 使用與文字頻道完全相同的名稱
                        if is_chat_only:
                            voice_channel_name = f"👥{animal}純聊天預約"
                        else:
                            voice_channel_name = f"👥{animal}即時預約聊天"  # 與文字頻道名稱一致
                        
                        # 設定語音頻道權限
                        voice_overwrites = {
                            guild.default_role: discord.PermissionOverwrite(view_channel=False),
                        }
                        
                        # 為顧客添加權限
                        if customer_member_vc:
                            voice_overwrites[customer_member_vc] = discord.PermissionOverwrite(view_channel=True, connect=True, speak=True)
                            print(f"✅ 為顧客 {customer_member_vc.name} 設置語音頻道權限")
                        else:
                            print(f"⚠️ 未找到顧客成員，將創建匿名語音頻道")
                        
                        # 為夥伴添加權限
                        if partner_member_vc:
                            voice_overwrites[partner_member_vc] = discord.PermissionOverwrite(view_channel=True, connect=True, speak=True)
                            print(f"✅ 為夥伴 {partner_member_vc.name} 設置語音頻道權限")
                        else:
                            print(f"⚠️ 未找到夥伴成員，將創建匿名語音頻道")
                        
                        # 🔥 即使找不到成員，也要創建語音頻道（匿名頻道）
                        print(f"🔍 準備創建語音頻道: {voice_channel_name}")
                        print(f"   類別: {category.name if category else 'None'}")
                        print(f"   權限覆蓋數量: {len(voice_overwrites)}")
                        
                        # 創建語音頻道
                        voice_channel = await guild.create_voice_channel(
                            name=voice_channel_name,
                            category=category,
                            overwrites=voice_overwrites,
                            user_limit=2
                        )
                        print(f"✅ 語音頻道已創建: {voice_channel.name} (ID: {voice_channel.id})")
                        
                        # 更新資料庫，保存語音頻道 ID
                        with Session() as update_s:
                            update_s.execute(
                                text("UPDATE \"Booking\" SET \"discordVoiceChannelId\" = :voice_channel_id WHERE id = :booking_id"),
                                {
                                    "voice_channel_id": str(voice_channel.id),
                                    "booking_id": booking_id
                                }
                            )
                            update_s.commit()
                        
                        print(f"✅ 已為即時預約 {booking_id} 創建語音頻道: {voice_channel_name}")
                        
                        # 在文字頻道發送通知
                        if text_channel:
                            embed = discord.Embed(
                                title="🎤 語音頻道已創建！",
                                description=f"語音頻道 {voice_channel.mention} 已準備就緒，您可以開始使用。",
                                color=0x00ff00,
                                timestamp=datetime.now(timezone.utc)
                            )
                            embed.add_field(name="⏰ 預約時長", value=f"{duration_minutes} 分鐘", inline=True)
                            embed.add_field(name="🎤 語音頻道", value=f"{voice_channel.mention}", inline=True)
                            await text_channel.send(embed=embed)
                    except Exception as e:
                        print(f"❌ 創建語音頻道失敗: {e}")
                        import traceback
                        traceback.print_exc()
                
                # 啟動創建語音頻道任務（在預約開始前 5 分鐘）
                # 🔥 避免重複啟動任務
                if booking_id not in active_voice_channel_tasks:
                    active_voice_channel_tasks.add(booking_id)
                    bot.loop.create_task(create_voice_channel_5min_before())
                    # 🔥 減少日誌輸出，只在第一次啟動時輸出
                    # print(f"🔍 語音頻道創建任務已啟動: 預約 {booking_id}")
                
                # 🔥 啟動倒數計時任務（包含評價系統）
                # 注意：語音頻道會在預約開始前 5 分鐘創建，所以這裡先傳 None
                # 倒數計時任務會從資料庫讀取語音頻道 ID
                # 🔥 避免重複啟動任務
                if booking_id not in active_countdown_tasks:
                    active_countdown_tasks.add(booking_id)
                    bot.loop.create_task(countdown_with_rating(
                        None,  # vc_id（語音頻道尚未創建）
                        None,  # channel_name（語音頻道尚未創建）
                        text_channel, 
                        None,  # vc（語音頻道尚未創建）
                        [customer_member, partner_member] if customer_member and partner_member else [],
                        [customer_member, partner_member] if customer_member and partner_member else [],
                        None,  # record_id（如果找不到成員，可能為 None）
                        booking_id
                    ))
                
                # 發送預約通知到「創建通知」頻道
                notification_channel = bot.get_channel(1419585779432423546)
                if notification_channel:
                    # 🔥 減少重複日誌輸出
                    # print(f"🔍 準備發送即時預約通知: booking_id={booking_id}, notification_channel={notification_channel}")
                    try:
                        # 格式化時間（使用台灣時間，已經在上面計算過了）
                        start_time_str_full = start_time_str
                        end_time_str_full = end_time_str
                        
                        notification_embed = discord.Embed(
                            title="🎉 新預約通知",
                            description="新的預約已創建！",
                            color=0x00ff00,
                            timestamp=datetime.now(timezone.utc)
                        )
                        
                        # 第一行：時間和參與者
                        notification_embed.add_field(
                            name="📅 預約時間",
                            value=f"`{start_time_str_full} - {end_time_str_full}`",
                            inline=True
                        )
                        # 使用 Discord 用戶名或 mention
                        customer_display = customer_member.mention if customer_member else (f"@{customer_discord}" if customer_discord else customer_name)
                        partner_display = partner_member.mention if partner_member else (f"@{partner_discord}" if partner_discord else partner_name)
                        
                        notification_embed.add_field(
                            name="👥 參與者",
                            value=f"{customer_display} × {partner_display}",
                            inline=True
                        )
                        notification_embed.add_field(
                            name="💬 溝通頻道",
                            value=f"{text_channel.mention}",
                            inline=True
                        )
                        
                        # 第二行：時長和語音頻道
                        notification_embed.add_field(
                            name="⏰ 時長",
                            value=f"`{duration_minutes} 分鐘`",
                            inline=True
                        )
                        notification_embed.add_field(
                            name="🎤 語音頻道",
                            value="`將在預約開始前 5 分鐘自動創建`",
                            inline=True
                        )
                        notification_embed.add_field(
                            name="🆔 預約ID",
                            value=f"`{booking_id}`",
                            inline=True
                        )
                        
                        await notification_channel.send(embed=notification_embed)
                        print(f"✅ 已發送即時預約通知到創建通知頻道: {booking_id}")
                        
                        # 創建配對記錄（與手動創建頻道邏輯一致）
                        # 嘗試從 Discord ID 獲取用戶 ID
                        customer_discord = row.customer_discord
                        partner_discord = row.partner_discord
                        user1_id = None
                        user2_id = None
                        
                        # 嘗試從 Discord ID 獲取用戶 ID
                        try:
                            if customer_discord and customer_discord.replace('.', '').replace('-', '').isdigit():
                                user1_id = str(int(float(customer_discord)))
                        except (ValueError, TypeError):
                            pass
                        
                        try:
                            if partner_discord and partner_discord.replace('.', '').replace('-', '').isdigit():
                                user2_id = str(int(float(partner_discord)))
                        except (ValueError, TypeError):
                            pass
                        record_id = None
                        created_at = None
                        
                        if user1_id and user2_id:
                            try:
                                with Session() as s:
                                    # 先檢查是否已經存在配對記錄
                                    existing_record = s.execute(
                                        text("SELECT id, \"createdAt\" FROM \"PairingRecord\" WHERE \"bookingId\" = :booking_id"),
                                        {"booking_id": booking_id}
                                    ).fetchone()
                                    
                                    if existing_record:
                                        record_id = existing_record[0]
                                        created_at = existing_record[1]
                                        print(f"✅ 使用現有配對記錄: {record_id}")
                                    else:
                                        # 生成唯一的 ID
                                        import uuid
                                        record_id = str(uuid.uuid4())
                                        
                                        record = PairingRecord(
                                            id=record_id,
                                            user1Id=user1_id,
                                            user2Id=user2_id,
                                            duration=duration_minutes * 60,
                                            animalName="預約頻道",
                                            bookingId=booking_id
                                        )
                                        s.add(record)
                                        s.commit()
                                        created_at = record.createdAt
                                        print(f"✅ 創建新配對記錄: {record_id} (即時預約)")
                            except Exception as e:
                                print(f"⚠️ 創建配對記錄失敗: {e}")
                                import traceback
                                traceback.print_exc()
                    except Exception as notify_error:
                        print(f"⚠️ 發送即時預約通知失敗: {notify_error}")
                        import traceback
                        traceback.print_exc()
                else:
                    print(f"⚠️ 找不到創建通知頻道 (ID: 1419585779432423546)")
                
                
            except Exception as e:
                print(f"❌ 處理即時預約 {row.id} 時發生錯誤: {e}")
                continue
                    
    except Exception as e:
        print(f"❌ 檢查即時預約時發生錯誤: {e}")

# --- 檢查一般預約確認後立即創建文字頻道 ---
# ⚠️ 已停用：此函數會創建文字頻道但沒有啟動倒數計時和評價系統
# 一般預約的文字頻道應由 check_new_bookings 在預約開始前5分鐘創建
# 倒數計時和評價系統由 check_voice_channel_creation 在語音頻道創建時啟動
# @tasks.loop(seconds=60)  # 每60秒檢查一次（已停用）
async def check_regular_bookings_for_text_channel():
    """⚠️ 已停用：此函數會創建文字頻道但沒有啟動倒數計時和評價系統"""
    # 函數體已全部停用，防止創建沒有倒數計時和評價系統的文字頻道
    # 一般預約的文字頻道應由 check_new_bookings 在預約開始前5分鐘創建
    # 倒數計時和評價系統由 check_voice_channel_creation 在語音頻道創建時啟動
    return
    # ========== 以下代碼已全部註解，不再執行 ==========
    # """檢查已確認的一般預約並立即創建文字頻道（類似即時預約邏輯）"""
    await bot.wait_until_ready()
    
    try:
        # 將同步資料庫操作移到線程池，避免阻塞事件循環
        def query_regular_bookings():
            with Session() as s:
                now = datetime.now(timezone.utc)
                query = """
                    SELECT 
                        b.id, b."customerId", b."scheduleId", b.status, b."createdAt", b."updatedAt",
                        c.name as customer_name,
                        COALESCE(b."paymentInfo"->>'customerDiscord', cu.discord) as customer_discord,
                        p.name as partner_name, pu.discord as partner_discord,
                        s."startTime", s."endTime"
                    FROM "Booking" b
                    JOIN "Schedule" s ON s.id = b."scheduleId"
                    JOIN "Customer" c ON c.id = b."customerId"
                    JOIN "User" cu ON cu.id = c."userId"
                    JOIN "Partner" p ON p.id = s."partnerId"
                    JOIN "User" pu ON pu.id = p."userId"
                    WHERE b.status = 'CONFIRMED'
                    AND (b."paymentInfo"->>'isInstantBooking' IS NULL OR b."paymentInfo"->>'isInstantBooking' != 'true')
                    AND b."groupBookingId" IS NULL
                    AND b."multiPlayerBookingId" IS NULL
                    AND b."discordTextChannelId" IS NULL
                    AND s."startTime" > :now
                """
                result = s.execute(text(query), {"now": now})
                return result.fetchall()
        
        # 添加連接重試機制
        max_retries = 3
        rows = []
        for attempt in range(max_retries):
            try:
                # 在線程池中執行資料庫查詢
                rows = await asyncio.to_thread(query_regular_bookings)
                break  # 成功執行，跳出重試循環
            except Exception as db_error:
                if attempt < max_retries - 1:
                    print(f"⚠️ 資料庫連接失敗，重試 {attempt + 1}/{max_retries}: {db_error}")
                    await asyncio.sleep(2 ** attempt)  # 指數退避
                else:
                    print(f"❌ 資料庫連接失敗，已重試 {max_retries} 次: {db_error}")
                    return
        
        # 🔥 過濾掉已經處理過的預約（避免重複輸出）
        filtered_rows = [row for row in rows if row.id not in processed_text_channels]
        
        if len(filtered_rows) > 0:
            print(f"🔍 找到 {len(filtered_rows)} 個一般預約需要創建文字頻道")
        
        # 處理找到的一般預約
        for row in filtered_rows:
            try:
                booking_id = row.id
                
                # 檢查是否已經處理過
                if booking_id in processed_text_channels:
                    continue
                
                customer_discord = row.customer_discord
                partner_discord = row.partner_discord
                
                if not customer_discord or not partner_discord:
                    print(f"⚠️ 預約 {booking_id} 缺少 Discord ID，跳過")
                    continue
                
                guild = bot.get_guild(GUILD_ID)
                if not guild:
                    print("❌ 找不到 Discord 伺服器")
                    continue
                
                # 獲取成員
                customer_member = None
                partner_member = None
                
                try:
                    if customer_discord.replace('.', '').replace('-', '').isdigit():
                        customer_member = guild.get_member(int(float(customer_discord)))
                    else:
                        customer_member = find_member_by_discord_name(guild, customer_discord)
                except (ValueError, TypeError):
                    customer_member = None
                
                try:
                    if partner_discord.replace('.', '').replace('-', '').isdigit():
                        partner_member = guild.get_member(int(float(partner_discord)))
                    else:
                        partner_member = find_member_by_discord_name(guild, partner_discord)
                except (ValueError, TypeError):
                    partner_member = None
                
                if not customer_member or not partner_member:
                    # 🔥 將預約 ID 添加到已處理列表，避免重複處理和輸出
                    # 如果找不到成員，無法創建頻道，但我們不希望每次都重複檢查
                    processed_text_channels.add(booking_id)
                    # 只在第一次遇到時輸出詳細信息
                    if not hasattr(check_regular_bookings_for_text_channel, '_warned_bookings'):
                        check_regular_bookings_for_text_channel._warned_bookings = set()
                    if booking_id not in check_regular_bookings_for_text_channel._warned_bookings:
                        missing_info = []
                        if not customer_member:
                            missing_info.append(f"顧客={customer_discord}")
                        if not partner_member:
                            missing_info.append(f"夥伴={partner_discord}")
                        print(f"⚠️ 預約 {booking_id} 找不到 Discord 成員，已跳過: {', '.join(missing_info)}")
                        check_regular_bookings_for_text_channel._warned_bookings.add(booking_id)
                    continue
                
                # 生成頻道名稱
                start_time = row.startTime
                end_time = row.endTime
                
                # 🔥 使用 booking ID 來確定性地生成 emoji，確保文字和語音頻道使用相同的 emoji（與語音頻道邏輯一致）
                import hashlib
                hash_obj = hashlib.md5(str(booking_id).encode())
                hash_hex = hash_obj.hexdigest()
                cute_item_full = CUTE_ITEMS[int(hash_hex[:2], 16) % len(CUTE_ITEMS)]
                # 只提取 emoji 部分（去掉後面的文字）
                cute_item = cute_item_full.split()[0] if cute_item_full else "🎀"
                
                # 確保時間有時區資訊，並轉換為台灣時間
                if start_time.tzinfo is None:
                    start_time = start_time.replace(tzinfo=timezone.utc)
                if end_time.tzinfo is None:
                    end_time = end_time.replace(tzinfo=timezone.utc)
                
                start_time_tw = start_time.astimezone(TW_TZ)
                end_time_tw = end_time.astimezone(TW_TZ)
                
                date_str = start_time_tw.strftime("%m%d")
                start_time_str = start_time_tw.strftime("%H:%M")
                end_time_str = end_time_tw.strftime("%H:%M")
                
                text_channel_name = f"📅{date_str} {start_time_str}-{end_time_str} {cute_item}"
                
                # 檢查頻道是否已存在
                existing_channel = discord.utils.get(guild.text_channels, name=text_channel_name)
                if existing_channel:
                    print(f"⚠️ 文字頻道已存在: {text_channel_name}")
                    continue
                
                # 允許建立文字頻道（429 安全，一般預約）
                try:
                    text_channel = await safe_create_text_channel(
                        guild,
                        name=text_channel_name,
                        overwrites=overwrites,
                        category=category
                    )
                except Exception as e:
                    print(f"❌ 一般預約 {booking_id} 創建文字頻道失敗: {e}")
                    continue
                
                # 建立成功後，更新資料庫的文字頻道 ID
                try:
                    with Session() as s:
                        s.execute(
                            text("""
                                UPDATE "Booking"
                                SET "discordTextChannelId" = :channel_id
                                WHERE id = :booking_id
                            """),
                            {"channel_id": str(text_channel.id), "booking_id": booking_id}
                        )
                        s.commit()
                except Exception as db_err:
                    print(f"❌ 一般預約 {booking_id} 保存文字頻道 ID 失敗: {db_err}")
                    continue
                
                # 發送歡迎訊息
                embed = discord.Embed(
                    title="🎮 預約溝通頻道",
                    description=f"歡迎 {customer_member.mention} 和 {partner_member.mention}！",
                    color=0x00ff00
                )
                embed.add_field(name="預約時間", value=f"{start_time_str} - {end_time_str}", inline=True)
                embed.add_field(name="⏰ 提醒", value="語音頻道將在預約開始前5分鐘自動創建", inline=False)
                embed.add_field(name="💬 溝通", value="請在這裡提前溝通遊戲相關事宜", inline=False)
                
                await text_channel.send(embed=embed)
                
                # 發送預約通知到「創建通知」頻道（與即時預約邏輯一致）
                notification_channel = bot.get_channel(1419585779432423546)
                if notification_channel:
                    try:
                        # 計算時長
                        duration_minutes = int((end_time - start_time).total_seconds() / 60)
                        
                        # 格式化時間（使用台灣時間）
                        start_time_str_full = start_time_tw.strftime("%Y/%m/%d %H:%M")
                        end_time_str_full = end_time_tw.strftime("%H:%M")
                        
                        notification_embed = discord.Embed(
                            title="🎉 新預約通知",
                            description="新的預約已創建！",
                            color=0x00ff00,
                            timestamp=datetime.now(timezone.utc)
                        )
                        
                        notification_embed.add_field(
                            name="📅 預約時間",
                            value=f"`{start_time_str_full} - {end_time_str_full}`",
                            inline=True
                        )
                        notification_embed.add_field(
                            name="👥 參與者",
                            value=f"{customer_member.mention} × {partner_member.mention}",
                            inline=True
                        )
                        notification_embed.add_field(
                            name="💬 溝通頻道",
                            value=f"{text_channel.mention}",
                            inline=True
                        )
                        
                        notification_embed.add_field(
                            name="⏰ 時長",
                            value=f"`{duration_minutes} 分鐘`",
                            inline=True
                        )
                        notification_embed.add_field(
                            name="🎤 語音頻道",
                            value="`將在預約開始前 5 分鐘自動創建`",
                            inline=True
                        )
                        notification_embed.add_field(
                            name="🆔 預約ID",
                            value=f"`{booking_id}`",
                            inline=True
                        )
                        
                        await notification_channel.send(embed=notification_embed)
                        print(f"✅ 已發送一般預約通知到創建通知頻道: {booking_id}")
                        
                        # 創建配對記錄（與即時預約邏輯一致）
                        user1_id = str(customer_member.id) if customer_member else None
                        user2_id = str(partner_member.id) if partner_member else None
                        record_id = None
                        created_at = None
                        
                        if user1_id and user2_id:
                            try:
                                with Session() as s:
                                    # 先檢查是否已經存在配對記錄
                                    existing_record = s.execute(
                                        text("SELECT id, \"createdAt\" FROM \"PairingRecord\" WHERE \"bookingId\" = :booking_id"),
                                        {"booking_id": booking_id}
                                    ).fetchone()
                                    
                                    if existing_record:
                                        record_id = existing_record[0]
                                        created_at = existing_record[1]
                                        print(f"✅ 使用現有配對記錄: {record_id}")
                                    else:
                                        # 生成唯一的 ID
                                        import uuid
                                        record_id = str(uuid.uuid4())
                                        
                                        record = PairingRecord(
                                            id=record_id,
                                            user1Id=user1_id,
                                            user2Id=user2_id,
                                            duration=duration_minutes * 60,
                                            animalName="預約頻道",
                                            bookingId=booking_id
                                        )
                                        s.add(record)
                                        s.commit()
                                        created_at = record.createdAt
                                        print(f"✅ 創建新配對記錄: {record_id} (一般預約)")
                            except Exception as e:
                                print(f"⚠️ 創建配對記錄失敗: {e}")
                                import traceback
                                traceback.print_exc()
                    except Exception as notify_error:
                        print(f"⚠️ 發送一般預約通知失敗: {notify_error}")
                        import traceback
                        traceback.print_exc()
                else:
                    print(f"⚠️ 找不到創建通知頻道 (ID: 1419585779432423546)")
                
                
            except Exception as e:
                print(f"❌ 處理一般預約 {row.id} 時發生錯誤: {e}")
                continue
                    
    except Exception as e:
        print(f"❌ 檢查一般預約時發生錯誤: {e}")
    # """

# --- 自動取消多人陪玩訂單任務 ---
@tasks.loop(seconds=60)  # 每1分鐘檢查一次
async def auto_cancel_multiplayer_bookings():
    """自動取消多人陪玩訂單：如果時間快到了但夥伴全部都拒絕或都沒有回應"""
    await bot.wait_until_ready()
    
    try:
        now = datetime.now(timezone.utc)
        # 檢查開始時間在5分鐘內，但還沒有任何夥伴確認的訂單
        cancel_window_start = now
        cancel_window_end = now + timedelta(minutes=5)
        
        # ADDED FOR TRANSACTION SAFETY: 每輪創建新 session，確保異常時 rollback
        def query_bookings_to_cancel():
            # ADDED FOR TRANSACTION SAFETY: 使用 with Session() 確保自動關閉
            with Session() as s:
                try:
                    query = """
                        SELECT 
                            mpb.id as multi_player_booking_id,
                            mpb."startTime",
                            mpb."endTime",
                            COUNT(DISTINCT b.id) FILTER (WHERE b.status IN ('CONFIRMED', 'PARTNER_ACCEPTED')) as confirmed_count,
                            COUNT(DISTINCT b.id) FILTER (WHERE b.status IN ('REJECTED', 'PARTNER_REJECTED')) as rejected_count,
                            COUNT(DISTINCT b.id) as total_count
                        FROM "MultiPlayerBooking" mpb
                        JOIN "Booking" b ON b."multiPlayerBookingId" = mpb.id
                        WHERE mpb.status IN ('ACTIVE', 'PENDING')
                        AND mpb."startTime" >= :window_start
                        AND mpb."startTime" <= :window_end
                        GROUP BY mpb.id, mpb."startTime", mpb."endTime"
                        HAVING 
                            -- 沒有任何夥伴確認，且所有夥伴都拒絕或沒有回應
                            (COUNT(DISTINCT b.id) FILTER (WHERE b.status IN ('CONFIRMED', 'PARTNER_ACCEPTED')) = 0
                            AND COUNT(DISTINCT b.id) FILTER (WHERE b.status IN ('REJECTED', 'PARTNER_REJECTED')) = COUNT(DISTINCT b.id))
                            OR
                            -- 或者所有夥伴都拒絕
                            (COUNT(DISTINCT b.id) FILTER (WHERE b.status IN ('REJECTED', 'PARTNER_REJECTED')) = COUNT(DISTINCT b.id))
                    """
                    result = s.execute(text(query), {
                        "window_start": cancel_window_start,
                        "window_end": cancel_window_end
                    })
                    return list(result)
                except Exception as e:
                    # ADDED FOR TRANSACTION SAFETY: 確保異常時 rollback
                    s.rollback()
                    raise
        
        try:
            bookings_to_cancel = await asyncio.to_thread(query_bookings_to_cancel)
            
            for row in bookings_to_cancel:
                try:
                    multi_player_booking_id = row.multi_player_booking_id
                    
                    # 取消所有相關的 Booking
                    # ADDED FOR TRANSACTION SAFETY: 每輪創建新 session，確保異常時 rollback
                    def cancel_booking():
                        # ADDED FOR TRANSACTION SAFETY: 使用 with Session() 確保自動關閉
                        with Session() as s:
                            try:
                                # 更新所有 Booking 狀態為 CANCELLED
                                s.execute(text("""
                                    UPDATE "Booking"
                                    SET status = 'CANCELLED'
                                    WHERE "multiPlayerBookingId" = :multi_player_booking_id
                                    AND status NOT IN ('CANCELLED', 'REJECTED', 'PARTNER_REJECTED')
                                """), {"multi_player_booking_id": multi_player_booking_id})
                                
                                # 更新 MultiPlayerBooking 狀態為 CANCELLED
                                s.execute(text("""
                                    UPDATE "MultiPlayerBooking"
                                    SET status = 'CANCELLED'
                                    WHERE id = :multi_player_booking_id
                                """), {"multi_player_booking_id": multi_player_booking_id})
                                
                                s.commit()
                            except Exception as e:
                                # ADDED FOR TRANSACTION SAFETY: 確保異常時 rollback
                                s.rollback()
                                raise
                    
                    await asyncio.to_thread(cancel_booking)
                    print(f"✅ 自動取消多人陪玩訂單: {multi_player_booking_id} (所有夥伴都拒絕或沒有回應)")
                    
                    # 🔥 發送 email 通知（異步，不阻塞）
                    try:
                        api_url = os.getenv('NEXTJS_API_URL', 'https://peiplay.vercel.app')
                        response = requests.post(
                            f"{api_url}/api/multi-player-booking/notify-auto-cancelled",
                            json={
                                "multiPlayerBookingId": multi_player_booking_id,
                                "reason": "所有夥伴都拒絕或沒有回應，系統自動取消訂單"
                            },
                            timeout=10
                        )
                        if response.status_code == 200:
                            print(f"✅ 自動取消通知已發送: {multi_player_booking_id}")
                        else:
                            print(f"⚠️ 自動取消通知發送失敗: {response.status_code}")
                    except Exception as e:
                        print(f"⚠️ 發送自動取消通知失敗: {e}")
                except Exception as e:
                    print(f"❌ 自動取消多人陪玩訂單失敗: {e}")
                    continue
        except Exception as e:
            print(f"❌ 查詢需要取消的多人陪玩訂單失敗: {e}")
    except Exception as e:
        print(f"❌ 自動取消多人陪玩訂單時發生錯誤: {e}")

# --- 清理過期頻道任務 ---
@tasks.loop(seconds=60)  # 每1分鐘檢查一次
async def cleanup_expired_channels():
    """清理已過期的預約頻道"""
    await bot.wait_until_ready()
    
    try:
        guild = bot.get_guild(GUILD_ID)
        if not guild:
            print("❌ 找不到 Discord 伺服器")
            return
        
        # 查詢已結束但仍有頻道的預約
        now = datetime.now(timezone.utc)
        
        # 將同步資料庫操作移到線程池，避免阻塞事件循環
        def query_expired_bookings():
            with Session() as s:
                # 查詢已結束的預約（給評價系統留出15分鐘時間）
                expired_query = """
                SELECT 
                    b.id, b."discordTextChannelId", b."discordVoiceChannelId",
                    s."endTime", b.status
                FROM "Booking" b
                JOIN "Schedule" s ON s.id = b."scheduleId"
                WHERE (b."discordTextChannelId" IS NOT NULL OR b."discordVoiceChannelId" IS NOT NULL)
                AND s."endTime" < :now_time_minus_15min
                AND (b.status IN ('COMPLETED', 'CANCELLED', 'REJECTED') OR s."endTime" < :now_time_minus_60min)
                """
                
                # 🔥 查詢已結束的多人陪玩群組（MultiPlayerBooking 表的頻道）
                # 修改邏輯：
                # 1. 如果評價完成，在評價完成後15分鐘清理頻道
                # 2. 如果沒有評價，在預約時段結束30分鐘後清理頻道
                expired_multi_player_query = """
                SELECT 
                    mpb.id, mpb."discordTextChannelId", mpb."discordVoiceChannelId",
                    mpb."endTime", mpb.status,
                    -- 檢查是否有評價（通過 GroupBookingReview 表，因為多人陪玩使用群組評價系統）
                    (SELECT COUNT(*) FROM "GroupBookingReview" gbr WHERE gbr."groupBookingId" = mpb.id) as review_count,
                    -- 獲取最新評價的時間
                    (SELECT MAX(gbr."createdAt") FROM "GroupBookingReview" gbr WHERE gbr."groupBookingId" = mpb.id) as last_review_time
                FROM "MultiPlayerBooking" mpb
                WHERE (mpb."discordTextChannelId" IS NOT NULL OR mpb."discordVoiceChannelId" IS NOT NULL)
                AND (
                    -- 情況1：有評價，且最新評價時間超過15分鐘
                    (
                        (SELECT COUNT(*) FROM "GroupBookingReview" gbr WHERE gbr."groupBookingId" = mpb.id) > 0
                        AND (SELECT MAX(gbr."createdAt") FROM "GroupBookingReview" gbr WHERE gbr."groupBookingId" = mpb.id) < :now_time_minus_15min
                    )
                    OR
                    -- 情況2：沒有評價，且結束時間超過30分鐘
                    (
                        (SELECT COUNT(*) FROM "GroupBookingReview" gbr WHERE gbr."groupBookingId" = mpb.id) = 0
                        AND mpb."endTime" < :now_time_minus_30min
                    )
                    OR
                    -- 情況3：已取消的訂單
                    (mpb.status = 'CANCELLED')
                )
                """
                
                # 🔥 查詢已結束的群組預約（GroupBooking 表的頻道）
                expired_group_booking_query = """
                SELECT 
                    gb.id, gb."discordTextChannelId", gb."discordVoiceChannelId",
                    gb."startTime", gb."endTime", gb.status
                FROM "GroupBooking" gb
                WHERE (gb."discordTextChannelId" IS NOT NULL OR gb."discordVoiceChannelId" IS NOT NULL)
                AND gb."endTime" < :now_time_minus_15min
                AND (gb.status IN ('COMPLETED', 'CANCELLED') OR gb."endTime" < :now_time_minus_60min)
                """
                
                # 計算時間閾值
                now_minus_15min = now - timedelta(minutes=15)
                now_minus_30min = now - timedelta(minutes=30)
                now_minus_60min = now - timedelta(minutes=60)
                expired_bookings = s.execute(text(expired_query), {
                    "now_time_minus_15min": now_minus_15min,
                    "now_time_minus_60min": now_minus_60min
                }).fetchall()
                
                # 🔥 查詢已結束的多人陪玩群組（需要 now_time_minus_15min 和 now_time_minus_30min 參數）
                expired_multi_player_bookings = s.execute(text(expired_multi_player_query), {
                    "now_time_minus_15min": now_minus_15min,
                    "now_time_minus_30min": now_minus_30min
                }).fetchall()
                
                # 🔥 查詢已結束的群組預約
                expired_group_bookings = s.execute(text(expired_group_booking_query), {
                    "now_time_minus_15min": now_minus_15min,
                    "now_time_minus_60min": now_minus_60min
                }).fetchall()
                
                return list(expired_bookings), list(expired_multi_player_bookings), list(expired_group_bookings)
        
        # 在線程池中執行資料庫查詢
        expired_bookings, expired_multi_player_bookings, expired_group_bookings = await asyncio.to_thread(query_expired_bookings)
        
        # 處理一般預約的過期頻道
        for booking in expired_bookings:
            booking_id = booking.id
            text_channel_id = booking.discordTextChannelId
            voice_channel_id = booking.discordVoiceChannelId
            
            deleted_channels = []
            
            # 刪除文字頻道
            if text_channel_id:
                try:
                    text_channel = guild.get_channel(int(text_channel_id))
                    if text_channel:
                        await text_channel.delete()
                        deleted_channels.append(f"文字頻道 {text_channel.name}")
                        # 已清理過期文字頻道，減少日誌輸出
                except Exception as e:
                    print(f"❌ 清理文字頻道失敗: {e}")
            
            # 刪除語音頻道
            if voice_channel_id:
                try:
                    voice_channel = guild.get_channel(int(voice_channel_id))
                    if voice_channel:
                        await voice_channel.delete()
                        deleted_channels.append(f"語音頻道 {voice_channel.name}")
                        # 已清理過期語音頻道，減少日誌輸出
                except Exception as e:
                    print(f"❌ 清理語音頻道失敗: {e}")
            
            # 清除資料庫中的頻道 ID
            if deleted_channels:
                try:
                    def update_booking_channels(booking_id):
                        with Session() as s:
                            s.execute(
                                text("UPDATE \"Booking\" SET \"discordTextChannelId\" = NULL, \"discordVoiceChannelId\" = NULL WHERE id = :booking_id"),
                                {"booking_id": booking_id}
                            )
                            s.commit()
                    await asyncio.to_thread(update_booking_channels, booking_id)
                    # 已清除預約的頻道ID，減少日誌輸出
                except Exception as e:
                    print(f"❌ 清除頻道 ID 失敗: {e}")
        
        # 🔥 處理多人陪玩群組的過期頻道
        for mpb in expired_multi_player_bookings:
            mpb_id = mpb.id
            text_channel_id = mpb.discordTextChannelId
            voice_channel_id = mpb.discordVoiceChannelId
            review_count = mpb.review_count or 0
            last_review_time = mpb.last_review_time
            
            deleted_channels = []
            
            # 判斷清理原因
            cleanup_reason = ""
            if mpb.status == 'CANCELLED':
                cleanup_reason = "已取消"
            elif review_count > 0:
                cleanup_reason = f"評價完成後15分鐘（評價數: {review_count}）"
            else:
                cleanup_reason = "無評價，結束後30分鐘"
            
            # 刪除文字頻道
            if text_channel_id:
                try:
                    text_channel = guild.get_channel(int(text_channel_id))
                    if text_channel:
                        await text_channel.delete()
                        deleted_channels.append(f"文字頻道 {text_channel.name}")
                        print(f"✅ 已刪除過期多人陪玩文字頻道: {text_channel.name} (ID: {mpb_id}, 原因: {cleanup_reason})")
                except Exception as e:
                    print(f"❌ 清理多人陪玩文字頻道失敗: {e}")
            
            # 刪除語音頻道
            if voice_channel_id:
                try:
                    voice_channel = guild.get_channel(int(voice_channel_id))
                    if voice_channel:
                        await voice_channel.delete()
                        deleted_channels.append(f"語音頻道 {voice_channel.name}")
                        print(f"✅ 已刪除過期多人陪玩語音頻道: {voice_channel.name} (ID: {mpb_id}, 原因: {cleanup_reason})")
                except Exception as e:
                    print(f"❌ 清理多人陪玩語音頻道失敗: {e}")
            
            # 清除資料庫中的頻道 ID
            if deleted_channels:
                try:
                    def update_multi_player_channels(mpb_id):
                        with Session() as s:
                            s.execute(
                                text("UPDATE \"MultiPlayerBooking\" SET \"discordTextChannelId\" = NULL, \"discordVoiceChannelId\" = NULL WHERE id = :mpb_id"),
                                {"mpb_id": mpb_id}
                            )
                            s.commit()
                    await asyncio.to_thread(update_multi_player_channels, mpb_id)
                    print(f"✅ 已清除多人陪玩 {mpb_id} 的頻道ID")
                except Exception as e:
                    print(f"❌ 清除多人陪玩頻道 ID 失敗: {e}")
        
        # 🔥 處理群組預約的過期頻道
        for gb in expired_group_bookings:
            gb_id = gb.id
            text_channel_id = gb.discordTextChannelId
            voice_channel_id = gb.discordVoiceChannelId
            
            deleted_channels = []
            
            # 刪除文字頻道
            if text_channel_id:
                try:
                    text_channel = guild.get_channel(int(text_channel_id))
                    if text_channel:
                        await text_channel.delete()
                        deleted_channels.append(f"文字頻道 {text_channel.name}")
                        print(f"✅ 已刪除過期群組預約文字頻道: {text_channel.name} (群組預約 {gb_id})")
                except Exception as e:
                    print(f"❌ 清理群組預約文字頻道失敗: {e}")
            
            # 刪除語音頻道
            if voice_channel_id:
                try:
                    voice_channel = guild.get_channel(int(voice_channel_id))
                    if voice_channel:
                        await voice_channel.delete()
                        deleted_channels.append(f"語音頻道 {voice_channel.name}")
                        print(f"✅ 已刪除過期群組預約語音頻道: {voice_channel.name} (群組預約 {gb_id})")
                except Exception as e:
                    print(f"❌ 清理群組預約語音頻道失敗: {e}")
            
            # 清除資料庫中的頻道 ID
            if deleted_channels:
                try:
                    def update_group_booking_channels(gb_id):
                        with Session() as s:
                            s.execute(
                                text("UPDATE \"GroupBooking\" SET \"discordTextChannelId\" = NULL, \"discordVoiceChannelId\" = NULL WHERE id = :gb_id"),
                                {"gb_id": gb_id}
                            )
                            s.commit()
                    await asyncio.to_thread(update_group_booking_channels, gb_id)
                    print(f"✅ 已清除群組預約 {gb_id} 的頻道ID")
                except Exception as e:
                    print(f"❌ 清除群組預約頻道 ID 失敗: {e}")
        
        # 清理 active_voice_channels 中已結束的頻道
        current_time = datetime.now(timezone.utc)
        expired_vc_ids = []
        
        for vc_id, vc_data in active_voice_channels.items():
            if vc_data['remaining'] <= 0:
                expired_vc_ids.append(vc_id)
        
        for vc_id in expired_vc_ids:
            try:
                vc_data = active_voice_channels[vc_id]
                if 'vc' in vc_data:
                    await vc_data['vc'].delete()
                if 'text_channel' in vc_data and vc_data['text_channel']:
                    await vc_data['text_channel'].delete()
                del active_voice_channels[vc_id]
                # 已清理過期活躍頻道，減少日誌輸出
            except Exception as e:
                print(f"❌ 清理活躍頻道失敗: {e}")
                # 即使刪除失敗，也要從字典中移除
                if vc_id in active_voice_channels:
                    del active_voice_channels[vc_id]
        
        # 清理群組預約評價頻道（超過5分鐘未完成評價）
        now = datetime.now(timezone.utc)
        expired_group_channels = []
        
        for group_booking_id, created_time in list(group_rating_channel_created_time.items()):
            # 檢查是否超過5分鐘（300秒）
            time_diff = (now - created_time).total_seconds()
            if time_diff >= 300:  # 5分鐘
                if group_booking_id in group_rating_text_channels:
                    text_channel = group_rating_text_channels[group_booking_id]
                    expired_group_channels.append((group_booking_id, text_channel))
        
        # 刪除過期的群組預約評價頻道
        for group_booking_id, text_channel in expired_group_channels:
            try:
                if text_channel:
                    # 🔥 使用 try-except 來檢查頻道是否已刪除，而不是檢查 deleted 屬性
                    try:
                        # 嘗試訪問頻道屬性來檢查是否還存在
                        _ = text_channel.name
                        await text_channel.delete()
                        print(f"✅ 5分鐘內未完成評價，已刪除群組預約文字頻道: {text_channel.name} (group_booking_id: {group_booking_id})")
                    except (discord.errors.NotFound, AttributeError):
                        # 頻道已經被刪除，靜默處理
                        pass
                    # 清理追蹤
                    group_rating_text_channels.pop(group_booking_id, None)
                    group_rating_channel_created_time.pop(group_booking_id, None)
            except Exception as e:
                print(f"❌ 刪除過期群組預約評價頻道失敗: {e}")
                # 即使刪除失敗，也清理追蹤
                group_rating_text_channels.pop(group_booking_id, None)
                group_rating_channel_created_time.pop(group_booking_id, None)
        
        # 額外檢查：清理所有「匿名文字區」頻道，如果它們包含評價系統且超過5分鐘
        anonymous_text_channels = [ch for ch in guild.text_channels if "匿名文字區" in ch.name or "🔒匿名文字區" in ch.name]
        for text_channel in anonymous_text_channels:
            try:
                # 檢查頻道中是否有評價系統訊息
                has_rating_system = False
                rating_message_time = None
                try:
                    async for message in text_channel.history(limit=20):
                        if message.author == bot.user and ("評價" in message.content or "評分" in message.content or "⭐" in message.content):
                            has_rating_system = True
                            rating_message_time = message.created_at.replace(tzinfo=timezone.utc)
                            break
                except:
                    pass
                
                # 如果有評價系統且超過5分鐘，則刪除
                if has_rating_system and rating_message_time:
                    time_since_rating = (now - rating_message_time).total_seconds()
                    if time_since_rating >= 300:  # 5分鐘
                        await text_channel.delete()
                        print(f"✅ 已刪除過期匿名文字區頻道（評價系統超過5分鐘）: {text_channel.name}")
            except discord.errors.NotFound:
                # 頻道已經被刪除，跳過
                pass
            except Exception as e:
                print(f"❌ 檢查匿名文字區頻道失敗: {e}")
        
    except Exception as e:
        print(f"❌ 清理過期頻道時發生錯誤: {e}")

# --- 檢查超時預約任務 ---
@tasks.loop(seconds=60)  # 每1分鐘檢查一次
async def check_booking_timeouts():
    """檢查夥伴回應超時的即時預約並自動取消"""
    await bot.wait_until_ready()
    
    try:
        # 將同步資料庫操作移到線程池，避免阻塞事件循環
        # ADDED FOR TRANSACTION SAFETY: 每輪創建新 session，確保異常時 rollback
        def query_timeout_bookings():
            # ADDED FOR TRANSACTION SAFETY: 使用 with Session() 確保自動關閉
            with Session() as s:
                try:
                    now = datetime.now(timezone.utc)
                    
                    # 查詢超時的等待夥伴回覆的預約
                    timeout_query = """
                        SELECT 
                            b.id, b.status, b."partnerResponseDeadline",
                            c.name as customer_name, p.name as partner_name,
                            p.id as partner_id
                        FROM "Booking" b
                        JOIN "Schedule" sch ON sch.id = b."scheduleId"
                        JOIN "Customer" c ON c.id = b."customerId"
                        JOIN "Partner" p ON p.id = sch."partnerId"
                        WHERE b.status = 'PAID_WAITING_PARTNER_CONFIRMATION'
                        AND b."isWaitingPartnerResponse" = true
                        AND b."partnerResponseDeadline" < :now
                    """
                    
                    timeout_bookings = s.execute(text(timeout_query), {"now": now}).fetchall()
                    return list(timeout_bookings)
                except Exception as e:
                    # ADDED FOR TRANSACTION SAFETY: 確保異常時 rollback
                    s.rollback()
                    raise
        
        # 在線程池中執行資料庫查詢
        timeout_bookings = await asyncio.to_thread(query_timeout_bookings)
        
        if timeout_bookings:
            print(f"🔍 找到 {len(timeout_bookings)} 個超時預約需要處理")
            
            for booking in timeout_bookings:
                try:
                    booking_id = booking.id
                    partner_id = booking.partner_id
                    partner_name = booking.partner_name
                    customer_name = booking.customer_name
                    
                    # 更新預約狀態為取消（在線程中執行）
                    async def update_booking_cancelled(booking_id, partner_id):
                        # ADDED FOR TRANSACTION SAFETY: 每輪創建新 session，確保異常時 rollback
                        def update():
                            # ADDED FOR TRANSACTION SAFETY: 使用 with Session() 確保自動關閉
                            with Session() as s:
                                try:
                                    s.execute(
                                        text("""
                                            UPDATE "Booking" 
                                            SET status = 'CANCELLED', 
                                                "rejectReason" = '夥伴未在10分鐘內回覆，自動取消',
                                                "isWaitingPartnerResponse" = false,
                                                "partnerResponseDeadline" = null
                                            WHERE id = :booking_id
                                        """),
                                        {"booking_id": booking_id}
                                    )
                                    
                                    # 更新夥伴的未回覆計數
                                    s.execute(
                                        text("""
                                            UPDATE "Partner" 
                                            SET "noResponseCount" = "noResponseCount" + 1
                                            WHERE id = :partner_id
                                        """),
                                        {"partner_id": partner_id}
                                    )
                                    
                                    s.commit()
                                except Exception as e:
                                    # ADDED FOR TRANSACTION SAFETY: 確保異常時 rollback
                                    s.rollback()
                                    raise
                        await asyncio.to_thread(update)
                    
                    await update_booking_cancelled(booking_id, partner_id)
                    
                    print(f"❌ 預約 {booking_id} 因夥伴 {partner_name} 未回覆已自動取消")
                    
                    # 檢查是否需要通知管理員（累積3次）
                    async def check_partner_no_response(partner_id, partner_name):
                        # ADDED FOR TRANSACTION SAFETY: 每輪創建新 session，確保異常時 rollback
                        def query():
                            # ADDED FOR TRANSACTION SAFETY: 使用 with Session() 確保自動關閉
                            with Session() as s:
                                try:
                                    partner_result = s.execute(
                                        text("SELECT \"noResponseCount\" FROM \"Partner\" WHERE id = :partner_id"),
                                        {"partner_id": partner_id}
                                    ).fetchone()
                                    return partner_result[0] if partner_result else 0
                                except Exception as e:
                                    # ADDED FOR TRANSACTION SAFETY: 確保異常時 rollback
                                    s.rollback()
                                    raise
                        
                        no_response_count = await asyncio.to_thread(query)
                        
                        if no_response_count >= 3:
                            admin_channel = bot.get_channel(ADMIN_CHANNEL_ID)
                            if admin_channel:
                                await admin_channel.send(
                                    f"⚠️ **夥伴回應超時警告**\n"
                                    f"👤 夥伴: {partner_name}\n"
                                    f"📊 本月未回覆次數: {no_response_count} 次\n"
                                    f"🔴 累積達到3次，需要管理員關注！"
                                )
                            print(f"⚠️ 夥伴 {partner_name} 已累積 {no_response_count} 次未回覆")
                    
                    await check_partner_no_response(partner_id, partner_name)
                    
                except Exception as e:
                    print(f"❌ 處理超時預約 {booking.id} 時發生錯誤: {e}")
        
    except Exception as e:
        print(f"❌ 檢查超時預約時發生錯誤: {e}")

# --- 檢查遺失評價任務 ---
@tasks.loop(seconds=600)  # 每10分鐘檢查一次，減少資料庫負載
async def check_missing_ratings():
    """檢查遺失的評價並自動提交"""
    await bot.wait_until_ready()
    
    try:
        # ADDED FOR TRANSACTION SAFETY: 每輪創建新 session，確保異常時 rollback
        def _check():
            # ADDED FOR TRANSACTION SAFETY: 使用 with Session() 確保自動關閉
            with Session() as s:
                try:
                    # 查找已結束但沒有評價記錄的預約
                    now = datetime.now(timezone.utc)
                    
                    # 查找所有已結束的預約（放寬時間條件）
                    missing_ratings = s.execute(text("""
                        SELECT 
                            b.id, c.name as customer_name, p.name as partner_name,
                            s."endTime"
                        FROM "Booking" b
                        JOIN "Schedule" s ON s.id = b."scheduleId"
                        JOIN "Customer" c ON c.id = b."customerId"
                        JOIN "Partner" p ON p.id = s."partnerId"
                        WHERE b.status = 'CONFIRMED'
                        AND s."endTime" < :now
                        AND s."endTime" >= :recent_time
                        AND (b."discordVoiceChannelId" IS NOT NULL OR b."discordTextChannelId" IS NOT NULL)
                    """), {
                        "now": now,
                        "recent_time": now - timedelta(hours=48)  # 檢查最近48小時的預約
                    }).fetchall()
                    
                    return missing_ratings
                except Exception as e:
                    # ADDED FOR TRANSACTION SAFETY: 確保異常時 rollback
                    s.rollback()
                    raise
        
        missing_ratings = await asyncio.to_thread(lambda: safe_db_execute(_check))
        if missing_ratings is None:
            return  # 資料庫連線錯誤，安全跳過該輪檢查
        
        if missing_ratings:
            print(f"🔍 處理 {len(missing_ratings)} 個遺失評價")
            
            admin_channel = bot.get_channel(ADMIN_CHANNEL_ID)
            if admin_channel:
                for booking in missing_ratings:
                    try:
                        # 計算結束時間
                        now = datetime.now(timezone.utc)
                        end_time = booking.endTime
                        if end_time.tzinfo is None:
                            end_time = end_time.replace(tzinfo=timezone.utc)
                        
                        time_since_end = (now - end_time).total_seconds() / 60  # 分鐘
                        
                        await admin_channel.send(
                            f"**{booking.customer_name}** 評價 **{booking.partner_name}**\n"
                            f"⭐ 未評價\n"
                            f"💬 顧客未填寫評價（預約已結束 {time_since_end:.0f} 分鐘）"
                        )
                        # 已發送遺失評價，減少日誌輸出
                    except Exception as e:
                        print(f"❌ 發送遺失評價失敗: {e}")
            
            # 清除頻道記錄，避免重複處理
            def _update():
                booking_ids = [b.id for b in missing_ratings]
                with Session() as s:
                    s.execute(text("""
                        UPDATE "Booking" 
                        SET "discordVoiceChannelId" = NULL, "discordTextChannelId" = NULL
                        WHERE id = ANY(:booking_ids)
                    """), {"booking_ids": booking_ids})
                    s.commit()
            
            await asyncio.to_thread(lambda: safe_db_execute(_update))
                
    except Exception as e:
        # 資料庫連線錯誤時安全跳過，不讓 bot 崩潰
        if is_db_connection_error(e):
            return  # 安全跳過該輪檢查
        print(f"❌ 檢查遺失評價時發生錯誤: {e}")

# --- 自動檢查群組預約和多人陪玩的文字頻道創建任務 ---
@tasks.loop(seconds=CHECK_INTERVAL)
async def check_group_and_multiplayer_text_channels():
    """檢查群組預約（開始前1小時）和多人陪玩（開始前5分鐘）是否需要創建文字頻道"""
    global db_connection_error_reported
    await bot.wait_until_ready()
    
    try:
        guild = bot.get_guild(GUILD_ID)
        if not guild:
            print("❌ 找不到 Discord 伺服器")
            return
        
        now = datetime.now(timezone.utc)
        
        # 群組預約：開始前10分鐘創建文字頻道
        group_window_start = now + timedelta(minutes=10) - timedelta(minutes=2)  # 10分鐘前，容許2分鐘誤差
        group_window_end = now + timedelta(minutes=10) + timedelta(minutes=2)
        
        # 多人陪玩：如果所有夥伴都確認
        # 🔥 修改邏輯：
        # 1. 如果距離開始時間少於30分鐘，立即創建文字頻道
        # 2. 如果距離開始時間多於30分鐘，等到開始前30分鐘才創建
        # 🔥 允許一些時間容差，處理「壓線創建」的情況（開始時間在過去5分鐘內或未來30分鐘內）
        multi_player_window_start = now - timedelta(minutes=5)  # 允許過去5分鐘內（處理壓線創建）
        multi_player_window_end = now + timedelta(minutes=30)  # 未來30分鐘內（少於30分鐘的立即創建）
        multi_player_window_30min = now + timedelta(minutes=30) - timedelta(minutes=2)  # 30分鐘前，容許2分鐘誤差
        multi_player_window_30min_end = now + timedelta(minutes=30) + timedelta(minutes=2)  # 30分鐘前，容許2分鐘誤差
        
        # 減少日誌輸出，只在有預約需要處理時才顯示
        
        def query_group_and_multiplayer():
            with Session() as s:
                # 查詢群組預約（開始前10分鐘，還沒有文字頻道）
                # 同時查詢 GroupBookingParticipant 和 Booking 表以獲取所有參與者
                group_query = """
                    SELECT DISTINCT
                        gb.id as group_booking_id,
                        gb."startTime",
                        gb."endTime",
                        gb.title,
                        -- 獲取顧客 Discord（優先從 GroupBookingParticipant，其次從 Booking）
                        COALESCE(
                            (SELECT cu.discord FROM "GroupBookingParticipant" gbp2
                             JOIN "Customer" c ON c.id = gbp2."customerId"
                             JOIN "User" cu ON cu.id = c."userId"
                             WHERE gbp2."groupBookingId" = gb.id AND gbp2.status = 'ACTIVE'
                             LIMIT 1),
                            (SELECT cu.discord FROM "Booking" b2
                             JOIN "Customer" c ON c.id = b2."customerId"
                             JOIN "User" cu ON cu.id = c."userId"
                             WHERE b2."groupBookingId" = gb.id
                             LIMIT 1)
                        ) as customer_discord,
                        -- 獲取所有夥伴的 Discord ID（從 GroupBookingParticipant 和 Booking）
                        array_agg(DISTINCT COALESCE(pu.discord, pu2.discord)) FILTER (WHERE COALESCE(pu.discord, pu2.discord) IS NOT NULL) as partner_discords,
                        -- 獲取所有顧客的 Discord ID
                        array_agg(DISTINCT cu2.discord) FILTER (WHERE cu2.discord IS NOT NULL) as customer_discords_all
                    FROM "GroupBooking" gb
                    -- 方式1：通過 GroupBookingParticipant 表查詢
                    LEFT JOIN "GroupBookingParticipant" gbp ON gbp."groupBookingId" = gb.id AND gbp.status = 'ACTIVE'
                    LEFT JOIN "Partner" p ON p.id = gbp."partnerId"
                    LEFT JOIN "User" pu ON pu.id = p."userId"
                    -- 方式2：通過 Booking 表查詢
                    LEFT JOIN "Booking" b ON b."groupBookingId" = gb.id 
                        AND b.status IN ('CONFIRMED', 'PARTNER_ACCEPTED', 'PAID_WAITING_PARTNER_CONFIRMATION')
                    LEFT JOIN "Schedule" s ON s.id = b."scheduleId"
                    LEFT JOIN "Partner" p2 ON p2.id = s."partnerId"
                    LEFT JOIN "User" pu2 ON pu2.id = p2."userId"
                    LEFT JOIN "Customer" c2 ON c2.id = COALESCE(gbp."customerId", b."customerId")
                    LEFT JOIN "User" cu2 ON cu2.id = c2."userId"
                    WHERE gb.status IN ('ACTIVE', 'FULL')
                    AND gb."startTime" >= :window_start
                    AND gb."startTime" <= :window_end
                    AND gb."discordTextChannelId" IS NULL
                    GROUP BY gb.id, gb."startTime", gb."endTime", gb.title
                """
                
                group_result = s.execute(text(group_query), {
                    "window_start": group_window_start,
                    "window_end": group_window_end
                })
                
                # 查詢多人陪玩（所有夥伴都確認後）
                # 🔥 修改邏輯：
                # 1. 如果距離開始時間少於30分鐘，立即創建
                # 2. 如果距離開始時間多於30分鐘，等到開始前30分鐘才創建
                # 必須所有夥伴都 CONFIRMED 或 PARTNER_ACCEPTED，且沒有 REJECTED 的夥伴
                
                # 查詢1：距離開始時間少於30分鐘的（立即創建）
                # 🔥 允許開始時間在過去5分鐘內或未來30分鐘內（處理壓線創建）
                multi_player_query_immediate = """
                    SELECT DISTINCT
                        mpb.id as multi_player_booking_id,
                        mpb."startTime",
                        mpb."endTime",
                        cu.discord as customer_discord,
                        array_agg(DISTINCT pu.discord) FILTER (WHERE b.status IN ('CONFIRMED', 'PARTNER_ACCEPTED') AND pu.discord IS NOT NULL) as partner_discords,
                        COUNT(DISTINCT b.id) FILTER (WHERE b.status IN ('CONFIRMED', 'PARTNER_ACCEPTED')) as confirmed_count,
                        COUNT(DISTINCT b.id) FILTER (WHERE b.status IN ('REJECTED', 'PARTNER_REJECTED')) as rejected_count,
                        COUNT(DISTINCT b.id) as total_count
                    FROM "MultiPlayerBooking" mpb
                    JOIN "Booking" b ON b."multiPlayerBookingId" = mpb.id
                    JOIN "Schedule" s ON s.id = b."scheduleId"
                    JOIN "Customer" c ON c.id = mpb."customerId"
                    JOIN "User" cu ON cu.id = c."userId"
                    JOIN "Partner" p ON p.id = s."partnerId"
                    JOIN "User" pu ON pu.id = p."userId"
                    WHERE mpb.status IN ('ACTIVE', 'PENDING')
                    AND mpb."startTime" >= :window_start
                    AND mpb."startTime" <= :window_end
                    AND mpb."discordTextChannelId" IS NULL
                    GROUP BY mpb.id, mpb."startTime", mpb."endTime", cu.discord
                    HAVING 
                        -- 必須所有夥伴都 CONFIRMED 或 PARTNER_ACCEPTED（沒有 PENDING 或 REJECTED）
                        COUNT(DISTINCT b.id) FILTER (WHERE b.status IN ('CONFIRMED', 'PARTNER_ACCEPTED')) = COUNT(DISTINCT b.id)
                        AND COUNT(DISTINCT b.id) FILTER (WHERE b.status IN ('REJECTED', 'PARTNER_REJECTED')) = 0
                        AND COUNT(DISTINCT pu.discord) FILTER (WHERE b.status IN ('CONFIRMED', 'PARTNER_ACCEPTED') AND pu.discord IS NOT NULL) > 0
                """
                
                # 查詢2：距離開始時間30分鐘的（開始前30分鐘創建）
                multi_player_query_30min = """
                    SELECT DISTINCT
                        mpb.id as multi_player_booking_id,
                        mpb."startTime",
                        mpb."endTime",
                        cu.discord as customer_discord,
                        array_agg(DISTINCT pu.discord) FILTER (WHERE b.status IN ('CONFIRMED', 'PARTNER_ACCEPTED') AND pu.discord IS NOT NULL) as partner_discords,
                        COUNT(DISTINCT b.id) FILTER (WHERE b.status IN ('CONFIRMED', 'PARTNER_ACCEPTED')) as confirmed_count,
                        COUNT(DISTINCT b.id) FILTER (WHERE b.status IN ('REJECTED', 'PARTNER_REJECTED')) as rejected_count,
                        COUNT(DISTINCT b.id) as total_count
                    FROM "MultiPlayerBooking" mpb
                    JOIN "Booking" b ON b."multiPlayerBookingId" = mpb.id
                    JOIN "Schedule" s ON s.id = b."scheduleId"
                    JOIN "Customer" c ON c.id = mpb."customerId"
                    JOIN "User" cu ON cu.id = c."userId"
                    JOIN "Partner" p ON p.id = s."partnerId"
                    JOIN "User" pu ON pu.id = p."userId"
                    WHERE mpb.status IN ('ACTIVE', 'PENDING')
                    AND mpb."startTime" >= :window_30min_start
                    AND mpb."startTime" <= :window_30min_end
                    AND mpb."discordTextChannelId" IS NULL
                    GROUP BY mpb.id, mpb."startTime", mpb."endTime", cu.discord
                    HAVING 
                        -- 必須所有夥伴都 CONFIRMED 或 PARTNER_ACCEPTED（沒有 PENDING 或 REJECTED）
                        COUNT(DISTINCT b.id) FILTER (WHERE b.status IN ('CONFIRMED', 'PARTNER_ACCEPTED')) = COUNT(DISTINCT b.id)
                        AND COUNT(DISTINCT b.id) FILTER (WHERE b.status IN ('REJECTED', 'PARTNER_REJECTED')) = 0
                        AND COUNT(DISTINCT pu.discord) FILTER (WHERE b.status IN ('CONFIRMED', 'PARTNER_ACCEPTED') AND pu.discord IS NOT NULL) > 0
                """
                
                # 執行兩個查詢
                immediate_result = s.execute(text(multi_player_query_immediate), {
                    "window_start": multi_player_window_start,
                    "window_end": multi_player_window_end
                })
                
                thirty_min_result = s.execute(text(multi_player_query_30min), {
                    "window_30min_start": multi_player_window_30min,
                    "window_30min_end": multi_player_window_30min_end
                })
                
                # 合併結果（使用 set 去重，避免重複）
                immediate_list = list(immediate_result)
                thirty_min_list = list(thirty_min_result)
                
                # 使用 booking_id 去重
                seen_ids = set()
                multi_player_result = []
                for row in immediate_list + thirty_min_list:
                    if row.multi_player_booking_id not in seen_ids:
                        seen_ids.add(row.multi_player_booking_id)
                        multi_player_result.append(row)
                
                return list(group_result), list(multi_player_result)
        
        try:
            try:
                group_results, multi_player_results = await asyncio.to_thread(query_group_and_multiplayer)
            except Exception as db_error:
                # 檢查是否為資料庫連接錯誤
                error_str = str(db_error).lower()
                if any(keyword in error_str for keyword in ['connection', 'server closed', 'operationalerror', 'timeout', 'could not translate host name']):
                    # 🔥 只在第一次報告錯誤時輸出，避免重複輸出
                    if not db_connection_error_reported:
                        print(f"⚠️ 資料庫連接問題（群組/多人陪玩查詢）: {db_error}")
                        print("🔄 嘗試重新建立連接...")
                        db_connection_error_reported = True
                    
                    if reconnect_database():
                        # 🔥 只在恢復成功時輸出一次
                        if db_connection_error_reported:
                            print("✅ 資料庫連接已恢復")
                            db_connection_error_reported = False
                    else:
                        # 🔥 只在第一次失敗時輸出
                        if db_connection_error_reported:
                            print("❌ 資料庫連接恢復失敗，將靜默重試（請檢查資料庫服務狀態）")
                    return  # 跳過這次檢查，等待下次重試
                else:
                    # 非連接錯誤，重新拋出
                    raise
            
            # 只在有預約需要處理時才顯示日誌
            if len(group_results) > 0 or len(multi_player_results) > 0:
                print(f"📋 需要創建頻道: {len(group_results)} 個群組預約, {len(multi_player_results)} 個多人陪玩")
            
            # 處理群組預約
            for row in group_results:
                try:
                    group_booking_id = row.group_booking_id
                    customer_discord = row.customer_discord
                    
                    if not customer_discord:
                        print(f"⚠️ 群組預約 {group_booking_id} 沒有顧客 Discord ID")
                        continue
                    
                    # 🔥 通過 Booking 表查詢，判斷誰是顧客（有付費記錄）和誰是夥伴
                    def get_customers_and_partners(group_booking_id):
                        with Session() as s:
                            # 查詢所有有 Booking 記錄的顧客（有付費的人）
                            customer_result = s.execute(text("""
                                SELECT DISTINCT cu.discord as customer_discord
                                FROM "GroupBooking" gb
                                JOIN "Booking" b ON b."groupBookingId" = gb.id
                                JOIN "Customer" c ON c.id = b."customerId"
                                JOIN "User" cu ON cu.id = c."userId"
                                WHERE gb.id = :group_booking_id
                                AND b.status IN ('CONFIRMED', 'PARTNER_ACCEPTED', 'PAID_WAITING_PARTNER_CONFIRMATION', 'COMPLETED')
                                AND cu.discord IS NOT NULL
                            """), {"group_booking_id": group_booking_id}).fetchall()
                            
                            # 查詢所有夥伴（在 GroupBookingParticipant 中有 partnerId 的人）
                            partner_result = s.execute(text("""
                                SELECT DISTINCT pu.discord as partner_discord
                                FROM "GroupBooking" gb
                                JOIN "GroupBookingParticipant" gbp ON gbp."groupBookingId" = gb.id
                                JOIN "Partner" p ON p.id = gbp."partnerId"
                                JOIN "User" pu ON pu.id = p."userId"
                                WHERE gb.id = :group_booking_id
                                AND gbp.status = 'ACTIVE'
                                AND pu.discord IS NOT NULL
                            """), {"group_booking_id": group_booking_id}).fetchall()
                            
                            customer_discords = [row.customer_discord for row in customer_result if row.customer_discord]
                            partner_discords = [row.partner_discord for row in partner_result if row.partner_discord]
                            
                            return customer_discords, partner_discords
                    
                    customer_discords, partner_discords = await asyncio.to_thread(get_customers_and_partners, group_booking_id)
                    # 查詢結果日誌太雜，已關閉詳細輸出
                    # print(f"🔍 群組預約 {group_booking_id} 參與者查詢結果:")
                    # print(f"   - 顧客（有付費記錄）: {customer_discords}")
                    # print(f"   - 夥伴: {partner_discords}")
                    
                    # 🔥 查找既有文字頻道，如果不存在則創建
                    text_channel = None
                    
                    # 先查找既有文字頻道
                    with Session() as s:
                        result = s.execute(text("""
                            SELECT "discordTextChannelId" 
                            FROM "GroupBooking" 
                            WHERE id = :group_booking_id
                        """), {"group_booking_id": group_booking_id}).fetchone()
                        
                        if result and result[0]:
                            try:
                                text_channel = guild.get_channel(int(result[0]))
                                if text_channel:
                                    print(f"✅ 找到群組預約 {group_booking_id} 的既有文字頻道: {text_channel.name}")
                                else:
                                    print(f"⚠️ 警告：群組預約 {group_booking_id} 的 discordTextChannelId ({result[0]}) 無效，找不到對應頻道，將創建新頻道")
                            except Exception as e:
                                print(f"⚠️ 警告：無法查找群組預約 {group_booking_id} 的文字頻道: {e}，將創建新頻道")
                    
                    # 如果找不到文字頻道，則創建新頻道
                    if not text_channel:
                        print(f"🔍 群組預約 {group_booking_id} 缺少文字頻道，開始創建...")
                        try:
                            # 轉換時間為台灣時區
                            start_time = row.startTime
                            end_time = row.endTime
                            if start_time.tzinfo is None:
                                start_time = start_time.replace(tzinfo=timezone.utc)
                            if end_time.tzinfo is None:
                                end_time = end_time.replace(tzinfo=timezone.utc)
                            
                            text_channel = await create_group_booking_text_channel(
                                group_booking_id,
                                customer_discords,
                                partner_discords,
                                start_time,
                                end_time,
                                is_multiplayer=False
                            )
                            
                            if text_channel:
                                print(f"✅ 已為群組預約 {group_booking_id} 創建文字頻道: {text_channel.name}")
                            else:
                                print(f"❌ 群組預約 {group_booking_id} 創建文字頻道失敗")
                        except Exception as e:
                            print(f"❌ 群組預約 {group_booking_id} 創建文字頻道時發生錯誤: {e}")
                            import traceback
                            traceback.print_exc()
                    
                    if text_channel:
                        # 🔥 避免重複啟動倒數計時任務
                        if group_booking_id not in active_countdown_tasks:
                            active_countdown_tasks.add(group_booking_id)
                            # 如果找到既有頻道或剛創建的頻道，啟動倒數計時任務（包含倒數提醒）
                            bot.loop.create_task(
                                countdown_with_group_rating(
                                    None,  # vc_id (群組預約可能還沒有語音頻道)
                                    text_channel.name,
                                    text_channel,
                                    None,  # vc (群組預約可能還沒有語音頻道)
                                    [],  # members (會在函數內部獲取)
                                    None,  # record_id (不需要)
                                    group_booking_id
                                )
                            )
                            # print(f"✅ 已啟動群組預約倒數計時: {group_booking_id}")
                        else:
                            # print(f"✅ 群組預約 {group_booking_id} 的倒數計時任務已存在，跳過啟動")
                            pass
                    else:
                        # print(f"⚠️ 警告：群組預約 {group_booking_id} 沒有文字頻道，無法啟動倒數計時")
                        pass
                except Exception as e:
                    print(f"❌ 處理群組預約文字頻道失敗: {e}")
                    continue
            
            # 處理多人陪玩
            for row in multi_player_results:
                try:
                    multi_player_booking_id = row.multi_player_booking_id
                    customer_discord = row.customer_discord
                    partner_discords = row.partner_discords if isinstance(row.partner_discords, list) else list(row.partner_discords) if row.partner_discords else []
                    confirmed_count = row.confirmed_count if hasattr(row, 'confirmed_count') else 0
                    total_count = row.total_count if hasattr(row, 'total_count') else 0
                    rejected_count = row.rejected_count if hasattr(row, 'rejected_count') else 0
                    
                    print(f"🔍 處理多人陪玩 {multi_player_booking_id}: 開始時間={row.startTime}, 已確認={confirmed_count}/{total_count}, 已拒絕={rejected_count}")
                    
                    if not customer_discord:
                        print(f"⚠️ 多人陪玩預約 {multi_player_booking_id} 沒有顧客 Discord ID")
                        continue
                    
                    # 過濾 None 值
                    partner_discords = [d for d in partner_discords if d]
                    
                    if not partner_discords:
                        print(f"⚠️ 多人陪玩預約 {multi_player_booking_id} 沒有已確認的夥伴，跳過創建文字頻道 (已確認: {confirmed_count}/{total_count})")
                        continue
                    
                    print(f"✅ 多人陪玩 {multi_player_booking_id} 符合創建條件: 顧客={customer_discord}, 夥伴={partner_discords}")
                    
                    # ✅ 統一判斷依據為 multiPlayerBookingId：檢查是否已經存在頻道
                    def check_existing_channels(multi_player_booking_id):
                        with Session() as s:
                            # 檢查 MultiPlayerBooking 表中是否已經有文字頻道ID
                            existing = s.execute(text("""
                                SELECT "discordTextChannelId", "discordVoiceChannelId"
                                FROM "MultiPlayerBooking"
                                WHERE id = :multi_player_booking_id
                            """), {'multi_player_booking_id': multi_player_booking_id}).fetchone()
                            return existing
                    
                    existing_channels = await asyncio.to_thread(check_existing_channels, multi_player_booking_id)
                    
                    # ✅ 若已存在文字或語音頻道，必須直接 return，不得再創建
                    if existing_channels and existing_channels[0]:
                        # 檢查頻道是否真的存在
                        guild = bot.get_guild(GUILD_ID)
                        if guild:
                            existing_text_channel = guild.get_channel(int(existing_channels[0]))
                            if existing_text_channel:
                                continue  # 跳過，不創建
                    
                    # 🔥 查找既有文字頻道，如果不存在則創建
                    text_channel = None
                    
                    # 先查找既有文字頻道
                    with Session() as s:
                        result = s.execute(text("""
                            SELECT "discordTextChannelId" 
                            FROM "MultiPlayerBooking" 
                            WHERE id = :multi_player_booking_id
                        """), {"multi_player_booking_id": multi_player_booking_id}).fetchone()
                        
                        if result and result[0]:
                            try:
                                text_channel = guild.get_channel(int(result[0]))
                                if text_channel:
                                    print(f"✅ 找到多人陪玩 {multi_player_booking_id} 的既有文字頻道: {text_channel.name}")
                                else:
                                    print(f"⚠️ 警告：多人陪玩 {multi_player_booking_id} 的 discordTextChannelId ({result[0]}) 無效，找不到對應頻道，將創建新頻道")
                            except Exception as e:
                                print(f"⚠️ 警告：無法查找多人陪玩 {multi_player_booking_id} 的文字頻道: {e}，將創建新頻道")
                    
                    # 如果找不到文字頻道，則創建新頻道
                    if not text_channel:
                        print(f"🔍 多人陪玩 {multi_player_booking_id} 缺少文字頻道，開始創建...")
                        try:
                            # 轉換時間為台灣時區
                            start_time = row.startTime
                            end_time = row.endTime
                            if start_time.tzinfo is None:
                                start_time = start_time.replace(tzinfo=timezone.utc)
                            if end_time.tzinfo is None:
                                end_time = end_time.replace(tzinfo=timezone.utc)
                            
                            text_channel = await create_group_booking_text_channel(
                                multi_player_booking_id,
                                [customer_discord] if customer_discord else [],
                                partner_discords,
                                start_time,
                                end_time,
                                is_multiplayer=True
                            )
                            
                            if text_channel:
                                print(f"✅ 已為多人陪玩 {multi_player_booking_id} 創建文字頻道: {text_channel.name}")
                            else:
                                print(f"❌ 多人陪玩 {multi_player_booking_id} 創建文字頻道失敗")
                        except Exception as e:
                            print(f"❌ 多人陪玩 {multi_player_booking_id} 創建文字頻道時發生錯誤: {e}")
                            import traceback
                            traceback.print_exc()
                    
                    # 🔥 如果找到或創建了文字頻道，啟動倒數計時任務
                    if text_channel:
                        # 🔥 避免重複啟動倒數計時任務
                        if multi_player_booking_id not in active_countdown_tasks:
                            active_countdown_tasks.add(multi_player_booking_id)
                            # 啟動倒數計時任務（多人陪玩）
                            bot.loop.create_task(
                                countdown_with_group_rating(
                                    None,  # vc_id (多人陪玩可能還沒有語音頻道)
                                    text_channel.name,
                                    text_channel,
                                    None,  # vc (多人陪玩可能還沒有語音頻道)
                                    [],  # members (會在函數內部獲取)
                                    None,  # record_id (不需要)
                                    multi_player_booking_id,
                                    is_multiplayer=True  # 🔥 標記為多人陪玩
                                )
                            )
                            # print(f"✅ 已啟動多人陪玩倒數計時: {multi_player_booking_id}")
                        else:
                            # print(f"✅ 多人陪玩 {multi_player_booking_id} 的倒數計時任務已存在，跳過啟動")
                            pass
                    else:
                        # print(f"⚠️ 警告：多人陪玩 {multi_player_booking_id} 沒有文字頻道，無法啟動倒數計時")
                        pass
                except Exception as e:
                    print(f"❌ 處理多人陪玩文字頻道失敗: {e}")
                    continue
                    
        except Exception as e:
            # 檢查是否為資料庫連接錯誤
            error_str = str(e).lower()
            if any(keyword in error_str for keyword in ['connection', 'server closed', 'operationalerror', 'timeout', 'could not translate host name']):
                # 🔥 只在第一次報告錯誤時輸出，避免重複輸出
                if not db_connection_error_reported:
                    print(f"⚠️ 資料庫連接問題（群組/多人陪玩檢查）: {e}")
                    db_connection_error_reported = True
            else:
                # 非連接錯誤，正常輸出
                print(f"❌ 檢查群組和多人陪玩文字頻道時發生錯誤: {e}")
    
    except Exception as e:
        # 檢查是否為資料庫連接錯誤
        error_str = str(e).lower()
        if any(keyword in error_str for keyword in ['connection', 'server closed', 'operationalerror', 'timeout', 'could not translate host name']):
            # 🔥 只在第一次報告錯誤時輸出，避免重複輸出
            if not db_connection_error_reported:
                print(f"⚠️ 資料庫連接問題（群組/多人陪玩任務）: {e}")
                db_connection_error_reported = True
        else:
            # 非連接錯誤，正常輸出
            print(f"❌ 檢查群組和多人陪玩文字頻道任務錯誤: {e}")

# --- 自動檢查預約任務 ---
@tasks.loop(seconds=CHECK_INTERVAL)
async def check_bookings():
    """定期檢查已付款的預約並創建語音頻道"""
    global db_connection_error_reported
    await bot.wait_until_ready()
    
    try:
        # 減少日誌輸出
        guild = bot.get_guild(GUILD_ID)
        if not guild:
            print("❌ 找不到 Discord 伺服器")
            return
        
        # 🔥 確保成員已載入（chunk members）
        if not guild.chunked:
            await guild.chunk()
        
        # 查詢已確認且即將開始的預約（只創建語音頻道）
        now = datetime.now(timezone.utc)
        window_start = now - timedelta(minutes=10)  # 擴展到過去10分鐘，處理延遲的情況
        window_end = now + timedelta(minutes=5)  # 5分鐘內即將開始
        
        # 查詢即時預約（夥伴確認後延遲開啟）
        # 🔥 擴大時間窗口，確保能捕獲到所有即時預約（包括純聊天即時預約）
        instant_window_start = now - timedelta(hours=24)  # 擴展到過去24小時，確保能捕獲到所有已確認的即時預約
        instant_window_end = now + timedelta(hours=24)  # 未來24小時內
        
        # 使用原生 SQL 查詢避免 orderNumber 欄位問題
        # 添加檢查：只處理還沒有 Discord 頻道的預約
        # 修改：排除即時預約和多人陪玩預約，避免重複處理
        query = """
            SELECT 
                b.id, b."customerId", b."scheduleId", b.status, b."createdAt", b."updatedAt",
                c.name as customer_name,
                COALESCE(b."paymentInfo"->>'customerDiscord', cu.discord) as customer_discord,
                p.name as partner_name, pu.discord as partner_discord,
                s."startTime", s."endTime",
                b."paymentInfo"->>'isInstantBooking' as is_instant_booking,
                b."paymentInfo"->>'discordDelayMinutes' as discord_delay_minutes
            FROM "Booking" b
            JOIN "Schedule" s ON s.id = b."scheduleId"
            JOIN "Customer" c ON c.id = b."customerId"
            JOIN "User" cu ON cu.id = c."userId"
            JOIN "Partner" p ON p.id = s."partnerId"
            JOIN "User" pu ON pu.id = p."userId"
            WHERE b.status = 'CONFIRMED'
            AND (b."paymentInfo"->>'isInstantBooking' IS NULL OR b."paymentInfo"->>'isInstantBooking' != 'true')
            AND b."multiPlayerBookingId" IS NULL
            AND b."groupBookingId" IS NULL
            AND (b.processed IS NULL OR b.processed = false)
            AND s."startTime" >= :start_time_1
            AND s."startTime" <= :start_time_2
            AND b."discordVoiceChannelId" IS NULL
            AND b."discordTextChannelId" IS NULL
            AND s."endTime" > :current_time
            """
            
        # 即時預約查詢（排除多人陪玩和群組預約）
        instant_query = """
            SELECT 
                b.id, b."customerId", b."scheduleId", b.status, b."createdAt", b."updatedAt",
                c.name as customer_name,
                COALESCE(b."paymentInfo"->>'customerDiscord', cu.discord) as customer_discord,
                p.name as partner_name, pu.discord as partner_discord,
                s."startTime", s."endTime",
                b."paymentInfo"->>'isInstantBooking' as is_instant_booking,
                b."paymentInfo"->>'discordDelayMinutes' as discord_delay_minutes
            FROM "Booking" b
            JOIN "Schedule" s ON s.id = b."scheduleId"
            JOIN "Customer" c ON c.id = b."customerId"
            JOIN "User" cu ON cu.id = c."userId"
            JOIN "Partner" p ON p.id = s."partnerId"
            JOIN "User" pu ON pu.id = p."userId"
            WHERE b.status = 'CONFIRMED'
            AND b."paymentInfo"->>'isInstantBooking' = 'true'
            AND b."multiPlayerBookingId" IS NULL
            AND b."groupBookingId" IS NULL
            AND s."startTime" >= :instant_start_time_1
            AND s."startTime" <= :instant_start_time_2
            AND b."discordVoiceChannelId" IS NULL
            AND s."endTime" > :current_time
        """
        
        # 將同步資料庫操作移到線程池，避免阻塞事件循環
        # ADDED FOR TRANSACTION SAFETY: 每輪創建新 session，確保異常時 rollback
        def query_all_bookings():
            # ADDED FOR TRANSACTION SAFETY: 使用 with Session() 確保自動關閉
            with Session() as s:
                try:
                    # 查詢一般預約（processed 欄位如果不存在，b.processed IS NULL 會返回 true，所以查詢仍能正常工作）
                    try:
                        result = s.execute(text(query), {"start_time_1": window_start, "start_time_2": window_end, "current_time": now})
                    except Exception as query_error:
                        # ADDED FOR TRANSACTION SAFETY: 確保異常時 rollback
                        s.rollback()
                        # 如果查詢失敗（可能是 processed 欄位不存在），移除 processed 條件重試
                        if "processed" in str(query_error).lower():
                            query_without_processed = query.replace("AND (b.processed IS NULL OR b.processed = false)", "")
                            result = s.execute(text(query_without_processed), {"start_time_1": window_start, "start_time_2": window_end, "current_time": now})
                        else:
                            raise
                    
                    # 查詢即時預約
                    instant_result = s.execute(text(instant_query), {"instant_start_time_1": instant_window_start, "instant_start_time_2": instant_window_end, "current_time": now})
                    
                    # 查詢群組預約（通過 groupBookingId 判斷）
                    group_query = """
                        SELECT 
                            b."groupBookingId", b."customerId", b."scheduleId", b.status, b."createdAt", b."updatedAt",
                            c.name as customer_name, cu.discord as customer_discord,
                            p.name as partner_name, pu.discord as partner_discord,
                            s."startTime", s."endTime"
                        FROM "Booking" b
                        JOIN "Schedule" s ON s.id = b."scheduleId"
                        JOIN "Customer" c ON c.id = b."customerId"
                        JOIN "User" cu ON cu.id = c."userId"
                        JOIN "Partner" p ON p.id = s."partnerId"
                        JOIN "User" pu ON pu.id = p."userId"
                        WHERE b.status = 'CONFIRMED'
                        AND b."groupBookingId" IS NOT NULL
                        AND s."startTime" >= :start_time_1
                        AND s."startTime" <= :start_time_2
                        AND s."endTime" > :current_time
                        AND b."discordVoiceChannelId" IS NULL
                    """
                    
                    group_result = s.execute(text(group_query), {"start_time_1": window_start, "start_time_2": window_end, "current_time": now})
                    
                    # ✅ 查詢多人陪玩預約（開始前3-5分鐘創建語音頻道）
                    # 🔥 修改：必須所有夥伴都 CONFIRMED，且沒有 REJECTED 的夥伴
                    # ✅ 時間窗口：開始前5分鐘到開始前3分鐘（確保在開始前3-5分鐘創建）
                    multi_player_window_start = now + timedelta(minutes=3)  # 開始前3分鐘
                    multi_player_window_end = now + timedelta(minutes=5)    # 開始前5分鐘
                    
                    multi_player_query = """
                        SELECT 
                            mpb.id as multi_player_booking_id,
                            mpb."customerId",
                            mpb."startTime",
                            mpb."endTime",
                            c.name as customer_name,
                            cu.discord as customer_discord,
                            array_agg(DISTINCT p.name) FILTER (WHERE b.status IN ('CONFIRMED', 'PARTNER_ACCEPTED')) as partner_names,
                            array_agg(DISTINCT pu.discord) FILTER (WHERE b.status IN ('CONFIRMED', 'PARTNER_ACCEPTED') AND pu.discord IS NOT NULL) as partner_discords,
                            COUNT(DISTINCT b.id) FILTER (WHERE b.status IN ('CONFIRMED', 'PARTNER_ACCEPTED')) as confirmed_count,
                            COUNT(DISTINCT b.id) as total_count
                        FROM "MultiPlayerBooking" mpb
                        JOIN "Booking" b ON b."multiPlayerBookingId" = mpb.id
                        JOIN "Schedule" s ON s.id = b."scheduleId"
                        JOIN "Customer" c ON c.id = mpb."customerId"
                        JOIN "User" cu ON cu.id = c."userId"
                        JOIN "Partner" p ON p.id = s."partnerId"
                        JOIN "User" pu ON pu.id = p."userId"
                        WHERE mpb.status IN ('ACTIVE', 'PENDING')
                        AND mpb."startTime" >= :start_time_1
                        AND mpb."startTime" <= :start_time_2
                        AND mpb."endTime" > :current_time
                        AND mpb."discordVoiceChannelId" IS NULL
                        GROUP BY mpb.id, mpb."customerId", mpb."startTime", mpb."endTime", c.name, cu.discord
                        HAVING 
                            -- 必須所有夥伴都 CONFIRMED 或 PARTNER_ACCEPTED（沒有 PENDING 或 REJECTED）
                            COUNT(DISTINCT b.id) FILTER (WHERE b.status IN ('CONFIRMED', 'PARTNER_ACCEPTED')) = COUNT(DISTINCT b.id)
                            AND COUNT(DISTINCT b.id) FILTER (WHERE b.status IN ('REJECTED', 'PARTNER_REJECTED')) = 0
                            AND COUNT(DISTINCT pu.discord) FILTER (WHERE b.status IN ('CONFIRMED', 'PARTNER_ACCEPTED') AND pu.discord IS NOT NULL) > 0
                    """
                    
                    multi_player_result = s.execute(text(multi_player_query), {"start_time_1": multi_player_window_start, "start_time_2": multi_player_window_end, "current_time": now})
                    
                    # 轉換為列表，避免在線程外訪問結果
                    result_list = list(result)
                    instant_result_list = list(instant_result)
                    group_result_list = list(group_result)
                    multi_player_result_list = list(multi_player_result)
                    
                    return result_list, instant_result_list, group_result_list, multi_player_result_list
                except Exception as e:
                    # ADDED FOR TRANSACTION SAFETY: 確保異常時 rollback
                    s.rollback()
                    raise
        
        try:
            # 在線程池中執行資料庫查詢
            result_list, instant_result_list, group_result_list, multi_player_result_list = await asyncio.to_thread(query_all_bookings)
            
            # 合併四種預約
            all_bookings = []
            
            # 處理多人陪玩預約
            for row in multi_player_result_list:
                try:
                    # 解析 PostgreSQL 數組
                    partner_names = row.partner_names if isinstance(row.partner_names, list) else list(row.partner_names) if row.partner_names else []
                    partner_discords = row.partner_discords if isinstance(row.partner_discords, list) else list(row.partner_discords) if row.partner_discords else []
                    
                    booking = type('Booking', (), {
                        'id': row.multi_player_booking_id,
                        'customerId': row.customerId,
                        'status': 'CONFIRMED',
                        'serviceType': 'MULTI_PLAYER',
                        'customer': type('Customer', (), {
                            'user': type('User', (), {
                                'discord': row.customer_discord
                            })()
                        })(),
                        'schedule': type('Schedule', (), {
                            'startTime': row.startTime,
                            'endTime': row.endTime,
                            'partners': [{'name': name, 'discord': disc} for name, disc in zip(partner_names, partner_discords)]
                        })(),
                        'isInstantBooking': None,
                        'discordDelayMinutes': None
                    })()
                    all_bookings.append(booking)
                except Exception as e:
                    print(f"⚠️ 處理多人陪玩預約失敗: {e}")
                    continue
            
            # 處理群組預約
            group_bookings = {}
            for row in group_result_list:
                    group_id = row.groupBookingId
                    if group_id not in group_bookings:
                        group_bookings[group_id] = {
                            'id': group_id,
                            'customerId': row.customerId,
                            'customer_name': row.customer_name,
                            'customer_discord': row.customer_discord,
                            'startTime': row.startTime,
                            'endTime': row.endTime,
                            'partners': []
                        }
                    
                    group_bookings[group_id]['partners'].append({
                        'name': row.partner_name,
                        'discord': row.partner_discord
                    })
            
            # 為每個群組創建預約對象
            for group_id, group_data in group_bookings.items():
                    booking = type('Booking', (), {
                        'id': group_id,
                        'customerId': group_data['customerId'],
                        'status': 'CONFIRMED',
                        'serviceType': 'GROUP',
                        'customer': type('Customer', (), {
                            'user': type('User', (), {
                                'discord': group_data['customer_discord']
                            })()
                        })(),
                        'schedule': type('Schedule', (), {
                            'startTime': group_data['startTime'],
                            'endTime': group_data['endTime'],
                            'partners': group_data['partners']
                        })(),
                        'isInstantBooking': None,
                        'discordDelayMinutes': None
                    })()
                    all_bookings.append(booking)
                
            # 處理一般預約
            general_count = 0
            for row in result_list:
                general_count += 1
                booking = type('Booking', (), {
                    'id': row.id,
                    'customerId': row.customerId,
                    'scheduleId': row.scheduleId,
                    'status': row.status,
                    'createdAt': row.createdAt,
                    'updatedAt': row.updatedAt,
                    'customer': type('Customer', (), {
                        'name': getattr(row, 'customer_name', None),
                        'user': type('User', (), {
                            'discord': row.customer_discord
                        })()
                    })(),
                    'schedule': type('Schedule', (), {
                        'startTime': row.startTime,
                        'endTime': row.endTime,
                        'partner': type('Partner', (), {
                            'name': getattr(row, 'partner_name', None),
                            'user': type('User', (), {
                                'discord': row.partner_discord
                            })()
                        })()
                    })(),
                    'isInstantBooking': getattr(row, 'is_instant_booking', None),
                    'discordDelayMinutes': getattr(row, 'discord_delay_minutes', None)
                })()
                all_bookings.append(booking)
            
            # 處理即時預約
            instant_count = 0
            for row in instant_result_list:
                instant_count += 1
                booking = type('Booking', (), {
                    'id': row.id,
                    'customerId': row.customerId,
                    'scheduleId': row.scheduleId,
                    'status': row.status,
                    'createdAt': row.createdAt,
                    'updatedAt': row.updatedAt,
                    'customer': type('Customer', (), {
                        'name': getattr(row, 'customer_name', None),
                        'user': type('User', (), {
                            'discord': row.customer_discord
                        })()
                    })(),
                    'schedule': type('Schedule', (), {
                        'startTime': row.startTime,
                        'endTime': row.endTime,
                        'partner': type('Partner', (), {
                            'name': getattr(row, 'partner_name', None),
                            'user': type('User', (), {
                                'discord': row.partner_discord
                            })()
                        })()
                    })(),
                    'isInstantBooking': getattr(row, 'is_instant_booking', None),
                    'discordDelayMinutes': getattr(row, 'discord_delay_minutes', None)
                })()
                all_bookings.append(booking)
            
            bookings = all_bookings
            
            # ✅ 只在有預約需要處理時才顯示，並且使用去重邏輯避免重複日誌
            # 使用集合追蹤已顯示的預約組合，避免重複輸出
            if len(bookings) > 0:
                # 生成唯一標識符（基於預約數量和類型）
                log_key = f"{general_count}_{instant_count}_{len(bookings)}"
                if not hasattr(check_bookings, '_last_log_key'):
                    check_bookings._last_log_key = None
                
                # 只在組合改變時才輸出日誌
                if check_bookings._last_log_key != log_key:
                    print(f"📋 需要處理: {general_count} 個一般預約, {instant_count} 個即時預約, 總共 {len(bookings)} 個")
                    check_bookings._last_log_key = log_key
            
            for booking in bookings:
                try:
                    # 只在創建頻道時才顯示詳細信息
                    
                    # 🔥 檢查是否為即時預約，如果是則跳過（即時預約由 check_instant_bookings_for_text_channel 處理）
                    is_instant_booking = getattr(booking, 'isInstantBooking', None) == 'true' or getattr(booking, 'isInstantBooking', None) == True
                    if is_instant_booking:
                        # 🔥 即時預約由 check_instant_bookings_for_text_channel 處理，這裡跳過
                        continue
                    
                    # 獲取顧客和夥伴的 Discord 名稱（直接從查詢結果取得，確保使用 paymentInfo->>'customerDiscord'）
                    # 一般預約的 booking 對象已經從查詢結果構建，customer_discord 應該來自 paymentInfo->>'customerDiscord'
                    customer_discord = booking.customer.user.discord if booking.customer and booking.customer.user else None
                    
                    # 🔥 多人陪玩和群組預約不需要檢查連續時段（因為它們使用不同的邏輯）
                    # 先檢查是否為多人陪玩或群組預約
                    is_multi_player = hasattr(booking, 'serviceType') and booking.serviceType == 'MULTI_PLAYER'
                    is_group_booking = hasattr(booking, 'serviceType') and booking.serviceType == 'GROUP'
                    
                    # ✅ 額外檢查：如果 booking.id 是群組預約或多人陪玩 ID，也應該跳過一般預約邏輯
                    # 檢查是否是群組預約（通過 groupBookingId）
                    if not is_group_booking:
                        # ADDED FOR TRANSACTION SAFETY: 每輪創建新 session，確保異常時 rollback
                        def check_is_group_booking_by_id(booking_id):
                            # ADDED FOR TRANSACTION SAFETY: 使用 with Session() 確保自動關閉
                            with Session() as s:
                                try:
                                    # 檢查 GroupBooking 表中是否有這個 ID
                                    result = s.execute(text("""
                                        SELECT id FROM "GroupBooking" WHERE id = :booking_id
                                    """), {"booking_id": booking_id}).fetchone()
                                    return result is not None
                                except Exception as e:
                                    # ADDED FOR TRANSACTION SAFETY: 確保異常時 rollback
                                    s.rollback()
                                    raise
                        
                        is_group_booking = await asyncio.to_thread(check_is_group_booking_by_id, booking.id)
                    
                    # 檢查是否是多人陪玩（通過 multiPlayerBookingId）
                    if not is_multi_player:
                        # ADDED FOR TRANSACTION SAFETY: 每輪創建新 session，確保異常時 rollback
                        def check_is_multiplayer_by_id(booking_id):
                            # ADDED FOR TRANSACTION SAFETY: 使用 with Session() 確保自動關閉
                            with Session() as s:
                                try:
                                    # 檢查 MultiPlayerBooking 表中是否有這個 ID
                                    result = s.execute(text("""
                                        SELECT id FROM "MultiPlayerBooking" WHERE id = :booking_id
                                    """), {"booking_id": booking_id}).fetchone()
                                    return result is not None
                                except Exception as e:
                                    # ADDED FOR TRANSACTION SAFETY: 確保異常時 rollback
                                    s.rollback()
                                    raise
                        
                        is_multi_player = await asyncio.to_thread(check_is_multiplayer_by_id, booking.id)
                    
                    # 只有一般預約才需要檢查連續時段
                    if not is_multi_player and not is_group_booking:
                        partner_discord = booking.schedule.partner.user.discord if booking.schedule and booking.schedule.partner and booking.schedule.partner.user else None
                        
                        # 🔥 檢查是否有連續時段的預約已經有頻道（相同顧客和夥伴）
                        # 如果有，就延長現有頻道而不是創建新頻道
                        # ADDED FOR TRANSACTION SAFETY: 每輪創建新 session，確保異常時 rollback
                        def check_consecutive_booking():
                            # ADDED FOR TRANSACTION SAFETY: 使用 with Session() 確保自動關閉
                            with Session() as s:
                                try:
                                    # 獲取當前預約的夥伴 ID
                                    partner_id_query = """
                                        SELECT s."partnerId"
                                        FROM "Booking" b
                                        JOIN "Schedule" s ON s.id = b."scheduleId"
                                        WHERE b.id = :booking_id
                                    """
                                    partner_result = s.execute(text(partner_id_query), {"booking_id": booking.id})
                                    partner_row = partner_result.fetchone()
                                    if not partner_row:
                                        return None
                                    
                                    partner_id = partner_row[0]
                                    
                                    # 查詢相同顧客和夥伴的連續時段預約（已確認且有頻道）
                                    # 連續時段：前一個預約的結束時間 = 當前預約的開始時間
                                    query = """
                                        SELECT 
                                            b.id, b."discordTextChannelId", b."discordVoiceChannelId",
                                            s."startTime", s."endTime"
                                        FROM "Booking" b
                                        JOIN "Schedule" s ON s.id = b."scheduleId"
                                        WHERE b."customerId" = :customer_id
                                        AND s."partnerId" = :partner_id
                                        AND b.status = 'CONFIRMED'
                                        AND b.id != :current_booking_id
                                        AND (b."discordTextChannelId" IS NOT NULL OR b."discordVoiceChannelId" IS NOT NULL)
                                        AND s."endTime" = :current_start_time
                                        ORDER BY s."endTime" DESC
                                        LIMIT 1
                                    """
                                    result = s.execute(text(query), {
                                        "customer_id": booking.customerId,
                                        "partner_id": partner_id,
                                        "current_booking_id": booking.id,
                                        "current_start_time": booking.schedule.startTime
                                    })
                                    return result.fetchone()
                                except Exception as e:
                                    # ADDED FOR TRANSACTION SAFETY: 確保異常時 rollback
                                    s.rollback()
                                    raise
                        
                        consecutive_booking = await asyncio.to_thread(check_consecutive_booking)
                        
                        # 如果找到連續時段的預約，延長現有頻道
                        if consecutive_booking:
                            try:
                                print(f"🔄 發現連續時段預約，延長現有頻道: {consecutive_booking.id} -> {booking.id}")
                                
                                # 更新連續預約的結束時間為當前預約的結束時間
                                # ADDED FOR TRANSACTION SAFETY: 每輪創建新 session，確保異常時 rollback
                                def extend_booking_time():
                                    # ADDED FOR TRANSACTION SAFETY: 使用 with Session() 確保自動關閉
                                    with Session() as s:
                                        try:
                                            # 更新 Schedule 的結束時間
                                            s.execute(text("""
                                                UPDATE "Schedule"
                                                SET "endTime" = :new_end_time
                                                WHERE id = (
                                                    SELECT "scheduleId" FROM "Booking" WHERE id = :consecutive_booking_id
                                                )
                                            """), {
                                                "new_end_time": booking.schedule.endTime,
                                                "consecutive_booking_id": consecutive_booking.id
                                            })
                                            
                                            # 將當前預約的頻道 ID 指向連續預約的頻道
                                            update_data = {}
                                            if consecutive_booking.discordTextChannelId:
                                                update_data['discordTextChannelId'] = consecutive_booking.discordTextChannelId
                                            if consecutive_booking.discordVoiceChannelId:
                                                update_data['discordVoiceChannelId'] = consecutive_booking.discordVoiceChannelId
                                            
                                            if update_data:
                                                set_clause = ", ".join([f'"{k}" = :{k}' for k in update_data.keys()])
                                                s.execute(text(f"""
                                                    UPDATE "Booking"
                                                    SET {set_clause}
                                                    WHERE id = :current_booking_id
                                                """), {
                                                    **update_data,
                                                    "current_booking_id": booking.id
                                                })
                                            
                                            s.commit()
                                        except Exception as e:
                                            # ADDED FOR TRANSACTION SAFETY: 確保異常時 rollback
                                            s.rollback()
                                            raise
                                
                                await asyncio.to_thread(extend_booking_time)
                                
                                # 更新 Discord 頻道名稱
                                guild = bot.get_guild(GUILD_ID)
                                if guild:
                                    # 更新文字頻道名稱
                                    if consecutive_booking.discordTextChannelId:
                                        text_channel = guild.get_channel(int(consecutive_booking.discordTextChannelId))
                                        if text_channel:
                                            # 重新生成頻道名稱（使用連續預約的開始時間和當前預約的結束時間）
                                            start_time = consecutive_booking.startTime
                                            end_time = booking.schedule.endTime
                                            
                                            if start_time.tzinfo is None:
                                                start_time = start_time.replace(tzinfo=timezone.utc)
                                            if end_time.tzinfo is None:
                                                end_time = end_time.replace(tzinfo=timezone.utc)
                                            
                                            tw_start_time = start_time.astimezone(TW_TZ)
                                            tw_end_time = end_time.astimezone(TW_TZ)
                                            
                                            date_str = tw_start_time.strftime("%m%d")
                                            start_time_str = tw_start_time.strftime("%H:%M")
                                            end_time_str = tw_end_time.strftime("%H:%M")
                                            
                                            # 使用連續預約的 ID 來生成一致的 cute_item
                                            import hashlib
                                            hash_obj = hashlib.md5(str(consecutive_booking.id).encode())
                                            hash_hex = hash_obj.hexdigest()
                                            cute_item = CUTE_ITEMS[int(hash_hex[:2], 16) % len(CUTE_ITEMS)]
                                            
                                            new_text_name = f"📅{date_str} {start_time_str}-{end_time_str} {cute_item}"
                                            await text_channel.edit(name=new_text_name)
                                            print(f"✅ 已延長文字頻道名稱: {new_text_name}")
                                    
                                    # 更新語音頻道名稱
                                    if consecutive_booking.discordVoiceChannelId:
                                        voice_channel = guild.get_channel(int(consecutive_booking.discordVoiceChannelId))
                                        if voice_channel:
                                            # 重新生成頻道名稱
                                            start_time = consecutive_booking.startTime
                                            end_time = booking.schedule.endTime
                                            
                                            if start_time.tzinfo is None:
                                                start_time = start_time.replace(tzinfo=timezone.utc)
                                            if end_time.tzinfo is None:
                                                end_time = end_time.replace(tzinfo=timezone.utc)
                                            
                                            tw_start_time = start_time.astimezone(TW_TZ)
                                            tw_end_time = end_time.astimezone(TW_TZ)
                                            
                                            date_str = tw_start_time.strftime("%m%d")
                                            start_time_str = tw_start_time.strftime("%H:%M")
                                            end_time_str = tw_end_time.strftime("%H:%M")
                                            
                                            # 使用連續預約的 ID 來生成一致的 cute_item
                                            import hashlib
                                            hash_obj = hashlib.md5(str(consecutive_booking.id).encode())
                                            hash_hex = hash_obj.hexdigest()
                                            cute_item = CUTE_ITEMS[int(hash_hex[:2], 16) % len(CUTE_ITEMS)]
                                            
                                            new_voice_name = f"📅{date_str} {start_time_str}-{end_time_str} {cute_item}"
                                            await voice_channel.edit(name=new_voice_name)
                                            print(f"✅ 已延長語音頻道名稱: {new_voice_name}")
                                
                                print(f"✅ 已延長連續時段預約的頻道: {consecutive_booking.id} -> {booking.id}")
                                continue  # 跳過創建新頻道
                            except Exception as e:
                                print(f"⚠️ 延長頻道失敗，將創建新頻道: {e}")
                                # 如果延長失敗，繼續創建新頻道
                    
                    # 檢查是否為群組預約或多人陪玩預約
                    if hasattr(booking, 'serviceType') and booking.serviceType == 'GROUP':
                        # 群組預約
                        # 🔥 檢查是否已經有語音頻道（通過 groupBookingId 查詢 GroupBooking 表）
                        group_booking_id = None
                        if hasattr(booking, 'groupBookingId') and booking.groupBookingId:
                            group_booking_id = booking.groupBookingId
                        else:
                            # 如果沒有 groupBookingId，嘗試通過 booking.id 查詢
                            with Session() as s:
                                result = s.execute(text("""
                                    SELECT "groupBookingId" 
                                    FROM "Booking" 
                                    WHERE id = :booking_id
                                """), {'booking_id': booking.id}).fetchone()
                                if result and result[0]:
                                    group_booking_id = result[0]
                        
                        # 如果找到 groupBookingId，檢查是否已經有語音頻道
                        if group_booking_id:
                            with Session() as s:
                                existing = s.execute(text("""
                                    SELECT "discordVoiceChannelId" 
                                    FROM "GroupBooking" 
                                    WHERE id = :group_id
                                """), {'group_id': group_booking_id}).fetchone()
                                
                                if existing and existing[0]:
                                    # 檢查頻道是否真的存在
                                    guild = bot.get_guild(GUILD_ID)
                                    if guild:
                                        existing_channel = guild.get_channel(int(existing[0]))
                                        if existing_channel:
                                            continue
                        
                        partner_discords = [partner['discord'] for partner in booking.schedule.partners]
                        
                        if not customer_discord or not partner_discords:
                            print(f"❌ 群組預約 {booking.id} 缺少 Discord 名稱: 顧客={customer_discord}, 夥伴={partner_discords}")
                            continue
                        
                        # 使用 groupBookingId 或 booking.id 作為群組ID
                        group_id_to_use = group_booking_id if group_booking_id else booking.id
                        
                        # 創建多人開團語音頻道
                        vc = await create_group_booking_voice_channel(
                            group_id_to_use,
                            customer_discord,
                            partner_discords,
                            booking.schedule.startTime,
                            booking.schedule.endTime
                        )
                        
                        if vc:
                            # 只在成功創建新頻道時打印，避免重複日誌
                            # 如果頻道已存在，create_group_booking_voice_channel 會返回現有頻道但不打印
                            # 這裡只打印實際創建的情況
                            pass  # 頻道創建訊息已在 create_group_booking_voice_channel 中打印
                            # 如果使用了 groupBookingId，更新資料庫
                            if group_booking_id:
                                with Session() as s:
                                    s.execute(text("""
                                        UPDATE "GroupBooking" 
                                        SET "discordVoiceChannelId" = :channel_id
                                        WHERE id = :group_id
                                    """), {
                                        'channel_id': str(vc.id),
                                        'group_id': group_booking_id
                                    })
                                    s.commit()
                        else:
                            print(f"❌ 群組預約語音頻道創建失敗 (ID: {group_id_to_use})")
                        continue
                    elif hasattr(booking, 'serviceType') and booking.serviceType == 'MULTI_PLAYER':
                        # ✅ 多人陪玩預約：統一判斷依據為 multiPlayerBookingId
                        multi_player_booking_id = booking.id
                        
                        # ✅ 若已存在文字或語音頻道，必須直接 return，不得再創建
                        def check_multiplayer_existing_channels(multi_player_booking_id):
                            with Session() as s:
                                existing = s.execute(text("""
                                    SELECT "discordTextChannelId", "discordVoiceChannelId"
                                    FROM "MultiPlayerBooking"
                                    WHERE id = :multi_player_booking_id
                                """), {'multi_player_booking_id': multi_player_booking_id}).fetchone()
                                return existing
                        
                        existing_channels = await asyncio.to_thread(check_multiplayer_existing_channels, multi_player_booking_id)
                        
                        # ✅ 若已存在語音頻道，必須直接 return，不得再創建
                        if existing_channels and existing_channels[1]:
                            # 檢查頻道是否真的存在
                            guild = bot.get_guild(GUILD_ID)
                            if guild:
                                existing_voice_channel = guild.get_channel(int(existing_channels[1]))
                                if existing_voice_channel:
                                    continue  # 跳過，不創建
                        
                        partner_discords = [partner['discord'] for partner in booking.schedule.partners]
                        
                        if not customer_discord or not partner_discords:
                            print(f"❌ 多人陪玩缺少 Discord 名稱 (ID: {multi_player_booking_id})")
                            continue
                        
                        # ✅ 創建多人陪玩語音頻道（使用與群組預約相同的函數，傳遞 is_multiplayer=True）
                        vc = await create_group_booking_voice_channel(
                            multi_player_booking_id,
                            customer_discord,
                            partner_discords,
                            booking.schedule.startTime,
                            booking.schedule.endTime,
                            is_multiplayer=True  # ✅ 標記為多人陪玩
                        )
                        
                        if vc:
                            # ✅ 更新 MultiPlayerBooking 表的 discordVoiceChannelId（使用 multiPlayerBookingId）
                            def update_voice_channel_id(multi_player_booking_id, voice_id):
                                with Session() as s:
                                    s.execute(text("""
                                        UPDATE "MultiPlayerBooking"
                                        SET "discordVoiceChannelId" = :voice_id
                                        WHERE id = :multi_player_booking_id
                                    """), {
                                        "voice_id": str(voice_id),
                                        "multi_player_booking_id": multi_player_booking_id
                                    })
                                    s.commit()
                            
                            try:
                                await asyncio.to_thread(update_voice_channel_id, multi_player_booking_id, vc.id)
                                print(f"✅ 多人陪玩語音頻道已創建: {vc.name} (ID: {multi_player_booking_id})")
                                
                                # 🔥 發送 email 通知（異步，不阻塞）
                                try:
                                    api_url = os.getenv('NEXTJS_API_URL', 'https://peiplay.vercel.app')
                                    response = requests.post(
                                        f"{api_url}/api/multi-player-booking/notify-channels-created",
                                        json={"multiPlayerBookingId": booking.id},
                                        timeout=10
                                    )
                                    if response.status_code != 200:
                                        print(f"⚠️ 頻道創建通知發送失敗: {response.status_code}")
                                except Exception as e:
                                    print(f"⚠️ 發送頻道創建通知失敗: {e}")
                            except Exception as e:
                                print(f"⚠️ 更新多人陪玩語音頻道 ID 失敗: {e}")
                        continue
                    else:
                        # 一般預約
                        # ✅ 檢查是否是多人陪玩（通過 multiPlayerBookingId），如果是，直接跳過，不創建配對記錄和自動創建頻道
                        def check_is_multiplayer(booking_id):
                            with Session() as s:
                                result = s.execute(text("""
                                    SELECT "multiPlayerBookingId"
                                    FROM "Booking"
                                    WHERE id = :booking_id
                                """), {"booking_id": booking_id}).fetchone()
                                return result and result[0] is not None
                        
                        is_multiplayer_booking = await asyncio.to_thread(check_is_multiplayer, booking.id)
                        if is_multiplayer_booking:
                            # ✅ 多人陪玩不需要創建配對記錄和自動創建頻道，直接跳過
                            continue
                        
                        # ✅ 檢查是否是群組預約（通過 groupBookingId），如果是，直接跳過，不創建配對記錄和自動創建頻道
                        def check_is_group_booking(booking_id):
                            with Session() as s:
                                result = s.execute(text("""
                                    SELECT "groupBookingId"
                                    FROM "Booking"
                                    WHERE id = :booking_id
                                """), {"booking_id": booking_id}).fetchone()
                                return result and result[0] is not None
                        
                        is_group_booking = await asyncio.to_thread(check_is_group_booking, booking.id)
                        if is_group_booking:
                            # ✅ 群組預約不需要創建配對記錄和自動創建頻道，直接跳過
                            continue
                        
                        partner_discord = booking.schedule.partner.user.discord if booking.schedule and booking.schedule.partner and booking.schedule.partner.user else None
                    
                    # 🔥 不管 Discord 名稱有什麼特殊符號，都繼續處理（用戶可能尚未加入伺服器）
                    if not customer_discord or not partner_discord:
                        print(f"⚠️ 警告：一般預約 {booking.id} 缺少 Discord 名稱: 顧客={customer_discord}, 夥伴={partner_discord}，將繼續處理（用戶可能尚未加入伺服器）")
                        # 不標記為 processed，繼續創建頻道
                        # 繼續執行，不跳過
                    
                    # 🔥 查找 Discord 成員（完整複製即時預約邏輯）
                    customer_name = booking.customer.name if booking.customer else None
                    partner_name = booking.schedule.partner.name if booking.schedule and booking.schedule.partner else None
                    
                    customer_member = None
                    partner_member = None
                    
                    # 🔥 優先使用 Discord 字段查找（因為這是用戶在 Discord 中的實際用戶名，最可靠）
                    # 先嘗試用 Discord 字段查找
                    if customer_discord:
                        try:
                            # 🔥 不管 Discord 名稱有什麼特殊符號，都嘗試查找成員
                            # 先嘗試作為 Discord ID 查找（如果是純數字且長度足夠）
                            discord_id_clean = str(customer_discord).replace('.', '').replace('-', '') if isinstance(customer_discord, str) else str(customer_discord)
                            if discord_id_clean.isdigit() and len(discord_id_clean) >= 17:
                                # 這是 Discord ID，直接查找
                                customer_member = guild.get_member(int(discord_id_clean))
                                if customer_member:
                                    print(f"✅ 通過 Discord ID 找到顧客: {customer_member.name}")
                            else:
                                # 這是用戶名（可能包含特殊符號），使用 find_member_by_discord_name 查找
                                customer_member = find_member_by_discord_name(guild, str(customer_discord))
                        except (ValueError, TypeError) as e:
                            # 如果查找失敗，繼續嘗試用用戶名查找
                            customer_member = None
                    
                    # 如果 Discord 字段找不到，再嘗試用用戶名查找
                    if not customer_member and customer_name:
                        print(f"🔍 Discord 字段找不到，嘗試用用戶名查找顧客: '{customer_name}'")
                        customer_member = find_member_by_discord_name(guild, customer_name)
                    
                    # 🔥 優先使用 Discord 字段查找夥伴（因為這是用戶在 Discord 中的實際用戶名）
                    # 先嘗試用 Discord 字段查找（這是最可靠的）
                    if partner_discord:
                        try:
                            # 🔥 不管 Discord 名稱有什麼特殊符號，都嘗試查找成員
                            # 先嘗試作為 Discord ID 查找（如果是純數字且長度足夠）
                            discord_id_clean = str(partner_discord).replace('.', '').replace('-', '') if isinstance(partner_discord, str) else str(partner_discord)
                            if discord_id_clean.isdigit() and len(discord_id_clean) >= 17:
                                # 這是 Discord ID，直接查找
                                partner_member = guild.get_member(int(discord_id_clean))
                            else:
                                # 這是用戶名（可能包含特殊符號），使用 find_member_by_discord_name 查找
                                partner_member = find_member_by_discord_name(guild, str(partner_discord))
                        except (ValueError, TypeError) as e:
                            # 如果查找失敗，繼續嘗試用用戶名查找
                            partner_member = None
                    
                    # 如果 Discord 字段找不到，再嘗試用用戶名查找
                    if not partner_member and partner_name:
                        print(f"🔍 Discord 字段找不到，嘗試用用戶名查找夥伴: {partner_name}")
                        partner_member = find_member_by_discord_name(guild, partner_name)
                    
                    # 如果還是找不到，輸出警告並嘗試最後的查找方式
                    if not customer_member:
                        print(f"❌ 找不到 Discord 成員: 顧客={customer_name} (Discord: {customer_discord})")
                        # 🔥 最後嘗試：直接遍歷所有成員，查找完全匹配的用戶名
                        if customer_discord:
                            for member in guild.members:
                                if member.name == customer_discord or (member.display_name and member.display_name == customer_discord):
                                    customer_member = member
                                    print(f"✅ 最後嘗試成功找到 Discord 成員: {member.name} (顯示名稱: {member.display_name}) 匹配 {customer_discord}")
                                    break
                        # 🔥 如果 customer_discord 為 None，嘗試用 customer_name 進行更寬鬆的匹配
                        elif customer_name:
                            # 嘗試清理特殊字符後匹配
                            customer_name_clean = customer_name.lower().replace('_', '').replace('.', '').replace('-', '')
                            for member in guild.members:
                                member_name_clean = member.name.lower().replace('_', '').replace('.', '').replace('-', '')
                                member_display_clean = (member.display_name.lower() if member.display_name else "").replace('_', '').replace('.', '').replace('-', '')
                                if (member_name_clean == customer_name_clean or member_display_clean == customer_name_clean or
                                    customer_name_clean in member_name_clean or customer_name_clean in member_display_clean):
                                    customer_member = member
                                    print(f"✅ 通過清理特殊字符匹配找到顧客: {member.name} (查詢: {customer_name})")
                                    break
                    
                    if not partner_member:
                        print(f"❌ 找不到 Discord 成員: 夥伴={partner_name} (Discord: {partner_discord})")
                        # 🔥 最後嘗試：直接遍歷所有成員，查找完全匹配的用戶名
                        if partner_discord:
                            for member in guild.members:
                                if member.name == partner_discord or (member.display_name and member.display_name == partner_discord):
                                    partner_member = member
                                    print(f"✅ 最後嘗試成功找到 Discord 成員: {member.name} (顯示名稱: {member.display_name}) 匹配 {partner_discord}")
                                    break
                        # 🔥 如果 partner_discord 為 None，嘗試用 partner_name 進行更寬鬆的匹配
                        elif partner_name:
                            # 嘗試清理特殊字符後匹配
                            partner_name_clean = partner_name.lower().replace('_', '').replace('.', '').replace('-', '')
                            for member in guild.members:
                                member_name_clean = member.name.lower().replace('_', '').replace('.', '').replace('-', '')
                                member_display_clean = (member.display_name.lower() if member.display_name else "").replace('_', '').replace('.', '').replace('-', '')
                                if (member_name_clean == partner_name_clean or member_display_clean == partner_name_clean or
                                    partner_name_clean in member_name_clean or partner_name_clean in member_display_clean):
                                    partner_member = member
                                    print(f"✅ 通過清理特殊字符匹配找到夥伴: {member.name} (查詢: {partner_name})")
                                    break
                    
                    # 🔥 即使找不到 Discord 成員，也繼續創建頻道（用戶可能尚未加入伺服器）
                    if not customer_member or not partner_member:
                        missing_info = []
                        if not customer_member:
                            missing_info.append(f"顧客={customer_discord}")
                        if not partner_member:
                            missing_info.append(f"夥伴={partner_discord}")
                        print(f"⚠️ 一般預約 {booking.id} 找不到 Discord 成員: {', '.join(missing_info)}，將繼續創建頻道（用戶可能尚未加入伺服器）")
                        # 繼續創建頻道，即使找不到成員
                    
                    # 計算時長（完整複製即時預約邏輯）
                    if booking.schedule.startTime.tzinfo is None:
                        start_time = booking.schedule.startTime.replace(tzinfo=timezone.utc)
                    else:
                        start_time = booking.schedule.startTime
                    if booking.schedule.endTime.tzinfo is None:
                        end_time = booking.schedule.endTime.replace(tzinfo=timezone.utc)
                    else:
                        end_time = booking.schedule.endTime
                    duration_minutes = int((end_time - start_time).total_seconds() / 60)
                    
                    # 轉換為台灣時間
                    tw_start_time = start_time.astimezone(TW_TZ)
                    tw_end_time = end_time.astimezone(TW_TZ)
                    start_time_str = tw_start_time.strftime("%Y/%m/%d %H:%M")
                    end_time_str = tw_end_time.strftime("%H:%M")
                    
                    # 🔥 判斷是否為純聊天（與群組預約邏輯一致）
                    is_chat_only = False
                    
                    # 🔥 使用 booking_id 的 hash 來確定性地選擇動物，確保文字和語音頻道使用相同的動物（與群組預約邏輯一致）
                    import hashlib
                    hash_obj = hashlib.md5(str(booking.id).encode())
                    hash_hex = hash_obj.hexdigest()
                    animal = CUTE_ITEMS[int(hash_hex[:2], 16) % len(CUTE_ITEMS)]
                    cute_item = animal.split()[0] if animal else "🎀"
                    
                    # 🔥 創建頻道名稱（一般預約：使用日期時間格式）
                    date_str = tw_start_time.strftime("%m%d")
                    start_time_str_short = tw_start_time.strftime("%H:%M")
                    end_time_str_short = tw_end_time.strftime("%H:%M")
                    channel_name = f"📅{date_str} {start_time_str_short}-{end_time_str_short} {cute_item}"
                    
                    # 🔥 檢查是否已存在相同名稱的文字頻道（防止重複創建，與群組預約邏輯一致）
                    existing_channels = [ch for ch in guild.text_channels if ch.name == channel_name]
                    if existing_channels:
                        # 🔥 只有在以下條件全部成立時，才允許標記為 processed：
                        # 1. 頻道存在且可用
                        # 2. Discord 成員成功取得 (customer_member 和 partner_member 都存在)
                        # 3. 至少完成一個實際 Discord 動作（如更新資料庫）
                        if customer_member and partner_member:
                            print(f"✅ 已存在相同名稱的文字頻道: {channel_name}，更新資料庫並標記為已處理")
                            with Session() as update_s:
                                update_s.execute(
                                    text("UPDATE \"Booking\" SET \"discordTextChannelId\" = :channel_id WHERE id = :booking_id"),
                                    {"channel_id": str(existing_channels[0].id), "booking_id": booking.id}
                                )
                                update_s.commit()
                            # 只有在成功更新資料庫且成員都存在時，才標記為 processed
                            continue
                        else:
                            print(f"⚠️ 已存在相同名稱的文字頻道: {channel_name}，但缺少 Discord 成員，不標記為 processed")
                            # 不標記為 processed，允許後續重試
                            continue
                    
                    # 🔥 找到分類（與群組預約邏輯一致）
                    category = discord.utils.get(guild.categories, name="Voice Channels")
                    if not category:
                        category = discord.utils.get(guild.categories, name="語音頻道")
                    if not category:
                        category = discord.utils.get(guild.categories, name="語音")
                    if not category:
                        if guild.categories:
                            category = guild.categories[0]
                        else:
                            print("❌ 找不到任何分類")
                            continue
                    
                    # 🔥 設定權限（與群組預約邏輯一致）
                    overwrites = {
                        guild.default_role: discord.PermissionOverwrite(view_channel=False),
                    }
                    
                    # 為顧客添加權限
                    if customer_member:
                        overwrites[customer_member] = discord.PermissionOverwrite(view_channel=True, send_messages=True)
                    
                    # 為夥伴添加權限
                    if partner_member:
                        overwrites[partner_member] = discord.PermissionOverwrite(view_channel=True, send_messages=True)
                    
                    # 🔥 為一般預約創建文字頻道（429 安全，用於倒數計時和評價系統）
                    try:
                        text_channel = await safe_create_text_channel(
                            guild,
                            name=channel_name,
                            category=category,
                            overwrites=overwrites
                        )
                    except Exception as e:
                        print(f"❌ 一般預約 {booking.id} 創建文字頻道失敗: {e}")
                        continue
                    
                    # 建立成功後，更新資料庫的文字頻道 ID（一般預約使用 discordTextChannelId）
                    try:
                        with Session() as s:
                            s.execute(
                                text("""
                                    UPDATE "Booking"
                                    SET "discordTextChannelId" = :channel_id
                                    WHERE id = :booking_id
                                """),
                                {"channel_id": str(text_channel.id), "booking_id": booking.id}
                            )
                            s.commit()
                    except Exception as db_err:
                        print(f"❌ 一般預約 {booking.id} 保存文字頻道 ID 失敗: {db_err}")
                        continue
                    
                    # 🔥 發送歡迎訊息（一般預約格式）
                    welcome_title = "🎮 預約溝通頻道"
                    welcome_title = "🎮 預約溝通頻道"
                    welcome_desc = f"歡迎 {customer_member.mention if customer_member else customer_discord} 和 {partner_member.mention if partner_member else partner_discord}！"
                    
                    welcome_embed = discord.Embed(
                        title=welcome_title,
                        description=welcome_desc,
                        color=0x9b59b6,
                        timestamp=datetime.now(timezone.utc)
                    )
                    
                    # 顯示顧客（優先使用 Discord mention，如果找不到則使用 Discord 用戶名）
                    if customer_member:
                        welcome_embed.add_field(
                            name="👤 顧客",
                            value=customer_member.mention,
                            inline=False
                        )
                    elif customer_discord:
                        # 使用 Discord 用戶名（格式：@username），這樣才能正確抓取用戶
                        welcome_embed.add_field(
                            name="👤 顧客",
                            value=f"@{customer_discord}",
                            inline=False
                        )
                    else:
                        welcome_embed.add_field(
                            name="👤 顧客",
                            value=customer_name,
                            inline=False
                        )
                    
                    # 顯示夥伴（優先使用 Discord mention，如果找不到則使用 Discord 用戶名）
                    if partner_member:
                        welcome_embed.add_field(
                            name="👥 夥伴們",
                            value=partner_member.mention,
                            inline=False
                        )
                    elif partner_discord:
                        # 使用 Discord 用戶名（格式：@username），這樣才能正確抓取用戶
                        welcome_embed.add_field(
                            name="👥 夥伴們",
                            value=f"@{partner_discord}",
                            inline=False
                        )
                    else:
                        welcome_embed.add_field(
                            name="👥 夥伴們",
                            value=partner_name,
                            inline=False
                        )
                    
                    welcome_embed.add_field(
                        name="預約時間",
                        value=f"{start_time_str.split()[1] if ' ' in start_time_str else start_time_str} - {end_time_str}",
                        inline=True
                    )
                    welcome_embed.add_field(
                        name="⏰ 提醒",
                        value="語音頻道將在預約開始前5分鐘自動創建",
                        inline=False
                    )
                    welcome_embed.add_field(
                        name="💬 溝通",
                        value="請在這裡提前溝通遊戲相關事宜",
                        inline=False
                    )
                    
                    # 🔥 發送歡迎訊息（一般預約格式）
                    await text_channel.send(embed=welcome_embed)
                    
                    # 🔥 發送安全規範（與即時預約格式一致）
                    safety_embed = discord.Embed(
                        title="🎙️ 一般預約聊天頻道使用規範與警告",
                        description="為了您的安全，請務必遵守以下規範：",
                        color=0xff6b6b,
                        timestamp=datetime.now(timezone.utc)
                    )
                    
                    safety_embed.add_field(
                        name="📌 頻道性質",
                        value="此聊天頻道為【一般預約用途】。\n僅限遊戲討論、戰術交流、團隊協作使用。\n禁止任何涉及交易、暗示、或其他非遊戲用途的行為。",
                        inline=False
                    )
                    
                    safety_embed.add_field(
                        name="⚠️ 使用規範（請務必遵守）",
                        value="• 禁止挑釁、辱罵、騷擾他人，保持禮貌尊重\n"
                              "• 禁止使用色情、暴力、血腥、歧視等不當言語或內容\n"
                              "• 不得進行金錢交易、索取或提供個資（例如 LINE、IG、電話）\n"
                              "• 不得錄音、偷拍或截圖他人對話，除非經雙方同意\n"
                              "• 禁止惡意模仿或干擾他人聊天\n"
                              "• 禁止使用變聲器或播放音效干擾頻道秩序",
                        inline=False
                    )
                    
                    safety_embed.add_field(
                        name="🚨 警告事項",
                        value="• 系統將隨機錄取部分聊天內容以進行安全稽核\n"
                              "• 如被舉報違規，管理員可立即封鎖或禁言，不另行通知\n"
                              "• 為了您的安全，禁止隨意透漏個人資訊，包括(身分證、住家地址、等等......)\n"
                              "• 若你無法接受以上規範，請勿加入頻道",
                        inline=False
                    )
                    
                    await text_channel.send(embed=safety_embed)
                    
                    # 🔥 語音頻道將在預約開始前 5 分鐘創建（不在這裡創建）
                    # 更新資料庫，保存文字頻道 ID（用於倒數計時和評價系統）
                    # 注意：一般預約已在上面更新 discordTextChannelId
                    # 這裡不需要再次更新
                    
                    # 🔥 創建語音頻道的任務（在預約開始前 5 分鐘執行）
                    async def create_voice_channel_5min_before():
                        try:
                            # 獲取當前時間
                            current_now = datetime.now(timezone.utc)
                            
                            # 計算等待時間：預約開始時間 - 3 分鐘 - 現在時間
                            wait_seconds = (start_time - current_now).total_seconds() - 180  # 減去 3 分鐘（180 秒）
                            
                            # 🔥 只在第一次啟動時輸出日誌，避免重複輸出
                            if wait_seconds > 0:
                                # 只在等待時間較長時輸出一次日誌
                                if wait_seconds > 300:  # 只在大於5分鐘時輸出
                                    print(f"⏰ 語音頻道將在 {wait_seconds/60:.1f} 分鐘後創建: 預約 {booking.id}")
                                await asyncio.sleep(wait_seconds)
                            else:
                                print(f"⚡ 立即創建語音頻道（已超過開始前 3 分鐘）: 預約 {booking.id}")
                            
                            # 檢查預約狀態是否仍然是 CONFIRMED，以及是否已經創建過語音頻道
                            with Session() as check_s:
                                current_booking = check_s.execute(
                                    text("SELECT status, \"discordVoiceChannelId\" FROM \"Booking\" WHERE id = :booking_id"),
                                    {"booking_id": booking.id}
                                ).fetchone()
                                
                                if not current_booking or current_booking.status != 'CONFIRMED':
                                    print(f"⚠️ 預約 {booking.id} 狀態已改變，取消創建語音頻道")
                                    return
                                
                                # 🔥 檢查是否已經創建過語音頻道，避免重複創建
                                if current_booking.discordVoiceChannelId:
                                    print(f"✅ 預約 {booking.id} 的語音頻道已存在，跳過創建")
                                    return
                            
                            # 重新查找 Discord 成員（可能現在已經在伺服器中了）
                            customer_member_vc = None
                            partner_member_vc = None
                            
                            if customer_discord:
                                try:
                                    if customer_discord.replace('.', '').replace('-', '').isdigit():
                                        customer_member_vc = guild.get_member(int(float(customer_discord)))
                                    else:
                                        customer_member_vc = find_member_by_discord_name(guild, customer_discord)
                                except (ValueError, TypeError):
                                    customer_member_vc = None
                            
                            if partner_discord:
                                try:
                                    if partner_discord.replace('.', '').replace('-', '').isdigit():
                                        partner_member_vc = guild.get_member(int(float(partner_discord)))
                                    else:
                                        partner_member_vc = find_member_by_discord_name(guild, partner_discord)
                                except (ValueError, TypeError):
                                    partner_member_vc = None
                            
                            # 如果找不到成員，嘗試使用用戶名查找
                            if not customer_member_vc and customer_name:
                                customer_member_vc = find_member_by_discord_name(guild, customer_name)
                            
                            if not partner_member_vc and partner_name:
                                partner_member_vc = find_member_by_discord_name(guild, partner_name)
                            
                            # 🔥 判斷是否為即時預約，使用對應的頻道名稱格式
                            is_instant = getattr(booking, 'isInstantBooking', None) == 'true' or getattr(booking, 'isInstantBooking', None) == True
                            
                            if is_instant:
                                # 🔥 即時預約：使用與文字頻道完全相同的名稱格式
                                voice_channel_name = f"👥{animal}即時預約聊天"  # 與文字頻道名稱一致
                            else:
                                # 一般預約：使用日期時間格式
                                date_str = tw_start_time.strftime("%m%d")
                                start_time_str_short = tw_start_time.strftime("%H:%M")
                                end_time_str_short = tw_end_time.strftime("%H:%M")
                                voice_channel_name = f"📅{date_str} {start_time_str_short}-{end_time_str_short} {cute_item}"
                            
                            # 設定語音頻道權限
                            voice_overwrites = {
                                guild.default_role: discord.PermissionOverwrite(view_channel=False),
                            }
                            
                            # 為顧客添加權限
                            if customer_member_vc:
                                voice_overwrites[customer_member_vc] = discord.PermissionOverwrite(view_channel=True, connect=True, speak=True)
                                print(f"✅ 為顧客 {customer_member_vc.name} 設置語音頻道權限")
                            else:
                                print(f"⚠️ 未找到顧客成員，將創建匿名語音頻道")
                            
                            # 為夥伴添加權限
                            if partner_member_vc:
                                voice_overwrites[partner_member_vc] = discord.PermissionOverwrite(view_channel=True, connect=True, speak=True)
                                print(f"✅ 為夥伴 {partner_member_vc.name} 設置語音頻道權限")
                            else:
                                print(f"⚠️ 未找到夥伴成員，將創建匿名語音頻道")
                            
                            # 🔥 即使找不到成員，也要創建語音頻道（匿名頻道）
                            print(f"🔍 準備創建語音頻道: {voice_channel_name}")
                            print(f"   類別: {category.name if category else 'None'}")
                            print(f"   權限覆蓋數量: {len(voice_overwrites)}")
                            
                            # 創建語音頻道
                            voice_channel = await guild.create_voice_channel(
                                name=voice_channel_name,
                                category=category,
                                overwrites=voice_overwrites,
                                user_limit=2
                            )
                            print(f"✅ 語音頻道已創建: {voice_channel.name} (ID: {voice_channel.id})")
                            
                            # 更新資料庫，保存語音頻道 ID
                            with Session() as update_s:
                                update_s.execute(
                                    text("UPDATE \"Booking\" SET \"discordVoiceChannelId\" = :voice_channel_id WHERE id = :booking_id"),
                                    {
                                        "voice_channel_id": str(voice_channel.id),
                                        "booking_id": booking.id
                                    }
                                )
                                update_s.commit()
                            
                            # 🔥 判斷預約類型（檢查是否為即時預約）
                            is_instant = getattr(booking, 'isInstantBooking', None) == 'true' or getattr(booking, 'isInstantBooking', None) == True
                            booking_type = "即時預約" if is_instant else "一般預約"
                            print(f"✅ 已為{booking_type} {booking.id} 創建語音頻道: {voice_channel_name}")
                            
                            # 在文字頻道發送通知
                            if text_channel:
                                embed = discord.Embed(
                                    title="🎤 語音頻道已創建！",
                                    description=f"語音頻道 {voice_channel.mention} 已準備就緒，您可以開始使用。",
                                    color=0x00ff00,
                                    timestamp=datetime.now(timezone.utc)
                                )
                                embed.add_field(name="⏰ 預約時長", value=f"{duration_minutes} 分鐘", inline=True)
                                embed.add_field(name="🎤 語音頻道", value=f"{voice_channel.mention}", inline=True)
                                await text_channel.send(embed=embed)
                        except Exception as e:
                            print(f"❌ 創建語音頻道失敗: {e}")
                            import traceback
                            traceback.print_exc()
                    
                    # 啟動創建語音頻道任務（在預約開始前 5 分鐘）
                    # 🔥 避免重複啟動任務
                    if booking.id not in active_voice_channel_tasks:
                        active_voice_channel_tasks.add(booking.id)
                        bot.loop.create_task(create_voice_channel_5min_before())
                        # 🔥 減少日誌輸出
                        # print(f"🔍 語音頻道創建任務已啟動: 預約 {booking.id}")
                    
                    # 🔥 啟動倒數計時任務（包含評價系統）
                    # 注意：語音頻道會在預約開始前 5 分鐘創建，所以這裡先傳 None
                    # 倒數計時任務會從資料庫讀取語音頻道 ID
                    # 🔥 避免重複啟動任務
                    if booking.id not in active_countdown_tasks:
                        active_countdown_tasks.add(booking.id)
                        bot.loop.create_task(countdown_with_rating(
                            None,  # vc_id（語音頻道尚未創建）
                            None,  # channel_name（語音頻道尚未創建）
                            text_channel, 
                            None,  # vc（語音頻道尚未創建）
                            [customer_member, partner_member] if customer_member and partner_member else [],
                            [customer_member, partner_member] if customer_member and partner_member else [],
                            None,  # record_id（如果找不到成員，可能為 None）
                            booking.id
                        ))
                    
                except Exception as e:
                    print(f"❌ 處理預約 {booking.id} 時發生錯誤: {e}")
                    continue
                    
        except Exception as db_error:
            # 檢查是否為連接錯誤
            error_str = str(db_error).lower()
            if any(keyword in error_str for keyword in ['connection', 'server closed', 'operationalerror', 'timeout', 'could not translate host name']):
                # 🔥 只在第一次報告錯誤時輸出，避免重複輸出
                if not db_connection_error_reported:
                    print(f"⚠️ 資料庫連接問題: {db_error}")
                    print("🔄 嘗試重新建立連接...")
                    db_connection_error_reported = True
                
                if reconnect_database():
                    # 🔥 只在恢復成功時輸出一次
                    if db_connection_error_reported:
                        print("✅ 資料庫連接已恢復")
                        db_connection_error_reported = False
                else:
                    # 🔥 只在第一次失敗時輸出
                    if db_connection_error_reported:
                        print("❌ 資料庫連接恢復失敗，將靜默重試（請檢查資料庫服務狀態）")
                return  # 跳過這次檢查，等待下次重試
            else:
                # 非連接錯誤，正常輸出
                print(f"❌ 資料庫查詢失敗: {db_error}")
                    
    except Exception as e:
        print(f"❌ 檢查預約時發生錯誤: {e}")

# --- 檢查預約的定時功能（包括即時預約和一般預約）---
@tasks.loop(seconds=60)  # 每1分鐘檢查一次，確保及時處理
async def check_instant_booking_timing():
    """檢查預約的定時功能：10分鐘提醒、5分鐘延長按鈕、評價系統、頻道刪除（包括即時預約和一般預約）"""
    await bot.wait_until_ready()
    
    try:
        guild = bot.get_guild(GUILD_ID)
        if not guild:
            return
        
        # 🔥 確保成員已載入（chunk members）
        if not guild.chunked:
            await guild.chunk()
        
        now = datetime.now(timezone.utc)
        
        # 將同步資料庫操作移到線程池，避免阻塞事件循環
        # ADDED FOR TRANSACTION SAFETY: 每輪創建新 session，確保異常時 rollback
        def query_instant_bookings():
            # ADDED FOR TRANSACTION SAFETY: 使用 with Session() 確保自動關閉
            with Session() as session:
                try:
                    # 檢查 tenMinuteReminderShown 列是否存在
                    column_exists = False
                    try:
                        result = session.execute(text("""
                            SELECT column_name 
                            FROM information_schema.columns 
                            WHERE table_name='Booking' AND column_name='tenMinuteReminderShown'
                        """))
                        if result.fetchone():
                            column_exists = True
                    except:
                        pass
                    
                    # 1. 檢查需要顯示10分鐘提醒的預約（包括即時預約、一般預約、群組預約和多人陪玩）
                    # 精確計算：結束時間在未來9-11分鐘之間（避免重複發送）
                    ten_minutes_start = now + timedelta(minutes=9)
                    ten_minutes_end = now + timedelta(minutes=11)
                    if column_exists:
                        bookings_10min = session.execute(text("""
                            SELECT b.id, 
                                   COALESCE(b."discordTextChannelId", b."discordEarlyTextChannelId") as text_channel_id,
                                   s."endTime", s."startTime",
                                   c.name as customer_name, p.name as partner_name,
                                   b."paymentInfo"->>'isInstantBooking' as is_instant_booking, 'SINGLE' as booking_type
                            FROM "Booking" b
                            JOIN "Schedule" s ON b."scheduleId" = s.id
                            JOIN "Customer" c ON b."customerId" = c.id
                            JOIN "Partner" p ON s."partnerId" = p.id
                            WHERE b.status = 'CONFIRMED'
                            AND (b."discordTextChannelId" IS NOT NULL OR b."discordEarlyTextChannelId" IS NOT NULL)
                            AND b."tenMinuteReminderShown" = false
                            AND b."groupBookingId" IS NULL
                            AND b."multiPlayerBookingId" IS NULL
                            AND s."startTime" <= :now
                            AND s."endTime" >= :ten_minutes_start
                            AND s."endTime" <= :ten_minutes_end
                        """), {'now': now, 'ten_minutes_start': ten_minutes_start, 'ten_minutes_end': ten_minutes_end}).fetchall()
                    else:
                        # 如果列不存在，使用簡化查詢（不檢查是否已顯示過提醒）
                        bookings_10min = session.execute(text("""
                            SELECT b.id, 
                                   COALESCE(b."discordTextChannelId", b."discordEarlyTextChannelId") as text_channel_id,
                                   s."endTime", s."startTime",
                                   c.name as customer_name, p.name as partner_name,
                                   b."paymentInfo"->>'isInstantBooking' as is_instant_booking, 'SINGLE' as booking_type
                            FROM "Booking" b
                            JOIN "Schedule" s ON b."scheduleId" = s.id
                            JOIN "Customer" c ON b."customerId" = c.id
                            JOIN "Partner" p ON s."partnerId" = p.id
                            WHERE b.status = 'CONFIRMED'
                            AND (b."discordTextChannelId" IS NOT NULL OR b."discordEarlyTextChannelId" IS NOT NULL)
                            AND b."groupBookingId" IS NULL
                            AND b."multiPlayerBookingId" IS NULL
                            AND s."startTime" <= :now
                            AND s."endTime" >= :ten_minutes_start
                            AND s."endTime" <= :ten_minutes_end
                        """), {'now': now, 'ten_minutes_start': ten_minutes_start, 'ten_minutes_end': ten_minutes_end}).fetchall()
                    
                    # 群組預約 10 分鐘提醒
                    # 🔥 必須滿足以下條件：
                    # 1. 預約已經開始（startTime <= now）
                    # 2. 語音頻道已經創建（discordVoiceChannelId IS NOT NULL）
                    # 3. 文字頻道已經創建（discordTextChannelId IS NOT NULL）
                    # 4. 結束時間在未來9-11分鐘之間
                    group_bookings_10min = session.execute(text("""
                        SELECT gb.id, gb."discordTextChannelId", gb."endTime", gb."startTime", gb.title,
                               'GROUP' as booking_type
                        FROM "GroupBooking" gb
                        WHERE gb.status IN ('ACTIVE', 'FULL')
                        AND gb."discordTextChannelId" IS NOT NULL
                        AND gb."discordVoiceChannelId" IS NOT NULL
                        AND gb."startTime" <= :now
                        AND gb."endTime" >= :ten_minutes_start
                        AND gb."endTime" <= :ten_minutes_end
                    """), {'now': now, 'ten_minutes_start': ten_minutes_start, 'ten_minutes_end': ten_minutes_end}).fetchall()
                    
                    # 多人陪玩 10 分鐘提醒
                    # 🔥 必須滿足以下條件：
                    # 1. 預約已經開始（startTime <= now）
                    # 2. 語音頻道已經創建（discordVoiceChannelId IS NOT NULL）
                    # 3. 文字頻道已經創建（discordTextChannelId IS NOT NULL）
                    # 4. 結束時間在未來9-11分鐘之間
                    multi_player_bookings_10min = session.execute(text("""
                        SELECT mpb.id, mpb."discordTextChannelId", mpb."endTime", mpb."startTime",
                               'MULTI_PLAYER' as booking_type
                        FROM "MultiPlayerBooking" mpb
                        WHERE mpb.status = 'ACTIVE'
                        AND mpb."discordTextChannelId" IS NOT NULL
                        AND mpb."discordVoiceChannelId" IS NOT NULL
                        AND mpb."startTime" <= :now
                        AND mpb."endTime" >= :ten_minutes_start
                        AND mpb."endTime" <= :ten_minutes_end
                    """), {'now': now, 'ten_minutes_start': ten_minutes_start, 'ten_minutes_end': ten_minutes_end}).fetchall()
                    
                    return column_exists, list(bookings_10min), list(group_bookings_10min), list(multi_player_bookings_10min)
                except Exception as e:
                    # ADDED FOR TRANSACTION SAFETY: 確保異常時 rollback
                    session.rollback()
                    raise
        
        # 在線程池中執行資料庫查詢
        column_exists, bookings_10min, group_bookings_10min, multi_player_bookings_10min = await asyncio.to_thread(query_instant_bookings)
        
        # 處理一般預約和即時預約的 10 分鐘提醒
        for booking in bookings_10min:
            try:
                # 檢查是否已經發送過提醒（防止重複）
                reminder_key = (booking.id, '10min')
                if reminder_key in sent_reminders:
                    continue
                
                # 🔥 使用 text_channel_id（可能是 discordTextChannelId 或 discordEarlyTextChannelId）
                text_channel_id = booking.text_channel_id if hasattr(booking, 'text_channel_id') else booking.discordTextChannelId
                text_channel = guild.get_channel(int(text_channel_id)) if text_channel_id else None
                if text_channel:
                    # 計算實際剩餘時間（確保時區一致）
                    end_time = booking.endTime
                    # 如果 endTime 沒有時區資訊，假設它是 UTC 時間
                    if end_time.tzinfo is None:
                        end_time = end_time.replace(tzinfo=timezone.utc)
                    remaining_seconds = (end_time - now).total_seconds()
                    remaining_minutes = int(remaining_seconds / 60)
                    
                    embed = discord.Embed(
                        title="⏰ 預約提醒",
                        description=f"預約還有 {remaining_minutes} 分鐘結束，請準備結束遊戲。",
                        color=0xff9900
                    )
                    await text_channel.send(embed=embed)
                    
                    # 標記為已發送
                    sent_reminders.add(reminder_key)
                    
                    # 更新資料庫（使用新的 session）
                    if column_exists:
                        async def update_reminder_shown(booking_id):
                            def update():
                                with Session() as s:
                                    try:
                                        s.execute(text("""
                                            UPDATE "Booking" 
                                            SET "tenMinuteReminderShown" = true
                                            WHERE id = :booking_id
                                        """), {'booking_id': booking_id})
                                        s.commit()
                                    except Exception as e:
                                        print(f"⚠️ 更新10分鐘提醒標記失敗: {e}")
                            await asyncio.to_thread(update)
                        
                        await update_reminder_shown(booking.id)
            except Exception as e:
                print(f"⚠️ 發送10分鐘提醒失敗: {e}")
        
        # 處理群組預約的 10 分鐘提醒
        for booking in group_bookings_10min:
            try:
                reminder_key = (booking.id, '10min', 'GROUP')
                if reminder_key in sent_reminders:
                    continue
                
                text_channel = guild.get_channel(int(booking.discordTextChannelId))
                if text_channel:
                    end_time = booking.endTime
                    if end_time.tzinfo is None:
                        end_time = end_time.replace(tzinfo=timezone.utc)
                    remaining_seconds = (end_time - now).total_seconds()
                    remaining_minutes = int(remaining_seconds / 60)
                    
                    embed = discord.Embed(
                        title="⏰ 群組預約提醒",
                        description=f"群組預約還有 {remaining_minutes} 分鐘結束，請準備結束遊戲。",
                        color=0xff9900
                    )
                    await text_channel.send(embed=embed)
                    sent_reminders.add(reminder_key)
            except Exception as e:
                print(f"⚠️ 發送群組預約10分鐘提醒失敗: {e}")
        
        # 處理多人陪玩的 10 分鐘提醒
        for booking in multi_player_bookings_10min:
            try:
                reminder_key = (booking.id, '10min', 'MULTI_PLAYER')
                if reminder_key in sent_reminders:
                    continue
                
                text_channel = guild.get_channel(int(booking.discordTextChannelId))
                if text_channel:
                    end_time = booking.endTime
                    if end_time.tzinfo is None:
                        end_time = end_time.replace(tzinfo=timezone.utc)
                    remaining_seconds = (end_time - now).total_seconds()
                    remaining_minutes = int(remaining_seconds / 60)
                    
                    embed = discord.Embed(
                        title="⏰ 多人陪玩提醒",
                        description=f"多人陪玩還有 {remaining_minutes} 分鐘結束，請準備結束遊戲。",
                        color=0xff9900
                    )
                    await text_channel.send(embed=embed)
                    sent_reminders.add(reminder_key)
            except Exception as e:
                print(f"⚠️ 發送多人陪玩10分鐘提醒失敗: {e}")
        
        # 2. 檢查需要顯示5分鐘延長按鈕的預約（包括即時預約、一般預約、群組預約和多人陪玩）
        # ADDED FOR TRANSACTION SAFETY: 每輪創建新 session，確保異常時 rollback
        def query_bookings_5min():
            # ADDED FOR TRANSACTION SAFETY: 使用 with Session() 確保自動關閉
            with Session() as session:
                try:
                    # 精確計算：結束時間在未來4-6分鐘之間（避免重複發送）
                    five_minutes_start = now + timedelta(minutes=4)
                    five_minutes_end = now + timedelta(minutes=6)
                    # 一般預約和即時預約 5 分鐘延長按鈕
                    # 🔥 必須滿足以下條件：
                    # 1. 預約已經開始（startTime <= now）
                    # 2. 語音頻道已經創建（discordVoiceChannelId IS NOT NULL）
                    # 3. 文字頻道已經創建（discordTextChannelId IS NOT NULL）
                    # 4. 結束時間在未來4-6分鐘之間
                    # 5. 總時長超過30分鐘（endTime - startTime > 30分鐘）
                    bookings_5min = session.execute(text("""
                        SELECT b.id, 
                               COALESCE(b."discordTextChannelId", b."discordEarlyTextChannelId") as text_channel_id,
                               b."discordVoiceChannelId", s."endTime", s."startTime", 
                               c.name as customer_name, p.name as partner_name,
                               b."paymentInfo"->>'isInstantBooking' as is_instant_booking, 'SINGLE' as booking_type
                        FROM "Booking" b
                        JOIN "Schedule" s ON b."scheduleId" = s.id
                        JOIN "Customer" c ON b."customerId" = c.id
                        JOIN "Partner" p ON s."partnerId" = p.id
                        WHERE b.status = 'CONFIRMED'
                        AND (b."discordTextChannelId" IS NOT NULL OR b."discordEarlyTextChannelId" IS NOT NULL)
                        AND b."discordVoiceChannelId" IS NOT NULL
                        AND b."extensionButtonShown" = false
                        AND b."groupBookingId" IS NULL
                        AND b."multiPlayerBookingId" IS NULL
                        AND s."startTime" <= :now
                        AND s."endTime" >= :five_minutes_start
                        AND s."endTime" <= :five_minutes_end
                        AND EXTRACT(EPOCH FROM (s."endTime" - s."startTime")) / 60 > 30
                    """), {'now': now, 'five_minutes_start': five_minutes_start, 'five_minutes_end': five_minutes_end}).fetchall()
                    
                    # 群組預約 5 分鐘延長按鈕
                    # 🔥 必須滿足以下條件：
                    # 1. 預約已經開始（startTime <= now）
                    # 2. 語音頻道已經創建（discordVoiceChannelId IS NOT NULL）
                    # 3. 文字頻道已經創建（discordTextChannelId IS NOT NULL）
                    # 4. 結束時間在未來4-6分鐘之間
                    # 5. 總時長超過30分鐘（endTime - startTime > 30分鐘）
                    group_bookings_5min = session.execute(text("""
                        SELECT gb.id, gb."discordTextChannelId", gb."discordVoiceChannelId", gb."endTime", gb."startTime", gb.title,
                               'GROUP' as booking_type
                        FROM "GroupBooking" gb
                        WHERE gb.status IN ('ACTIVE', 'FULL')
                        AND gb."discordTextChannelId" IS NOT NULL
                        AND gb."discordVoiceChannelId" IS NOT NULL
                        AND gb."startTime" <= :now
                        AND gb."endTime" >= :five_minutes_start
                        AND gb."endTime" <= :five_minutes_end
                        AND EXTRACT(EPOCH FROM (gb."endTime" - gb."startTime")) / 60 > 30
                    """), {'now': now, 'five_minutes_start': five_minutes_start, 'five_minutes_end': five_minutes_end}).fetchall()
                    
                    # 多人陪玩 5 分鐘延長按鈕
                    # 🔥 必須滿足以下條件：
                    # 1. 預約已經開始（startTime <= now）
                    # 2. 語音頻道已經創建（discordVoiceChannelId IS NOT NULL）
                    # 3. 文字頻道已經創建（discordTextChannelId IS NOT NULL）
                    # 4. 結束時間在未來4-6分鐘之間
                    # 5. 總時長超過30分鐘（endTime - startTime > 30分鐘）
                    multi_player_bookings_5min = session.execute(text("""
                        SELECT mpb.id, mpb."discordTextChannelId", mpb."discordVoiceChannelId", mpb."endTime", mpb."startTime",
                               'MULTI_PLAYER' as booking_type
                        FROM "MultiPlayerBooking" mpb
                        WHERE mpb.status = 'ACTIVE'
                        AND mpb."discordTextChannelId" IS NOT NULL
                        AND mpb."discordVoiceChannelId" IS NOT NULL
                        AND mpb."startTime" <= :now
                        AND mpb."endTime" >= :five_minutes_start
                        AND mpb."endTime" <= :five_minutes_end
                        AND EXTRACT(EPOCH FROM (mpb."endTime" - mpb."startTime")) / 60 > 30
                    """), {'now': now, 'five_minutes_start': five_minutes_start, 'five_minutes_end': five_minutes_end}).fetchall()
                    
                    return list(bookings_5min), list(group_bookings_5min), list(multi_player_bookings_5min)
                except Exception as e:
                    # ADDED FOR TRANSACTION SAFETY: 確保異常時 rollback
                    session.rollback()
                    raise
        
        bookings_5min, group_bookings_5min, multi_player_bookings_5min = await asyncio.to_thread(query_bookings_5min)
        
        # 處理一般預約和即時預約的 5 分鐘延長按鈕
        for booking in bookings_5min:
            try:
                # 檢查是否已經發送過提醒（防止重複）
                reminder_key = (booking.id, '5min')
                if reminder_key in sent_reminders:
                    continue
                
                # 🔥 使用 text_channel_id（可能是 discordTextChannelId 或 discordEarlyTextChannelId）
                text_channel_id = booking.text_channel_id if hasattr(booking, 'text_channel_id') else booking.discordTextChannelId
                text_channel = guild.get_channel(int(text_channel_id)) if text_channel_id else None
                if text_channel:
                    # 獲取語音頻道（Extend5MinView 需要）
                    vc = None
                    if booking.discordVoiceChannelId:
                        vc = guild.get_channel(int(booking.discordVoiceChannelId))
                    
                    if not vc:
                        print(f"⚠️ 找不到語音頻道，無法創建延長按鈕: {booking.id}")
                        continue
                    
                    # 計算實際剩餘時間（確保時區一致）
                    end_time = booking.endTime
                    # 如果 endTime 沒有時區資訊，假設它是 UTC 時間
                    if end_time.tzinfo is None:
                        end_time = end_time.replace(tzinfo=timezone.utc)
                    remaining_seconds = (end_time - now).total_seconds()
                    remaining_minutes = int(remaining_seconds / 60)
                    
                    embed = discord.Embed(
                        title="⏰ 預約即將結束",
                        description=f"預約還有 {remaining_minutes} 分鐘結束，是否需要延長 5 分鐘？",
                        color=0xff9900
                    )
                    
                    # 使用 Extend5MinView 類來創建延長按鈕（與手動創建頻道邏輯一致）
                    channel_name = text_channel.name
                    view = Extend5MinView(booking.id, vc, channel_name, text_channel)
                    
                    await text_channel.send(embed=embed, view=view)
                    
                    print(f"✅ 已發送延長按鈕到文字頻道: {booking.id}")
                    
                    # 標記為已發送
                    sent_reminders.add(reminder_key)
                    
                    # 更新資料庫（在線程中執行）
                    async def update_extension_shown(booking_id):
                        def update():
                            with Session() as s:
                                try:
                                    s.execute(text("""
                                        UPDATE "Booking" 
                                        SET "extensionButtonShown" = true
                                        WHERE id = :booking_id
                                    """), {'booking_id': booking_id})
                                    s.commit()
                                except Exception as e:
                                    print(f"⚠️ 更新5分鐘延長按鈕標記失敗: {e}")
                        await asyncio.to_thread(update)
                    
                    await update_extension_shown(booking.id)
            except Exception as e:
                print(f"⚠️ 發送5分鐘延長按鈕失敗: {e}")
        
        # 處理群組預約的 5 分鐘延長按鈕
        for booking in group_bookings_5min:
            try:
                reminder_key = (booking.id, '5min', 'GROUP')
                if reminder_key in sent_reminders:
                    continue
                
                text_channel = guild.get_channel(int(booking.discordTextChannelId))
                vc = guild.get_channel(int(booking.discordVoiceChannelId)) if booking.discordVoiceChannelId else None
                
                if text_channel and vc:
                    end_time = booking.endTime
                    if end_time.tzinfo is None:
                        end_time = end_time.replace(tzinfo=timezone.utc)
                    remaining_seconds = (end_time - now).total_seconds()
                    remaining_minutes = int(remaining_seconds / 60)
                    
                    embed = discord.Embed(
                        title="⏰ 群組預約即將結束",
                        description=f"群組預約還有 {remaining_minutes} 分鐘結束，是否需要延長 5 分鐘？",
                        color=0xff9900
                    )
                    
                    channel_name = text_channel.name
                    view = Extend5MinView(booking.id, vc, channel_name, text_channel)
                    await text_channel.send(embed=embed, view=view)
                    sent_reminders.add(reminder_key)
                    print(f"✅ 已發送群組預約延長按鈕: {booking.id}")
            except Exception as e:
                print(f"⚠️ 發送群組預約5分鐘延長按鈕失敗: {e}")
        
        # 處理多人陪玩的 5 分鐘延長按鈕
        for booking in multi_player_bookings_5min:
            try:
                reminder_key = (booking.id, '5min', 'MULTI_PLAYER')
                if reminder_key in sent_reminders:
                    continue
                
                text_channel = guild.get_channel(int(booking.discordTextChannelId))
                vc = guild.get_channel(int(booking.discordVoiceChannelId)) if booking.discordVoiceChannelId else None
                
                if text_channel and vc:
                    end_time = booking.endTime
                    if end_time.tzinfo is None:
                        end_time = end_time.replace(tzinfo=timezone.utc)
                    remaining_seconds = (end_time - now).total_seconds()
                    remaining_minutes = int(remaining_seconds / 60)
                    
                    embed = discord.Embed(
                        title="⏰ 多人陪玩即將結束",
                        description=f"多人陪玩還有 {remaining_minutes} 分鐘結束，是否需要延長 5 分鐘？",
                        color=0xff9900
                    )
                    
                    channel_name = text_channel.name
                    view = Extend5MinView(booking.id, vc, channel_name, text_channel)
                    await text_channel.send(embed=embed, view=view)
                    sent_reminders.add(reminder_key)
                    print(f"✅ 已發送多人陪玩5分鐘提醒和延長按鈕: {booking.id}")
            except Exception as e:
                print(f"⚠️ 發送多人陪玩5分鐘延長按鈕失敗: {e}")
        
        # 2.5. 檢查需要顯示1分鐘提醒的預約（包括多人陪玩）
        # ADDED FOR TRANSACTION SAFETY: 每輪創建新 session，確保異常時 rollback
        def query_bookings_1min():
            # ADDED FOR TRANSACTION SAFETY: 使用 with Session() 確保自動關閉
            with Session() as session:
                try:
                    # 精確計算：結束時間在未來0.5-1.5分鐘之間（避免重複發送）
                    one_minute_start = now + timedelta(seconds=30)
                    one_minute_end = now + timedelta(minutes=1, seconds=30)
                    
                    # 多人陪玩 1 分鐘提醒
                    multi_player_bookings_1min = session.execute(text("""
                        SELECT mpb.id, mpb."discordTextChannelId", mpb."endTime",
                               'MULTI_PLAYER' as booking_type
                        FROM "MultiPlayerBooking" mpb
                        WHERE mpb.status = 'ACTIVE'
                        AND mpb."discordTextChannelId" IS NOT NULL
                        AND mpb."endTime" >= :one_minute_start
                        AND mpb."endTime" <= :one_minute_end
                    """), {'one_minute_start': one_minute_start, 'one_minute_end': one_minute_end}).fetchall()
                    
                    return list(multi_player_bookings_1min)
                except Exception as e:
                    # ADDED FOR TRANSACTION SAFETY: 確保異常時 rollback
                    session.rollback()
                    raise
        
        multi_player_bookings_1min = await asyncio.to_thread(query_bookings_1min)
        
        # 處理多人陪玩的 1 分鐘提醒
        for booking in multi_player_bookings_1min:
            try:
                reminder_key = (booking.id, '1min', 'MULTI_PLAYER')
                if reminder_key in sent_reminders:
                    continue
                
                text_channel = guild.get_channel(int(booking.discordTextChannelId))
                if text_channel:
                    end_time = booking.endTime
                    if end_time.tzinfo is None:
                        end_time = end_time.replace(tzinfo=timezone.utc)
                    remaining_seconds = (end_time - now).total_seconds()
                    remaining_minutes = int(remaining_seconds / 60)
                    remaining_seconds_only = int(remaining_seconds % 60)
                    
                    embed = discord.Embed(
                        title="⏰ 多人陪玩即將結束",
                        description=f"多人陪玩還有 {remaining_minutes} 分 {remaining_seconds_only} 秒結束，請準備結束遊戲！",
                        color=0xff0000
                    )
                    await text_channel.send(embed=embed)
                    sent_reminders.add(reminder_key)
                    print(f"✅ 已發送多人陪玩1分鐘提醒: {booking.id}")
            except Exception as e:
                print(f"⚠️ 發送多人陪玩1分鐘提醒失敗: {e}")
        
        # 3. 檢查需要結束的預約（時間結束，包括即時預約、一般預約、群組預約和多人陪玩）
        # ADDED FOR TRANSACTION SAFETY: 每輪創建新 session，確保異常時 rollback
        def query_bookings_ended():
            # ADDED FOR TRANSACTION SAFETY: 使用 with Session() 確保自動關閉
            with Session() as session:
                try:
                    # 一般預約和即時預約
                    bookings_ended = session.execute(text("""
                        SELECT b.id, b."discordVoiceChannelId", 
                               COALESCE(b."discordTextChannelId", b."discordEarlyTextChannelId") as text_channel_id,
                               b."ratingCompleted",
                               c.name as customer_name, p.name as partner_name, s."endTime",
                               b."paymentInfo"->>'isInstantBooking' as is_instant_booking,
                               'SINGLE' as booking_type
                        FROM "Booking" b
                        JOIN "Customer" c ON b."customerId" = c.id
                        JOIN "Schedule" s ON b."scheduleId" = s.id
                        JOIN "Partner" p ON s."partnerId" = p.id
                        WHERE b.status = 'CONFIRMED'
                        AND b."discordVoiceChannelId" IS NOT NULL
                        AND (b."discordTextChannelId" IS NOT NULL OR b."discordEarlyTextChannelId" IS NOT NULL)
                        AND b."groupBookingId" IS NULL
                        AND b."multiPlayerBookingId" IS NULL
                        AND s."endTime" <= :now
                    """), {'now': now}).fetchall()
                    
                    # 群組預約
                    group_bookings_ended = session.execute(text("""
                        SELECT gb.id, gb."discordVoiceChannelId", gb."discordTextChannelId",
                               gb."endTime", gb.title,
                               'GROUP' as booking_type
                        FROM "GroupBooking" gb
                        WHERE gb.status = 'ACTIVE'
                        AND gb."discordVoiceChannelId" IS NOT NULL
                        AND gb."endTime" <= :now
                    """), {'now': now}).fetchall()
                    
                    # 多人陪玩
                    multi_player_bookings_ended = session.execute(text("""
                        SELECT mpb.id, mpb."discordVoiceChannelId", mpb."discordTextChannelId",
                               mpb."endTime",
                               'MULTI_PLAYER' as booking_type
                        FROM "MultiPlayerBooking" mpb
                        WHERE mpb.status = 'ACTIVE'
                        AND mpb."discordVoiceChannelId" IS NOT NULL
                        AND mpb."endTime" <= :now
                    """), {'now': now}).fetchall()
                    
                    return list(bookings_ended), list(group_bookings_ended), list(multi_player_bookings_ended)
                except Exception as e:
                    # ADDED FOR TRANSACTION SAFETY: 確保異常時 rollback
                    session.rollback()
                    raise
        
        bookings_ended, group_bookings_ended, multi_player_bookings_ended = await asyncio.to_thread(query_bookings_ended)
        
        # 處理一般預約和即時預約
        for booking in bookings_ended:
            try:
                # 檢查是否已經處理過（防止重複處理）
                completed_key = (booking.id, 'completed')
                if completed_key in sent_reminders:
                    continue
                
                is_instant = getattr(booking, 'is_instant_booking', None) == 'true'
                booking_type = "即時預約" if is_instant else "一般預約"
                print(f"🔍 處理已結束的{booking_type}: {booking.id}, 結束時間: {booking.endTime}")
                
                # 刪除語音頻道
                if booking.discordVoiceChannelId:
                    voice_channel = guild.get_channel(int(booking.discordVoiceChannelId))
                    if voice_channel:
                        try:
                            await voice_channel.delete()
                            print(f"✅ 已刪除語音頻道: {voice_channel.name} (預約 {booking.id})")
                        except Exception as e:
                            print(f"⚠️ 刪除語音頻道失敗: {e}")
                
                # 在文字頻道顯示評價系統（只發送一次）
                # 🔥 使用 text_channel_id（可能是 discordTextChannelId 或 discordEarlyTextChannelId）
                text_channel_id = booking.text_channel_id if hasattr(booking, 'text_channel_id') else booking.discordTextChannelId
                if text_channel_id:
                    text_channel = guild.get_channel(int(text_channel_id))
                    if text_channel:
                        # 檢查是否已經發送過評價系統
                        if booking.id not in rating_sent_bookings:
                            embed = discord.Embed(
                                title=f"⭐ {booking_type}結束 - 請給予評價",
                                description=f"{booking_type}已結束，請為您的遊戲體驗給予評價。",
                                color=0x00ff88
                            )
                            embed.add_field(name="顧客", value=f"@{booking.customer_name}", inline=True)
                            embed.add_field(name="夥伴", value=f"@{booking.partner_name}", inline=True)
                            embed.add_field(name="評價說明", value="請點擊下方的星等按鈕來評價這次的遊戲體驗。", inline=False)
                            
                            # 創建評價視圖（使用 BookingRatingView，與手動創建頻道邏輯一致）
                            view = BookingRatingView(booking.id)
                            await text_channel.send(embed=embed, view=view)
                            rating_sent_bookings.add(booking.id)
                            print(f"✅ 已發送評價系統: {booking.id}")
                            
                            # 啟動10分鐘後自動提交評價回饋的任務（與手動創建頻道邏輯一致）
                            async def auto_submit_rating_feedback():
                                try:
                                    # 等待10分鐘讓用戶填寫評價
                                    await asyncio.sleep(600)  # 10 分鐘 = 600 秒
                                    
                                    # 10分鐘後自動提交未完成的評價（與手動創建頻道邏輯一致）
                                    await submit_auto_rating(booking.id, text_channel)
                                    print(f"✅ 已為{booking_type} {booking.id} 發送評價回饋到管理員頻道")
                                except Exception as e:
                                    print(f"⚠️ 自動提交{booking_type}評價回饋失敗: {e}")
                                    import traceback
                                    traceback.print_exc()
                            
                            # 啟動自動提交評價回饋任務
                            bot.loop.create_task(auto_submit_rating_feedback())
                        else:
                            print(f"⚠️ 預約 {booking.id} 已發送過評價系統，跳過")
                
                # 標記為已處理
                sent_reminders.add(completed_key)
                
                # 更新資料庫狀態（在線程中執行）
                async def update_booking_completed(booking_id):
                    def update():
                        with Session() as s:
                            try:
                                s.execute(text("""
                                    UPDATE "Booking" 
                                    SET status = 'COMPLETED',
                                        "discordVoiceChannelId" = NULL
                                    WHERE id = :booking_id
                                """), {'booking_id': booking_id})
                                s.commit()
                                # 狀態更新成功，略過終端輸出以降低雜訊
                                # print(f"✅ 已更新預約狀態為 COMPLETED: {booking_id}")
                            except Exception as e:
                                print(f"⚠️ 更新預約狀態失敗: {e}")
                    await asyncio.to_thread(update)
                
                await update_booking_completed(booking.id)
                
            except Exception as e:
                print(f"⚠️ 處理已結束預約時發生錯誤: {e}")
        
        # 處理群組預約
        for booking in group_bookings_ended:
            try:
                completed_key = (booking.id, 'completed', 'GROUP')
                if completed_key in sent_reminders:
                    continue
                
                print(f"🔍 處理已結束的群組預約: {booking.id}, 結束時間: {booking.endTime}")
                
                # 刪除語音頻道
                if booking.discordVoiceChannelId:
                    voice_channel = guild.get_channel(int(booking.discordVoiceChannelId))
                    if voice_channel:
                        try:
                            await voice_channel.delete()
                            print(f"✅ 已刪除群組預約語音頻道: {voice_channel.name} (群組 {booking.id})")
                        except Exception as e:
                            print(f"⚠️ 刪除群組預約語音頻道失敗: {e}")
                
                # 在文字頻道顯示評價系統
                text_channel = None
                if booking.discordTextChannelId:
                    text_channel = guild.get_channel(int(booking.discordTextChannelId))
                
                # 如果文字頻道不存在，嘗試創建一個
                if not text_channel:
                    print(f"⚠️ 群組預約 {booking.id} 沒有文字頻道，嘗試創建...")
                    # 獲取群組預約的參與者列表（通過 Booking 表判斷顧客和夥伴）
                    def get_group_booking_participants(group_booking_id):
                        with Session() as s:
                            # 查詢所有有 Booking 記錄的顧客（有付費的人）
                            customer_result = s.execute(text("""
                                SELECT DISTINCT cu.discord as customer_discord
                                FROM "GroupBooking" gb
                                JOIN "Booking" b ON b."groupBookingId" = gb.id
                                JOIN "Customer" c ON c.id = b."customerId"
                                JOIN "User" cu ON cu.id = c."userId"
                                WHERE gb.id = :group_booking_id
                                AND b.status IN ('CONFIRMED', 'PARTNER_ACCEPTED', 'PAID_WAITING_PARTNER_CONFIRMATION', 'COMPLETED')
                                AND cu.discord IS NOT NULL
                            """), {"group_booking_id": group_booking_id}).fetchall()
                            
                            # 查詢所有夥伴（在 GroupBookingParticipant 中有 partnerId 的人）
                            partner_result = s.execute(text("""
                                SELECT DISTINCT pu.discord as partner_discord
                                FROM "GroupBooking" gb
                                JOIN "GroupBookingParticipant" gbp ON gbp."groupBookingId" = gb.id
                                JOIN "Partner" p ON p.id = gbp."partnerId"
                                JOIN "User" pu ON pu.id = p."userId"
                                WHERE gb.id = :group_booking_id
                                AND gbp.status = 'ACTIVE'
                                AND pu.discord IS NOT NULL
                            """), {"group_booking_id": group_booking_id}).fetchall()
                            
                            customer_discords = [row.customer_discord for row in customer_result if row.customer_discord]
                            partner_discords = [row.partner_discord for row in partner_result if row.partner_discord]
                            
                            return customer_discords, partner_discords
                    
                    customer_discords, partner_discords = await asyncio.to_thread(get_group_booking_participants, booking.id)
                    # 查詢結果日誌太雜，已關閉詳細輸出
                    # print(f"🔍 群組預約 {booking.id} 結束時參與者查詢結果:")
                    # print(f"   - 顧客（有付費記錄）: {customer_discords}")
                    # print(f"   - 夥伴: {partner_discords}")
                    
                    # 🔥 如果找不到文字頻道，則創建新頻道（用於發送評價系統）
                    if not customer_discords:
                        print(f"⚠️ 群組預約 {booking.id} 沒有顧客，無法創建文字頻道")
                    else:
                        try:
                            # 獲取群組預約的開始和結束時間
                            def get_group_booking_times(group_booking_id):
                                with Session() as s:
                                    result = s.execute(text("""
                                        SELECT "startTime", "endTime" 
                                        FROM "GroupBooking" 
                                        WHERE id = :group_booking_id
                                    """), {"group_booking_id": group_booking_id}).fetchone()
                                    return result[0], result[1] if result else (None, None)
                            
                            start_time, end_time = await asyncio.to_thread(get_group_booking_times, booking.id)
                            
                            if start_time and end_time:
                                # 轉換時間為台灣時區
                                if start_time.tzinfo is None:
                                    start_time = start_time.replace(tzinfo=timezone.utc)
                                if end_time.tzinfo is None:
                                    end_time = end_time.replace(tzinfo=timezone.utc)
                                
                                text_channel = await create_group_booking_text_channel(
                                    booking.id,
                                    customer_discords,
                                    partner_discords,
                                    start_time,
                                    end_time,
                                    is_multiplayer=False
                                )
                                
                                if text_channel:
                                    print(f"✅ 已為群組預約 {booking.id} 創建文字頻道（用於評價系統）: {text_channel.name}")
                                else:
                                    print(f"❌ 群組預約 {booking.id} 創建文字頻道失敗")
                            else:
                                print(f"⚠️ 群組預約 {booking.id} 缺少開始或結束時間，無法創建文字頻道")
                        except Exception as e:
                            print(f"❌ 群組預約 {booking.id} 創建文字頻道時發生錯誤: {e}")
                            import traceback
                            traceback.print_exc()
                
                if text_channel:
                    if booking.id not in rating_sent_bookings:
                        # 🔥 獲取群組預約的參與者列表（通過 Booking 表判斷顧客和夥伴）
                        def get_group_booking_members(group_booking_id):
                            with Session() as s:
                                # 查詢所有有 Booking 記錄的顧客（有付費的人）
                                customer_result = s.execute(text("""
                                    SELECT DISTINCT cu.discord as customer_discord
                                    FROM "GroupBooking" gb
                                    JOIN "Booking" b ON b."groupBookingId" = gb.id
                                    JOIN "Customer" c ON c.id = b."customerId"
                                    JOIN "User" cu ON cu.id = c."userId"
                                    WHERE gb.id = :group_booking_id
                                    AND b.status IN ('CONFIRMED', 'PARTNER_ACCEPTED', 'PAID_WAITING_PARTNER_CONFIRMATION', 'COMPLETED')
                                    AND cu.discord IS NOT NULL
                                """), {"group_booking_id": group_booking_id}).fetchall()
                                
                                # 查詢所有夥伴
                                partner_result = s.execute(text("""
                                    SELECT DISTINCT pu.discord as partner_discord
                                    FROM "GroupBooking" gb
                                    JOIN "GroupBookingParticipant" gbp ON gbp."groupBookingId" = gb.id
                                    JOIN "Partner" p ON p.id = gbp."partnerId"
                                    JOIN "User" pu ON pu.id = p."userId"
                                    WHERE gb.id = :group_booking_id
                                    AND gbp.status = 'ACTIVE'
                                    AND pu.discord IS NOT NULL
                                """), {"group_booking_id": group_booking_id}).fetchall()
                                
                                # 合併所有參與者
                                members = []
                                for row in customer_result:
                                    if row.customer_discord:
                                        members.append(row.customer_discord)
                                for row in partner_result:
                                    if row.partner_discord:
                                        members.append(row.partner_discord)
                                # 去重
                                return list(set(members))
                        
                        members = await asyncio.to_thread(get_group_booking_members, booking.id)
                        
                        # 使用群組評價系統，傳入參與者列表
                        # 🔥 使用與一般預約相同的評價系統
                        view = BookingRatingView(booking.id)
                        await text_channel.send(
                            "🎉 預約時間結束！\n"
                            "請為您的遊戲夥伴評分：\n\n"
                            "點擊下方按鈕選擇星等，系統會彈出評價表單讓您填寫評論。",
                            view=view
                        )
                        rating_sent_bookings.add(booking.id)
                        print(f"✅ 已發送群組預約評價系統: {booking.id}")
                    else:
                        print(f"⚠️ 群組預約 {booking.id} 已發送過評價系統，跳過")
                else:
                    print(f"⚠️ 群組預約 {booking.id} 無法創建文字頻道，無法發送評價系統")
                
                sent_reminders.add(completed_key)
                
                # 更新資料庫狀態
                async def update_group_booking_completed(booking_id):
                    def update():
                        with Session() as s:
                            try:
                                s.execute(text("""
                                    UPDATE "GroupBooking" 
                                    SET status = 'COMPLETED',
                                        "discordVoiceChannelId" = NULL
                                    WHERE id = :booking_id
                                """), {'booking_id': booking_id})
                                s.commit()
                            except Exception as e:
                                print(f"⚠️ 更新群組預約狀態失敗: {e}")
                    await asyncio.to_thread(update)
                
                await update_group_booking_completed(booking.id)
                
            except Exception as e:
                print(f"⚠️ 處理已結束群組預約時發生錯誤: {e}")
        
        # 處理多人陪玩
        for booking in multi_player_bookings_ended:
            try:
                completed_key = (booking.id, 'completed', 'MULTI_PLAYER')
                if completed_key in sent_reminders:
                    continue
                
                print(f"🔍 處理已結束的多人陪玩: {booking.id}, 結束時間: {booking.endTime}")
                
                # 刪除語音頻道
                if booking.discordVoiceChannelId:
                    voice_channel = guild.get_channel(int(booking.discordVoiceChannelId))
                    if voice_channel:
                        try:
                            await voice_channel.delete()
                            print(f"✅ 已刪除多人陪玩語音頻道: {voice_channel.name} (多人陪玩 {booking.id})")
                        except Exception as e:
                            print(f"⚠️ 刪除多人陪玩語音頻道失敗: {e}")
                
                # 在文字頻道顯示評價系統
                if booking.discordTextChannelId:
                    text_channel = guild.get_channel(int(booking.discordTextChannelId))
                    if text_channel:
                        if booking.id not in rating_sent_bookings:
                            # 🔥 獲取多人陪玩的參與者列表（包括顧客和所有夥伴）
                            def get_multi_player_booking_members(multi_player_booking_id):
                                with Session() as s:
                                    # 查詢多人陪玩的所有參與者
                                    result = s.execute(text("""
                                        SELECT 
                                            cu.discord as customer_discord,
                                            pu.discord as partner_discord
                                        FROM "MultiPlayerBooking" mpb
                                        JOIN "Booking" b ON b."multiPlayerBookingId" = mpb.id
                                        JOIN "Customer" c ON c.id = b."customerId"
                                        JOIN "User" cu ON cu.id = c."userId"
                                        JOIN "Schedule" s ON s.id = b."scheduleId"
                                        JOIN "Partner" p ON p.id = s."partnerId"
                                        JOIN "User" pu ON pu.id = p."userId"
                                        WHERE mpb.id = :multi_player_booking_id
                                        AND b.status IN ('CONFIRMED', 'PARTNER_ACCEPTED')
                                    """), {"multi_player_booking_id": booking.id}).fetchall()
                                    
                                    # 收集所有參與者的 Discord ID
                                    members = []
                                    for row in result:
                                        if row.customer_discord:
                                            members.append(row.customer_discord)
                                        if row.partner_discord:
                                            members.append(row.partner_discord)
                                    # 去重
                                    return list(set(members))
                            
                            members = await asyncio.to_thread(get_multi_player_booking_members, booking.id)
                            
                            # 使用群組評價系統（多人陪玩也使用群組評價系統），傳入參與者列表
                            await show_group_rating_system(text_channel, booking.id, members, is_multiplayer=True)
                            rating_sent_bookings.add(booking.id)
                            print(f"✅ 已發送多人陪玩評價系統: {booking.id}, 參與人數: {len(members)}")
                            
                            # 🔥 啟動10分鐘後自動清理評價頻道的任務
                            async def auto_cleanup_rating_channel():
                                try:
                                    # 等待10分鐘讓用戶填寫評價
                                    await asyncio.sleep(600)  # 10 分鐘 = 600 秒
                                    print(f"✅ 多人陪玩 {booking.id} 評價時間已過，將在下次清理時刪除頻道")
                                except Exception as e:
                                    print(f"⚠️ 自動清理多人陪玩評價頻道失敗: {e}")
                            
                            # 啟動自動清理任務
                            bot.loop.create_task(auto_cleanup_rating_channel())
                        else:
                            print(f"⚠️ 多人陪玩 {booking.id} 已發送過評價系統，跳過")
                
                sent_reminders.add(completed_key)
                
                # 更新資料庫狀態
                async def update_multi_player_booking_completed(booking_id):
                    def update():
                        with Session() as s:
                            try:
                                s.execute(text("""
                                    UPDATE "MultiPlayerBooking" 
                                    SET status = 'COMPLETED',
                                        "discordVoiceChannelId" = NULL
                                    WHERE id = :booking_id
                                """), {'booking_id': booking_id})
                                s.commit()
                            except Exception as e:
                                print(f"⚠️ 更新多人陪玩狀態失敗: {e}")
                    await asyncio.to_thread(update)
                
                await update_multi_player_booking_completed(booking.id)
                
            except Exception as e:
                print(f"⚠️ 處理已結束多人陪玩時發生錯誤: {e}")
                import traceback
                traceback.print_exc()
        
        # 4. 檢查需要清理文字頻道的預約（評價完成後，包括即時預約和一般預約）
        # ADDED FOR TRANSACTION SAFETY: 每輪創建新 session，確保異常時 rollback
        def query_bookings_cleanup():
            # ADDED FOR TRANSACTION SAFETY: 使用 with Session() 確保自動關閉
            with Session() as session:
                try:
                    bookings_cleanup = session.execute(text("""
                        SELECT b.id, b."discordTextChannelId", b."ratingCompleted", b."textChannelCleaned"
                        FROM "Booking" b
                        WHERE b."ratingCompleted" = true
                        AND b."textChannelCleaned" = false
                        AND b."groupBookingId" IS NULL
                        AND b."multiPlayerBookingId" IS NULL
                        AND b."discordTextChannelId" IS NOT NULL
                    """)).fetchall()
                    return list(bookings_cleanup)
                except Exception as e:
                    # ADDED FOR TRANSACTION SAFETY: 確保異常時 rollback
                    session.rollback()
                    raise
        
        bookings_cleanup = await asyncio.to_thread(query_bookings_cleanup)
        
        for booking in bookings_cleanup:
            try:
                # 刪除文字頻道
                text_channel = guild.get_channel(int(booking.discordTextChannelId))
                if text_channel:
                    try:
                        await text_channel.delete()
                    except Exception as e:
                        print(f"⚠️ 刪除文字頻道失敗: {e}")
                
                # 更新資料庫
                # 更新資料庫（在線程中執行）
                async def update_text_channel_cleaned(booking_id):
                    def update():
                        with Session() as s:
                            try:
                                s.execute(text("""
                                    UPDATE "Booking" 
                                    SET "textChannelCleaned" = true
                                    WHERE id = :booking_id
                                """), {'booking_id': booking_id})
                                s.commit()
                            except:
                                pass
                    await asyncio.to_thread(update)
                
                await update_text_channel_cleaned(booking.id)
                
            except Exception:
                pass
        
        session.close()
        
    except Exception:
        pass

# --- 發送評價到管理員頻道 ---
async def send_rating_to_admin(record_id, rating_data, user1_id, user2_id):
    """發送評價結果到管理員頻道"""
    try:
        admin_channel = bot.get_channel(ADMIN_CHANNEL_ID)
        if not admin_channel:
            print(f"❌ 找不到管理員頻道 (ID: {ADMIN_CHANNEL_ID})")
            return
        
        # 獲取用戶資訊
        try:
            from_user = await bot.fetch_user(int(rating_data['user1']))
            from_user_display = from_user.display_name
        except:
            from_user_display = f"用戶 {rating_data['user1']}"
        
        try:
            to_user = await bot.fetch_user(int(rating_data['user2']))
            to_user_display = to_user.display_name
        except:
            to_user_display = f"用戶 {rating_data['user2']}"
        
        # 創建評價嵌入訊息
        embed = discord.Embed(
            title="⭐ 新評價回饋",
            color=0x00ff00,
            timestamp=datetime.now(timezone.utc)
        )
        
        embed.add_field(
            name="👤 評價者",
            value=from_user_display,
            inline=True
        )
        
        embed.add_field(
            name="👤 被評價者", 
            value=to_user_display,
            inline=True
        )
        
        embed.add_field(
            name="⭐ 評分",
            value="⭐" * rating_data['rating'],
            inline=True
        )
        
        # 添加身份資訊
        if 'role' in rating_data:
            role_display = "顧客" if rating_data['role'] == 'customer' else "夥伴"
            embed.add_field(
                name="👤 評價者身份",
                value=role_display,
                inline=True
            )
        
        if rating_data['comment']:
            embed.add_field(
                name="💬 留言",
                value=rating_data['comment'],
                inline=False
            )
        
        embed.add_field(
            name="📋 配對記錄ID",
            value=f"`{record_id}`",
            inline=True
        )
        
        embed.set_footer(text="PeiPlay 評價系統")
        
        await admin_channel.send(embed=embed)
        print(f"✅ 評價已發送到管理員頻道: {from_user_display} → {to_user_display} ({rating_data['rating']}⭐)")
        
    except Exception as e:
        print(f"❌ 發送評價到管理員頻道失敗: {e}")
        import traceback
        traceback.print_exc()

# --- 評分 Modal ---
# --- 新的評價系統：星星按鈕和身份選擇 ---
class RatingView(View):
    def __init__(self, record_id, user1_id, user2_id):
        super().__init__(timeout=600)  # 10分鐘超時
        self.record_id = record_id
        self.user1_id = user1_id  # 顧客 ID
        self.user2_id = user2_id  # 夥伴 ID
        self.selected_rating = 0
        self.submitted = False
    
    def get_user_role(self, user_id: str) -> str:
        """根據用戶ID自動判斷身份"""
        if str(user_id) == str(self.user1_id):
            return 'customer'  # 顧客
        elif str(user_id) == str(self.user2_id):
            return 'partner'  # 夥伴
        else:
            return None
        
    @discord.ui.button(label="☆ 1星", style=discord.ButtonStyle.secondary, row=0)
    async def star1(self, interaction: discord.Interaction, button: Button):
        await self.select_rating(interaction, 1)
    
    @discord.ui.button(label="☆ 2星", style=discord.ButtonStyle.secondary, row=0)
    async def star2(self, interaction: discord.Interaction, button: Button):
        await self.select_rating(interaction, 2)
    
    @discord.ui.button(label="☆ 3星", style=discord.ButtonStyle.secondary, row=0)
    async def star3(self, interaction: discord.Interaction, button: Button):
        await self.select_rating(interaction, 3)
    
    @discord.ui.button(label="☆ 4星", style=discord.ButtonStyle.secondary, row=0)
    async def star4(self, interaction: discord.Interaction, button: Button):
        await self.select_rating(interaction, 4)
    
    @discord.ui.button(label="☆ 5星", style=discord.ButtonStyle.secondary, row=0)
    async def star5(self, interaction: discord.Interaction, button: Button):
        await self.select_rating(interaction, 5)
    
    @discord.ui.button(label="提交評價", style=discord.ButtonStyle.success, row=1)
    async def submit_rating(self, interaction: discord.Interaction, button: Button):
        try:
            if self.submitted:
                if not interaction.response.is_done():
                    await interaction.response.send_message("❗ 已提交過評價。", ephemeral=True)
                else:
                    await interaction.followup.send("❗ 已提交過評價。", ephemeral=True)
                return
            
            if self.selected_rating == 0:
                if not interaction.response.is_done():
                    await interaction.response.send_message("❗ 請先選擇評分（點擊星星）", ephemeral=True)
                else:
                    await interaction.followup.send("❗ 請先選擇評分（點擊星星）", ephemeral=True)
                return
            
            # 根據用戶ID自動判斷身份
            user_role = self.get_user_role(str(interaction.user.id))
            if not user_role:
                if not interaction.response.is_done():
                    await interaction.response.send_message("❗ 您不是此配對的參與者，無法提交評價", ephemeral=True)
                else:
                    await interaction.followup.send("❗ 您不是此配對的參與者，無法提交評價", ephemeral=True)
                return
            
            # 打開留言輸入的 Modal
            if not interaction.response.is_done():
                await interaction.response.send_modal(RatingCommentModal(self.record_id, self.selected_rating, user_role, self.user1_id, self.user2_id))
            else:
                await interaction.followup.send("❗ 請重新點擊提交按鈕", ephemeral=True)
            self.submitted = True
        except discord.errors.NotFound:
            # Interaction 已過期，忽略錯誤
            pass
        except Exception as e:
            print(f"❌ 提交評價按鈕錯誤: {e}")
    
    async def select_rating(self, interaction: discord.Interaction, rating: int):
        try:
            self.selected_rating = rating
            # 更新按鈕樣式和 emoji
            stars = [
                (self.star1, "1"),
                (self.star2, "2"),
                (self.star3, "3"),
                (self.star4, "4"),
                (self.star5, "5")
            ]
            
            for i, (star_button, num) in enumerate(stars, 1):
                if i == rating:
                    star_button.style = discord.ButtonStyle.success
                    # 更新 label，使用 ⭐ 表示已選擇
                    star_button.label = f"⭐ {num}星"
                else:
                    star_button.style = discord.ButtonStyle.secondary
                    # 更新 label，使用 ☆ 表示未選擇
                    star_button.label = f"☆ {num}星"
            
            if not interaction.response.is_done():
                await interaction.response.edit_message(view=self)
                await interaction.followup.send(f"✅ 已選擇 {rating} 星評分", ephemeral=True)
            else:
                await interaction.edit_original_response(view=self)
                await interaction.followup.send(f"✅ 已選擇 {rating} 星評分", ephemeral=True)
        except discord.errors.NotFound:
            # Interaction 已過期，忽略錯誤
            pass
        except Exception as e:
            print(f"❌ 選擇評分錯誤: {e}")
    

class RatingCommentModal(Modal, title="匿名評分與留言"):
    def __init__(self, record_id, rating, role, user1_id, user2_id):
        super().__init__()
        self.record_id = record_id
        self.rating = rating
        self.role = role
        self.user1_id = user1_id
        self.user2_id = user2_id
        
        # 顯示已選擇的評分（只讀）
        self.rating_display = TextInput(
            label="評分",
            default=f"{'⭐' * rating} ({rating} 星)",
            style=discord.TextStyle.short,
            required=False,
            max_length=20
        )
        self.rating_display.disabled = True  # 設為只讀
        self.add_item(self.rating_display)
        
        # 顯示已選擇的身份（只讀）
        role_display_text = "顧客" if role == 'customer' else "夥伴"
        self.role_display = TextInput(
            label="身份",
            default=role_display_text,
            style=discord.TextStyle.short,
            required=False,
            max_length=10
        )
        self.role_display.disabled = True  # 設為只讀
        self.add_item(self.role_display)
        
        # 留言輸入框
        self.comment = TextInput(
            label="留下你的留言（選填）",
            required=False,
            style=discord.TextStyle.paragraph,
            placeholder="可以留下您的意見或建議...",
            max_length=4000
        )
        self.add_item(self.comment)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            print(f"🔍 收到評價提交: record_id={self.record_id}, rating={self.rating}, role={self.role}, comment={self.comment.value}")
            
            # 使用新的 session 來避免連接問題
            with Session() as s:
                record = s.get(PairingRecord, self.record_id)
                if not record:
                    print(f"❌ 找不到配對記錄: {self.record_id}")
                    await interaction.response.send_message("❌ 找不到配對記錄", ephemeral=True)
                    return
                
                record.rating = self.rating
                record.comment = str(self.comment.value) if self.comment.value else None
                s.commit()
            
            await interaction.response.send_message("✅ 感謝你的匿名評價！", ephemeral=True)

            # 標記用戶已提交評價（統一使用字符串格式）
            if self.record_id not in rating_submitted_users:
                rating_submitted_users[self.record_id] = set()
            rating_submitted_users[self.record_id].add(str(interaction.user.id))

            if self.record_id not in pending_ratings:
                pending_ratings[self.record_id] = []
            
            comment_text = str(self.comment.value) if self.comment.value else ""
            rating_data = {
                'rating': self.rating,
                'comment': comment_text,
                'role': self.role,  # 添加身份資訊
                'user1': str(interaction.user.id),
                'user2': str(self.user2_id if str(interaction.user.id) == self.user1_id else self.user1_id)
            }
            pending_ratings[self.record_id].append(rating_data)
            print(f"✅ 評價已添加到待處理列表: {rating_data}")

            # 立即發送評價到管理員頻道
            await send_rating_to_admin(self.record_id, rating_data, self.user1_id, self.user2_id)

            evaluated_records.add(self.record_id)
            print(f"✅ 評價流程完成")
            
            # 檢查是否所有用戶都已提交評價，如果是則刪除文字頻道
            if self.record_id in rating_text_channels:
                text_channel = rating_text_channels[self.record_id]
                
                # 檢查是否所有相關用戶都已提交
                submitted_users = rating_submitted_users.get(self.record_id, set())
                
                # 檢查兩個用戶是否都已提交評價（統一使用字符串格式比較）
                user1_submitted = str(self.user1_id) in submitted_users
                user2_submitted = str(self.user2_id) in submitted_users
                
                # 檢查是否只有一個用戶（自己配對自己）
                is_single_user = str(self.user1_id) == str(self.user2_id)
                
                # 如果兩個用戶都已提交，或者只有一個用戶且已提交，則刪除頻道
                if (user1_submitted and user2_submitted) or (is_single_user and user1_submitted):
                    try:
                        if text_channel:
                            # 🔥 使用 try-except 來檢查頻道是否已刪除，而不是檢查 deleted 屬性
                            try:
                                # 嘗試訪問頻道屬性來檢查是否還存在
                                _ = text_channel.name
                                await text_channel.delete()
                                print(f"✅ 所有用戶已提交評價，已刪除文字頻道: {text_channel.name}")
                            except (discord.errors.NotFound, AttributeError):
                                # 頻道已經被刪除，靜默處理
                                pass
                            # 清理追蹤
                            rating_text_channels.pop(self.record_id, None)
                            rating_channel_created_time.pop(self.record_id, None)
                    except Exception as e:
                        print(f"❌ 刪除文字頻道失敗: {e}")
        except Exception as e:
            print(f"❌ 評分提交錯誤: {e}")
            import traceback
            traceback.print_exc()
            try:
                await interaction.response.send_message("❌ 提交失敗，請稍後再試", ephemeral=True)
            except:
                pass

# --- 延長按鈕 ---
class Extend5MinView(View):
    def __init__(self, booking_id, vc, channel_name, text_channel):
        super().__init__(timeout=300)  # 5分鐘超時
        self.booking_id = booking_id
        self.vc = vc
        # ✅ 修復：檢查 vc 是否存在再訪問 id 屬性
        self.vc_id = vc.id if vc else None
        self.channel_name = channel_name
        self.text_channel = text_channel
        self.extended = False  # 追蹤是否已延長

    @discord.ui.button(label="⏰ 延長 5 分鐘", style=discord.ButtonStyle.success, custom_id="extend_5min")
    async def extend_5_minutes(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.extended:
            await interaction.response.send_message("❌ 已經延長過了，無法再次延長！", ephemeral=True)
            return
        
        try:
            # 更新資料庫中的預約結束時間
            with Session() as s:
                # 首先檢查是否是多人陪玩（MultiPlayerBooking 表的 ID）
                multi_player_check = s.execute(text("""
                    SELECT id FROM "MultiPlayerBooking" WHERE id = :booking_id
                """), {"booking_id": self.booking_id}).fetchone()
                
                if multi_player_check:
                    # 多人陪玩：直接更新 MultiPlayerBooking 表的 endTime
                    s.execute(text("""
                        UPDATE "MultiPlayerBooking" 
                        SET "endTime" = "endTime" + INTERVAL '5 minutes'
                        WHERE id = :booking_id
                    """), {"booking_id": self.booking_id})
                    print(f"✅ 已延長多人陪玩 {self.booking_id} 的結束時間 5 分鐘")
                else:
                    # 檢查是否是群組預約（GroupBooking 表的 ID）
                    group_booking_check = s.execute(text("""
                        SELECT id FROM "GroupBooking" WHERE id = :booking_id
                    """), {"booking_id": self.booking_id}).fetchone()
                    
                    if group_booking_check:
                        # 群組預約：更新 GroupBooking 表的 endTime
                        s.execute(text("""
                            UPDATE "GroupBooking" 
                            SET "endTime" = "endTime" + INTERVAL '5 minutes'
                            WHERE id = :booking_id
                        """), {"booking_id": self.booking_id})
                        print(f"✅ 已延長群組預約 {self.booking_id} 的結束時間 5 分鐘")
                    else:
                        # 單人預約：更新 Schedule 表的 endTime（通過 Booking 表找到 Schedule）
                        booking_info = s.execute(text("""
                            SELECT "scheduleId" FROM "Booking" WHERE id = :booking_id
                        """), {"booking_id": self.booking_id}).fetchone()
                        
                        if booking_info:
                            s.execute(text("""
                                UPDATE "Schedule" 
                                SET "endTime" = "endTime" + INTERVAL '5 minutes'
                                WHERE id = :schedule_id
                            """), {"schedule_id": booking_info[0]})
                            print(f"✅ 已延長單人預約 {self.booking_id} 的結束時間 5 分鐘")
                        else:
                            # 如果都找不到，嘗試直接更新 Schedule（向後兼容）
                            s.execute(text("""
                                UPDATE "Schedule" 
                                SET "endTime" = "endTime" + INTERVAL '5 minutes'
                                WHERE id = (
                                    SELECT "scheduleId" FROM "Booking" WHERE id = :booking_id
                                )
                            """), {"booking_id": self.booking_id})
                            print(f"⚠️ 未找到 booking 信息，使用預設方式延長 {self.booking_id}")
                
                s.commit()
            
            # 標記為已延長
            self.extended = True
            
            # 更新 active_voice_channels 中的剩餘時間（延長5分鐘 = 300秒）
            if hasattr(self, 'vc_id') and self.vc_id in active_voice_channels:
                active_voice_channels[self.vc_id]['remaining'] += 300  # 延長5分鐘
                active_voice_channels[self.vc_id]['extended'] += 1
                # print(f"✅ 已更新 active_voice_channels 中的頻道 {self.vc_id}，延長5分鐘")
            
            # 更新按鈕狀態
            button.label = "✅ 已延長 5 分鐘"
            button.style = discord.ButtonStyle.secondary
            button.disabled = True
            
            await interaction.response.edit_message(view=self)
            
            # 發送確認訊息
            await interaction.followup.send(
                "✅ **預約時間已延長 5 分鐘！**\n"
                "新的結束時間已更新，語音頻道和文字頻道將多留存 5 分鐘。",
                ephemeral=False
            )
            
            print(f"✅ 預約 {self.booking_id} 已延長 5 分鐘")
            
            # 重新啟動倒數計時，但這次是延長後的時間
            bot.loop.create_task(
                countdown_with_rating_extended(
                    self.vc.id, self.channel_name, self.text_channel, 
                    self.vc, None, [], None, self.booking_id
                )
            )
            
        except Exception as e:
            print(f"❌ 延長預約時間失敗: {e}")
            await interaction.response.send_message("❌ 延長時間時發生錯誤，請稍後再試", ephemeral=True)

class BookingRatingView(View):
    def __init__(self, booking_id):
        super().__init__(timeout=600)  # 10 分鐘超時
        self.booking_id = booking_id
        self.ratings = {}  # 儲存用戶的評分
        self.submitted_users = set()  # 儲存已提交評價的用戶

    @discord.ui.button(label="⭐ 1星", style=discord.ButtonStyle.secondary, custom_id="rating_1")
    async def rate_1_star(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.handle_rating(interaction, 1)

    @discord.ui.button(label="⭐⭐ 2星", style=discord.ButtonStyle.secondary, custom_id="rating_2")
    async def rate_2_star(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.handle_rating(interaction, 2)

    @discord.ui.button(label="⭐⭐⭐ 3星", style=discord.ButtonStyle.secondary, custom_id="rating_3")
    async def rate_3_star(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.handle_rating(interaction, 3)

    @discord.ui.button(label="⭐⭐⭐⭐ 4星", style=discord.ButtonStyle.secondary, custom_id="rating_4")
    async def rate_4_star(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.handle_rating(interaction, 4)

    @discord.ui.button(label="⭐⭐⭐⭐⭐ 5星", style=discord.ButtonStyle.secondary, custom_id="rating_5")
    async def rate_5_star(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.handle_rating(interaction, 5)

    async def handle_rating(self, interaction: discord.Interaction, rating: int):
        user_id = interaction.user.id
        user_discord = interaction.user.name
        
        # 🔥 檢查是否為群組預約，如果是，檢查用戶是否是夥伴
        try:
            with Session() as s:
                # 檢查是否為群組預約
                group_booking_check = s.execute(text("""
                    SELECT id, "initiatorId", "initiatorType" 
                    FROM "GroupBooking" 
                    WHERE id = :booking_id
                """), {"booking_id": self.booking_id}).fetchone()
                
                if group_booking_check:
                    # 這是群組預約，檢查用戶是否是夥伴
                    # 查詢該群組預約的所有夥伴 Discord ID
                    partner_result = s.execute(text("""
                        SELECT DISTINCT pu.discord as partner_discord
                        FROM "GroupBooking" gb
                        JOIN "GroupBookingParticipant" gbp ON gbp."groupBookingId" = gb.id
                        JOIN "Partner" p ON p.id = gbp."partnerId"
                        JOIN "User" pu ON pu.id = p."userId"
                        WHERE gb.id = :group_booking_id
                        AND gbp."partnerId" IS NOT NULL
                    """), {"group_booking_id": self.booking_id}).fetchall()
                    
                    # 檢查發起者是否為夥伴
                    initiator_id = group_booking_check[1]
                    initiator_type = group_booking_check[2]
                    
                    if initiator_type == 'PARTNER':
                        # 查詢發起者夥伴的 Discord ID
                        initiator_partner_result = s.execute(text("""
                            SELECT pu.discord as partner_discord
                            FROM "Partner" p
                            JOIN "User" pu ON pu.id = p."userId"
                            WHERE p.id = :initiator_id
                        """), {"initiator_id": initiator_id}).fetchone()
                        
                        if initiator_partner_result:
                            partner_discords = [row.partner_discord for row in partner_result if row.partner_discord]
                            partner_discords.append(initiator_partner_result[0])
                    else:
                        partner_discords = [row.partner_discord for row in partner_result if row.partner_discord]
                    
                    # 檢查當前用戶是否是夥伴
                    user_discord_lower = user_discord.lower().strip()
                    for partner_discord in partner_discords:
                        if partner_discord:
                            partner_discord_lower = partner_discord.lower().strip()
                            # 支持多種匹配方式（與 find_member_by_discord_name 邏輯一致）
                            if (user_discord_lower == partner_discord_lower or
                                user_discord_lower.startswith(partner_discord_lower) or
                                partner_discord_lower.startswith(user_discord_lower) or
                                str(user_id) == partner_discord or
                                partner_discord == str(user_id)):
                                await interaction.response.send_message(
                                    "❌ 夥伴不需要進行評價。評價系統僅供顧客使用。",
                                    ephemeral=True
                                )
                                print(f"⚠️ 夥伴 {user_discord} 嘗試使用評價系統，已拒絕")
                                return
        except Exception as e:
            print(f"⚠️ 檢查用戶是否為夥伴時發生錯誤: {e}")
            # 如果檢查失敗，繼續執行（不阻擋評價）
        
        self.ratings[user_id] = rating
        
        # 直接彈出包含星等和評論的模態對話框
        modal = BookingRatingModal(rating, self.booking_id, self)
        await interaction.response.send_modal(modal)


class BookingRatingModal(discord.ui.Modal):
    def __init__(self, rating: int, booking_id: str, parent_view):
        super().__init__(title="提交評價")
        self.rating = rating
        self.booking_id = booking_id
        self.parent_view = parent_view
        
        # 星等顯示
        self.rating_display = discord.ui.TextInput(
            label="評分",
            default=f"{'⭐' * rating} ({rating} 星)",
            style=discord.TextStyle.short,
            required=False,
            max_length=20
        )
        self.rating_display.disabled = True  # 設為只讀
        self.add_item(self.rating_display)
        
        # 評論輸入
        self.comment_input = discord.ui.TextInput(
            label="評論內容",
            placeholder="請輸入您對這次遊戲體驗的評論...",
            style=discord.TextStyle.paragraph,
            required=False,
            max_length=500
        )
        self.add_item(self.comment_input)

    async def on_submit(self, interaction: discord.Interaction):
        comment = self.comment_input.value or "無評論"
        
        # 獲取顧客和夥伴信息
        try:
            # 重試機制處理資料庫連接問題
            max_retries = 3
            result = None
            is_group_booking = False
            
            for attempt in range(max_retries):
                try:
                    with Session() as s:
                        # 首先嘗試查詢一般預約（Booking 表）
                        result = s.execute(text("""
                            SELECT 
                                c.name as customer_name, p.name as partner_name,
                                cu.discord as customer_discord, pu.discord as partner_discord
                            FROM "Booking" b
                            JOIN "Schedule" s ON s.id = b."scheduleId"
                            JOIN "Customer" c ON c.id = b."customerId"
                            JOIN "User" cu ON cu.id = c."userId"
                            JOIN "Partner" p ON p.id = s."partnerId"
                            JOIN "User" pu ON pu.id = p."userId"
                            WHERE b.id = :booking_id
                        """), {"booking_id": self.booking_id}).fetchone()
                        
                        # 如果找不到一般預約，嘗試查詢群組預約或多人陪玩
                        if not result:
                            # ✅ 檢查是否為群組預約或多人陪玩
                            group_booking_check = s.execute(text("""
                                SELECT id FROM "GroupBooking" WHERE id = :booking_id
                            """), {"booking_id": self.booking_id}).fetchone()
                            
                            multi_player_check = s.execute(text("""
                                SELECT id FROM "MultiPlayerBooking" WHERE id = :booking_id
                            """), {"booking_id": self.booking_id}).fetchone()
                            
                            is_multiplayer = bool(multi_player_check and not group_booking_check)
                            
                            if group_booking_check or multi_player_check:
                                is_group_booking = True
                                # 對於群組預約，使用 GroupBookingReview 的邏輯
                                # ✅ 修正用戶查找：使用 normalize_discord_username 標準化 Discord 用戶名
                                normalized_discord_name = normalize_discord_username(interaction.user.name)
                                discord_id_str = str(interaction.user.id)
                                
                                # 獲取用戶的 Customer ID
                                # ✅ 使用改進的用戶查找邏輯（支持多種匹配方式）
                                user_result = s.execute(text("""
                                    SELECT c.id FROM "Customer" c
                                    JOIN "User" u ON u.id = c."userId"
                                    WHERE u.discord = :discord_name 
                                       OR u.discord = :normalized_name 
                                       OR u.discord = :discord_id
                                       OR LOWER(TRIM(u.discord)) = LOWER(TRIM(:discord_name))
                                       OR LOWER(TRIM(u.discord)) = LOWER(TRIM(:normalized_name))
                                """), {
                                    "discord_name": interaction.user.name,
                                    "normalized_name": normalized_discord_name,
                                    "discord_id": discord_id_str
                                }).fetchone()
                                
                                # 如果第一次查詢失敗，嘗試使用 Discord global_name
                                if not user_result:
                                    global_name = getattr(interaction.user, 'global_name', None)
                                    if global_name:
                                        user_result = s.execute(text("""
                                            SELECT c.id FROM "Customer" c
                                            JOIN "User" u ON u.id = c."userId"
                                            WHERE u.discord = :global_name 
                                               OR LOWER(TRIM(u.discord)) = LOWER(TRIM(:global_name))
                                        """), {
                                            "global_name": global_name
                                        }).fetchone()
                                
                                if not user_result:
                                    # 如果找不到顧客記錄，嘗試使用 Discord ID 查找
                                    user_info = s.execute(text("""
                                        SELECT id FROM "User"
                                        WHERE discord = :discord_name OR discord = :normalized_name OR discord = :discord_id
                                    """), {
                                        "discord_name": interaction.user.name,
                                        "normalized_name": normalized_discord_name,
                                        "discord_id": discord_id_str
                                    }).fetchone()
                                    
                                    if user_info:
                                        user_id = user_info[0]
                                        user_result = s.execute(text("""
                                            SELECT id FROM "Customer" WHERE "userId" = :user_id
                                        """), {"user_id": user_id}).fetchone()
                                
                                if not user_result:
                                    await interaction.response.send_message("❌ 找不到您的用戶記錄，請聯繫管理員", ephemeral=True)
                                    return
                                
                                reviewer_id = user_result[0]
                                
                                # 檢查是否已經評價過
                                existing_review = s.execute(text("""
                                    SELECT id FROM "GroupBookingReview" 
                                    WHERE "groupBookingId" = :group_id AND "reviewerId" = :reviewer_id
                                """), {
                                    'group_id': self.booking_id,
                                    'reviewer_id': reviewer_id
                                }).fetchone()
                                
                                if existing_review:
                                    await interaction.response.send_message("❌ 此群組預約已經評價過了。", ephemeral=True)
                                    return
                                
                                # 創建群組預約評價記錄
                                import uuid
                                review_id = f"gbr_{uuid.uuid4().hex[:12]}"
                                
                                s.execute(text("""
                                    INSERT INTO "GroupBookingReview" (id, "groupBookingId", "reviewerId", rating, comment, "createdAt")
                                    VALUES (:id, :group_id, :reviewer_id, :rating, :comment, :created_at)
                                """), {
                                    "id": review_id,
                                    "group_id": self.booking_id,
                                    "reviewer_id": reviewer_id,
                                    "rating": self.rating,
                                    "comment": comment,
                                    "created_at": datetime.now(timezone.utc)
                                })
                                s.commit()
                                
                                # ✅ 發送到管理員頻道：區分群組預約和多人陪玩
                                # ✅ 多人陪玩顧客對多人陪玩的評價是對的，但本身本來就不需要分別對每一位夥伴評價，所以管理員頻道不需要回饋顧客對每一位或夥伴的評價
                                if is_multiplayer:
                                    # ✅ 多人陪玩：使用「多人陪玩」類型，只發送一個整體評價回饋（不對每一位夥伴發送）
                                    await send_unified_rating_feedback(self.booking_id, "多人陪玩", self.rating, comment, interaction.user.name)
                                else:
                                    # 群組預約：使用「群組預約」類型
                                    await send_group_rating_to_admin(self.booking_id, self.rating, comment, interaction.user.name)
                                
                                # 標記用戶已提交評價
                                self.parent_view.submitted_users.add(interaction.user.id)
                                
                                # 確認收到評價
                                await interaction.response.send_message(
                                    f"✅ 感謝您的評價！\n"
                                    f"評分：{'⭐' * self.rating}\n"
                                    f"評論：{comment}",
                                    ephemeral=True
                                )
                                return
                        
                        break  # 成功則跳出重試循環
                except Exception as db_error:
                    print(f"❌ 資料庫查詢失敗 (嘗試 {attempt + 1}/{max_retries}): {db_error}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(1)  # 等待1秒後重試
                        continue
                    else:
                        raise db_error  # 最後一次嘗試失敗，拋出錯誤
                
            if result:
                # 保存評價到資料庫 Review 表
                try:
                    with Session() as s:
                        # 根據提交評價的 Discord 用戶名，判斷是顧客還是夥伴
                        reviewer_discord_name = interaction.user.name
                        # 標準化用戶名（去除尾隨空格和下劃線）
                        reviewer_discord_name_normalized = normalize_discord_username(reviewer_discord_name)
                        reviewer_user_id = None
                        reviewee_user_id = None
                        reviewer_name = None
                        reviewee_name = None
                        
                        # 獲取 customer 和 partner 的 userId 和 Discord 名稱
                        user_result = s.execute(text("""
                            SELECT 
                                cu.id as customer_user_id, pu.id as partner_user_id,
                                cu.discord as customer_discord, pu.discord as partner_discord
                            FROM "Booking" b
                            JOIN "Schedule" s ON s.id = b."scheduleId"
                            JOIN "Customer" c ON c.id = b."customerId"
                            JOIN "User" cu ON cu.id = c."userId"
                            JOIN "Partner" p ON p.id = s."partnerId"
                            JOIN "User" pu ON pu.id = p."userId"
                            WHERE b.id = :booking_id
                        """), {"booking_id": self.booking_id}).fetchone()
                        
                        if user_result:
                            customer_user_id = user_result[0]
                            partner_user_id = user_result[1]
                            customer_discord = user_result[2]
                            partner_discord = user_result[3]
                            
                            # 標準化資料庫中的 Discord 用戶名
                            customer_discord_normalized = normalize_discord_username(customer_discord) if customer_discord else ""
                            partner_discord_normalized = normalize_discord_username(partner_discord) if partner_discord else ""
                            
                            # 判斷提交評價的用戶是顧客還是夥伴（使用標準化後的用戶名進行比較）
                            if customer_discord_normalized and reviewer_discord_name_normalized.lower() == customer_discord_normalized.lower():
                                # 提交評價的是顧客，評價夥伴
                                reviewer_user_id = customer_user_id
                                reviewee_user_id = partner_user_id
                                reviewer_name = result.customer_name
                                reviewee_name = result.partner_name
                            elif partner_discord_normalized and reviewer_discord_name_normalized.lower() == partner_discord_normalized.lower():
                                # 提交評價的是夥伴，評價顧客
                                reviewer_user_id = partner_user_id
                                reviewee_user_id = customer_user_id
                                reviewer_name = result.partner_name
                                reviewee_name = result.customer_name
                            else:
                                # 找不到對應的用戶，拒絕評價
                                print(f"❌ 用戶 {reviewer_discord_name} (標準化後: {reviewer_discord_name_normalized}) 不是此預約的顧客或夥伴，拒絕評價")
                                print(f"   顧客 Discord: {customer_discord} (標準化後: {customer_discord_normalized})")
                                print(f"   夥伴 Discord: {partner_discord} (標準化後: {partner_discord_normalized})")
                                await interaction.response.send_message(
                                    "❌ 您不是此預約的顧客或夥伴，無法提交評價。",
                                    ephemeral=True
                                )
                                return
                            
                            if reviewer_user_id and reviewee_user_id:
                                # 檢查是否已經評價過
                                existing_review = s.execute(text("""
                                    SELECT id FROM "Review" 
                                    WHERE "bookingId" = :booking_id AND "reviewerId" = :reviewer_id
                                """), {
                                    "booking_id": self.booking_id,
                                    "reviewer_id": reviewer_user_id
                                }).fetchone()
                                
                                if not existing_review:
                                    # 創建評價記錄
                                    review_id = f"rev_{int(time.time())}_{reviewer_user_id}"
                                    s.execute(text("""
                                        INSERT INTO "Review" (id, "bookingId", "reviewerId", "revieweeId", rating, comment, "createdAt", "isApproved")
                                        VALUES (:id, :booking_id, :reviewer_id, :reviewee_id, :rating, :comment, :created_at, true)
                                    """), {
                                        "id": review_id,
                                        "booking_id": self.booking_id,
                                        "reviewer_id": reviewer_user_id,
                                        "reviewee_id": reviewee_user_id,
                                        "rating": self.rating,
                                        "comment": comment,
                                        "created_at": datetime.now(timezone.utc)
                                    })
                                    s.commit()
                                    print(f"✅ 評價已保存到資料庫: {reviewer_name} → {reviewee_name} ({self.rating}⭐)")
                                else:
                                    print(f"⚠️ 評價已存在，跳過保存: {self.booking_id}")
                            else:
                                print(f"❌ 無法確定評價者和被評價者: {self.booking_id}")
                except Exception as db_error:
                    print(f"❌ 保存評價到資料庫失敗: {db_error}")
                    import traceback
                    traceback.print_exc()
                
                # 標記用戶已提交評價
                self.parent_view.submitted_users.add(interaction.user.id)
                
                # 確認收到評價（不立即發送到管理員頻道，由 submit_auto_rating 統一處理）
                await interaction.response.send_message(
                    f"✅ 感謝您的評價！\n"
                    f"評分：{'⭐' * self.rating}\n"
                    f"評論：{comment}",
                    ephemeral=True
                )
            else:
                await interaction.response.send_message("❌ 找不到對應的預約記錄", ephemeral=True)
        except Exception as e:
            print(f"❌ 處理評價提交失敗: {e}")
            await interaction.response.send_message("❌ 處理評價時發生錯誤，請稍後再試", ephemeral=True)


class ExtendView(View):
    def __init__(self, vc_id):
        super().__init__(timeout=None)
        self.vc_id = vc_id

    @discord.ui.button(label="🔁 延長 5 分鐘", style=discord.ButtonStyle.primary)
    async def extend_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.vc_id not in active_voice_channels:
            await interaction.response.send_message("❗ 頻道資訊不存在或已刪除。", ephemeral=True)
            return
        active_voice_channels[self.vc_id]['remaining'] += 300
        active_voice_channels[self.vc_id]['extended'] += 1
        await interaction.response.send_message("⏳ 已延長 5 分鐘。", ephemeral=True)

# --- Bot 啟動 ---
@bot.event
async def cleanup_duplicate_channels():
    """清理重複的頻道"""
    try:
        guild = bot.get_guild(GUILD_ID)
        if not guild:
            return
        
        # 獲取所有文字頻道
        text_channels = [ch for ch in guild.channels if isinstance(ch, discord.TextChannel)]
        
        # 統計頻道名稱
        channel_names = {}
        for channel in text_channels:
            name = channel.name
            if name not in channel_names:
                channel_names[name] = []
            channel_names[name].append(channel)
        
        # 找出重複的頻道
        duplicate_channels = []
        for name, channels in channel_names.items():
            if len(channels) > 1:
                # 保留第一個，刪除其他的
                for channel in channels[1:]:
                    duplicate_channels.append(channel)
        
        # 刪除重複頻道
        for channel in duplicate_channels:
            try:
                await channel.delete()
            except Exception:
                pass
            
    except Exception:
        pass

@bot.event
async def on_ready():
    print(f"✅ Bot 已上線：{bot.user}")
    try:
        guild = discord.Object(id=GUILD_ID)
        synced = await bot.tree.sync(guild=guild)
        print(f"✅ 已同步 {len(synced)} 個指令")
        
        # 清理重複頻道
        await cleanup_duplicate_channels()
        
        # 啟動自動檢查任務（檢查是否已在運行，避免重複啟動）
        if not check_group_and_multiplayer_text_channels.is_running():
            check_group_and_multiplayer_text_channels.start()
        if not check_bookings.is_running():
            check_bookings.start()
        if not check_new_bookings.is_running():
            check_new_bookings.start()
        if not check_instant_bookings_for_text_channel.is_running():
            check_instant_bookings_for_text_channel.start()
        # ⚠️ 已停用：check_regular_bookings_for_text_channel 會創建文字頻道但沒有倒數計時和評價系統
        # check_regular_bookings_for_text_channel.start()
        if not check_instant_booking_timing.is_running():
            check_instant_booking_timing.start()
        if not cleanup_expired_channels.is_running():
            cleanup_expired_channels.start()
        if not auto_close_available_now.is_running():
            auto_close_available_now.start()
        if not check_booking_timeouts.is_running():
            check_booking_timeouts.start()
        if not check_missing_ratings.is_running():
            check_missing_ratings.start()
        
        # 啟動自動取消多人陪玩訂單任務（每60秒檢查一次）
        if not auto_cancel_multiplayer_bookings.is_running():
            auto_cancel_multiplayer_bookings.start()
    except Exception as e:
        print(f"❌ 啟動錯誤: {e}")

@bot.event
async def on_message(message):
    if message.author == bot.user:
        return
    
    # 評價系統現在使用按鈕和模態對話框，不需要處理文字訊息
    
    if message.content == "!ping":
        await message.channel.send("Pong!")
    await bot.process_commands(message)

@bot.event
async def on_voice_state_update(member, before, after):
    """監聽語音狀態更新，當用戶加入特定頻道時自動創建臨時語音頻道"""
    # 手動創建語音頻道的頻道ID列表
    MANUAL_CREATE_CHANNEL_IDS = [976829566490386505, 1443447481022025739]
    
    # 檢查用戶是否加入了指定的頻道
    if after.channel and after.channel.id in MANUAL_CREATE_CHANNEL_IDS:
        try:
            guild = after.channel.guild
            
            # 獲取或創建分類
            category = after.channel.category
            if not category:
                category = discord.utils.get(guild.categories, name="語音頻道")
            if not category:
                category = discord.utils.get(guild.categories, name="Voice Channels")
            
            # 創建臨時語音頻道
            channel_name = f"{member.display_name} 的頻道"
            
            # 設置權限：只有創建者和 @everyone 可以看到
            overwrites = {
                guild.default_role: discord.PermissionOverwrite(view_channel=False),
                member: discord.PermissionOverwrite(view_channel=True, connect=True, manage_channels=True),
            }
            
            # 創建語音頻道
            new_channel = await guild.create_voice_channel(
                name=channel_name,
                category=category,
                overwrites=overwrites,
                user_limit=0  # 無限制人數
            )
            
            # 移動用戶到新創建的頻道
            try:
                await member.move_to(new_channel)
                print(f"✅ 已為 {member.display_name} 創建臨時語音頻道: {channel_name}")
            except Exception as e:
                print(f"⚠️ 移動用戶到新頻道失敗: {e}")
                # 即使移動失敗，頻道也已創建，用戶可以手動加入
                
        except Exception as e:
            print(f"❌ 創建臨時語音頻道失敗: {e}")
            import traceback
            traceback.print_exc()


# --- 倒數邏輯 ---
async def countdown_with_rating(vc_id, channel_name, text_channel, vc, mentioned, members, record_id, booking_id):
    """倒數計時函數，包含評價系統（與群組預約邏輯一致）"""
    try:
        # 🔥 如果 text_channel 為 None，從資料庫讀取文字頻道 ID
        if not text_channel:
            with Session() as s:
                result = s.execute(text("""
                    SELECT "discordTextChannelId" 
                    FROM "Booking" 
                    WHERE id = :booking_id
                """), {"booking_id": booking_id}).fetchone()
                
                if result and result[0]:
                    guild = bot.get_guild(GUILD_ID)
                    if guild:
                        text_channel = guild.get_channel(int(result[0]))
                        if text_channel:
                            print(f"✅ 從資料庫讀取文字頻道: {text_channel.name} (預約 {booking_id})")
                        else:
                            print(f"⚠️ 無法找到文字頻道 ID {result[0]} (預約 {booking_id})")
                else:
                    print(f"⚠️ 預約 {booking_id} 沒有文字頻道 ID，無法啟動倒數計時")
                    return
        
        # 計算預約結束時間
        now = datetime.now(timezone.utc)
        
        # 從資料庫獲取預約開始和結束時間（用於計算總時長）
        with Session() as s:
            result = s.execute(text("""
                SELECT s."startTime", s."endTime" 
                FROM "Booking" b
                JOIN "Schedule" s ON s.id = b."scheduleId"
                WHERE b.id = :booking_id
            """), {"booking_id": booking_id}).fetchone()
            
            if not result:
                print(f"❌ 找不到預約 {booking_id} 的結束時間")
                return
                
            start_time = result[0]
            end_time = result[1]
            
            # 處理時區：確保時間有時區信息
            if start_time.tzinfo is None:
                start_time = start_time.replace(tzinfo=timezone.utc)
            if end_time.tzinfo is None:
                end_time = end_time.replace(tzinfo=timezone.utc)
        
        # 計算預約總時長（秒）
        total_duration_seconds = int((end_time - start_time).total_seconds())
        total_duration_minutes = total_duration_seconds / 60
        
        # 計算剩餘時間
        remaining_seconds = int((end_time - now).total_seconds())
        
        # 🔥 只在第一次啟動時輸出日誌，避免重複輸出
        # 使用函數屬性來追蹤已啟動的倒計時
        if not hasattr(countdown_with_rating, '_started_bookings'):
            countdown_with_rating._started_bookings = set()
        
        if booking_id not in countdown_with_rating._started_bookings:
            countdown_with_rating._started_bookings.add(booking_id)
            print(f"🔍 預約倒數計時開始: {booking_id} (總時長: {total_duration_minutes:.1f} 分鐘, 剩餘: {remaining_seconds / 60:.1f} 分鐘)")
        
        if remaining_seconds <= 0:
            print(f"⏰ 預約 {booking_id} 已結束")
            # 直接跳到評價系統
        else:
            # 🔥 發送倒數提醒（與群組預約邏輯一致）
            # 10分鐘提醒：只有在總時長超過10分鐘，且剩餘時間超過10分鐘時才發送
            if total_duration_seconds > 600 and remaining_seconds > 600:  # 總時長和剩餘時間都超過10分鐘
                # 等待到結束前10分鐘
                await asyncio.sleep(remaining_seconds - 600)
                
                # 發送10分鐘提醒
                embed = discord.Embed(
                    title="⏰ 預約提醒",
                    description="預約還有 10 分鐘結束，請準備結束遊戲。",
                    color=0xff9900
                )
                await text_channel.send(embed=embed)
                print(f"✅ 已發送預約10分鐘提醒: {booking_id}")
                
                # 等待剩餘的10分鐘
                remaining_seconds = 600
            
            # 5分鐘提醒：只有在總時長超過5分鐘，且剩餘時間超過5分鐘時才發送
            if total_duration_seconds > 300 and remaining_seconds > 300:  # 總時長和剩餘時間都超過5分鐘
                # 等待到結束前5分鐘
                await asyncio.sleep(remaining_seconds - 300)
                
                # 發送5分鐘提醒和延長按鈕
                await send_5min_reminder(text_channel, booking_id, vc, channel_name)
                print(f"✅ 已發送預約5分鐘提醒: {booking_id}")
                
                # 等待剩餘的5分鐘
                remaining_seconds = 300
            
            # 1分鐘提醒：只有在總時長超過1分鐘，且剩餘時間超過1分鐘時才發送
            if total_duration_seconds > 60 and remaining_seconds > 60:  # 總時長和剩餘時間都超過1分鐘
                # 等待到結束前1分鐘
                await asyncio.sleep(remaining_seconds - 60)
                
                # 發送1分鐘提醒
                await text_channel.send("⏰ 預約還有 1 分鐘結束！")
                print(f"✅ 已發送預約1分鐘提醒: {booking_id}")
                
                # 等待剩餘的1分鐘
                remaining_seconds = 60
            
            # 等待到結束時間
            if remaining_seconds > 0:
                await asyncio.sleep(remaining_seconds)
        
        # 預約時間結束，關閉語音頻道
        # 🔥 如果語音頻道尚未創建（vc 為 None），從資料庫讀取
        if not vc:
            with Session() as s:
                result = s.execute(text("""
                    SELECT "discordVoiceChannelId" 
                    FROM "Booking" 
                    WHERE id = :booking_id
                """), {"booking_id": booking_id}).fetchone()
                
                if result and result[0]:
                    guild = bot.get_guild(GUILD_ID)
                    if guild:
                        vc = guild.get_channel(int(result[0]))
        
        try:
            if vc:
                # 🔥 使用 try-except 來檢查頻道是否已刪除，而不是檢查 deleted 屬性
                try:
                    # 嘗試訪問頻道屬性來檢查是否還存在
                    _ = vc.name
                    await vc.delete()
                    print(f"✅ 已關閉語音頻道: {vc.name if vc else 'unknown'}")
                except (discord.errors.NotFound, AttributeError):
                    # 頻道已經被刪除，靜默處理
                    pass
            else:
                print(f"⚠️ 語音頻道已不存在或已刪除")
        except Exception as e:
            print(f"❌ 關閉語音頻道失敗: {e}")
        
        # 檢查是否已經發送過評價系統
        if booking_id not in rating_sent_bookings:
            # 在文字頻道顯示評價系統
            view = BookingRatingView(booking_id)
            await text_channel.send(
                "🎉 預約時間結束！\n"
                "請為您的遊戲夥伴評分：\n\n"
                "點擊下方按鈕選擇星等，系統會彈出評價表單讓您填寫評論。",
                view=view
            )
            # 標記為已發送評價系統
            rating_sent_bookings.add(booking_id)
            print(f"✅ 已發送評價系統: {booking_id}")
        else:
            print(f"⚠️ 預約 {booking_id} 已發送過評價系統，跳過")
        
        # 等待 10 分鐘讓用戶填寫評價
        await asyncio.sleep(600)  # 10 分鐘 = 600 秒
        
        # 10 分鐘後自動提交未完成的評價
        await submit_auto_rating(booking_id, text_channel)
        
        # 關閉文字頻道
        try:
            await text_channel.delete()
            print(f"✅ 已關閉文字頻道: {text_channel.name}")
        except Exception as e:
            print(f"❌ 關閉文字頻道失敗: {e}")
            
    except Exception as e:
        print(f"❌ countdown_with_rating 函數錯誤: {e}")

async def send_5min_reminder(text_channel, booking_id, vc, channel_name):
    """發送5分鐘提醒和延長按鈕"""
    try:
        # ✅ 檢查必要參數是否存在
        if not text_channel:
            print(f"❌ 發送5分鐘提醒失敗: text_channel 為 None (booking_id: {booking_id})")
            return
        if not vc:
            print(f"❌ 發送5分鐘提醒失敗: vc 為 None (booking_id: {booking_id})")
            return
        
        view = Extend5MinView(booking_id, vc, channel_name, text_channel)
        await text_channel.send(
            "⏰ **預約時間提醒**\n"
            "距離預約結束還有 **5 分鐘**！\n\n"
            "如果您需要更多時間，可以點擊下方按鈕延長 5 分鐘。",
            view=view
        )
        # 移除冗餘的提醒日誌
    except Exception as e:
        print(f"❌ 發送5分鐘提醒失敗: {e}")

async def submit_auto_rating(booking_id: str, text_channel):
    """10分鐘後自動提交未完成的評價（使用統一格式）"""
    try:
        # 檢查是否已經發送過評價回饋（確保每個預約只發送一條）
        with Session() as s_check:
            booking_check = s_check.execute(text("""
                SELECT "ratingCompleted" FROM "Booking" WHERE id = :booking_id
            """), {"booking_id": booking_id}).fetchone()
            
            if booking_check and booking_check[0]:
                print(f"⚠️ 預約 {booking_id} 已發送過評價回饋，跳過")
                return
        
        # 確定預約類型
        with Session() as s:
            # ✅ 修復：isInstantBooking 欄位不存在，應該從 paymentInfo JSON 中獲取
            booking_info = s.execute(text("""
                SELECT b."serviceType", b."paymentInfo"->>'isInstantBooking' as is_instant_booking, b."multiPlayerBookingId"
                FROM "Booking" b
                WHERE b.id = :booking_id
            """), {"booking_id": booking_id}).fetchone()
            
            if not booking_info:
                print(f"❌ 找不到預約 {booking_id} 的記錄")
                return
            
            service_type = booking_info[0]
            is_instant = booking_info[1] == 'true' or booking_info[1] is True
            multi_player_id = booking_info[2]
            
            # 確定預約類型和實際的預約ID
            if multi_player_id:
                booking_type = "多人陪玩"
                actual_booking_id = multi_player_id  # 使用 MultiPlayerBooking 的 ID
            elif service_type == "CHAT_ONLY":
                booking_type = "純聊天"
                actual_booking_id = booking_id
            elif is_instant:
                booking_type = "即時預約"
                actual_booking_id = booking_id
            else:
                booking_type = "一般預約"
                actual_booking_id = booking_id
        
        # 使用統一格式發送評價回饋
        await send_unified_rating_feedback(actual_booking_id, booking_type)
        
        # 標記已發送評價回饋
        with Session() as s_update:
            s_update.execute(text("""
                UPDATE "Booking" 
                SET "ratingCompleted" = true
                WHERE id = :booking_id
            """), {"booking_id": booking_id})
            s_update.commit()
        
        # 在文字頻道發送通知
        await text_channel.send(
            "⏰ 評價時間已結束，感謝您的使用！\n"
            "如果您想提供評價，請聯繫管理員。"
        )
                
    except Exception as e:
        print(f"❌ 自動提交評價失敗: {e}")
        import traceback
        traceback.print_exc()

async def countdown_with_rating_extended(vc_id, channel_name, text_channel, vc, mentioned, members, record_id, booking_id):
    """延長後的倒數計時函數，包含評價系統"""
    try:
        # 獲取 guild 對象
        guild = bot.get_guild(GUILD_ID)
        if not guild:
            print(f"❌ 找不到 Guild ID: {GUILD_ID}")
            return
        
        # 計算延長後的預約結束時間
        now = datetime.now(timezone.utc)
        
        # 從資料庫獲取延長後的預約結束時間
        with Session() as s:
            # 首先檢查是否是多人陪玩
            multi_player_result = s.execute(text("""
                SELECT "endTime" FROM "MultiPlayerBooking" WHERE id = :booking_id
            """), {"booking_id": booking_id}).fetchone()
            
            if multi_player_result:
                end_time = multi_player_result[0]
            else:
                # 檢查是否是群組預約
                group_booking_result = s.execute(text("""
                    SELECT "endTime" FROM "GroupBooking" WHERE id = :booking_id
                """), {"booking_id": booking_id}).fetchone()
                
                if group_booking_result:
                    end_time = group_booking_result[0]
                else:
                    # 單人預約：從 Booking 和 Schedule 表查詢
                    result = s.execute(text("""
                        SELECT s."endTime" 
                        FROM "Booking" b
                        JOIN "Schedule" s ON s.id = b."scheduleId"
                        WHERE b.id = :booking_id
                    """), {"booking_id": booking_id}).fetchone()
                    
                    if not result:
                        print(f"❌ 找不到預約 {booking_id} 的結束時間")
                        return
                    
                    end_time = result[0]
            
            if end_time.tzinfo is None:
                end_time = end_time.replace(tzinfo=timezone.utc)
        
        # 計算等待時間（延長後的時間）
        wait_seconds = (end_time - now).total_seconds()
        
        if wait_seconds > 0:
            # 移除冗餘的延長等待日誌
            await asyncio.sleep(wait_seconds)
        
        # 預約時間結束，關閉語音頻道
        try:
            await vc.delete()
            print(f"✅ 已關閉語音頻道: {channel_name}")
        except Exception as e:
            print(f"❌ 關閉語音頻道失敗: {e}")
        
        # 檢查是否已經發送過評價系統
        if booking_id not in rating_sent_bookings:
            # 判斷預約類型並使用對應的評價系統
            with Session() as s:
                # 檢查是否是多人陪玩
                multi_player_check = s.execute(text("""
                    SELECT id FROM "MultiPlayerBooking" WHERE id = :booking_id
                """), {"booking_id": booking_id}).fetchone()
                
                if multi_player_check:
                    # 多人陪玩：使用群組評價系統
                    # 獲取參與者列表
                    def get_multi_player_members(mpb_id):
                        with Session() as s_members:
                            result = s_members.execute(text("""
                                SELECT DISTINCT
                                    cu.discord as customer_discord,
                                    pu.discord as partner_discord
                                FROM "MultiPlayerBooking" mpb
                                JOIN "Booking" b ON b."multiPlayerBookingId" = mpb.id
                                JOIN "Customer" c ON c.id = b."customerId"
                                JOIN "User" cu ON cu.id = c."userId"
                                JOIN "Schedule" s ON s.id = b."scheduleId"
                                JOIN "Partner" p ON p.id = s."partnerId"
                                JOIN "User" pu ON pu.id = p."userId"
                                WHERE mpb.id = :mpb_id
                                AND b.status IN ('CONFIRMED', 'PARTNER_ACCEPTED')
                            """), {"mpb_id": mpb_id}).fetchall()
                            
                            members = []
                            for row in result:
                                if row.customer_discord:
                                    members.append(row.customer_discord)
                                if row.partner_discord:
                                    members.append(row.partner_discord)
                            return list(set(members))
                    
                    members = await asyncio.to_thread(get_multi_player_members, booking_id)
                    await show_group_rating_system(text_channel, booking_id, members, is_multiplayer=True)
                    rating_sent_bookings.add(booking_id)
                    print(f"✅ 已發送多人陪玩評價系統: {booking_id}, 參與人數: {len(members)}")
                else:
                    # 檢查是否是群組預約
                    group_booking_check = s.execute(text("""
                        SELECT id FROM "GroupBooking" WHERE id = :booking_id
                    """), {"booking_id": booking_id}).fetchone()
                    
                    if group_booking_check:
                        # 群組預約：使用群組評價系統
                        # 獲取參與者列表
                        def get_group_booking_members(gb_id):
                            with Session() as s_members:
                                # 查詢所有有 Booking 記錄的顧客（有付費的人）
                                customer_result = s_members.execute(text("""
                                    SELECT DISTINCT cu.discord as customer_discord
                                    FROM "GroupBooking" gb
                                    JOIN "Booking" b ON b."groupBookingId" = gb.id
                                    JOIN "Customer" c ON c.id = b."customerId"
                                    JOIN "User" cu ON cu.id = c."userId"
                                    WHERE gb.id = :gb_id
                                    AND b.status IN ('CONFIRMED', 'PARTNER_ACCEPTED', 'PAID_WAITING_PARTNER_CONFIRMATION', 'COMPLETED')
                                    AND cu.discord IS NOT NULL
                                """), {"gb_id": gb_id}).fetchall()
                                
                                # 查詢所有夥伴
                                partner_result = s_members.execute(text("""
                                    SELECT DISTINCT pu.discord as partner_discord
                                    FROM "GroupBooking" gb
                                    JOIN "GroupBookingParticipant" gbp ON gbp."groupBookingId" = gb.id
                                    JOIN "Partner" p ON p.id = gbp."partnerId"
                                    JOIN "User" pu ON pu.id = p."userId"
                                    WHERE gb.id = :gb_id
                                    AND gbp.status = 'ACTIVE'
                                    AND pu.discord IS NOT NULL
                                """), {"gb_id": gb_id}).fetchall()
                                
                                # 合併所有參與者
                                members = []
                                for row in customer_result:
                                    if row.customer_discord:
                                        members.append(row.customer_discord)
                                for row in partner_result:
                                    if row.partner_discord:
                                        members.append(row.partner_discord)
                                return list(set(members))
                        
                        # 🔥 使用與一般預約相同的評價系統
                        view = BookingRatingView(booking_id)
                        await text_channel.send(
                            "🎉 預約時間結束！\n"
                            "請為您的遊戲夥伴評分：\n\n"
                            "點擊下方按鈕選擇星等，系統會彈出評價表單讓您填寫評論。",
                            view=view
                        )
                        rating_sent_bookings.add(booking_id)
                        print(f"✅ 已發送群組預約評價系統: {booking_id}")
                    else:
                        # 單人預約：使用單人評價系統
                        view = BookingRatingView(booking_id)
                        await text_channel.send(
                            "🎉 預約時間結束！\n"
                            "請為您的遊戲夥伴評分：\n\n"
                            "點擊下方按鈕選擇星等，系統會彈出評價表單讓您填寫評論。",
                            view=view
                        )
                        rating_sent_bookings.add(booking_id)
                        print(f"✅ 已發送單人預約評價系統: {booking_id}")
        else:
            print(f"⚠️ 預約 {booking_id} 已發送過評價系統，跳過")
        
        # 等待 10 分鐘讓用戶填寫評價
        await asyncio.sleep(600)  # 10 分鐘 = 600 秒
        
        # 10 分鐘後自動提交未完成的評價（僅適用於單人預約）
        # 多人陪玩和群組預約的評價由 GroupRatingModal 處理
        with Session() as s:
            multi_player_check = s.execute(text("""
                SELECT id FROM "MultiPlayerBooking" WHERE id = :booking_id
            """), {"booking_id": booking_id}).fetchone()
            
            group_booking_check = s.execute(text("""
                SELECT id FROM "GroupBooking" WHERE id = :booking_id
            """), {"booking_id": booking_id}).fetchone()
            
            # 只有單人預約才需要自動提交評價回饋
            if not multi_player_check and not group_booking_check:
                await submit_auto_rating(booking_id, text_channel)
        
        # 關閉文字頻道
        try:
            await text_channel.delete()
            print(f"✅ 已關閉文字頻道: {text_channel.name}")
        except Exception as e:
            print(f"❌ 關閉文字頻道失敗: {e}")
            
    except Exception as e:
        print(f"❌ countdown_with_rating_extended 函數錯誤: {e}")

async def countdown(vc_id, animal_channel_name, text_channel, vc, interaction, mentioned, record_id):
    try:
        print(f"🔍 開始倒數計時: vc_id={vc_id}, record_id={record_id}")
        
        # 檢查 record_id 是否有效
        if not record_id:
            print(f"❌ 警告: record_id 為 None，評價系統可能無法正常工作")
        
        # 移動用戶到語音頻道（如果是自動創建的，mentioned 已經包含用戶）
        if mentioned:
            for user in mentioned:
                if user.voice and user.voice.channel:
                    await user.move_to(vc)

        # 注意：延長按鈕已在調用此函數之前發送，這裡不再重複發送

        while active_voice_channels[vc_id]['remaining'] > 0:
            remaining = active_voice_channels[vc_id]['remaining']
            if remaining == 60:
                await text_channel.send("⏰ 剩餘 1 分鐘。")
            await asyncio.sleep(1)
            active_voice_channels[vc_id]['remaining'] -= 1

        await vc.delete()
        print(f"🎯 語音頻道已刪除，開始評價流程: record_id={record_id}")
        
        # 在原始文字頻道顯示評價系統（不創建新頻道）
        try:
            # 檢查文字頻道是否存在
            if not text_channel:
                print(f"⚠️ 文字頻道不存在，無法顯示評價系統")
                active_voice_channels.pop(vc_id, None)
                return
            
            # 嘗試訪問頻道屬性來檢查頻道是否還存在
            try:
                _ = text_channel.name
            except (AttributeError, discord.errors.NotFound):
                print(f"⚠️ 文字頻道已刪除，無法顯示評價系統")
                active_voice_channels.pop(vc_id, None)
                return
            
            # 獲取配對記錄以取得用戶ID
            user1_id = None
            user2_id = None
            with Session() as s:
                record = s.get(PairingRecord, record_id)
                if record:
                    user1_id = record.user1Id
                    user2_id = record.user2Id
                    booking_id = record.bookingId
                    print(f"🔍 從資料庫獲取用戶ID: record_id={record_id}, user1_id={user1_id}, user2_id={user2_id}, booking_id={booking_id}")
                    
                    # 驗證用戶ID格式（應該是 Discord ID，通常是 17-19 位數字）
                    if not user1_id or not user2_id:
                        print(f"⚠️ 警告：PairingRecord {record_id} 中的用戶ID為空")
                    elif not user1_id.isdigit() or not user2_id.isdigit():
                        print(f"⚠️ 警告：PairingRecord {record_id} 中的用戶ID格式可能錯誤: user1_id={user1_id}, user2_id={user2_id}")
            
            if not user1_id or not user2_id:
                print(f"⚠️ 無法獲取用戶ID (user1_id={user1_id}, user2_id={user2_id})，使用預設值")
                # 如果無法獲取用戶ID，嘗試從 mentioned 獲取
                if mentioned and len(mentioned) >= 2:
                    user1_id = str(mentioned[0].id)
                    user2_id = str(mentioned[1].id)
                    print(f"🔍 從 mentioned 獲取用戶ID: user1_id={user1_id}, user2_id={user2_id}")
                else:
                    print(f"❌ 無法獲取用戶ID，評價系統可能無法正常工作")
                    # 即使無法獲取用戶ID，也發送評價系統（但可能無法正確識別身份）
                    user1_id = "unknown"
                    user2_id = "unknown"
            
            # 發送評價提示訊息
            embed = discord.Embed(
                title="⭐ 語音頻道已結束 - 請進行評價",
                description="感謝您使用 PeiPlay 服務！請花一點時間為您的夥伴進行匿名評價。",
                color=0xffd700
            )
            embed.add_field(
                name="📝 評價說明",
                value="• 點擊星星選擇評分（1-5 星）\n• 系統會自動識別您的身份\n• 留言為選填項目\n• 評價完全匿名\n• 評價結果會回報給管理員",
                inline=False
            )
            embed.set_footer(text="評價有助於我們提供更好的服務品質")
            
            await text_channel.send(embed=embed)
            print(f"✅ 評價提示訊息已發送到文字頻道")
            
            # 創建評價 View（包含星星按鈕和身份選擇）
            # 確保使用正確的 RatingView 類別（手動創建頻道用）
            # 創建一個局部類別來避免類別衝突
            class ManualRatingView(View):
                def __init__(self, record_id, user1_id, user2_id):
                    super().__init__(timeout=600)  # 10分鐘超時
                    self.record_id = record_id
                    self.user1_id = user1_id  # 顧客 ID
                    self.user2_id = user2_id  # 夥伴 ID
                    self.selected_rating = 0
                    self.submitted = False
                
                def get_user_role(self, user_id: str) -> str:
                    """根據用戶ID自動判斷身份"""
                    if str(user_id) == str(self.user1_id):
                        return 'customer'  # 顧客
                    elif str(user_id) == str(self.user2_id):
                        return 'partner'  # 夥伴
                    else:
                        return None
                
                @discord.ui.button(label="☆ 1星", style=discord.ButtonStyle.secondary, row=0)
                async def star1(self, interaction: discord.Interaction, button: Button):
                    await self.select_rating(interaction, 1)
                
                @discord.ui.button(label="☆ 2星", style=discord.ButtonStyle.secondary, row=0)
                async def star2(self, interaction: discord.Interaction, button: Button):
                    await self.select_rating(interaction, 2)
                
                @discord.ui.button(label="☆ 3星", style=discord.ButtonStyle.secondary, row=0)
                async def star3(self, interaction: discord.Interaction, button: Button):
                    await self.select_rating(interaction, 3)
                
                @discord.ui.button(label="☆ 4星", style=discord.ButtonStyle.secondary, row=0)
                async def star4(self, interaction: discord.Interaction, button: Button):
                    await self.select_rating(interaction, 4)
                
                @discord.ui.button(label="☆ 5星", style=discord.ButtonStyle.secondary, row=0)
                async def star5(self, interaction: discord.Interaction, button: Button):
                    await self.select_rating(interaction, 5)
                
                @discord.ui.button(label="提交評價", style=discord.ButtonStyle.success, row=1)
                async def submit_rating(self, interaction: discord.Interaction, button: Button):
                    try:
                        if self.submitted:
                            if not interaction.response.is_done():
                                await interaction.response.send_message("❗ 已提交過評價。", ephemeral=True)
                            return
                        
                        if self.selected_rating == 0:
                            if not interaction.response.is_done():
                                await interaction.response.send_message("❗ 請先選擇評分（點擊星星）", ephemeral=True)
                            return
                        
                        # 根據用戶ID自動判斷身份
                        user_role = self.get_user_role(str(interaction.user.id))
                        if not user_role:
                            if not interaction.response.is_done():
                                await interaction.response.send_message("❗ 您不是此配對的參與者，無法提交評價", ephemeral=True)
                            return
                        
                        if not interaction.response.is_done():
                            await interaction.response.send_modal(RatingCommentModal(self.record_id, self.selected_rating, user_role, self.user1_id, self.user2_id))
                        self.submitted = True
                    except Exception as e:
                        print(f"❌ 提交評價按鈕錯誤: {e}")
                
                async def select_rating(self, interaction: discord.Interaction, rating: int):
                    try:
                        self.selected_rating = rating
                        stars = [
                            (self.star1, "1"),
                            (self.star2, "2"),
                            (self.star3, "3"),
                            (self.star4, "4"),
                            (self.star5, "5")
                        ]
                        
                        for i, (star_button, num) in enumerate(stars, 1):
                            if i <= rating:
                                star_button.style = discord.ButtonStyle.success
                                # 更新 label，使用 ⭐ 表示已選擇
                                star_button.label = f"⭐ {num}星"
                            else:
                                star_button.style = discord.ButtonStyle.secondary
                                # 更新 label，使用 ☆ 表示未選擇
                                star_button.label = f"☆ {num}星"
                        
                        if not interaction.response.is_done():
                            await interaction.response.edit_message(view=self)
                            await interaction.followup.send(f"✅ 已選擇 {rating} 星評分", ephemeral=True)
                    except Exception as e:
                        print(f"❌ 選擇評分錯誤: {e}")
                        import traceback
                        traceback.print_exc()
                
            
            view = ManualRatingView(record_id, user1_id, user2_id)
            print(f"🔍 創建評價 View: record_id={record_id}, user1_id={user1_id}, user2_id={user2_id}")
            print(f"🔍 View 類型: {type(view).__name__}")
            print(f"🔍 View 按鈕數量: {len(view.children)}")
            
            # 確保文字頻道存在且可發送訊息
            if text_channel:
                # 嘗試訪問頻道屬性來檢查頻道是否還存在
                try:
                    _ = text_channel.name
                except (AttributeError, discord.errors.NotFound):
                    print(f"❌ 文字頻道已刪除，無法發送評價系統")
                    return
                
                try:
                    message = await text_channel.send("📝 請使用下方按鈕進行評價：", view=view)
                    print(f"✅ 評價系統已發送到文字頻道，訊息ID: {message.id}")
                except discord.errors.Forbidden:
                    print(f"❌ 沒有權限在文字頻道發送訊息: {text_channel.name}")
                except discord.errors.NotFound:
                    print(f"❌ 文字頻道不存在: {text_channel.name}")
                except Exception as send_error:
                    print(f"❌ 發送評價系統訊息失敗: {send_error}")
                    import traceback
                    traceback.print_exc()
            else:
                print(f"❌ 文字頻道無效或已刪除，無法發送評價系統")
            
        except Exception as e:
            print(f"❌ 顯示評價系統失敗: {e}")
            import traceback
            traceback.print_exc()

        # 使用新的 session 來更新記錄
        with Session() as s:
            record = s.get(PairingRecord, record_id)
            if record:
                record.extendedTimes = active_voice_channels[vc_id]['extended']
                record.duration += record.extendedTimes * 600
                s.commit()
                
                # 獲取更新後的記錄資訊
                user1_id = record.user1Id
                user2_id = record.user2Id
                duration = record.duration
                extended_times = record.extendedTimes
                booking_id = record.bookingId
                
                print(f"🔍 PairingRecord 資訊: record_id={record_id}, user1_id={user1_id}, user2_id={user2_id}, booking_id={booking_id}")
                
                # 驗證用戶ID格式（應該是 Discord ID，通常是 17-19 位數字）
                if not user1_id or not user2_id:
                    print(f"⚠️ 警告：PairingRecord {record_id} 中的用戶ID為空")
                elif not user1_id.isdigit() or not user2_id.isdigit():
                    print(f"⚠️ 警告：PairingRecord {record_id} 中的用戶ID格式可能錯誤: user1_id={user1_id}, user2_id={user2_id}")

        # 延遲發送管理員摘要訊息（等待評價視圖超時，10分鐘後）
        async def send_admin_summary_after_timeout():
            """在評價視圖超時後發送摘要訊息"""
            await asyncio.sleep(600)  # 等待10分鐘（評價視圖超時時間）
            
            admin = bot.get_channel(ADMIN_CHANNEL_ID)
            if admin:
                try:
                    # 如果有 bookingId，從 Booking 獲取正確的 customer 和 partner Discord ID
                    final_user1_id = user1_id
                    final_user2_id = user2_id
                    
                    if booking_id:
                        # 如果是 manual_ 前綴，表示這是手動配對，沒有對應的 Booking 記錄，直接使用 PairingRecord 中的用戶ID
                        if booking_id.startswith('manual_'):
                            print(f"ℹ️ 這是手動配對記錄 (booking_id={booking_id})，直接使用 PairingRecord 中的用戶ID")
                            print(f"✅ 使用 PairingRecord 中的用戶ID: user1_id={user1_id}, user2_id={user2_id}")
                        else:
                            print(f"🔍 嘗試從 Booking 獲取用戶資訊: booking_id={booking_id}")
                            
                            with Session() as s:
                                booking_result = s.execute(text("""
                                    SELECT 
                                        c."userId" as customer_user_id,
                                        p."userId" as partner_user_id
                                    FROM "Booking" b
                                    JOIN "Customer" c ON b."customerId" = c.id
                                    JOIN "Schedule" s ON b."scheduleId" = s.id
                                    JOIN "Partner" p ON s."partnerId" = p.id
                                    WHERE b.id = :booking_id
                                """), {"booking_id": booking_id}).fetchone()
                                
                                if booking_result:
                                    customer_user_id = booking_result[0]
                                    partner_user_id = booking_result[1]
                                    print(f"✅ 找到 Booking: customer_user_id={customer_user_id}, partner_user_id={partner_user_id}")
                                    
                                    # 從 User 表獲取 Discord ID
                                    customer_discord_result = s.execute(text("""
                                        SELECT discord FROM "User" WHERE id = :user_id
                                    """), {"user_id": customer_user_id}).fetchone()
                                    
                                    partner_discord_result = s.execute(text("""
                                        SELECT discord FROM "User" WHERE id = :user_id
                                    """), {"user_id": partner_user_id}).fetchone()
                                    
                                    if customer_discord_result and customer_discord_result[0]:
                                        final_user1_id = customer_discord_result[0]
                                        print(f"✅ 更新 user1_id 為: {final_user1_id}")
                                    else:
                                        print(f"⚠️ 找不到 customer 的 Discord ID: customer_user_id={customer_user_id}")
                                    
                                    if partner_discord_result and partner_discord_result[0]:
                                        final_user2_id = partner_discord_result[0]
                                        print(f"✅ 更新 user2_id 為: {final_user2_id}")
                                    else:
                                        print(f"⚠️ 找不到 partner 的 Discord ID: partner_user_id={partner_user_id}")
                                    
                                    print(f"🔍 最終 Discord ID: user1_id={final_user1_id}, user2_id={final_user2_id}")
                                else:
                                    print(f"⚠️ 找不到 Booking 記錄 (booking_id={booking_id})，使用 PairingRecord 中的用戶ID")
                                    print(f"⚠️ PairingRecord 中的用戶ID: user1_id={user1_id}, user2_id={user2_id}")
                    
                    # 嘗試獲取用戶資訊，如果失敗則使用用戶 ID
                    # final_user1_id 是顧客，final_user2_id 是夥伴
                    try:
                        customer_user = await bot.fetch_user(int(final_user1_id))
                        customer_display = customer_user.mention
                    except:
                        customer_display = f"<@{final_user1_id}>"
                    
                    try:
                        partner_user = await bot.fetch_user(int(final_user2_id))
                        partner_display = partner_user.mention
                    except:
                        partner_display = f"<@{final_user2_id}>"
                    
                    header = f"📋 配對紀錄\n👤 顧客：{customer_display}\n👥 夥伴：{partner_display}\n⏰ 時長：{duration//60} 分鐘 | 延長 {extended_times} 次"
                    
                    if booking_id:
                        header += f"\n🆔 預約ID: {booking_id}"

                    # 檢查 pending_ratings 和資料庫中的評價
                    has_ratings = False
                    feedback = "\n⭐ 評價回饋："
                    
                    # 檢查 pending_ratings
                    if record_id in pending_ratings and pending_ratings[record_id]:
                        has_ratings = True
                        for r in pending_ratings[record_id]:
                            try:
                                from_user = await bot.fetch_user(int(r['user1']))
                                from_user_display = from_user.mention
                            except:
                                from_user_display = f"<@{r['user1']}>"
                            
                            try:
                                to_user = await bot.fetch_user(int(r['user2']))
                                to_user_display = to_user.mention
                            except:
                                to_user_display = f"<@{r['user2']}>"
                            
                            feedback += f"\n- 「{from_user_display} → {to_user_display}」：{r['rating']} ⭐"
                            if r.get('comment'):
                                feedback += f"\n  💬 {r['comment']}"
                        del pending_ratings[record_id]
                    
                    # 檢查資料庫中的評價
                    with Session() as s:
                        record = s.get(PairingRecord, record_id)
                        if record and record.rating:
                            has_ratings = True
                            # 如果資料庫有評價但 pending_ratings 沒有，也顯示
                            if record_id not in pending_ratings or not pending_ratings.get(record_id):
                                feedback += f"\n- 評價：{record.rating} ⭐"
                                if record.comment:
                                    feedback += f"\n  💬 {record.comment}"
                    
                    if has_ratings:
                        await admin.send(f"{header}{feedback}")
                    else:
                        await admin.send(f"{header}\n⭐ 沒有收到任何評價。")
                except Exception as e:
                    print(f"推送管理區評價失敗：{e}")
                    import traceback
                    traceback.print_exc()
                    # 如果完全失敗，至少顯示基本的配對資訊
                    try:
                        basic_header = f"📋 配對紀錄\n👤 顧客：<@{final_user1_id}>\n👥 夥伴：<@{final_user2_id}>\n⏰ 時長：{duration//60} 分鐘 | 延長 {extended_times} 次"
                        if booking_id:
                            basic_header += f"\n🆔 預約ID: {booking_id}"
                        await admin.send(f"{basic_header}\n⭐ 沒有收到任何評價。")
                    except:
                        pass
        
        # 啟動背景任務，在10分鐘後發送摘要
        asyncio.create_task(send_admin_summary_after_timeout())

        active_voice_channels.pop(vc_id, None)
    except Exception as e:
        print(f"❌ 倒數錯誤: {e}")

# --- 指令：/createvc ---
@bot.tree.command(name="createvc", description="建立匿名語音頻道（指定開始時間）", guild=discord.Object(id=GUILD_ID))
@app_commands.describe(members="標註的成員們", minutes="存在時間（分鐘）", start_time="幾點幾分後啟動 (格式: HH:MM, 24hr)", limit="人數上限")
async def createvc(interaction: discord.Interaction, members: str, minutes: int, start_time: str, limit: int = 2):
    await interaction.response.defer()
    try:
        hour, minute = map(int, start_time.split(":"))
        now = datetime.now(TW_TZ)
        start_dt = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if start_dt < now:
            start_dt += timedelta(days=1)
        start_dt_utc = start_dt.astimezone(timezone.utc)
    except:
        await interaction.followup.send("❗ 時間格式錯誤，請使用 HH:MM 24 小時制。")
        return

    blocked_ids = []
    try:
        with Session() as s:
            blocked_ids = [b.blocked_id for b in s.query(BlockRecord).filter(BlockRecord.blocker_id == str(interaction.user.id)).all()]
    except Exception:
        # 如果 block_records 表不存在，跳過封鎖檢查
        pass
    mentioned = [m for m in interaction.guild.members if f"<@{m.id}>" in members and str(m.id) not in blocked_ids]
    if not mentioned:
        await interaction.followup.send("❗請標註至少一位成員。")
        return
    
    # 確保不會與自己配對
    mentioned = [m for m in mentioned if m.id != interaction.user.id]
    if not mentioned:
        await interaction.followup.send("❗請標註其他成員，不能與自己配對。")
        return

    animal = random.choice(CUTE_ITEMS)
    animal_channel_name = f"{animal}頻道"
    await interaction.followup.send(f"✅ 已排程配對頻道：{animal_channel_name} 將於 <t:{int(start_dt_utc.timestamp())}:t> 開啟")

    async def countdown_wrapper():
        await asyncio.sleep((start_dt_utc - datetime.now(timezone.utc)).total_seconds())

        overwrites = {
            interaction.guild.default_role: discord.PermissionOverwrite(view_channel=False),
            interaction.user: discord.PermissionOverwrite(view_channel=True, connect=True),
        }
        for m in mentioned:
            overwrites[m] = discord.PermissionOverwrite(view_channel=True, connect=True)

        category = discord.utils.get(interaction.guild.categories, name="語音頻道")
        vc = await interaction.guild.create_voice_channel(name=animal_channel_name, overwrites=overwrites, user_limit=limit, category=category)
        text_channel = await safe_create_text_channel(interaction.guild, "🔒匿名文字區", overwrites=overwrites, category=category)

        with Session() as s:
            # 確保記錄兩個不同的用戶
            user1_id = str(interaction.user.id)
            user2_id = str(mentioned[0].id)
            
            # 添加調試信息
            print(f"🔍 創建配對記錄: {user1_id} × {user2_id}")
            
            import uuid
            record_id = str(uuid.uuid4())
            record = PairingRecord(
                id=record_id,
                user1Id=user1_id,
                user2Id=user2_id,
                duration=minutes * 60,
                animalName=animal,
                bookingId=f"manual_{record_id}"  # 手動創建的記錄使用 manual_ 前綴
            )
            s.add(record)
            s.commit()
            created_at = record.createdAt

        active_voice_channels[vc.id] = {
            'text_channel': text_channel,
            'remaining': minutes * 60,
            'extended': 0,
            'record_id': record_id,  # 使用保存的 ID
            'vc': vc
        }

        # 發送歡迎訊息和延長按鈕
        view = ExtendView(vc.id)
        await text_channel.send(f"🎉 語音頻道 {vc.name} 已開啟！\n⏳ 可延長5分鐘 ( 為了您有更好的遊戲體驗，請到最後需要時再點選 ) 。", view=view)

        await countdown(vc.id, animal_channel_name, text_channel, vc, interaction, mentioned, record_id)

    bot.loop.create_task(countdown_wrapper())

# --- 指令：/createvc-now ---
@bot.tree.command(name="createvc-now", description="立即建立匿名語音頻道（可在私人頻道使用）", guild=discord.Object(id=GUILD_ID))
@app_commands.describe(
    customer="顧客（用戶ID、用戶名或用戶標註）", 
    partner="夥伴（用戶ID、用戶名或用戶標註）", 
    minutes="存在時間（分鐘）", 
    start_time="開始時間（台灣時間，格式: HH:MM，24小時制，選填）", 
    limit="人數上限"
)
async def createvc_now(interaction: discord.Interaction, customer: str, partner: str, minutes: int, start_time: str = None, limit: int = 10):
    """立即創建語音頻道，可在任何頻道（包括私人頻道）使用
    
    支援多種輸入格式：
    - 用戶ID：123456789012345678
    - 用戶名：username 或 username#1234
    - 用戶標註：@username（如果在同一頻道）
    
    參數說明：
    - customer: 顧客（用戶ID、用戶名或用戶標註）
    - partner: 夥伴（用戶ID、用戶名或用戶標註）
    """
    # 立即回應，避免 interaction 過期
    try:
        await interaction.response.defer(ephemeral=False)
    except discord.errors.InteractionResponded:
        # 如果已經回應過，使用 followup
        pass
    except Exception as e:
        print(f"❌ defer 失敗: {e}")
        try:
            await interaction.followup.send("❌ 處理請求時發生錯誤，請稍後再試。", ephemeral=True)
        except:
            pass
        return
    
    # 獲取 guild（語音頻道必須在 guild 中創建）
    guild = bot.get_guild(GUILD_ID)
    if not guild:
        error_msg = (
            "❌ **無法創建語音頻道**\n"
            "📋 **原因**：找不到伺服器\n"
            "💡 **解決方法**：請聯繫管理員檢查 Bot 設定"
        )
        await interaction.followup.send(error_msg)
        return
    
    # 檢查使用者是否在 guild 中
    caller_member = guild.get_member(interaction.user.id)
    if not caller_member:
        error_msg = (
            "❌ **無法使用此功能**\n"
            "📋 **原因**：您必須是伺服器成員才能創建語音頻道\n"
            "💡 **解決方法**：請先加入伺服器"
        )
        await interaction.followup.send(error_msg)
        return
    
    # 驗證參數
    if minutes <= 0 or minutes > 1440:  # 最多24小時
        error_msg = (
            "❌ **參數錯誤**\n"
            "📋 **原因**：時間必須在 1-1440 分鐘之間\n"
            "💡 **提示**：請輸入有效的時間範圍（1分鐘到24小時）"
        )
        await interaction.followup.send(error_msg)
        return
    
    if limit < 2 or limit > 99:
        error_msg = (
            "❌ **參數錯誤**\n"
            "📋 **原因**：人數上限必須在 2-99 之間\n"
            "💡 **提示**：請輸入有效的人數上限"
        )
        await interaction.followup.send(error_msg)
        return
    
    try:
        # 解析被標註的成員
        blocked_ids = []
        try:
            with Session() as s:
                blocked_ids = [b.blocked_id for b in s.query(BlockRecord).filter(BlockRecord.blocker_id == str(interaction.user.id)).all()]
        except Exception:
            # 如果 block_records 表不存在，跳過封鎖檢查
            pass
        
        # 輔助函數：解析單個用戶
        def parse_user(user_input: str, role_name: str):
            """解析單個用戶輸入，返回 member 對象或 None"""
            import re
            
            # 1. 先解析 Discord 標註格式 <@123456789> 或 <@!123456789>
            discord_mentions = re.findall(r'<@!?(\d+)>', user_input)
            if discord_mentions:
                user_id = int(discord_mentions[0])
                member = guild.get_member(user_id)
                if member:
                    return member
            
            # 2. 移除已解析的 Discord 標註格式，處理剩餘文本
            remaining_text = re.sub(r'<@!?\d+>', '', user_input).strip()
            
            # 3. 檢查是否為純數字（用戶ID）
            if remaining_text.isdigit():
                user_id = int(remaining_text)
                member = guild.get_member(user_id)
                if member:
                    return member
            
            # 4. 移除 @ 符號
            part = remaining_text.lstrip('@').strip()
            if not part:
                return None
            
            # 5. 嘗試從 guild 成員中查找匹配的用戶
            # 方法1：使用 Discord 的 utils.get 方法
            try:
                member = discord.utils.get(guild.members, name=part)
                if not member:
                    member = discord.utils.get(guild.members, display_name=part)
                if not member:
                    member = discord.utils.find(lambda m: m.global_name and m.global_name.lower() == part.lower(), guild.members)
                if member:
                    return member
            except:
                pass
            
            # 方法2：手動遍歷所有成員
            for member in guild.members:
                if member.name.lower() == part.lower():
                    return member
                if member.display_name and member.display_name.lower() == part.lower():
                    return member
                if member.global_name and member.global_name.lower() == part.lower():
                    return member
                if member.discriminator and member.discriminator != '0':
                    full_name = f"{member.name}#{member.discriminator}"
                    if full_name.lower() == part.lower():
                        return member
            
            return None
        
        # 分別解析顧客和夥伴
        customer_member = parse_user(customer, "顧客")
        partner_member = parse_user(partner, "夥伴")
        
        # 驗證解析結果
        error_messages = []
        
        if not customer_member:
            error_messages.append(f"❌ **無法找到顧客**：`{customer}`")
        
        if not partner_member:
            error_messages.append(f"❌ **無法找到夥伴**：`{partner}`")
        
        if error_messages:
            help_msg = (
                "\n".join(error_messages) + "\n\n"
                "💡 **支援的格式**：\n"
                "• **用戶ID**（推薦）：`123456789012345678`\n"
                "• **用戶名**：`username`\n"
                "• **顯示名稱**：`顯示名稱`\n"
                "• **用戶標註**：`@username` 或 `@顯示名稱`\n\n"
                "**範例**：\n"
                "• `/createvc-now customer:123456789012345678 partner:987654321098765432 minutes:60`\n"
                "• `/createvc-now customer:@username1 partner:@username2 minutes:60`\n"
                "• `/createvc-now customer:username1 partner:username2 minutes:60`\n\n"
                "**提示**：在私人頻道中，建議使用用戶ID或完整的用戶名"
            )
            await interaction.followup.send(help_msg)
            return
        
        # 檢查是否是自己
        if customer_member.id == interaction.user.id or partner_member.id == interaction.user.id:
            await interaction.followup.send("❌ **無法創建頻道**\n📋 **原因**：不能邀請自己\n💡 **提示**：請指定其他成員作為顧客和夥伴")
            return
        
        # 檢查是否被封鎖
        blocked_users = []
        if str(customer_member.id) in blocked_ids:
            blocked_users.append(f"顧客：{customer_member.display_name}")
        if str(partner_member.id) in blocked_ids:
            blocked_users.append(f"夥伴：{partner_member.display_name}")
        
        if blocked_users:
            error_msg = (
                "❌ **無法創建頻道**\n"
                f"📋 **原因**：以下用戶在您的封鎖名單中：{', '.join(blocked_users)}\n"
                "💡 **解決方法**：如需邀請，請先解除封鎖"
            )
            await interaction.followup.send(error_msg)
            return
        
        # 準備 mentioned 列表（用於頻道權限和通知）
        mentioned = [customer_member, partner_member]
        
        # 處理開始時間（台灣時間）
        delay_seconds = 0
        start_dt_utc = None
        if start_time:
            try:
                hour, minute = map(int, start_time.split(":"))
                now_tw = datetime.now(TW_TZ)
                start_dt_tw = now_tw.replace(hour=hour, minute=minute, second=0, microsecond=0)
                
                # 如果時間已過，設定為明天
                if start_dt_tw < now_tw:
                    start_dt_tw += timedelta(days=1)
                
                # 轉換為 UTC
                start_dt_utc = start_dt_tw.astimezone(timezone.utc)
                delay_seconds = (start_dt_utc - datetime.now(timezone.utc)).total_seconds()
                
                if delay_seconds < 0:
                    error_msg = (
                        "❌ **時間格式錯誤**\n"
                        "📋 **原因**：開始時間必須是未來時間\n"
                        "💡 **提示**：請使用 HH:MM 格式（24小時制，台灣時間）\n"
                        "**範例**：`14:30` 表示下午2點30分"
                    )
                    await interaction.followup.send(error_msg)
                    return
            except ValueError:
                error_msg = (
                    "❌ **時間格式錯誤**\n"
                    "📋 **原因**：時間格式不正確\n"
                    "💡 **提示**：請使用 HH:MM 格式（24小時制，台灣時間）\n"
                    "**範例**：`14:30` 表示下午2點30分"
                )
                await interaction.followup.send(error_msg)
                return
        
        # 生成頻道名稱
        animal = random.choice(CUTE_ITEMS)
        animal_channel_name = f"{animal}頻道"
        
        # 設置權限
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            caller_member: discord.PermissionOverwrite(view_channel=True, connect=True, manage_channels=True),
        }
        for m in mentioned:
            overwrites[m] = discord.PermissionOverwrite(view_channel=True, connect=True)
        
        # 獲取或創建分類
        category = discord.utils.get(guild.categories, name="語音頻道")
        if not category:
            category = discord.utils.get(guild.categories, name="Voice Channels")
        if not category:
            category = discord.utils.get(guild.categories, name="語音")
        
        # 檢查 Bot 權限
        bot_member = guild.get_member(bot.user.id)
        if not bot_member:
            error_msg = (
                "❌ **創建頻道失敗**\n"
                "📋 **原因**：Bot 不在伺服器中\n"
                "💡 **解決方法**：請聯繫管理員檢查 Bot 設定"
            )
            await interaction.followup.send(error_msg)
            return
        
        required_permissions = [
            ('manage_channels', '管理頻道'),
            ('move_members', '移動成員'),
            ('connect', '連接語音頻道')
        ]
        missing_permissions = []
        for perm_name, perm_display in required_permissions:
            if not getattr(bot_member.guild_permissions, perm_name, False):
                missing_permissions.append(perm_display)
        
        if missing_permissions:
            error_msg = (
                "❌ **創建頻道失敗**\n"
                f"📋 **原因**：Bot 缺少以下權限：{', '.join(missing_permissions)}\n"
                "💡 **解決方法**：請聯繫管理員授予 Bot 以下權限：\n"
                "   • 管理頻道\n"
                "   • 移動成員\n"
                "   • 連接語音頻道"
            )
            await interaction.followup.send(error_msg)
            return
        
        # 定義創建頻道的函數
        async def create_channels():
            try:
                # 創建語音頻道
                vc = await guild.create_voice_channel(
                    name=animal_channel_name, 
                    overwrites=overwrites, 
                    user_limit=limit, 
                    category=category
                )
                
                # 創建文字頻道（429 安全）
                text_channel = await safe_create_text_channel(
                    guild,
                    name="🔒匿名文字區",
                    overwrites=overwrites,
                    category=category
                )
                
                return vc, text_channel
            except discord.Forbidden:
                return None, None
            except Exception as e:
                print(f"❌ 創建頻道錯誤: {e}")
                return None, None
        
        # 如果有開始時間，先發送確認訊息，然後延遲創建
        if start_time and delay_seconds > 0:
            # 發送排程確認訊息
            confirm_msg = (
                f"✅ **已排程語音頻道：{animal_channel_name}**\n"
                f"🕐 **開始時間**：<t:{int(start_dt_utc.timestamp())}:F>（台灣時間）\n"
                f"⏰ **頻道將在 {minutes} 分鐘後自動刪除**\n\n"
                f"👥 **邀請成員**：{', '.join([m.mention for m in mentioned])}"
            )
            await interaction.followup.send(confirm_msg)
            
            # 延遲創建頻道
            async def delayed_create():
                await asyncio.sleep(delay_seconds)
                
                vc, text_channel = await create_channels()
                if not vc or not text_channel:
                    error_channel = interaction.channel
                    if error_channel:
                        await error_channel.send(
                            f"❌ **排程創建頻道失敗**\n"
                            f"📋 **原因**：Bot 權限不足或發生錯誤\n"
                            f"💡 **解決方法**：請聯繫管理員"
                        )
                    return
                
                # 移動用戶到語音頻道
                moved_users = []
                failed_users_not_in_vc = []
                
                if caller_member.voice:
                    try:
                        await caller_member.move_to(vc)
                        moved_users.append(caller_member.mention)
                    except:
                        failed_users_not_in_vc.append(caller_member.mention)
                
                for member in mentioned:
                    if member.voice:
                        try:
                            await member.move_to(vc)
                            moved_users.append(member.mention)
                        except:
                            failed_users_not_in_vc.append(member.mention)
                    else:
                        failed_users_not_in_vc.append(member.mention)
                
                # 發送通知
                notify_msg = f"✅ **語音頻道已開啟：{animal_channel_name}**\n"
                if moved_users:
                    notify_msg += f"✅ **已自動移動**：{', '.join(moved_users)}\n"
                # 移除無法移動的警告訊息
                
                await text_channel.send(notify_msg)
                
                # 在文字頻道發送歡迎訊息並 @ 提及用戶
                view = ExtendView(vc.id)
                # 只顯示被邀請的成員，不包含互動發起者
                mention_list = [m.mention for m in mentioned]
                
                mention_text = ' '.join(mention_list) if mention_list else ""
                
                welcome_msg = ""
                
                if mention_list:
                    welcome_msg += f"👥 **邀請成員**：{mention_text}\n\n"
                
                welcome_msg += (
                    "⏳ **可延長5分鐘** ( 為了您有更好的遊戲體驗，請到最後需要時再點選 )\n"
                    f"⏰ **頻道將在 {minutes} 分鐘後自動刪除**"
                )
                
                await text_channel.send(welcome_msg, view=view)
                
                # 創建配對記錄（明確指定顧客和夥伴）
                record_id = None
                try:
                    with Session() as s:
                        customer_id = str(customer_member.id)
                        partner_id = str(partner_member.id)
                        
                        import uuid
                        record_id = str(uuid.uuid4())
                        record = PairingRecord(
                            id=record_id,
                            user1Id=customer_id,  # user1Id 是顧客
                            user2Id=partner_id,   # user2Id 是夥伴
                            duration=minutes * 60,
                            animalName=animal,
                            bookingId=f"manual_{record_id}"  # 手動創建的記錄使用 manual_ 前綴
                        )
                        s.add(record)
                        s.commit()
                        created_at = record.createdAt
                        print(f"✅ 配對記錄已創建: record_id={record_id}, customer_id={customer_id}, partner_id={partner_id}")
                except Exception as e:
                    print(f"⚠️ 創建配對記錄失敗: {e}")
                    import traceback
                    traceback.print_exc()
                    record_id = "temp_" + str(int(time.time()))
                
                # 啟動倒數計時
                active_voice_channels[vc.id] = {
                    'text_channel': text_channel,
                    'remaining': minutes * 60,
                    'extended': 0,
                    'record_id': record_id,
                    'vc': vc
                }
                
                # 啟動倒數任務
                bot.loop.create_task(countdown(vc.id, animal_channel_name, text_channel, vc, interaction, mentioned, record_id))
            
            bot.loop.create_task(delayed_create())
            return
        
        # 立即創建頻道
        vc, text_channel = await create_channels()
        
        if not vc or not text_channel:
            error_msg = (
                "❌ **創建頻道失敗**\n"
                "📋 **原因**：Bot 沒有足夠權限創建頻道\n"
                "💡 **解決方法**：請聯繫管理員檢查 Bot 權限"
            )
            await interaction.followup.send(error_msg)
            return
        
        # 移動用戶到語音頻道
        moved_users = []
        failed_users_not_in_vc = []
        failed_users_permission = []
        
        # 移動發起者
        if caller_member.voice:
            try:
                await caller_member.move_to(vc)
                moved_users.append(caller_member.mention)
            except discord.HTTPException as e:
                if e.code == 40032:  # User not connected to voice
                    failed_users_not_in_vc.append(caller_member.mention)
                else:
                    failed_users_permission.append(caller_member.mention)
                print(f"⚠️ 移動 {caller_member.display_name} 失敗: {e}")
        else:
            failed_users_not_in_vc.append(caller_member.mention)
        
        # 移動被標註的成員
        for member in mentioned:
            if member.voice:
                try:
                    await member.move_to(vc)
                    moved_users.append(member.mention)
                except discord.HTTPException as e:
                    if e.code == 40032:  # User not connected to voice
                        failed_users_not_in_vc.append(member.mention)
                    else:
                        failed_users_permission.append(member.mention)
                    print(f"⚠️ 移動 {member.display_name} 失敗: {e}")
            else:
                failed_users_not_in_vc.append(member.mention)
        
        # 構建詳細的成功訊息
        success_msg = f"✅ **已創建語音頻道：{animal_channel_name}**\n"
        success_msg += f"🔗 **語音頻道**：{vc.mention}\n"
        success_msg += f"💬 **文字頻道**：{text_channel.mention}\n\n"
        
        if moved_users:
            success_msg += f"✅ **已自動移動**：{', '.join(moved_users)}\n"
        
        # 移除無法移動的警告訊息
        
        if failed_users_permission:
            success_msg += (
                f"⚠️ **移動失敗**：{', '.join(failed_users_permission)}\n"
                "📋 **原因**：權限不足或用戶狀態異常\n"
            )
        
        success_msg += f"\n⏰ **頻道將在 {minutes} 分鐘後自動刪除**"
        
        await interaction.followup.send(success_msg)
        
        # 在文字頻道發送歡迎訊息並 @ 提及用戶
        view = ExtendView(vc.id)
        
        # 構建 @ 提及的用戶列表（只顯示被邀請的成員，不包含互動發起者）
        mention_list = [m.mention for m in mentioned]
        
        mention_text = ' '.join(mention_list) if mention_list else ""
        
        welcome_msg = ""
        
        if mention_list:
            welcome_msg += f"👥 **邀請成員**：{mention_text}\n\n"
        
        welcome_msg += (
            "⏳ **可延長5分鐘** ( 為了您有更好的遊戲體驗，請到最後需要時再點選 )\n"
            f"⏰ **頻道將在 {minutes} 分鐘後自動刪除**"
        )
        
        await text_channel.send(welcome_msg, view=view)
        
        # 創建配對記錄（明確指定顧客和夥伴）
        record_id = None
        try:
            with Session() as s:
                customer_id = str(customer_member.id)
                partner_id = str(partner_member.id)
                
                import uuid
                record_id = str(uuid.uuid4())
                record = PairingRecord(
                    id=record_id,
                    user1Id=customer_id,  # user1Id 是顧客
                    user2Id=partner_id,   # user2Id 是夥伴
                    duration=minutes * 60,
                    animalName=animal,
                    bookingId=f"manual_{record_id}"  # 手動創建的記錄使用 manual_ 前綴
                )
                s.add(record)
                s.commit()
                created_at = record.createdAt
                print(f"✅ 配對記錄已創建: record_id={record_id}, customer_id={customer_id}, partner_id={partner_id}")
        except Exception as e:
            print(f"⚠️ 創建配對記錄失敗: {e}")
            import traceback
            traceback.print_exc()
            record_id = "temp_" + str(int(time.time()))
        
        # 啟動倒數計時
        active_voice_channels[vc.id] = {
            'text_channel': text_channel,
            'remaining': minutes * 60,
            'extended': 0,
            'record_id': record_id,
            'vc': vc
        }
        
        # 啟動倒數任務
        bot.loop.create_task(countdown(vc.id, animal_channel_name, text_channel, vc, interaction, mentioned, record_id))
        
    except Exception as e:
        error_msg = (
            f"❌ **創建語音頻道失敗**\n"
            f"📋 **原因**：{str(e)}\n"
            "💡 **解決方法**：請稍後再試或聯繫管理員"
        )
        await interaction.followup.send(error_msg)
        print(f"❌ 創建語音頻道錯誤: {e}")
        import traceback
        traceback.print_exc()

# --- 其他 Slash 指令 ---
@bot.tree.command(name="viewblocklist", description="查看你封鎖的使用者", guild=discord.Object(id=GUILD_ID))
async def view_blocklist(interaction: discord.Interaction):
    try:
        with Session() as s:
            blocks = s.query(BlockRecord).filter(BlockRecord.blocker_id == str(interaction.user.id)).all()
            if not blocks:
                await interaction.response.send_message("📭 你尚未封鎖任何人。", ephemeral=True)
                return
            blocked_mentions = [f"<@{b.blocked_id}>" for b in blocks]
            await interaction.response.send_message(f"🔒 你封鎖的使用者：\n" + "\n".join(blocked_mentions), ephemeral=True)
    except Exception:
        await interaction.response.send_message("📭 你尚未封鎖任何人。", ephemeral=True)

@bot.tree.command(name="unblock", description="解除你封鎖的某人", guild=discord.Object(id=GUILD_ID))
@app_commands.describe(member="要解除封鎖的使用者")
async def unblock(interaction: discord.Interaction, member: discord.Member):
    try:
        with Session() as s:
            block = s.query(BlockRecord).filter_by(blocker_id=str(interaction.user.id), blocked_id=str(member.id)).first()
            if block:
                s.delete(block)
                s.commit()
                await interaction.response.send_message(f"✅ 已解除對 <@{member.id}> 的封鎖。", ephemeral=True)
            else:
                await interaction.response.send_message("❗ 你沒有封鎖這位使用者。", ephemeral=True)
    except Exception:
        await interaction.response.send_message("❗ 封鎖功能暫時無法使用。", ephemeral=True)

@bot.tree.command(name="report", description="舉報不當行為", guild=discord.Object(id=GUILD_ID))
@app_commands.describe(member="被舉報的使用者", reason="舉報原因")
async def report(interaction: discord.Interaction, member: discord.Member, reason: str):
    admin = bot.get_channel(ADMIN_CHANNEL_ID)
    await interaction.response.send_message("✅ 舉報已提交，感謝你的協助。", ephemeral=True)
    if admin:
        await admin.send(f"🚨 舉報通知：<@{interaction.user.id}> 舉報 <@{member.id}>\n📄 理由：{reason}")

@bot.tree.command(name="mystats", description="查詢自己的配對統計", guild=discord.Object(id=GUILD_ID))
async def mystats(interaction: discord.Interaction):
    with Session() as s:
        records = s.query(PairingRecord).filter((PairingRecord.user1Id==str(interaction.user.id)) | (PairingRecord.user2Id==str(interaction.user.id))).all()
    count = len(records)
    ratings = [r.rating for r in records if r.rating]
    comments = [r.comment for r in records if r.comment]
    avg_rating = round(sum(ratings)/len(ratings), 1) if ratings else "無"
    await interaction.response.send_message(f"📊 你的配對紀錄：\n- 配對次數：{count} 次\n- 平均評分：{avg_rating} ⭐\n- 收到留言：{len(comments)} 則", ephemeral=True)

@bot.tree.command(name="stats", description="查詢他人配對統計 (限管理員)", guild=discord.Object(id=GUILD_ID))
@app_commands.describe(member="要查詢的使用者")
async def stats(interaction: discord.Interaction, member: discord.Member):
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("❌ 僅限管理員查詢。", ephemeral=True)
        return
    with Session() as s:
        records = s.query(PairingRecord).filter((PairingRecord.user1Id==str(member.id)) | (PairingRecord.user2Id==str(member.id))).all()
    count = len(records)
    ratings = [r.rating for r in records if r.rating]
    comments = [r.comment for r in records if r.comment]
    avg_rating = round(sum(ratings)/len(ratings), 1) if ratings else "無"
    await interaction.response.send_message(f"📊 <@{member.id}> 的配對紀錄：\n- 配對次數：{count} 次\n- 平均評分：{avg_rating} ⭐\n- 收到留言：{len(comments)} 則", ephemeral=True)

# --- Flask API ---
app = Flask(__name__)

@app.route("/move_user", methods=["POST"])
def move_user():
    data = request.get_json()
    discord_id = int(data.get("discord_id"))
    vc_id = int(data.get("vc_id"))

    async def mover():
        guild = bot.get_guild(GUILD_ID)
        member = guild.get_member(discord_id)
        vc = guild.get_channel(vc_id)
        if member and vc:
            await member.move_to(vc)

    bot.loop.create_task(mover())
    return jsonify({"status": "ok"})

@app.route("/pair", methods=["POST"])
def pair_users():
    data = request.get_json()
    user1_discord_name = data.get("user1_id")  # 實際上是 Discord 名稱
    user2_discord_name = data.get("user2_id")  # 實際上是 Discord 名稱
    minutes = data.get("minutes", 60)
    start_time = data.get("start_time")  # 可選的開始時間

    print(f"🔍 收到配對請求: {user1_discord_name} × {user2_discord_name}, {minutes} 分鐘")

    async def create_pairing():
        try:
            guild = bot.get_guild(GUILD_ID)
            if not guild:
                print("❌ 找不到伺服器")
                return

            # 根據 Discord 名稱查找用戶
            user1 = find_member_by_discord_name(guild, user1_discord_name)
            user2 = find_member_by_discord_name(guild, user2_discord_name)
            
            if not user1 or not user2:
                print(f"❌ 找不到用戶: {user1_discord_name}, {user2_discord_name}")
                print(f"🔍 伺服器中的成員: {[m.name for m in guild.members]}")
                return

            print(f"✅ 找到用戶: {user1.name} ({user1.id}), {user2.name} ({user2.id})")

            # 生成可愛物品名稱
            animal = random.choice(CUTE_ITEMS)
            channel_name = f"{animal}頻道"

            # 創建語音頻道 - 嘗試多種分類名稱
            category = discord.utils.get(guild.categories, name="Voice Channels")
            if not category:
                category = discord.utils.get(guild.categories, name="語音頻道")
            if not category:
                category = discord.utils.get(guild.categories, name="語音")
            if not category:
                # 嘗試使用第一個可用的分類
                if guild.categories:
                    category = guild.categories[0]
                    print(f"⚠️ 使用現有分類: {category.name}")
                else:
                    print("❌ 找不到任何分類，請在 Discord 伺服器中創建分類")
                    return

            # 設定權限
            overwrites = {
                guild.default_role: discord.PermissionOverwrite(view_channel=False),
                user1: discord.PermissionOverwrite(view_channel=True, connect=True, speak=True),
                user2: discord.PermissionOverwrite(view_channel=True, connect=True, speak=True),
            }

            # 創建文字頻道（429 安全，立即創建）
            text_channel = await safe_create_text_channel(
                guild,
                name=f"{animal}聊天",
                category=category,
                overwrites=overwrites
            )

            # 如果有開始時間，則排程創建語音頻道
            if start_time:
                try:
                    # 解析開始時間
                    start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                    now = datetime.now(timezone.utc)
                    delay_seconds = (start_dt - now).total_seconds()
                    
                    if delay_seconds > 300:  # 如果超過5分鐘
                        # 發送5分鐘提醒
                        reminder_time = start_dt - timedelta(minutes=5)
                        reminder_delay = (reminder_time - now).total_seconds()
                        
                        if reminder_delay > 0:
                            await asyncio.sleep(reminder_delay)
                            await text_channel.send(f"⏰ **預約提醒**\n🎮 您的語音頻道將在 5 分鐘後開啟！\n👥 參與者：{user1.mention} 和 {user2.mention}\n⏰ 開始時間：<t:{int(start_dt.timestamp())}:t>")
                    
                    # 等待到開始時間
                    if delay_seconds > 0:
                        await asyncio.sleep(delay_seconds)
                    
                    # 創建語音頻道
                    voice_channel = await guild.create_voice_channel(
                        name=channel_name,
                        category=category,
                        user_limit=2,
                        overwrites=overwrites
                    )
                    
                    # 移動用戶到語音頻道
                    if user1.voice:
                        await user1.move_to(voice_channel)
                    if user2.voice:
                        await user2.move_to(voice_channel)
                    
                    # 發送歡迎訊息（與手動創建相同）
                    await text_channel.send(f"🎉 語音頻道 {channel_name} 已開啟！\n⏳ 可延長5分鐘 ( 為了您有更好的遊戲體驗，請到最後需要時再點選 ) 。")
                    
                    print(f"✅ 成功創建排程配對頻道: {channel_name}")
                    
                except Exception as e:
                    print(f"❌ 排程創建頻道失敗: {e}")
                    await text_channel.send("❌ 創建語音頻道時發生錯誤，請聯繫管理員。")
            else:
                # 立即創建語音頻道
                voice_channel = await guild.create_voice_channel(
                    name=channel_name,
                    category=category,
                    user_limit=2,
                    overwrites=overwrites
                )
                
                # 移動用戶到語音頻道
                if user1.voice:
                    await user1.move_to(voice_channel)
                if user2.voice:
                    await user2.move_to(voice_channel)
                
                # 發送歡迎訊息
                await text_channel.send(f"🎮 歡迎 {user1.mention} 和 {user2.mention} 來到 {channel_name}！\n⏰ 時長：{minutes} 分鐘")
                
                print(f"✅ 成功創建即時配對頻道: {channel_name}")

        except Exception as e:
            print(f"❌ 創建配對頻道失敗: {e}")
            import traceback
            traceback.print_exc()

    bot.loop.create_task(create_pairing())
    return jsonify({"status": "ok", "message": "配對請求已處理"})

@app.route('/create-group-text-channel', methods=['POST'])
def create_group_text_channel():
    """創建群組文字頻道"""
    try:
        data = request.get_json()
        group_id = data.get('groupId')
        group_title = data.get('groupTitle', '')
        participants = data.get('participants', [])
        start_time = data.get('startTime')
        end_time = data.get('endTime')
        
        if not group_id:
            return jsonify({'error': '缺少 groupId 參數'}), 400
        
        # 檢查資料庫中是否已存在文字頻道
        with Session() as s:
            existing = s.execute(text("""
                SELECT "discordTextChannelId" 
                FROM "GroupBooking" 
                WHERE id = :group_id
            """), {'group_id': group_id}).fetchone()
            
            if existing and existing[0]:
                # 檢查頻道是否真的存在
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    guild = bot.get_guild(GUILD_ID)
                    if guild:
                        existing_channel = guild.get_channel(int(existing[0]))
                        if existing_channel:
                            print(f"⚠️ 群組文字頻道已存在: {existing_channel.name} (ID: {existing_channel.id})")
                            loop.close()
                            return jsonify({
                                'success': True,
                                'channelId': str(existing_channel.id)
                            })
                finally:
                    loop.close()
        
        # 解析參與者，分離顧客和夥伴（通過 Booking 表判斷）
        customer_discords = []
        partner_discords = []
        
        # 從資料庫獲取群組預約信息以確定顧客和夥伴
        with Session() as s:
            # 🔥 通過 Booking 表判斷顧客（有付費記錄）
            customer_result = s.execute(text("""
                SELECT DISTINCT cu.discord as customer_discord
                FROM "GroupBooking" gb
                JOIN "Booking" b ON b."groupBookingId" = gb.id
                JOIN "Customer" c ON c.id = b."customerId"
                JOIN "User" cu ON cu.id = c."userId"
                WHERE gb.id = :group_id
                AND b.status IN ('CONFIRMED', 'PARTNER_ACCEPTED', 'PAID_WAITING_PARTNER_CONFIRMATION', 'COMPLETED')
                AND cu.discord IS NOT NULL
            """), {'group_id': group_id}).fetchall()
            
            # 查詢所有夥伴
            partner_result = s.execute(text("""
                SELECT DISTINCT pu.discord as partner_discord
                FROM "GroupBooking" gb
                JOIN "GroupBookingParticipant" gbp ON gbp."groupBookingId" = gb.id
                JOIN "Partner" p ON p.id = gbp."partnerId"
                JOIN "User" pu ON pu.id = p."userId"
                WHERE gb.id = :group_id
                AND gbp.status = 'ACTIVE'
                AND pu.discord IS NOT NULL
            """), {'group_id': group_id}).fetchall()
            
            customer_discords = [row.customer_discord for row in customer_result if row.customer_discord]
            partner_discords = [row.partner_discord for row in partner_result if row.partner_discord]
        
        # 如果從資料庫找不到，使用傳入的 participants（但這種情況應該很少見）
        if not customer_discords and participants:
            # 假設第一個是顧客，其他是夥伴
            customer_discords = [participants[0]] if len(participants) > 0 else []
            partner_discords = participants[1:] if len(participants) > 1 else []
        
        if not customer_discords:
            return jsonify({'error': '找不到顧客 Discord ID（有付費記錄）'}), 400
        
        # 解析時間
        if isinstance(start_time, str):
            if start_time.endswith('Z'):
                start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            else:
                start_dt = datetime.fromisoformat(start_time)
        else:
            start_dt = start_time
        
        if isinstance(end_time, str):
            if end_time.endswith('Z'):
                end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
            else:
                end_dt = datetime.fromisoformat(end_time)
        else:
            end_dt = end_time
        
        # 使用 asyncio 運行 Discord 操作
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            text_channel = loop.run_until_complete(
                create_group_booking_text_channel(
                    group_id, 
                    customer_discords,  # 所有有付費記錄的顧客
                    partner_discords, 
                    start_dt, 
                    end_dt
                )
            )
            loop.close()
            
            if text_channel:
                # 更新資料庫
                with Session() as s:
                    s.execute(text("""
                        UPDATE "GroupBooking" 
                        SET "discordTextChannelId" = :channel_id
                        WHERE id = :group_id
                    """), {
                        'channel_id': str(text_channel.id),
                        'group_id': group_id
                    })
                    s.commit()
                
                return jsonify({
                    'success': True,
                    'channelId': str(text_channel.id)
                })
            else:
                return jsonify({'error': '創建文字頻道失敗'}), 500
        except Exception as e:
            loop.close()
            print(f"❌ 創建群組文字頻道時發生錯誤: {e}")
            import traceback
            traceback.print_exc()
            return jsonify({'error': f'Discord 操作失敗: {str(e)}'}), 500
            
    except Exception as e:
        print(f"❌ 創建群組文字頻道時發生錯誤: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'創建頻道失敗: {str(e)}'}), 500

@app.route('/create-group-voice-channel', methods=['POST'])
def create_group_voice_channel():
    """創建群組語音頻道"""
    try:
        data = request.get_json()
        group_id = data.get('groupId')
        group_title = data.get('groupTitle', '')
        participants = data.get('participants', [])
        start_time = data.get('startTime')
        end_time = data.get('endTime')
        
        if not group_id:
            return jsonify({'error': '缺少 groupId 參數'}), 400
        
        # 檢查資料庫中是否已存在語音頻道
        with Session() as s:
            existing = s.execute(text("""
                SELECT "discordVoiceChannelId" 
                FROM "GroupBooking" 
                WHERE id = :group_id
            """), {'group_id': group_id}).fetchone()
            
            if existing and existing[0]:
                # 檢查頻道是否真的存在
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    guild = bot.get_guild(GUILD_ID)
                    if guild:
                        existing_channel = guild.get_channel(int(existing[0]))
                        if existing_channel:
                            print(f"⚠️ 群組語音頻道已存在: {existing_channel.name} (ID: {existing_channel.id})")
                            loop.close()
                            return jsonify({
                                'success': True,
                                'channelId': str(existing_channel.id)
                            })
                finally:
                    loop.close()
        
        # 解析參與者，分離顧客和夥伴
        customer_discord = None
        partner_discords = []
        
        # 從資料庫獲取群組預約信息以確定顧客和夥伴
        with Session() as s:
            # 獲取群組預約的參與者
            group_data = s.execute(text("""
                SELECT 
                    b."customerId", c."userId" as customer_user_id, cu.discord as customer_discord,
                    p."userId" as partner_user_id, pu.discord as partner_discord
                FROM "GroupBooking" gb
                LEFT JOIN "Booking" b ON b."groupBookingId" = gb.id
                LEFT JOIN "Customer" c ON c.id = b."customerId"
                LEFT JOIN "User" cu ON cu.id = c."userId"
                LEFT JOIN "GroupBookingParticipant" gbp ON gbp."groupBookingId" = gb.id
                LEFT JOIN "Partner" p ON p.id = gbp."partnerId"
                LEFT JOIN "User" pu ON pu.id = p."userId"
                WHERE gb.id = :group_id
            """), {'group_id': group_id}).fetchall()
            
            # 收集所有參與者的 Discord ID
            for row in group_data:
                if row.customer_discord and row.customer_discord not in partner_discords:
                    customer_discord = row.customer_discord
                if row.partner_discord and row.partner_discord not in partner_discords:
                    partner_discords.append(row.partner_discord)
        
        # 如果從資料庫找不到，使用傳入的 participants
        if not customer_discord and participants:
            # 假設第一個是顧客，其他是夥伴
            customer_discord = participants[0] if len(participants) > 0 else None
            partner_discords = participants[1:] if len(participants) > 1 else []
        
        if not customer_discord:
            return jsonify({'error': '找不到顧客 Discord ID'}), 400
        
        # 解析時間
        if isinstance(start_time, str):
            if start_time.endswith('Z'):
                start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            else:
                start_dt = datetime.fromisoformat(start_time)
        else:
            start_dt = start_time
        
        if isinstance(end_time, str):
            if end_time.endswith('Z'):
                end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
            else:
                end_dt = datetime.fromisoformat(end_time)
        else:
            end_dt = end_time
        
        # 使用 asyncio 運行 Discord 操作
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            voice_channel = loop.run_until_complete(
                create_group_booking_voice_channel(
                    group_id, 
                    customer_discord, 
                    partner_discords, 
                    start_dt, 
                    end_dt
                )
            )
            loop.close()
            
            if voice_channel:
                # 更新資料庫
                with Session() as s:
                    s.execute(text("""
                        UPDATE "GroupBooking" 
                        SET "discordVoiceChannelId" = :channel_id
                        WHERE id = :group_id
                    """), {
                        'channel_id': str(voice_channel.id),
                        'group_id': group_id
                    })
                    s.commit()
                
                return jsonify({
                    'success': True,
                    'channelId': str(voice_channel.id)
                })
            else:
                return jsonify({'error': '創建語音頻道失敗'}), 500
        except Exception as e:
            loop.close()
            print(f"❌ 創建群組語音頻道時發生錯誤: {e}")
            import traceback
            traceback.print_exc()
            return jsonify({'error': f'Discord 操作失敗: {str(e)}'}), 500
            
    except Exception as e:
        print(f"❌ 創建群組語音頻道時發生錯誤: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'創建頻道失敗: {str(e)}'}), 500

@app.route('/delete', methods=['POST'])
def delete_booking():
    """刪除預約相關的 Discord 頻道"""
    try:
        data = request.get_json()
        booking_id = data.get('booking_id')
        
        if not booking_id:
            return jsonify({'error': '缺少預約 ID'}), 400
        
        # 使用 asyncio 運行 Discord 操作
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(
                delete_booking_channels(booking_id)
            )
            loop.close()
            
            if result:
                return jsonify({'success': True, 'message': '頻道已成功刪除'})
            else:
                return jsonify({'error': '刪除頻道失敗'}), 500
        except Exception as e:
            loop.close()
            return jsonify({'error': f'Discord 操作失敗: {str(e)}'}), 500
            
    except Exception as e:
        return jsonify({'error': f'刪除預約失敗: {str(e)}'}), 500

def run_flask():
    app.run(host="0.0.0.0", port=5001)

threading.Thread(target=run_flask, daemon=True).start()
bot.run(TOKEN) 
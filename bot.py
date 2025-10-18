import os 
import asyncio
import random
import time
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

# --- 環境與資料庫設定 ---
load_dotenv()
TOKEN = os.getenv("DISCORD_BOT_TOKEN")
GUILD_ID = int(os.getenv("DISCORD_GUILD_ID", "0"))
POSTGRES_CONN = os.getenv("POSTGRES_CONN")
ADMIN_CHANNEL_ID = int(os.getenv("ADMIN_CHANNEL_ID", "1419601068110778450"))

# 調試資訊
print("環境變數檢查:")
print(f"   ADMIN_CHANNEL_ID: {ADMIN_CHANNEL_ID}")

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

Base = declarative_base()
# 添加連接池設置和重連機制
engine = create_engine(
    POSTGRES_CONN,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,  # 自動重連
    pool_recycle=1800,   # 30分鐘後回收連接
    pool_timeout=30,     # 連接超時30秒
    echo=False
)
Session = sessionmaker(bind=engine)
session = Session()

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
processed_withdrawals = set()  # 記錄已處理的提領申請

# 可愛的動物和物品列表
CUTE_ITEMS = ["🦊 狐狸", "🐱 貓咪", "🐶 小狗", "🐻 熊熊", "🐼 貓熊", "🐯 老虎", "🦁 獅子", "🐸 青蛙", "🐵 猴子", "🐰 兔子", "🦄 獨角獸", "🐙 章魚", "🦋 蝴蝶", "🌸 櫻花", "⭐ 星星", "🌈 彩虹", "🍀 幸運草", "🎀 蝴蝶結", "🍭 棒棒糖", "🎈 氣球"]
TW_TZ = timezone(timedelta(hours=8))

# --- 成員搜尋函數 ---
def find_member_by_discord_name(guild, discord_name):
    """根據 Discord 名稱搜尋成員"""
    if not discord_name:
        return None
    
    discord_name_lower = discord_name.lower()
    for member in guild.members:
        if member.name.lower() == discord_name_lower or member.display_name.lower() == discord_name_lower:
            return member
    return None

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
        print(f"🔍 文字頻道時間轉換: UTC {start_time} -> TW {tw_start_time} -> {start_time_str}")
        
        # 創建統一的頻道名稱 - 加上隨機可愛物品
        cute_item = random.choice(CUTE_ITEMS)
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
        
        # 創建文字頻道
        text_channel = await guild.create_text_channel(
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
async def create_group_booking_voice_channel(group_booking_id, customer_discord, partner_discords, start_time, end_time):
    """為多人開團預約創建語音頻道"""
    try:
        guild = bot.get_guild(GUILD_ID)
        if not guild:
            print("❌ 找不到 Discord 伺服器")
            return None
        
        # 查找 Discord 成員
        customer_member = find_member_by_discord_name(guild, customer_discord)
        partner_members = []
        
        for partner_discord in partner_discords:
            partner_member = find_member_by_discord_name(guild, partner_discord)
            if partner_member:
                partner_members.append(partner_member)
        
        if not customer_member or not partner_members:
            print(f"❌ 找不到 Discord 成員: 顧客={customer_discord}, 夥伴={partner_discords}")
            return None
        
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
        
        cute_item = random.choice(CUTE_ITEMS)
        channel_name = f"👥多人{date_str} {start_time_str}-{end_time_str} {cute_item}"
        
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
        
        # 創建配對記錄
        user1_id = str(customer_member.id)
        user2_id = str(partner_members[0].id) if partner_members else None
        
        if user2_id:
            try:
                record_id = f"group_{uuid.uuid4().hex[:12]}"
                record = PairingRecord(
                    id=record_id,
                    user1Id=user1_id,
                    user2Id=user2_id,
                    duration=duration_minutes * 60,
                    animalName="多人開團",
                    bookingId=group_booking_id
                )
                s.add(record)
                s.commit()
                record_id = record.id
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
                title="👥 多人開團語音頻道已創建",
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
        
        print(f"✅ 多人開團語音頻道已創建: {channel_name} (群組 {group_booking_id})")
        return vc
        
    except Exception as e:
        print(f"❌ 創建多人開團語音頻道失敗: {e}")
        return None

async def create_group_booking_text_channel(group_booking_id, customer_discord, partner_discords, start_time, end_time):
    """為多人開團創建文字頻道"""
    try:
        guild = bot.get_guild(GUILD_ID)
        if not guild:
            print("❌ 找不到 Discord 伺服器")
            return None
        
        # 查找所有成員
        customer_member = find_member_by_discord_name(guild, customer_discord)
        if not customer_member:
            print(f"❌ 找不到顧客: {customer_discord}")
            return None
        
        partner_members = []
        for partner_discord in partner_discords:
            partner_member = find_member_by_discord_name(guild, partner_discord)
            if partner_member:
                partner_members.append(partner_member)
            else:
                print(f"⚠️ 找不到夥伴: {partner_discord}")
        
        if not partner_members:
            print("❌ 找不到任何夥伴")
            return None
        
        # 生成頻道名稱
        animal = random.choice(CUTE_ITEMS)
        channel_name = f"👥{animal}多人開團聊天"
        
        # 轉換為台灣時間
        start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
        tw_start_time = start_dt.astimezone(timezone(timedelta(hours=8)))
        
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
            customer_member: discord.PermissionOverwrite(view_channel=True, send_messages=True),
        }
        
        # 為所有夥伴添加權限
        for partner_member in partner_members:
            overwrites[partner_member] = discord.PermissionOverwrite(view_channel=True, send_messages=True)
        
        # 創建文字頻道
        text_channel = await guild.create_text_channel(
            name=channel_name,
            category=category,
            overwrites=overwrites
        )
        
        # 發送歡迎訊息
        welcome_embed = discord.Embed(
            title="🎮 多人開團聊天頻道",
            description="歡迎來到多人開團聊天頻道！",
            color=0x9b59b6,
            timestamp=datetime.now(timezone.utc)
        )
        
        welcome_embed.add_field(
            name="👤 顧客",
            value=f"{customer_member.mention}",
            inline=True
        )
        
        partner_mentions = [partner.mention for partner in partner_members]
        welcome_embed.add_field(
            name="👥 夥伴們",
            value="\n".join(partner_mentions),
            inline=False
        )
        
        welcome_embed.add_field(
            name="⏰ 開始時間",
            value=f"`{tw_start_time.strftime('%Y/%m/%d %H:%M')}`",
            inline=True
        )
        
        welcome_embed.add_field(
            name="📋 群組預約ID",
            value=f"`{group_booking_id}`",
            inline=True
        )
        
        await text_channel.send(embed=welcome_embed)
        
        # 發送安全規範
        safety_embed = discord.Embed(
            title="🎙️ 多人開團聊天頻道使用規範與警告",
            description="為了您的安全，請務必遵守以下規範：",
            color=0xff6b6b,
            timestamp=datetime.now(timezone.utc)
        )
        safety_embed.add_field(
            name="📌 頻道性質",
            value="此聊天頻道為【多人開團用途】。\n僅限遊戲討論、戰術交流、團隊協作使用。\n禁止任何涉及交易、暗示、或其他非遊戲用途的行為。",
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
        
        print(f"✅ 多人開團文字頻道已創建: {channel_name} (群組 {group_booking_id})")
        return text_channel
        
    except Exception as e:
        print(f"❌ 創建多人開團文字頻道失敗: {e}")
        return None

async def countdown_with_group_rating(vc_id, channel_name, text_channel, vc, members, record_id, group_booking_id):
    """多人開團的倒數計時函數，包含評價系統"""
    try:
        # 獲取 guild 對象
        guild = bot.get_guild(GUILD_ID)
        if not guild:
            print(f"❌ 找不到 Guild ID: {GUILD_ID}")
            return
        
        # 計算預約結束時間
        now = datetime.now(timezone.utc)
        
        # 從資料庫獲取預約結束時間
        with Session() as s:
            result = s.execute(text("""
                SELECT gb."endTime", gb."currentParticipants", gb."maxParticipants"
                FROM "GroupBooking" gb
                WHERE gb.id = :group_booking_id
            """), {"group_booking_id": group_booking_id}).fetchone()
            
            if not result:
                print(f"❌ 找不到群組預約記錄: {group_booking_id}")
                return
            
            end_time = result[0]
            current_participants = result[1]
            max_participants = result[2]
        
        # 計算剩餘時間
        remaining_seconds = int((end_time - now).total_seconds())
        
        if remaining_seconds <= 0:
            print(f"⏰ 群組預約 {group_booking_id} 已結束")
            await text_channel.send("⏰ 多人開團時間已結束！")
            await show_group_rating_system(text_channel, group_booking_id, members)
            return
        
        # 等待到結束時間
        await asyncio.sleep(remaining_seconds)
        
        # 時間結束，顯示評價系統
        await text_channel.send("⏰ 多人開團時間已結束！")
        await show_group_rating_system(text_channel, group_booking_id, members)
        
    except Exception as e:
        print(f"❌ 多人開團倒數計時錯誤: {e}")

async def show_group_rating_system(text_channel, group_booking_id, members):
    """顯示多人開團評價系統"""
    try:
        guild = bot.get_guild(GUILD_ID)
        if not guild:
            print(f"❌ 找不到 Guild ID: {GUILD_ID}")
            return
        
        # 創建評價頻道
        evaluation_channel_name = f"📝多人開團評價-{group_booking_id[:8]}"
        
        # 設置頻道權限
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
        }
        
        # 添加所有成員權限
        for member in members:
            if member:
                overwrites[member] = discord.PermissionOverwrite(view_channel=True, send_messages=True)
        
        # 獲取分類
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
                return
        
        evaluation_channel = await guild.create_text_channel(
            name=evaluation_channel_name,
            category=category,
            overwrites=overwrites
        )
        
        # 發送評價提示訊息
        embed = discord.Embed(
            title="⭐ 多人開團結束 - 請進行整體評價",
            description="感謝您參與多人開團！請花一點時間為這次開團體驗進行評價。",
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
            name="🆔 群組ID",
            value=f"`{group_booking_id}`",
            inline=True
        )
        embed.set_footer(text="評價有助於我們提供更好的多人開團服務品質")
        
        await evaluation_channel.send(embed=embed)
        await evaluation_channel.send("📝 請點擊以下按鈕進行匿名評分：")
        
        class GroupRatingView(View):
            def __init__(self, group_booking_id):
                super().__init__(timeout=600)  # 10分鐘超時
                self.group_booking_id = group_booking_id
                self.submitted_users = set()

            @discord.ui.button(label="⭐ 匿名評分", style=discord.ButtonStyle.success, emoji="⭐")
            async def submit_rating(self, interaction: discord.Interaction, button: Button):
                if interaction.user.id in self.submitted_users:
                    await interaction.response.send_message("❗ 您已經提交過評價。", ephemeral=True)
                    return
                
                await interaction.response.send_modal(GroupRatingModal(self.group_booking_id, self))
        
        await evaluation_channel.send(view=GroupRatingView(group_booking_id))
        
        # 10分鐘後刪除評價頻道
        await asyncio.sleep(600)
        try:
            await evaluation_channel.delete()
            print(f"🗑️ 多人開團評價頻道已刪除: {evaluation_channel_name}")
        except Exception as e:
            print(f"❌ 刪除多人開團評價頻道失敗: {e}")
        
    except Exception as e:
        print(f"❌ 顯示多人開團評價系統失敗: {e}")

class GroupRatingModal(Modal, title="多人開團匿名評分與留言"):
    rating = TextInput(label="給予評分（1～5 星）", required=True, placeholder="請輸入 1-5 的數字")
    comment = TextInput(label="留下你的留言（選填）", required=False, placeholder="分享您的開團體驗...")

    def __init__(self, group_booking_id, parent_view):
        super().__init__()
        self.group_booking_id = group_booking_id
        self.parent_view = parent_view

    async def on_submit(self, interaction: discord.Interaction):
        try:
            # 驗證評分
            try:
                rating = int(str(self.rating))
                if rating < 1 or rating > 5:
                    await interaction.response.send_message("❌ 評分必須在 1-5 之間", ephemeral=True)
                    return
            except ValueError:
                await interaction.response.send_message("❌ 請輸入有效的數字", ephemeral=True)
                return
            
            # 保存評價到資料庫
            with Session() as s:
                # 獲取客戶記錄
                customer_result = s.execute(text("""
                    SELECT c.id FROM "Customer" c
                    JOIN "User" u ON u.id = c."userId"
                    WHERE u.discord = :discord_name
                """), {"discord_name": interaction.user.name}).fetchone()
                
                if not customer_result:
                    await interaction.response.send_message("❌ 找不到您的客戶記錄", ephemeral=True)
                    return
                
                customer_id = customer_result[0]
                
                # 創建多人開團評價記錄
                review = GroupBookingReview(
                    groupBookingId=self.group_booking_id,
                    reviewerId=customer_id,
                    rating=rating,
                    comment=str(self.comment) if self.comment else None
                )
                s.add(review)
                s.commit()
            
            # 發送到管理員頻道
            await send_group_rating_to_admin(self.group_booking_id, rating, str(self.comment), interaction.user.name)
            
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
            print(f"❌ 處理多人開團評價提交失敗: {e}")
            await interaction.response.send_message("❌ 處理評價時發生錯誤，請稍後再試", ephemeral=True)

async def send_group_rating_to_admin(group_booking_id, rating, comment, reviewer_name):
    """發送多人開團評價結果到管理員頻道"""
    try:
        admin_channel = bot.get_channel(ADMIN_CHANNEL_ID)
        if not admin_channel:
            print(f"❌ 找不到管理員頻道 (ID: {ADMIN_CHANNEL_ID})")
            return
        
        # 獲取群組預約資訊
        with Session() as s:
            result = s.execute(text("""
                SELECT gb.title, gb."currentParticipants", gb."maxParticipants"
                FROM "GroupBooking" gb
                WHERE gb.id = :group_booking_id
            """), {"group_booking_id": group_booking_id}).fetchone()
            
            if not result:
                print(f"❌ 找不到群組預約記錄: {group_booking_id}")
                return
            
            title = result[0] or "多人開團"
            current_participants = result[1]
            max_participants = result[2]
        
        # 創建評價嵌入訊息
        embed = discord.Embed(
            title="⭐ 多人開團評價回饋",
            color=0x00ff00,
            timestamp=datetime.now(timezone.utc)
        )
        
        embed.add_field(
            name="🎮 開團標題",
            value=title,
            inline=True
        )
        
        embed.add_field(
            name="👤 評價者",
            value=reviewer_name,
            inline=True
        )
        
        embed.add_field(
            name="⭐ 評分",
            value="⭐" * rating,
            inline=True
        )
        
        embed.add_field(
            name="👥 參與人數",
            value=f"{current_participants}/{max_participants}",
            inline=True
        )
        
        if comment:
            embed.add_field(
                name="💬 留言",
                value=comment,
                inline=False
            )
        
        embed.add_field(
            name="📋 群組預約ID",
            value=f"`{group_booking_id}`",
            inline=True
        )
        
        embed.set_footer(text="PeiPlay 多人開團評價系統")
        
        await admin_channel.send(embed=embed)
        print(f"✅ 多人開團評價已發送到管理員頻道: {reviewer_name} → {title} ({rating}⭐)")
        
    except Exception as e:
        print(f"❌ 發送多人開團評價到管理員頻道失敗: {e}")

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
                print(f"✅ 配對記錄已創建: {record_id}")
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
            print(f"⚡ 即時預約語音頻道已創建: {channel_name} (預約 {booking_id})")
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
        with Session() as s:
            # 查詢預約開始前 5 分鐘的已確認預約
            now = datetime.now(timezone.utc)
            # 檢查預約開始時間在 5 分鐘內且還沒有創建文字頻道的預約
            five_minutes_from_now = now + timedelta(minutes=5)
            
            # 檢查是否已創建文字頻道
            processed_list = list(processed_text_channels)
            
            # 查詢預約開始時間在 5 分鐘內且還沒有創建文字頻道的已確認預約
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
                AND s."startTime" <= :five_minutes_from_now
                AND s."startTime" > :now
                AND s."endTime" > :now
                AND b."discordTextChannelId" IS NULL
            """
            result = s.execute(text(query), {
                "five_minutes_from_now": five_minutes_from_now,
                "now": now
            })
            
            for row in result:
                try:
                    # 檢查是否已經創建過文字頻道
                    if row.id in processed_text_channels:
                        print(f"⚠️ 預約 {row.id} 已在記憶體中標記為已處理，跳過")
                        continue  # 靜默跳過，不輸出日誌
                    
                    # 檢查資料庫中是否已經有文字頻道ID
                    with Session() as check_s:
                        existing_channel = check_s.execute(
                            text("SELECT \"discordTextChannelId\" FROM \"Booking\" WHERE id = :booking_id"),
                            {"booking_id": row.id}
                        ).fetchone()
                        
                        if existing_channel and existing_channel[0]:
                            print(f"⚠️ 預約 {row.id} 在資料庫中已有文字頻道ID，跳過")
                            processed_text_channels.add(row.id)
                            continue
                    
                    # 創建文字頻道（預約開始前 2 小時）
                    print(f"🔍 預約 {row.id} 將在 5 分鐘內開始，創建文字頻道")
                    text_channel = await create_booking_text_channel(
                        row.id, 
                        row.customer_discord, 
                        row.partner_discord, 
                        row.startTime, 
                        row.endTime
                    )
                    
                    if text_channel:
                        # 標記為已處理
                        processed_text_channels.add(row.id)
                        # 已標記預約為已處理，減少日誌輸出
                        
                except Exception as e:
                    print(f"❌ 處理新預約 {row.id} 時發生錯誤: {e}")
                    continue
                    
    except Exception as e:
        print(f"❌ 檢查新預約時發生錯誤: {e}")

# --- 自動關閉「現在有空」狀態任務 ---
@tasks.loop(seconds=60)  # 每1分鐘檢查一次
async def auto_close_available_now():
    """自動關閉開啟超過30分鐘的「現在有空」狀態"""
    await bot.wait_until_ready()
    
    try:
        # 計算30分鐘前的時間
        thirty_minutes_ago = datetime.now(timezone.utc) - timedelta(minutes=30)
        
        with Session() as s:
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
                
                print(f"🕐 自動關閉了 {len(expired_partners)} 個夥伴的「現在有空」狀態")
                for partner in expired_partners:
                    print(f"   - {partner.name} (ID: {partner.id})")
            else:
                pass  # 沒有需要關閉的狀態，不輸出日誌
                
    except Exception as e:
        print(f"❌ 自動關閉「現在有空」狀態時發生錯誤: {e}")

# --- 檢查即時預約並立即創建文字頻道 ---
@tasks.loop(seconds=60)  # 每60秒檢查一次，減少資料庫負載
async def check_instant_bookings_for_text_channel():
    """檢查新的即時預約並立即創建文字頻道"""
    await bot.wait_until_ready()
    
    try:
        # 添加連接重試機制
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with Session() as s:
                    # 查詢即時預約：已確認但還沒有文字頻道的（只處理未來的預約）
                    now = datetime.now(timezone.utc)
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
                        AND b."paymentInfo"->>'isInstantBooking' = 'true'
                        AND b."discordTextChannelId" IS NULL
                        AND s."startTime" > :now
                    """
                    
                    result = s.execute(text(query), {"now": now})
                    rows = result.fetchall()
                    break  # 成功執行，跳出重試循環
            except Exception as db_error:
                if attempt < max_retries - 1:
                    print(f"⚠️ 資料庫連接失敗，重試 {attempt + 1}/{max_retries}: {db_error}")
                    await asyncio.sleep(2 ** attempt)  # 指數退避
                else:
                    print(f"❌ 資料庫連接失敗，已重試 {max_retries} 次: {db_error}")
                    return
            
            if len(rows) > 0:
                print(f"🔍 找到 {len(rows)} 個即時預約需要創建文字頻道")
                for row in rows:
                    print(f"  - 預約ID: {row.id}, 開始時間: {row.startTime}, 狀態: {row.status}")
            # 移除重複的「沒有找到」訊息，避免日誌混亂
            
            for row in rows:
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
                        # 靜默處理無效的 Discord ID
                        customer_member = None
                    
                    try:
                        if partner_discord.replace('.', '').replace('-', '').isdigit():
                            partner_member = guild.get_member(int(float(partner_discord)))
                        else:
                            partner_member = find_member_by_discord_name(guild, partner_discord)
                    except (ValueError, TypeError):
                        # 靜默處理無效的 Discord ID
                        partner_member = None
                    
                    if not customer_member or not partner_member:
                        print(f"⚠️ 找不到成員")
                        continue
                    
                    # 生成頻道名稱
                    start_time = row.startTime
                    end_time = row.endTime
                    cute_item = random.choice(CUTE_ITEMS)
                    
                    start_time_tw = start_time.astimezone(TW_TZ)
                    end_time_tw = end_time.astimezone(TW_TZ)
                    
                    date_str = start_time_tw.strftime("%m%d")  # 改為 1016 格式
                    start_time_str = start_time_tw.strftime("%H:%M")
                    end_time_str = end_time_tw.strftime("%H:%M")
                    
                    text_channel_name = f"📅{date_str} {start_time_str}-{end_time_str} {cute_item}"
                    
                    # 檢查頻道是否已存在
                    existing_channel = discord.utils.get(guild.text_channels, name=text_channel_name)
                    if existing_channel:
                        print(f"⚠️ 文字頻道已存在: {text_channel_name}")
                        continue
                    
                    # 創建文字頻道
                    category = discord.utils.get(guild.categories, name="文字頻道")
                    if not category:
                        category = await guild.create_category("文字頻道")
                    
                    text_channel = await guild.create_text_channel(
                        name=text_channel_name,
                        category=category,
                        overwrites={
                            guild.default_role: discord.PermissionOverwrite(view_channel=False),
                            customer_member: discord.PermissionOverwrite(view_channel=True, send_messages=True),
                            partner_member: discord.PermissionOverwrite(view_channel=True, send_messages=True),
                        }
                    )
                    
                    # 更新資料庫
                    with Session() as update_s:
                        update_s.execute(
                            text("UPDATE \"Booking\" SET \"discordTextChannelId\" = :channel_id WHERE id = :booking_id"),
                            {"channel_id": str(text_channel.id), "booking_id": booking_id}
                        )
                        update_s.commit()
                    
                    # 標記為已處理
                    processed_text_channels.add(booking_id)
                    
                    # 發送歡迎訊息
                    embed = discord.Embed(
                        title="🎮 即時預約溝通頻道",
                        description=f"歡迎 {customer_member.mention} 和 {partner_member.mention}！",
                        color=0x00ff00
                    )
                    embed.add_field(name="預約時間", value=f"{start_time_str} - {end_time_str}", inline=True)
                    embed.add_field(name="⏰ 提醒", value="語音頻道將在預約開始前3分鐘自動創建", inline=False)
                    embed.add_field(name="💬 溝通", value="請在這裡提前溝通遊戲相關事宜", inline=False)
                    
                    await text_channel.send(embed=embed)
                    
                    print(f"✅ 已為即時預約 {booking_id} 創建文字頻道: {text_channel_name}")
                    
                except Exception as e:
                    print(f"❌ 處理即時預約 {row.id} 時發生錯誤: {e}")
                    continue
                    
    except Exception as e:
        print(f"❌ 檢查即時預約時發生錯誤: {e}")

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
            
            # 計算15分鐘前的時間，如果超過1小時則忽略狀態直接清理
            now_minus_15min = now - timedelta(minutes=15)
            now_minus_60min = now - timedelta(minutes=60)
            expired_bookings = s.execute(text(expired_query), {
                "now_time_minus_15min": now_minus_15min,
                "now_time_minus_60min": now_minus_60min
            }).fetchall()
            
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
                        s.execute(
                            text("UPDATE \"Booking\" SET \"discordTextChannelId\" = NULL, \"discordVoiceChannelId\" = NULL WHERE id = :booking_id"),
                            {"booking_id": booking_id}
                        )
                        s.commit()
                        # 已清除預約的頻道ID，減少日誌輸出
                    except Exception as e:
                        print(f"❌ 清除頻道 ID 失敗: {e}")
        
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
        
    except Exception as e:
        print(f"❌ 清理過期頻道時發生錯誤: {e}")

# --- 檢查超時預約任務 ---
@tasks.loop(seconds=60)  # 每1分鐘檢查一次
async def check_booking_timeouts():
    """檢查夥伴回應超時的即時預約並自動取消"""
    await bot.wait_until_ready()
    
    try:
        with Session() as s:
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
            
            if timeout_bookings:
                print(f"🔍 找到 {len(timeout_bookings)} 個超時預約需要處理")
                
                for booking in timeout_bookings:
                    try:
                        booking_id = booking.id
                        partner_id = booking.partner_id
                        partner_name = booking.partner_name
                        customer_name = booking.customer_name
                        
                        # 更新預約狀態為取消
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
                        
                        print(f"❌ 預約 {booking_id} 因夥伴 {partner_name} 未回覆已自動取消")
                        
                        # 檢查是否需要通知管理員（累積3次）
                        partner_result = s.execute(
                            text("SELECT \"noResponseCount\" FROM \"Partner\" WHERE id = :partner_id"),
                            {"partner_id": partner_id}
                        ).fetchone()
                        
                        if partner_result and partner_result[0] >= 3:
                            admin_channel = bot.get_channel(ADMIN_CHANNEL_ID)
                            if admin_channel:
                                await admin_channel.send(
                                    f"⚠️ **夥伴回應超時警告**\n"
                                    f"👤 夥伴: {partner_name}\n"
                                    f"📊 本月未回覆次數: {partner_result[0]} 次\n"
                                    f"🔴 累積達到3次，需要管理員關注！"
                                )
                            print(f"⚠️ 夥伴 {partner_name} 已累積 {partner_result[0]} 次未回覆")
                            
                    except Exception as e:
                        print(f"❌ 處理超時預約 {booking.id} 時發生錯誤: {e}")
                        s.rollback()
        
    except Exception as e:
        print(f"❌ 檢查超時預約時發生錯誤: {e}")

# --- 檢查遺失評價任務 ---
@tasks.loop(seconds=600)  # 每10分鐘檢查一次，減少資料庫負載
async def check_missing_ratings():
    """檢查遺失的評價並自動提交"""
    await bot.wait_until_ready()
    
    try:
        with Session() as s:
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
            
            if missing_ratings:
                print(f"🔍 處理 {len(missing_ratings)} 個遺失評價")
                
                admin_channel = bot.get_channel(ADMIN_CHANNEL_ID)
                if admin_channel:
                    for booking in missing_ratings:
                        try:
                            # 計算結束時間
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
                booking_ids = [b.id for b in missing_ratings]
                s.execute(text("""
                    UPDATE "Booking" 
                    SET "discordVoiceChannelId" = NULL, "discordTextChannelId" = NULL
                    WHERE id = ANY(:booking_ids)
                """), {"booking_ids": booking_ids})
                s.commit()
                
    except Exception as e:
        print(f"❌ 檢查遺失評價時發生錯誤: {e}")

# --- 檢查提領申請任務 ---
@tasks.loop(seconds=60)  # 每分鐘檢查一次
async def check_withdrawal_requests_task():
    """定期檢查新的提領申請並通知管理員"""
    await bot.wait_until_ready()
    await check_withdrawal_requests()

async def check_withdrawal_requests():
    """檢查新的提領申請並通知管理員"""
    try:
        guild = bot.get_guild(GUILD_ID)
        if not guild:
            return
        
        admin_channel = guild.get_channel(ADMIN_CHANNEL_ID)
        if not admin_channel:
            print("❌ 找不到管理員頻道")
            return
        
        # 查詢新的提領申請
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    wr.id, wr.amount, wr."requestedAt",
                    p.name as partner_name, u.email as partner_email,
                    u.discord as partner_discord
                FROM "WithdrawalRequest" wr
                JOIN "Partner" p ON p.id = wr."partnerId"
                JOIN "User" u ON u.id = p."userId"
                WHERE wr.status = 'PENDING'
                ORDER BY wr."requestedAt" ASC
            """))
            
            all_withdrawals = result.fetchall()
            # 在 Python 中過濾已處理的提領申請
            withdrawals = [w for w in all_withdrawals if w[0] not in processed_withdrawals]
            
            for withdrawal in withdrawals:
                withdrawal_id = withdrawal[0]
                amount = withdrawal[1]
                requested_at = withdrawal[2]
                partner_name = withdrawal[3]
                partner_email = withdrawal[4]
                partner_discord = withdrawal[5]
                
                # 獲取夥伴的詳細統計
                stats_result = conn.execute(text("""
                    SELECT 
                        COALESCE(SUM(b."finalAmount"), 0) as total_earnings,
                        COUNT(b.id) as total_orders
                    FROM "Booking" b
                    JOIN "Schedule" s ON s.id = b."scheduleId"
                    WHERE s."partnerId" = (
                        SELECT "partnerId" FROM "WithdrawalRequest" WHERE id = :withdrawal_id
                    )
                    AND b.status IN ('COMPLETED', 'CONFIRMED', 'PAID_WAITING_PARTNER_CONFIRMATION')
                """), {"withdrawal_id": withdrawal_id})
                
                stats = stats_result.fetchone()
                total_earnings = stats[0] if stats else 0
                total_orders = stats[1] if stats else 0
                
                # 計算已提領總額
                withdrawn_result = conn.execute(text("""
                    SELECT COALESCE(SUM(amount), 0) as total_withdrawn
                    FROM "WithdrawalRequest"
                    WHERE "partnerId" = (
                        SELECT "partnerId" FROM "WithdrawalRequest" WHERE id = :withdrawal_id
                    )
                    AND status IN ('APPROVED', 'COMPLETED')
                """), {"withdrawal_id": withdrawal_id})
                
                total_withdrawn = withdrawn_result.fetchone()[0] if withdrawn_result.fetchone() else 0
                available_balance = total_earnings - total_withdrawn
                
                # 獲取最近5筆訂單
                recent_orders_result = conn.execute(text("""
                    SELECT 
                        b."orderNumber", b."finalAmount", b."createdAt",
                        c.name as customer_name,
                        s."startTime", s."endTime"
        FROM "Booking" b
        JOIN "Schedule" s ON s.id = b."scheduleId"
        JOIN "Customer" c ON c.id = b."customerId"
                    WHERE s."partnerId" = (
                        SELECT "partnerId" FROM "WithdrawalRequest" WHERE id = :withdrawal_id
                    )
                    AND b.status IN ('COMPLETED', 'CONFIRMED', 'PAID_WAITING_PARTNER_CONFIRMATION')
                    ORDER BY b."createdAt" DESC
                    LIMIT 5
                """), {"withdrawal_id": withdrawal_id})
                
                recent_orders = recent_orders_result.fetchall()
                
                # 創建 Discord Embed
                embed = discord.Embed(
                    title="💰 新的提領申請",
                    color=0xff6b35,
                    timestamp=datetime.now(timezone.utc)
                )
                
                # 第一行：基本資訊
                embed.add_field(
                    name="👤 夥伴資訊",
                    value=f"**{partner_name}**\n`{partner_email}`\n`{partner_discord}`",
                    inline=True
                )
                embed.add_field(
                    name="💵 提領金額",
                    value=f"**NT$ {amount:,.0f}**",
                    inline=True
                )
                embed.add_field(
                    name="📅 申請時間",
                    value=f"`{requested_at.strftime('%Y/%m/%d %H:%M')}`",
                    inline=True
                )
                
                # 第二行：統計資訊
                embed.add_field(
                    name="📊 收入統計",
                    value=f"**總收入：** NT$ {total_earnings:,.0f}\n**總接單：** {total_orders} 筆\n**可提領：** NT$ {available_balance:,.0f}",
                    inline=True
                )
                embed.add_field(
                    name="🆔 提領ID",
                    value=f"`{withdrawal_id}`",
                    inline=True
                )
                embed.add_field(
                    name="✅ 狀態",
                    value="`待審核`",
                    inline=True
                )
                
                # 第三行：最近訂單
                if recent_orders:
                    recent_orders_text = ""
                    for order in recent_orders[:3]:  # 只顯示最近3筆
                        order_number = order[0] or "無編號"
                        order_amount = order[1]
                        order_date = order[2].strftime('%m/%d %H:%M')
                        customer_name = order[3]
                        recent_orders_text += f"• {order_number}: NT$ {order_amount:,.0f} ({customer_name}) - {order_date}\n"
                    
                    embed.add_field(
                        name="📋 最近訂單",
                        value=recent_orders_text or "無訂單記錄",
                        inline=False
                    )
                
                # 添加審核提醒
                embed.add_field(
                    name="⚠️ 審核提醒",
                    value="請檢查夥伴的接單記錄和收入統計，確認提領金額是否合理。",
                    inline=False
                )
                
                await admin_channel.send(embed=embed)
                
                # 標記為已處理
                processed_withdrawals.add(withdrawal_id)
                # 已發送提領申請通知，減少日誌輸出
                
    except Exception as e:
        print(f"❌ 檢查提領申請時發生錯誤: {e}")

# --- 自動檢查預約任務 ---
@tasks.loop(seconds=CHECK_INTERVAL)
async def check_bookings():
    """定期檢查已付款的預約並創建語音頻道"""
    await bot.wait_until_ready()
    
    try:
        # 減少日誌輸出
        guild = bot.get_guild(GUILD_ID)
        if not guild:
            print("❌ 找不到 Discord 伺服器")
            return
        
        # 查詢已確認且即將開始的預約（只創建語音頻道）
        now = datetime.now(timezone.utc)
        window_start = now - timedelta(minutes=10)  # 擴展到過去10分鐘，處理延遲的情況
        window_end = now + timedelta(minutes=5)  # 5分鐘內即將開始
        
        # 查詢即時預約（夥伴確認後延遲開啟）
        instant_window_start = now - timedelta(minutes=5)  # 擴展到過去5分鐘
        instant_window_end = now + timedelta(minutes=5)  # 5分鐘內即將開始
        
        # 使用原生 SQL 查詢避免 orderNumber 欄位問題
        # 添加檢查：只處理還沒有 Discord 頻道的預約
        # 修改：排除即時預約，避免重複處理
        query = """
            SELECT 
                b.id, b."customerId", b."scheduleId", b.status, b."createdAt", b."updatedAt",
                c.name as customer_name, cu.discord as customer_discord,
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
            AND s."startTime" >= :start_time_1
            AND s."startTime" <= :start_time_2
            AND b."discordVoiceChannelId" IS NULL
            AND s."endTime" > :current_time
            """
            
        # 即時預約查詢
        instant_query = """
            SELECT 
                b.id, b."customerId", b."scheduleId", b.status, b."createdAt", b."updatedAt",
                c.name as customer_name, cu.discord as customer_discord,
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
            AND s."startTime" >= :instant_start_time_1
            AND s."startTime" <= :instant_start_time_2
            AND b."discordVoiceChannelId" IS NULL
            AND s."endTime" > :current_time
        """
        
        try:
            with Session() as s:
                # 查詢一般預約
                result = s.execute(text(query), {"start_time_1": window_start, "start_time_2": window_end, "current_time": now})
                
                # 查詢即時預約
                instant_result = s.execute(text(instant_query), {"instant_start_time_1": instant_window_start, "instant_start_time_2": instant_window_end, "current_time": now})
                
                # 添加調試信息（只在有預約時顯示）
                # print(f"🔍 檢查預約時間窗口: {window_start} 到 {window_end}")
                # print(f"🔍 即時預約時間窗口: {instant_window_start} 到 {instant_window_end}")
                # print(f"🔍 當前時間: {now}")
                
                # 查詢多人開團預約
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
                    AND b."serviceType" = 'GROUP'
                    AND b."groupBookingId" IS NOT NULL
                    AND s."startTime" >= :start_time_1
                    AND s."startTime" <= :start_time_2
                    AND s."endTime" > :current_time
                    AND b."discordVoiceChannelId" IS NULL
                """
                
                group_result = s.execute(text(group_query), {"start_time_1": window_start, "start_time_2": window_end, "current_time": now})
                
                # 合併三種預約
                all_bookings = []
                
                # 處理多人開團預約
                group_bookings = {}
                for row in group_result:
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
                for row in result:
                    general_count += 1
                    booking = type('Booking', (), {
                        'id': row.id,
                        'customerId': row.customerId,
                        'scheduleId': row.scheduleId,
                        'status': row.status,
                        'createdAt': row.createdAt,
                        'updatedAt': row.updatedAt,
                        'customer': type('Customer', (), {
                            'user': type('User', (), {
                                'discord': row.customer_discord
                            })()
                        })(),
                        'schedule': type('Schedule', (), {
                            'startTime': row.startTime,
                            'endTime': row.endTime,
                            'partner': type('Partner', (), {
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
            for row in instant_result:
                instant_count += 1
                booking = type('Booking', (), {
                    'id': row.id,
                    'customerId': row.customerId,
                    'scheduleId': row.scheduleId,
                    'status': row.status,
                    'createdAt': row.createdAt,
                    'updatedAt': row.updatedAt,
                    'customer': type('Customer', (), {
                        'user': type('User', (), {
                            'discord': row.customer_discord
                        })()
                    })(),
                    'schedule': type('Schedule', (), {
                        'startTime': row.startTime,
                        'endTime': row.endTime,
                        'partner': type('Partner', (), {
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
            
            # 只在有預約需要處理時才顯示
            if len(bookings) > 0:
                print(f"🔍 找到 {general_count} 個一般預約，{instant_count} 個即時預約，總共 {len(bookings)} 個預約需要處理")
            
            for booking in bookings:
                try:
                    print(f"🔍 處理預約 {booking.id}: 狀態={booking.status}, 開始時間={booking.schedule.startTime}, 結束時間={booking.schedule.endTime}")
                    
                    # 獲取顧客和夥伴的 Discord 名稱
                    customer_discord = booking.customer.user.discord if booking.customer and booking.customer.user else None
                    
                    # 檢查是否為多人開團預約
                    if hasattr(booking, 'serviceType') and booking.serviceType == 'GROUP':
                        # 多人開團預約
                        partner_discords = [partner['discord'] for partner in booking.schedule.partners]
                        
                        if not customer_discord or not partner_discords:
                            print(f"❌ 多人開團預約 {booking.id} 缺少 Discord 名稱: 顧客={customer_discord}, 夥伴={partner_discords}")
                            continue
                        
                        # 創建多人開團語音頻道
                        vc = await create_group_booking_voice_channel(
                            booking.id,
                            customer_discord,
                            partner_discords,
                            booking.schedule.startTime,
                            booking.schedule.endTime
                        )
                        
                        if vc:
                            print(f"✅ 多人開團語音頻道已創建: {vc.name} (群組 {booking.id})")
                        continue
                    else:
                        # 一般預約
                        partner_discord = booking.schedule.partner.user.discord if booking.schedule and booking.schedule.partner and booking.schedule.partner.user else None
                    
                    if not customer_discord or not partner_discord:
                        print(f"❌ 預約 {booking.id} 缺少 Discord 名稱: 顧客={customer_discord}, 夥伴={partner_discord}")
                        continue
                    
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
                            # 靜默處理無效的 Discord ID，不顯示警告
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
                            # 靜默處理無效的 Discord ID，不顯示警告
                            partner_member = None
                    
                    if not customer_member or not partner_member:
                        print(f"❌ 找不到 Discord 成員: 顧客={customer_discord}, 夥伴={partner_discord}")
                        continue
                    
                    # 計算頻道持續時間
                    duration_minutes = int((booking.schedule.endTime - booking.schedule.startTime).total_seconds() / 60)
                    
                    # 檢查是否為即時預約
                    is_instant_booking = getattr(booking, 'isInstantBooking', None) == 'true'
                    discord_delay_minutes = int(getattr(booking, 'discordDelayMinutes', 0) or 0)
                    
                    # 創建語音頻道（預約時間前 5 分鐘，即時預約延遲開啟）
                    # 確保時間有時區資訊，並轉換為台灣時間
                    if booking.schedule.startTime.tzinfo is None:
                        start_time = booking.schedule.startTime.replace(tzinfo=timezone.utc)
                    else:
                        start_time = booking.schedule.startTime
                    
                    if booking.schedule.endTime.tzinfo is None:
                        end_time = booking.schedule.endTime.replace(tzinfo=timezone.utc)
                    else:
                        end_time = booking.schedule.endTime
                    
                    # 轉換為台灣時間
                    tw_start_time = start_time.astimezone(TW_TZ)
                    tw_end_time = end_time.astimezone(TW_TZ)
                    
                    # 格式化日期和時間
                    date_str = tw_start_time.strftime("%m/%d")
                    start_time_str = tw_start_time.strftime("%H:%M")
                    end_time_str = tw_end_time.strftime("%H:%M")
                     
                    # 創建統一的頻道名稱（與文字頻道相同）
                    # 嘗試從文字頻道名稱中提取相同的 emoji
                    cute_item = "🎀"  # 預設 emoji
                    try:
                        # 查找對應的文字頻道來獲取相同的 emoji
                        time_pattern = f"{date_str} {start_time_str}-{end_time_str}"
                        for channel in guild.text_channels:
                            if time_pattern in channel.name:
                                # 從文字頻道名稱中提取 emoji
                                import re
                                emoji_match = re.search(r'[🎀🦁🐻🐱🐶🐰🐼🦄🍀⭐🎈🍭🌈🦋🐯🐸🦊🐨🐮🐷]', channel.name)
                                if emoji_match:
                                    cute_item = emoji_match.group()
                                    print(f"✅ 從文字頻道 {channel.name} 提取 emoji: {cute_item}")
                                break
                    except Exception as e:
                        print(f"⚠️ 提取 emoji 失敗，使用預設: {e}")
                    
                    if is_instant_booking:
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
                            continue
                    
                    vc = await guild.create_voice_channel(
                        name=channel_name, 
                        overwrites=overwrites, 
                        user_limit=2, 
                        category=category
                    )
                    
                    # 保留文字頻道用於評價系統，不刪除
                    text_channel = None
                    try:
                        # 查找對應的文字頻道
                        text_channel_name = f"📅{date_str} {start_time_str}-{end_time_str} {cute_item}"
                        text_channel = discord.utils.get(guild.text_channels, name=text_channel_name)
                        
                        if text_channel:
                            print(f"✅ 找到文字頻道: {text_channel_name} (保留用於評價系統)")
                        else:
                            print(f"⚠️ 找不到對應的文字頻道: {text_channel_name}")
                    except Exception as e:
                        print(f"❌ 查找文字頻道失敗: {e}")
                    
                    # 創建配對記錄 - 檢查是否已存在
                    user1_id = str(customer_member.id)
                    user2_id = str(partner_member.id)
                    
                    try:
                        # 先檢查是否已經存在配對記錄
                        existing_record = s.execute(
                            text("SELECT id FROM \"PairingRecord\" WHERE \"bookingId\" = :booking_id"),
                            {"booking_id": booking.id}
                        ).fetchone()
                        
                        if existing_record:
                            record_id = existing_record[0]
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
                                bookingId=booking.id
                            )
                            s.add(record)
                            s.commit()
                            print(f"✅ 創建新配對記錄: {record_id}")
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
                        'record_id': record_id,  # 使用保存的 ID
                        'vc': vc,
                        'booking_id': booking.id
                    }
                    
                    # 保存語音頻道 ID 到資料庫
                    try:
                        with Session() as save_s:
                            # 先檢查欄位是否存在
                            check_column = save_s.execute(text("""
                                SELECT column_name 
                                FROM information_schema.columns 
                                WHERE table_name = 'Booking' 
                                AND column_name = 'discordVoiceChannelId'
                            """)).fetchone()
                            
                            if check_column:
                                # 更新預約記錄，保存 Discord 語音頻道 ID
                                save_s.execute(
                                    text("UPDATE \"Booking\" SET \"discordVoiceChannelId\" = :channel_id WHERE id = :booking_id"),
                                    {"channel_id": str(vc.id), "booking_id": booking.id}
                                )
                                save_s.commit()
                                # 已保存語音頻道ID，減少日誌輸出
                            else:
                                print(f"⚠️ Discord 語音頻道欄位尚未創建，跳過保存頻道 ID")
                    except Exception as db_error:
                        print(f"❌ 保存語音頻道 ID 到資料庫失敗: {db_error}")
                    
                    # 標記為已處理
                    processed_bookings.add(booking.id)
                    
                    if is_instant_booking:
                        print(f"⚡ 即時預約語音頻道已創建: {channel_name} (預約 {booking.id})")
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
                                value=f"`{booking.id}`",
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
                            await asyncio.sleep(discord_delay_minutes * 60)  # 等待指定分鐘數
                            try:
                                # 檢查預約狀態是否仍然是 PARTNER_ACCEPTED
                                with Session() as check_s:
                                    current_booking = check_s.execute(
                                        text("SELECT status FROM \"Booking\" WHERE id = :booking_id"),
                                        {"booking_id": booking.id}
                                    ).fetchone()
                                    
                                    if current_booking and current_booking.status == 'CONFIRMED':
                                        # 開啟語音頻道
                                        await vc.set_permissions(guild.default_role, view_channel=True)
                                        
                                        # 保留文字頻道用於評價系統，不刪除
                                        try:
                                            # 查找對應的文字頻道
                                            text_channel_name = f"📅{date_str} {start_time_str}-{end_time_str} {cute_item}"
                                            text_channel = discord.utils.get(guild.text_channels, name=text_channel_name)
                                            
                                            if text_channel:
                                                print(f"✅ 找到即時預約文字頻道: {text_channel_name} (保留用於評價系統)")
                                            else:
                                                print(f"⚠️ 找不到對應的即時預約文字頻道: {text_channel_name}")
                                        except Exception as e:
                                            print(f"❌ 查找即時預約文字頻道失敗: {e}")
                                        
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
                                        print(f"⚠️ 預約 {booking.id} 狀態已改變，取消延遲開啟")
                            except Exception as e:
                                print(f"❌ 延遲開啟語音頻道失敗: {e}")
                        
                        # 啟動延遲開啟任務
                        bot.loop.create_task(delayed_open_voice())
                        
                        # 啟動倒數計時任務（包含評價系統）
                        if text_channel:
                            bot.loop.create_task(countdown_with_rating(
                                vc.id, channel_name, text_channel, vc, 
                                [customer_member, partner_member], 
                                [customer_member, partner_member], 
                                record_id, booking.id
                            ))
                        
                    else:
                        # 通知創建頻道頻道 - 修正時區顯示
                        channel_creation_channel = bot.get_channel(CHANNEL_CREATION_CHANNEL_ID)
                        if channel_creation_channel:
                             # 確保時間有時區資訊，並轉換為台灣時間
                             if booking.schedule.startTime.tzinfo is None:
                                 start_time = booking.schedule.startTime.replace(tzinfo=timezone.utc)
                             else:
                                 start_time = booking.schedule.startTime
                             
                             tw_start_time = start_time.astimezone(TW_TZ)
                             start_time_str = tw_start_time.strftime("%Y/%m/%d %H:%M")
                             
                             await channel_creation_channel.send(
                                 f"🎉 自動創建語音頻道：\n"
                                 f"📋 預約ID: {booking.id}\n"
                                 f"👤 顧客: {customer_member.mention} ({customer_discord})\n"
                                 f"👥 夥伴: {partner_member.mention} ({partner_discord})\n"
                                 f"⏰ 開始時間: {start_time_str}\n"
                                 f"⏱️ 時長: {duration_minutes} 分鐘\n"
                                 f"🎮 頻道: {vc.mention}"
                             )
                        
                        # 啟動倒數計時 - 需要找到對應的文字頻道
                        # 查找對應的文字頻道
                        text_channel = None
                        # 使用更靈活的匹配方式
                        time_pattern = f"{date_str} {start_time_str}-{end_time_str}"
                        
                        for channel in guild.text_channels:
                            # 檢查頻道名稱是否包含時間模式
                            if time_pattern in channel.name:
                                text_channel = channel
                                print(f"✅ 找到對應的文字頻道: {channel.name}")
                                break
                        
                        if text_channel:
                            # 啟動倒數計時和評價系統
                            bot.loop.create_task(
                                countdown_with_rating(vc.id, channel_name, text_channel, vc, None, [customer_member, partner_member], record_id, booking.id)
                            )
                            # 已啟動倒數計時和評價系統，減少日誌輸出
                        else:
                            print(f"⚠️ 找不到對應的文字頻道: {channel_name}")
                            # 如果找不到文字頻道，創建一個臨時的
                            try:
                                text_channel = await guild.create_text_channel(
                                    name=f"📅{date_str} {start_time_str}-{end_time_str} {cute_item}",
                                    overwrites={
                                        guild.default_role: discord.PermissionOverwrite(view_channel=False),
                                        customer_member: discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True),
                                        partner_member: discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True),
                                    },
                                    category=category
                                )
                                # 創建臨時文字頻道成功，減少日誌輸出
                                
                                # 啟動倒數計時和評價系統
                                bot.loop.create_task(
                                    countdown_with_rating(vc.id, channel_name, text_channel, vc, None, [customer_member, partner_member], record_id, booking.id)
                                )
                                # 已啟動倒數計時和評價系統，減少日誌輸出
                            except Exception as e:
                                print(f"❌ 創建臨時文字頻道失敗: {e}")
                         
                        print(f"✅ 自動創建頻道成功: {channel_name} for booking {booking.id}")
                    
                except Exception as e:
                    print(f"❌ 處理預約 {booking.id} 時發生錯誤: {e}")
                    continue
                    
        except Exception as db_error:
            print(f"❌ 資料庫查詢失敗: {db_error}")
            # 嘗試重新建立連接
            try:
                engine.dispose()
                print("🔄 重新建立資料庫連接...")
                return  # 跳過這次檢查，等待下次重試
            except Exception as reconnect_error:
                print(f"❌ 重新連接失敗: {reconnect_error}")
                return
                    
    except Exception as e:
        print(f"❌ 檢查預約時發生錯誤: {e}")

# --- 檢查即時預約的定時功能 ---
@tasks.loop(seconds=120)  # 每2分鐘檢查一次，減少資料庫負載
async def check_instant_booking_timing():
    """檢查即時預約的定時功能：10分鐘提醒、5分鐘延長按鈕、評價系統、頻道刪除"""
    await bot.wait_until_ready()
    
    try:
        guild = bot.get_guild(GUILD_ID)
        if not guild:
            print("❌ 找不到 Discord 伺服器")
            return
        
        now = datetime.now(timezone.utc)
        session = Session()
        
        # 1. 檢查需要顯示10分鐘提醒的即時預約
        ten_minutes_later = now + timedelta(minutes=10)
        bookings_10min = session.execute(text("""
            SELECT b.id, b."discordTextChannelId", s."endTime", c.name as customer_name, p.name as partner_name
            FROM "Booking" b
            JOIN "Schedule" s ON b."scheduleId" = s.id
            JOIN "Customer" c ON b."customerId" = c.id
            JOIN "Partner" p ON s."partnerId" = p.id
            WHERE b.status = 'CONFIRMED'
            AND b."isInstantBooking" = true
            AND b."discordTextChannelId" IS NOT NULL
            AND b."tenMinuteReminderShown" = false
            AND s."endTime" <= :ten_minutes_later
            AND s."endTime" > :now
        """), {'ten_minutes_later': ten_minutes_later, 'now': now}).fetchall()
        
        for booking in bookings_10min:
            try:
                text_channel = guild.get_channel(int(booking.discordTextChannelId))
                if text_channel:
                    embed = discord.Embed(
                        title="⏰ 預約提醒",
                        description="預約還有 10 分鐘結束，請準備結束遊戲。",
                        color=0xff9900
                    )
                    await text_channel.send(embed=embed)
                    
                    # 更新資料庫
                    session.execute(text("""
                        UPDATE "Booking" 
                        SET "tenMinuteReminderShown" = true
                        WHERE id = :booking_id
                    """), {'booking_id': booking.id})
                    session.commit()
                    
                    print(f"✅ 顯示10分鐘提醒: {booking.id}")
            except Exception as e:
                print(f"❌ 處理10分鐘提醒 {booking.id} 時發生錯誤: {e}")
        
        # 2. 檢查需要顯示5分鐘延長按鈕的即時預約
        five_minutes_later = now + timedelta(minutes=5)
        bookings_5min = session.execute(text("""
            SELECT b.id, b."discordTextChannelId", s."endTime", c.name as customer_name, p.name as partner_name
            FROM "Booking" b
            JOIN "Schedule" s ON b."scheduleId" = s.id
            JOIN "Customer" c ON b."customerId" = c.id
            JOIN "Partner" p ON s."partnerId" = p.id
            WHERE b.status = 'CONFIRMED'
            AND b."isInstantBooking" = true
            AND b."discordTextChannelId" IS NOT NULL
            AND b."extensionButtonShown" = false
            AND s."endTime" <= :five_minutes_later
            AND s."endTime" > :now
        """), {'five_minutes_later': five_minutes_later, 'now': now}).fetchall()
        
        for booking in bookings_5min:
            try:
                text_channel = guild.get_channel(int(booking.discordTextChannelId))
                if text_channel:
                    embed = discord.Embed(
                        title="⏰ 預約即將結束",
                        description="預約還有 5 分鐘結束，是否需要延長 5 分鐘？",
                        color=0xff9900
                    )
                    
                    view = discord.ui.View()
                    extend_button = discord.ui.Button(
                        label="延長 5 分鐘",
                        style=discord.ButtonStyle.primary,
                        custom_id=f"extend_instant_booking_{booking.id}"
                    )
                    view.add_item(extend_button)
                    
                    await text_channel.send(embed=embed, view=view)
                    
                    # 更新資料庫
                    session.execute(text("""
                        UPDATE "Booking" 
                        SET "extensionButtonShown" = true
                        WHERE id = :booking_id
                    """), {'booking_id': booking.id})
                    session.commit()
                    
                    print(f"✅ 顯示5分鐘延長按鈕: {booking.id}")
            except Exception as e:
                print(f"❌ 處理5分鐘延長按鈕 {booking.id} 時發生錯誤: {e}")
        
        # 3. 檢查需要結束的即時預約（時間結束）
        bookings_ended = session.execute(text("""
            SELECT b.id, b."discordVoiceChannelId", b."discordTextChannelId", b."ratingCompleted",
                   c.name as customer_name, p.name as partner_name, s."endTime"
            FROM "Booking" b
            JOIN "Customer" c ON b."customerId" = c.id
            JOIN "Schedule" s ON b."scheduleId" = s.id
            JOIN "Partner" p ON s."partnerId" = p.id
            WHERE b.status = 'CONFIRMED'
            AND b."isInstantBooking" = true
            AND b."discordVoiceChannelId" IS NOT NULL
            AND s."endTime" <= :now
        """), {'now': now}).fetchall()
        
        for booking in bookings_ended:
            try:
                # 刪除語音頻道
                if booking.discordVoiceChannelId:
                    voice_channel = guild.get_channel(int(booking.discordVoiceChannelId))
                    if voice_channel:
                        await voice_channel.delete()
                        print(f"✅ 刪除即時預約語音頻道: {booking.id}")
                
                # 在文字頻道顯示評價系統
                text_channel = guild.get_channel(int(booking.discordTextChannelId))
                if text_channel:
                    embed = discord.Embed(
                        title="⭐ 即時預約結束 - 請給予評價",
                        description="即時預約已結束，請為您的遊戲體驗給予評價。",
                        color=0x00ff88
                    )
                    embed.add_field(name="顧客", value=f"@{booking.customer_name}", inline=True)
                    embed.add_field(name="夥伴", value=f"@{booking.partner_name}", inline=True)
                    embed.add_field(name="評價說明", value="請點擊下方的星等按鈕來評價這次的遊戲體驗。", inline=False)
                    
                    # 創建評價視圖
                    view = discord.ui.View()
                    for i in range(1, 6):
                        star_button = discord.ui.Button(
                            label=f"{i}⭐",
                            style=discord.ButtonStyle.secondary,
                            custom_id=f"rate_instant_{booking.id}_{i}"
                        )
                        view.add_item(star_button)
                    
                    await text_channel.send(embed=embed, view=view)
                
                # 更新資料庫狀態
                session.execute(text("""
                    UPDATE "Booking" 
                    SET status = 'COMPLETED',
                        "discordVoiceChannelId" = NULL
                    WHERE id = :booking_id
                """), {'booking_id': booking.id})
                session.commit()
                
                print(f"✅ 即時預約結束並顯示評價系統: {booking.id}")
                
            except Exception as e:
                print(f"❌ 處理即時預約結束 {booking.id} 時發生錯誤: {e}")
        
        # 4. 檢查需要清理文字頻道的即時預約（評價完成後）
        bookings_cleanup = session.execute(text("""
            SELECT b.id, b."discordTextChannelId", b."ratingCompleted", b."textChannelCleaned"
            FROM "Booking" b
            WHERE b."ratingCompleted" = true
            AND b."isInstantBooking" = true
            AND b."textChannelCleaned" = false
            AND b."discordTextChannelId" IS NOT NULL
        """)).fetchall()
        
        for booking in bookings_cleanup:
            try:
                # 刪除文字頻道
                text_channel = guild.get_channel(int(booking.discordTextChannelId))
                if text_channel and not text_channel.deleted:
                    await text_channel.delete()
                    print(f"✅ 刪除即時預約文字頻道: {booking.id}")
                
                # 更新資料庫
                session.execute(text("""
                    UPDATE "Booking" 
                    SET "textChannelCleaned" = true
                    WHERE id = :booking_id
                """), {'booking_id': booking.id})
                session.commit()
                
            except Exception as e:
                print(f"❌ 清理即時預約文字頻道 {booking.id} 時發生錯誤: {e}")
        
        session.close()
        
    except Exception as e:
        print(f"❌ 檢查即時預約定時功能時發生錯誤: {e}")

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
class RatingModal(Modal, title="匿名評分與留言"):
    rating = TextInput(label="給予評分（1～5 星）", required=True)
    comment = TextInput(label="留下你的留言（選填）", required=False)

    def __init__(self, record_id):
        super().__init__()
        self.record_id = record_id

    async def on_submit(self, interaction: discord.Interaction):
        try:
            print(f"🔍 收到評價提交: record_id={self.record_id}, rating={self.rating}, comment={self.comment}")
            
            # 使用新的 session 來避免連接問題
            with Session() as s:
                record = s.get(PairingRecord, self.record_id)
                if not record:
                    print(f"❌ 找不到配對記錄: {self.record_id}")
                    await interaction.response.send_message("❌ 找不到配對記錄", ephemeral=True)
                    return
                
                # 在 session 內獲取需要的資料
                user1_id = record.user1Id
                user2_id = record.user2Id
                
                # 配對記錄資訊，減少日誌輸出
                
                record.rating = int(str(self.rating))
                record.comment = str(self.comment)
                s.commit()
                # 評價已保存到資料庫，減少日誌輸出
            
            await interaction.response.send_message("✅ 感謝你的匿名評價！", ephemeral=True)

            if self.record_id not in pending_ratings:
                pending_ratings[self.record_id] = []
            
            rating_data = {
                'rating': int(str(self.rating)),
                'comment': str(self.comment),
                'user1': str(interaction.user.id),
                'user2': str(user2_id if str(interaction.user.id) == user1_id else user1_id)
            }
            pending_ratings[self.record_id].append(rating_data)
            print(f"✅ 評價已添加到待處理列表: {rating_data}")

            # 立即發送評價到管理員頻道
            await send_rating_to_admin(self.record_id, rating_data, user1_id, user2_id)

            evaluated_records.add(self.record_id)
            print(f"✅ 評價流程完成")
        except Exception as e:
            print(f"❌ 評分提交錯誤: {e}")
            import traceback
            traceback.print_exc()
            try:
                await interaction.response.send_message("❌ 提交失敗，請稍後再試", ephemeral=True)
            except:
                # 如果已經回應過，就忽略錯誤
                pass

# --- 延長按鈕 ---
class Extend5MinView(View):
    def __init__(self, booking_id, vc, channel_name, text_channel):
        super().__init__(timeout=300)  # 5分鐘超時
        self.booking_id = booking_id
        self.vc = vc
        self.vc_id = vc.id  # 添加 vc_id 屬性
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
                # 延長5分鐘
                s.execute(text("""
                    UPDATE "Schedule" 
                    SET "endTime" = "endTime" + INTERVAL '5 minutes'
                    WHERE id = (
                        SELECT "scheduleId" FROM "Booking" WHERE id = :booking_id
                    )
                """), {"booking_id": self.booking_id})
                s.commit()
            
            # 標記為已延長
            self.extended = True
            
            # 更新 active_voice_channels 中的剩餘時間（延長5分鐘 = 300秒）
            if hasattr(self, 'vc_id') and self.vc_id in active_voice_channels:
                active_voice_channels[self.vc_id]['remaining'] += 300  # 延長5分鐘
                active_voice_channels[self.vc_id]['extended'] += 1
                print(f"✅ 已更新 active_voice_channels 中的頻道 {self.vc_id}，延長5分鐘")
            
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

class RatingView(View):
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
        self.ratings[user_id] = rating
        
        # 直接彈出包含星等和評論的模態對話框
        modal = RatingModal(rating, self.booking_id, self)
        await interaction.response.send_modal(modal)


class RatingModal(discord.ui.Modal):
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
            for attempt in range(max_retries):
                try:
                    with Session() as s:
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
                        break  # 成功則跳出重試循環
                except Exception as db_error:
                    print(f"❌ 資料庫查詢失敗 (嘗試 {attempt + 1}/{max_retries}): {db_error}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(1)  # 等待1秒後重試
                        continue
                    else:
                        raise db_error  # 最後一次嘗試失敗，拋出錯誤
                
            if result:
                # 發送到管理員頻道
                admin_channel = bot.get_channel(ADMIN_CHANNEL_ID)
                if admin_channel:
                    await admin_channel.send(
                        f"**{result.customer_name}** 評價 **{result.partner_name}**\n"
                        f"⭐ {'⭐' * self.rating}\n"
                        f"💬 {comment}"
                    )
                    print(f"✅ 評價已發送到管理員頻道: {result.customer_name} → {result.partner_name} ({self.rating}⭐)")
                
                # 標記用戶已提交評價
                self.parent_view.submitted_users.add(interaction.user.id)
                
                # 確認收到評價
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
            print("❌ 找不到 Discord 伺服器")
            return
        
        print("🔍 開始清理重複頻道...")
        
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
                print(f"🔍 發現重複頻道: {name} (共 {len(channels)} 個)")
                # 保留第一個，刪除其他的
                for i, channel in enumerate(channels[1:], 1):
                    duplicate_channels.append(channel)
                    print(f"  - 將刪除: {channel.name} (ID: {channel.id})")
        
        if not duplicate_channels:
            print("✅ 沒有發現重複頻道")
        else:
            print(f"🗑️ 準備刪除 {len(duplicate_channels)} 個重複頻道...")
            
            # 刪除重複頻道
            deleted_count = 0
            for channel in duplicate_channels:
                try:
                    await channel.delete()
                    deleted_count += 1
                    # 已刪除頻道，減少日誌輸出
                except Exception as e:
                    print(f"❌ 刪除頻道失敗 {channel.name}: {e}")
            
            print(f"🎉 清理完成！共刪除 {deleted_count} 個重複頻道")
            
    except Exception as e:
        print(f"❌ 清理重複頻道時發生錯誤: {e}")

@bot.event
async def on_ready():
    print(f"✅ Bot 上線：{bot.user}")
    try:
        guild = discord.Object(id=GUILD_ID)
        synced = await bot.tree.sync(guild=guild)
        print(f"✅ Slash 指令已同步：{len(synced)} 個指令")
        
        # 清理重複頻道
        await cleanup_duplicate_channels()
        
        # 啟動自動檢查任務
        check_bookings.start()
        check_new_bookings.start()
        check_instant_bookings_for_text_channel.start()  # 新增：即時預約文字頻道
        check_instant_booking_timing.start()  # 新增：即時預約定時功能
        cleanup_expired_channels.start()
        auto_close_available_now.start()
        check_booking_timeouts.start()  # 新增：檢查超時預約
        check_missing_ratings.start()
        check_withdrawal_requests_task.start()
        print(f"✅ 所有自動任務已啟動")
    except Exception as e:
        print(f"❌ 指令同步失敗: {e}")

@bot.event
async def on_message(message):
    if message.author == bot.user:
        return
    
    # 評價系統現在使用按鈕和模態對話框，不需要處理文字訊息
    
    if message.content == "!ping":
        await message.channel.send("Pong!")
    await bot.process_commands(message)


# --- 倒數邏輯 ---
async def countdown_with_rating(vc_id, channel_name, text_channel, vc, mentioned, members, record_id, booking_id):
    """倒數計時函數，包含評價系統"""
    try:
        # 計算預約結束時間
        now = datetime.now(timezone.utc)
        
        # 從資料庫獲取預約結束時間
        with Session() as s:
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
        
        # 計算等待時間
        wait_seconds = (end_time - now).total_seconds()
        
        if wait_seconds > 0:
            # 移除冗餘的等待日誌
            
            # 檢查是否需要在結束前5分鐘提醒
            if wait_seconds > 300:  # 如果還有超過5分鐘
                # 等待到結束前5分鐘
                await asyncio.sleep(wait_seconds - 300)
                
                # 發送5分鐘提醒和延長按鈕
                await send_5min_reminder(text_channel, booking_id, vc, channel_name)
                
                # 等待剩餘的5分鐘
                await asyncio.sleep(300)
            else:
                # 如果已經少於5分鐘，直接等待結束
                await asyncio.sleep(wait_seconds)
        
        # 預約時間結束，關閉語音頻道
        try:
            await vc.delete()
            print(f"✅ 已關閉語音頻道: {channel_name}")
        except Exception as e:
            print(f"❌ 關閉語音頻道失敗: {e}")
        
        # 檢查是否已經發送過評價系統
        if booking_id not in rating_sent_bookings:
            # 在文字頻道顯示評價系統
            view = RatingView(booking_id)
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
    """10分鐘後自動提交未完成的評價"""
    try:
        # 獲取顧客和夥伴信息
        with Session() as s:
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
            """), {"booking_id": booking_id}).fetchone()
            
            if result:
                # 檢查是否有 RatingView 實例
                rating_view = None
                # 使用 persistent_views 或直接跳過檢查
                try:
                    for view in bot.persistent_views:
                        if hasattr(view, 'booking_id') and view.booking_id == booking_id:
                            rating_view = view
                            break
                except AttributeError:
                    # 如果 persistent_views 不存在，直接跳過
                    rating_view = None
                
                # 發送到管理員頻道 - 未評價
                admin_channel = bot.get_channel(ADMIN_CHANNEL_ID)
                if admin_channel:
                    # 檢查是否有用戶已經提交了評價
                    if rating_view and rating_view.submitted_users:
                        # 有部分用戶已提交，只為未提交的用戶發送
                        await admin_channel.send(
                            f"**{result.customer_name}** 評價 **{result.partner_name}**\n"
                            f"⭐ 部分用戶未評價\n"
                            f"💬 部分顧客未填寫評價"
                        )
                        print(f"✅ 自動提交部分未評價到管理員頻道: {result.customer_name} → {result.partner_name}")
                    else:
                        # 沒有用戶提交評價
                        await admin_channel.send(
                            f"**{result.customer_name}** 評價 **{result.partner_name}**\n"
                            f"⭐ 未評價\n"
                            f"💬 顧客未填寫評價"
                        )
                        print(f"✅ 自動提交未評價到管理員頻道: {result.customer_name} → {result.partner_name}")
                
                # 在文字頻道發送通知
                await text_channel.send(
                    "⏰ 評價時間已結束，感謝您的使用！\n"
                    "如果您想提供評價，請聯繫管理員。"
                )
            else:
                print(f"❌ 找不到預約 {booking_id} 的記錄")
                
    except Exception as e:
        print(f"❌ 自動提交評價失敗: {e}")

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
            # 在文字頻道顯示評價系統
            view = RatingView(booking_id)
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

        view = ExtendView(vc.id)
        await text_channel.send(f"🎉 語音頻道 {vc.name} 已開啟！\n⏳ 可延長5分鐘 ( 為了您有更好的遊戲體驗，請到最後需要時再點選 ) 。", view=view)

        while active_voice_channels[vc_id]['remaining'] > 0:
            remaining = active_voice_channels[vc_id]['remaining']
            if remaining == 60:
                await text_channel.send("⏰ 剩餘 1 分鐘。")
            await asyncio.sleep(1)
            active_voice_channels[vc_id]['remaining'] -= 1

        await vc.delete()
        print(f"🎯 語音頻道已刪除，開始評價流程: record_id={record_id}")
        
        # 創建臨時評價頻道（因為預約前的溝通頻道已經被刪除）
        try:
            # 從 members 中提取 customer_member 和 partner_member
            customer_member = None
            partner_member = None
            if members and len(members) >= 2:
                customer_member = members[0]  # 假設第一個是顧客
                partner_member = members[1]   # 假設第二個是夥伴
            
            # 查找語音頻道所屬的分類
            category = vc.category if vc.category else None
            if not category:
                category = discord.utils.get(guild.categories, name="Voice Channels")
            if not category:
                category = discord.utils.get(guild.categories, name="語音頻道")
            
            # 創建臨時評價頻道
            evaluation_channel_name = f"📝評價-{vc.name.replace('📅', '').replace('⚡即時', '')}"
            
            # 設置頻道權限
            overwrites = {
                guild.default_role: discord.PermissionOverwrite(view_channel=False),
            }
            
            # 添加成員權限（如果成員存在）
            if customer_member:
                overwrites[customer_member] = discord.PermissionOverwrite(view_channel=True, send_messages=True)
            if partner_member:
                overwrites[partner_member] = discord.PermissionOverwrite(view_channel=True, send_messages=True)
            
            evaluation_channel = await guild.create_text_channel(
                name=evaluation_channel_name,
                category=category,
                overwrites=overwrites
            )
            
            # 發送評價提示訊息
            embed = discord.Embed(
                title="⭐ 預約結束 - 請進行評價",
                description="感謝您使用 PeiPlay 服務！請花一點時間為您的夥伴進行匿名評價。",
                color=0xffd700
            )
            embed.add_field(
                name="📝 評價說明",
                value="• 評分範圍：1-5 星\n• 留言為選填項目\n• 評價完全匿名\n• 評價結果會回報給管理員",
                inline=False
            )
            embed.set_footer(text="評價有助於我們提供更好的服務品質")
            
            await evaluation_channel.send(embed=embed)
            await evaluation_channel.send("📝 請點擊以下按鈕進行匿名評分：")
            
            # 更新 text_channel 變數為新的評價頻道
            text_channel = evaluation_channel
            
        except Exception as e:
            print(f"❌ 創建評價頻道失敗: {e}")
            return

        class SubmitButton(View):
            def __init__(self):
                super().__init__(timeout=600)  # 延長到10分鐘
                self.clicked = False

            @discord.ui.button(label="⭐ 匿名評分", style=discord.ButtonStyle.success, emoji="⭐")
            async def submit(self, interaction: discord.Interaction, button: Button):
                print(f"🔍 用戶 {interaction.user.id} 點擊了評價按鈕")
                if self.clicked:
                    await interaction.response.send_message("❗ 已提交過評價。", ephemeral=True)
                    return
                self.clicked = True
                await interaction.response.send_modal(RatingModal(record_id))

        await text_channel.send(view=SubmitButton())
        print(f"⏰ 評價按鈕已發送，等待 600 秒後刪除文字頻道")
        await asyncio.sleep(600)  # 延長到10分鐘，給用戶更多時間評價
        await text_channel.delete()
        print(f"🗑️ 文字頻道已刪除，評價流程結束")

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

        admin = bot.get_channel(ADMIN_CHANNEL_ID)
        if admin:
            try:
                # 嘗試獲取用戶資訊，如果失敗則使用用戶 ID
                try:
                    u1 = await bot.fetch_user(int(user1_id))
                    user1_display = u1.mention
                except:
                    user1_display = f"<@{user1_id}>"
                
                try:
                    u2 = await bot.fetch_user(int(user2_id))
                    user2_display = u2.mention
                except:
                    user2_display = f"<@{user2_id}>"
                
                header = f"📋 配對紀錄：{user1_display} × {user2_display} | {duration//60} 分鐘 | 延長 {extended_times} 次"
                
                if booking_id:
                    header += f" | 預約ID: {booking_id}"

                if record_id in pending_ratings:
                    feedback = "\n⭐ 評價回饋："
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
                        if r['comment']:
                            feedback += f"\n  💬 {r['comment']}"
                    del pending_ratings[record_id]
                    await admin.send(f"{header}{feedback}")
                else:
                    await admin.send(f"{header}\n⭐ 沒有收到任何評價。")
            except Exception as e:
                print(f"推送管理區評價失敗：{e}")
                # 如果完全失敗，至少顯示基本的配對資訊
                try:
                    basic_header = f"📋 配對紀錄：用戶 {user1_id} × 用戶 {user2_id} | {duration//60} 分鐘 | 延長 {extended_times} 次"
                    if booking_id:
                        basic_header += f" | 預約ID: {booking_id}"
                    await admin.send(f"{basic_header}\n⭐ 沒有收到任何評價。")
                except:
                    pass

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

    with Session() as s:
        blocked_ids = [b.blocked_id for b in s.query(BlockRecord).filter(BlockRecord.blocker_id == str(interaction.user.id)).all()]
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
        text_channel = await interaction.guild.create_text_channel(name="🔒匿名文字區", overwrites=overwrites, category=category)

        with Session() as s:
            # 確保記錄兩個不同的用戶
            user1_id = str(interaction.user.id)
            user2_id = str(mentioned[0].id)
            
            # 添加調試信息
            print(f"🔍 創建配對記錄: {user1_id} × {user2_id}")
            
            record = PairingRecord(
                user1Id=user1_id,
                user2Id=user2_id,
                duration=minutes * 60,
                animalName=animal
            )
            s.add(record)
            s.commit()
            record_id = record.id  # 保存 ID，避免 Session 關閉後無法訪問

        active_voice_channels[vc.id] = {
            'text_channel': text_channel,
            'remaining': minutes * 60,
            'extended': 0,
            'record_id': record_id,  # 使用保存的 ID
            'vc': vc
        }

        await countdown(vc.id, animal_channel_name, text_channel, vc, interaction, mentioned, record_id)

    bot.loop.create_task(countdown_wrapper())

# --- 其他 Slash 指令 ---
@bot.tree.command(name="viewblocklist", description="查看你封鎖的使用者", guild=discord.Object(id=GUILD_ID))
async def view_blocklist(interaction: discord.Interaction):
    with Session() as s:
        blocks = s.query(BlockRecord).filter(BlockRecord.blocker_id == str(interaction.user.id)).all()
        if not blocks:
            await interaction.response.send_message("📭 你尚未封鎖任何人。", ephemeral=True)
            return
        blocked_mentions = [f"<@{b.blocked_id}>" for b in blocks]
        await interaction.response.send_message(f"🔒 你封鎖的使用者：\n" + "\n".join(blocked_mentions), ephemeral=True)

@bot.tree.command(name="unblock", description="解除你封鎖的某人", guild=discord.Object(id=GUILD_ID))
@app_commands.describe(member="要解除封鎖的使用者")
async def unblock(interaction: discord.Interaction, member: discord.Member):
    with Session() as s:
        block = s.query(BlockRecord).filter_by(blocker_id=str(interaction.user.id), blocked_id=str(member.id)).first()
        if block:
            s.delete(block)
            s.commit()
            await interaction.response.send_message(f"✅ 已解除對 <@{member.id}> 的封鎖。", ephemeral=True)
        else:
            await interaction.response.send_message("❗ 你沒有封鎖這位使用者。", ephemeral=True)

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

            # 創建文字頻道（立即創建）
            text_channel = await guild.create_text_channel(
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
import os
import asyncio
import random
import discord
from discord.ext import commands, tasks
from discord import app_commands
from discord.ui import View, Button, Modal, TextInput
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Enum, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from datetime import datetime, timedelta, timezone
from flask import Flask, request, jsonify
import threading

# --- 環境與資料庫設定 ---
load_dotenv()
TOKEN = os.getenv("DISCORD_BOT_TOKEN")
GUILD_ID = int(os.getenv("DISCORD_GUILD_ID", "0"))
POSTGRES_CONN = os.getenv("POSTGRES_CONN")
ADMIN_CHANNEL_ID = int(os.getenv("ADMIN_CHANNEL_ID", "0"))
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "30")) 

Base = declarative_base()
engine = create_engine(POSTGRES_CONN)
Session = sessionmaker(bind=engine)
session = Session()

# --- 資料表模型 ---
class User(Base):
    __tablename__ = 'User'
    id = Column(String, primary_key=True)
    discord = Column(String)  # Discord 名稱

class Customer(Base):
    __tablename__ = 'Customer'
    id = Column(String, primary_key=True)
    userId = Column(String, ForeignKey('User.id'))
    user = relationship("User")

class Partner(Base):
    __tablename__ = 'Partner'
    id = Column(String, primary_key=True)
    userId = Column(String, ForeignKey('User.id'))
    user = relationship("User")

class Schedule(Base):
    __tablename__ = 'Schedule'
    id = Column(String, primary_key=True)
    partnerId = Column(String, ForeignKey('Partner.id'))
    startTime = Column(DateTime)
    partner = relationship("Partner")

class Booking(Base):
    __tablename__ = 'Booking'
    id = Column(String, primary_key=True)
    customerId = Column(String, ForeignKey('Customer.id'))
    scheduleId = Column(String, ForeignKey('Schedule.id'))
    status = Column(String)  # BookingStatus
    createdAt = Column(DateTime)
    customer = relationship("Customer")
    schedule = relationship("Schedule")

class PairingRecord(Base):
    __tablename__ = 'pairing_records'
    id = Column(Integer, primary_key=True)
    user1_id = Column(String)
    user2_id = Column(String)
    timestamp = Column(DateTime, default=datetime.utcnow)
    extended_times = Column(Integer, default=0)
    duration = Column(Integer, default=0)
    rating = Column(Integer, nullable=True)
    comment = Column(String, nullable=True)
    animal_name = Column(String)

class BlockRecord(Base):
    __tablename__ = 'block_records'
    id = Column(Integer, primary_key=True)
    blocker_id = Column(String)
    blocked_id = Column(String)

Base.metadata.create_all(engine)

intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True
intents.members = True
intents.voice_states = True

bot = commands.Bot(command_prefix="!", intents=intents)
active_voice_channels = {}
evaluated_records = set()
pending_ratings = {}
opened_channels = set()
ANIMALS = ["🦊 狐狸", "🐱 貓咪", "🐶 小狗", "🐻 熊熊", "🐼 貓熊", "🐯 老虎", "🦁 獅子", "🐸 青蛙", "🐵 猴子"]
TW_TZ = timezone(timedelta(hours=8))

# --- 成員搜尋函數 ---
def find_member_by_name(guild, name):
    """不區分大小寫搜尋成員"""
    name_lower = name.lower()
    print(f"搜尋名稱: {name} (轉小寫: {name_lower})")
    
    for member in guild.members:
        print(f"檢查成員: {member.name} (小寫: {member.name.lower()})")
        if member.name.lower() == name_lower:
            print(f"找到匹配: {member.name}")
            return member
    
    print(f"未找到匹配的成員: {name}")
    return None

#自動開設頻道
async def setup_pairing_channel(
    guild, 
    customer_member, 
    partner_member, 
    duration_minutes, 
    animal, 
    record=None, 
    booking_id=None, 
    interaction=None, 
    mentioned=None
):
    # 建立語音頻道
    overwrites = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False),
        customer_member: discord.PermissionOverwrite(view_channel=True, connect=True),
        partner_member: discord.PermissionOverwrite(view_channel=True, connect=True),
    }
    category = discord.utils.get(guild.categories, name="語音頻道")
    channel_name = f"匿名配對-{customer_member.name[:6]}-{partner_member.name[:6]}"
    vc = await guild.create_voice_channel(name=channel_name, overwrites=overwrites, category=category)

    # 建立匿名文字區
    text_channel = await guild.create_text_channel(
        name="🔒匿名文字區", overwrites=overwrites, category=category
    )

    # 初始化 active_voice_channels
    active_voice_channels[vc.id] = {
        'text_channel': text_channel,
        'remaining': duration_minutes * 60,
        'extended': 0,
        'record_id': booking_id or (record.id if record else f"manual_{vc.id}"),
        'vc': vc
    }

    # 頻道剛開啟時的提示訊息
    #view = ExtendView(vc.id)
    #await text_channel.send(
        #f"🎉 語音頻道 {channel_name} 已開啟！\n⏳ 可延長10分鐘 ( 為了您有更好的遊戲體驗，請到最後需要時再點選 ) 。",
        #view=view
    #)

    # 啟動倒數
    bot.loop.create_task(
        countdown(vc.id, channel_name, text_channel, vc, interaction, mentioned or [customer_member, partner_member], record)
    )

    return vc, text_channel

# --- 自動查詢與開頻道任務 ---
@tasks.loop(seconds=CHECK_INTERVAL)
async def check_and_create_channels():
    await bot.wait_until_ready()
    session = Session()
    now = datetime.now(timezone.utc)
    window_start = now + timedelta(seconds=0)
    window_end = now + timedelta(minutes=2)  # 2分鐘內即將開始

    # 查詢即將開始且已同意的預約
    bookings = session.query(Booking).join(Schedule).filter(
        Booking.status == "CONFIRMED",
        Schedule.startTime >= window_start,
        Schedule.startTime < window_end
    ).all()

    guild = bot.get_guild(GUILD_ID)
    if not guild:
        print("找不到 Discord 伺服器")
        return

    for booking in bookings:
        if booking.id in opened_channels:
            continue

        customer_discord = booking.customer.user.discord if booking.customer and booking.customer.user else None
        partner_discord = booking.schedule.partner.user.discord if booking.schedule and booking.schedule.partner and booking.schedule.partner.user else None

        if not customer_discord or not partner_discord:
            print(f"找不到 Discord 名稱: {booking.id}")
            continue

        # 使用新的搜尋函數
        customer_member = find_member_by_name(guild, customer_discord)
        partner_member = find_member_by_name(guild, partner_discord)

        if not customer_member or not partner_member:
            print(f"找不到 Discord 成員: {customer_discord}, {partner_discord}")
            continue

        animal = "自動配對"
        duration_minutes = 30  # 或根據 Booking 設定
        # 用共用 function 建立頻道
        vc, text_channel = await setup_pairing_channel(
            guild, customer_member, partner_member, duration_minutes, animal, booking_id=booking.id
        )

        admin_channel = guild.get_channel(ADMIN_CHANNEL_ID)
        if admin_channel:
            await admin_channel.send(f"已自動為預約 {booking.id} 建立語音頻道：{vc.mention}")

        opened_channels.add(booking.id)

    session.close()


# --- 評分 Modal ---
class RatingModal(Modal, title="匿名評分與留言"):
    rating = TextInput(label="給予評分（1～5 星）", required=True)
    comment = TextInput(label="留下你的留言（選填）", required=False)

    def __init__(self, record_id):
        super().__init__()
        self.record_id = record_id

    async def on_submit(self, interaction: discord.Interaction):
        try:
            record = session.get(PairingRecord, self.record_id)
            record.rating = int(str(self.rating))
            record.comment = str(self.comment)
            session.commit()
            await interaction.response.send_message("✅ 感謝你的匿名評價！", ephemeral=True)

            if self.record_id not in pending_ratings:
                pending_ratings[self.record_id] = []
            pending_ratings[self.record_id].append({
                'rating': record.rating,
                'comment': record.comment,
                'user1': str(interaction.user.id),
                'user2': str(record.user2_id if str(interaction.user.id) == record.user1_id else record.user1_id)
            })

            evaluated_records.add(self.record_id)
        except Exception as e:
            await interaction.response.send_message(f"❌ 提交失敗：{e}", ephemeral=True)

# --- 延長按鈕 ---
class ExtendView(View):
    def __init__(self, vc_id):
        super().__init__(timeout=None)
        self.vc_id = vc_id

    @discord.ui.button(label="🔁 延長 10 分鐘", style=discord.ButtonStyle.primary)
    async def extend_button(self, interaction: discord.Interaction, button: Button):
        if self.vc_id not in active_voice_channels:
            await interaction.response.send_message("❗ 頻道資訊不存在或已刪除。", ephemeral=True)
            return
        active_voice_channels[self.vc_id]['remaining'] += 600
        active_voice_channels[self.vc_id]['extended'] += 1
        await interaction.response.send_message("⏳ 已延長 10 分鐘。", ephemeral=True)

# --- Bot 啟動 ---
@bot.event
async def on_ready():
    print(f"✅ Bot 上線：{bot.user}")
    try:
        guild = discord.Object(id=GUILD_ID)
        synced = await bot.tree.sync(guild=guild)
        print(f"✅ Slash 指令已同步：{len(synced)} 個指令")
        
        # 啟動自動查詢任務
        check_and_create_channels.start()
        print(f"✅ 自動查詢任務已啟動，檢查間隔：{CHECK_INTERVAL} 秒")
    except Exception as e:
        print(f"❌ 指令同步失敗: {e}")

@bot.event
async def on_message(message):
    if message.author == bot.user:
        return
    if message.content == "!ping":
        await message.channel.send("Pong!")
    await bot.process_commands(message)

# --- 倒數邏輯 ---
async def countdown(vc_id, animal_channel_name, text_channel, vc, interaction, mentioned, record):
    try:
        for user in [interaction.user] + mentioned:
            if user.voice and user.voice.channel:
                await user.move_to(vc)

        view = ExtendView(vc.id)
        await text_channel.send(f"🎉 語音頻道 {animal_channel_name} 已開啟！\n⏳ 可延長10分鐘 ( 為了您有更好的遊戲體驗，請到最後需要時再點選 ) 。", view=view)

        while active_voice_channels[vc_id]['remaining'] > 0:
            remaining = active_voice_channels[vc_id]['remaining']
            if remaining == 60:
                await text_channel.send("⏰ 剩餘 1 分鐘。")
            await asyncio.sleep(1)
            active_voice_channels[vc_id]['remaining'] -= 1

        await vc.delete()
        await text_channel.send("📝 請點擊以下按鈕進行匿名評分。")

        class SubmitButton(View):
            def __init__(self):
                super().__init__(timeout=300)
                self.clicked = False

            @discord.ui.button(label="匿名評分", style=discord.ButtonStyle.success)
            async def submit(self, interaction: discord.Interaction, button: Button):
                if self.clicked:
                    await interaction.response.send_message("❗ 已提交過評價。", ephemeral=True)
                    return
                self.clicked = True
                await interaction.response.send_modal(RatingModal(record.id))

        await text_channel.send(view=SubmitButton())
        await asyncio.sleep(300)
        await text_channel.delete()

        record.extended_times = active_voice_channels[vc_id]['extended']
        record.duration += record.extended_times * 600
        session.commit()

        admin = bot.get_channel(ADMIN_CHANNEL_ID)
        if admin:
            try:
                u1 = await bot.fetch_user(int(record.user1_id))
                u2 = await bot.fetch_user(int(record.user2_id))
                header = f"📋 配對紀錄：{u1.name} × {u2.name} | {record.duration//60} 分鐘 | 延長 {record.extended_times} 次"

                if record.id in pending_ratings:
                    feedback = "\n⭐ 評價回饋："
                    for r in pending_ratings[record.id]:
                        from_user = await bot.fetch_user(int(r['user1']))
                        to_user = await bot.fetch_user(int(r['user2']))
                        feedback += f"\n- 「{from_user.name} → {to_user.name}」：{r['rating']} ⭐"
                        if r['comment']:
                            feedback += f"\n  💬 {r['comment']}"
                    del pending_ratings[record.id]
                    await admin.send(f"{header}{feedback}")
                else:
                    await admin.send(f"{header}\n⭐ 沒有收到任何評價。")
            except Exception as e:
                print(f"推送管理區評價失敗：{e}")

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

    # 解析成員名稱（假設格式是 "name1,name2" 或 "name1 name2"）
    member_names = [name.strip() for name in members.replace(',', ' ').split() if name.strip()]

    # 使用新的搜尋函數
    mentioned = []
    for name in member_names:
        member = find_member_by_name(interaction.guild, name)
        if member:
            mentioned.append(member)
        else:
            await interaction.followup.send(f"❗ 找不到成員：{name}")
            return

    if not mentioned:
        await interaction.followup.send("❗ 請提供至少一位有效的成員名稱。")
        return

    animal = random.choice(ANIMALS)
    animal_channel_name = f"{animal}頻道"
    await interaction.followup.send(f"✅ 已排程配對頻道：{animal_channel_name} 將於 <t:{int(start_dt_utc.timestamp())}:t> 開啟")

    async def countdown_wrapper():
        await asyncio.sleep((start_dt_utc - datetime.now(timezone.utc)).total_seconds())

        record = PairingRecord(
            user1_id=str(interaction.user.id),
            user2_id=str(mentioned[0].id),
            duration=minutes * 60,
            animal_name=animal
        )
        session.add(record)
        session.commit()

        # 用共用 function 建立頻道
        await setup_pairing_channel(
            interaction.guild, interaction.user, mentioned[0], minutes, animal, record=record, interaction=interaction, mentioned=mentioned
        )
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
    records = session.query(PairingRecord).filter((PairingRecord.user1_id==str(interaction.user.id)) | (PairingRecord.user2_id==str(interaction.user.id))).all()
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
    records = session.query(PairingRecord).filter((PairingRecord.user1_id==str(member.id)) | (PairingRecord.user2_id==str(member.id))).all()
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

def run_flask():
    app.run(host="0.0.0.0", port=5000)

threading.Thread(target=run_flask, daemon=True).start()
bot.run(TOKEN) 
import os
import asyncio
import random
import discord
from discord.ext import commands
from discord import app_commands
from discord.ui import View, Button, Modal, TextInput
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime, timedelta, timezone
from flask import Flask, request, jsonify
import threading

# --- 環境與資料庫設定 ---
load_dotenv()
TOKEN = os.getenv("DISCORD_BOT_TOKEN")
GUILD_ID = int(os.getenv("DISCORD_GUILD_ID", "0"))
POSTGRES_CONN = os.getenv("POSTGRES_CONN")
ADMIN_CHANNEL_ID = int(os.getenv("ADMIN_CHANNEL_ID", "0"))

Base = declarative_base()
engine = create_engine(POSTGRES_CONN)
Session = sessionmaker(bind=engine)
session = Session()

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

class Rating(Base):
    __tablename__ = 'ratings'
    id = Column(Integer, primary_key=True)
    pairing_id = Column(Integer)
    rater_id = Column(String)
    rating = Column(Integer)
    comment = Column(String)


intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True
intents.members = True
intents.voice_states = True

bot = commands.Bot(command_prefix="!", intents=intents)
active_voice_channels = {}
evaluated_records = set()

ANIMALS = ["🦊 狐狸", "🐱 貓咪", "🐶 小狗", "🐻 熊熊", "🐼 貓熊", "🐯 老虎", "🦁 獅子", "🐸 青蛙", "🐵 猴子"]

# 封鎖名單
class BlockRecord(Base):
    __tablename__ = 'block_records'
    id = Column(Integer, primary_key=True)
    blocker_id = Column(String)
    blocked_id = Column(String)

Base.metadata.create_all(engine)

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
            new_rating = Rating(
                pairing_id=self.record_id,
                rater_id=str(interaction.user.id),
                rating=int(str(self.rating)),
                comment=str(self.comment)
            )
            session.add(new_rating)
            session.commit()

            await interaction.response.send_message("✅ 感謝你的匿名評價！", ephemeral=True)

            admin = bot.get_channel(ADMIN_CHANNEL_ID)
            if admin:
                await admin.send(
                    f"⭐ 評分：{new_rating.rating} 星\n"
                    f"💬 留言：{new_rating.comment or '（無留言）'}\n"
                    f"👤 配對：<@{record.user1_id}> × <@{record.user2_id}>\n"
                    f"📋 配對紀錄：<@{record.user1_id}> × <@{record.user2_id}> | {record.duration // 60} 分鐘 | 延長 {record.extended_times} 次"
                )

            evaluated_records.add(self.record_id)
        except Exception as e:
            print(f"❌ 評分推送失敗：{e}")  # 加這行印出錯誤 log
            admin = bot.get_channel(ADMIN_CHANNEL_ID)
            if admin:
                await admin.send(f"❗ 評分提交錯誤：{e}")
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
    except Exception as e:
        print(f"❌ 指令同步失敗: {e}")

# --- 建立頻道指令 ---
@bot.event
async def on_message(message):
    # 忽略自己發的訊息，避免無限迴圈
    if message.author == bot.user:
        return

    # 如果有人在 Discord 輸入 "!ping"，Bot 回傳 "Pong!"
    if message.content == "!ping":
        await message.channel.send("Pong!")

    # 讓其他指令繼續被處理
    await bot.process_commands(message)

# 台灣時區設定now = datetime.now(TW_TZ)  # 使用台灣時間
TW_TZ = timezone(timedelta(hours=8))

@bot.tree.command(name="createvc", description="建立匿名語音頻道（指定開始時間）", guild=discord.Object(id=GUILD_ID))
@app_commands.describe(
    partner="標註一位要配對的夥伴",
    minutes="存在時間（分鐘）",
    start_time="幾點幾分後啟動 (格式: HH:MM, 24hr)",
    limit="人數上限"
)
async def createvc(interaction: discord.Interaction, partner: discord.Member, minutes: int, start_time: str, limit: int = 2):
    await interaction.response.defer()

    try:
        now = datetime.now(TW_TZ)  # 使用台灣時間
        hour, minute = map(int, start_time.split(":"))
        now = datetime.now(TW_TZ)
        start_dt = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if start_dt < now:
            start_dt += timedelta(days=1)
        start_dt_utc = start_dt.astimezone(timezone.utc)
    except:
        await interaction.followup.send("❗ 時間格式錯誤，請使用 HH:MM 24 小時制。")
        return

    # 封鎖檢查
    with Session() as s:
        is_blocked = s.query(BlockRecord).filter_by(blocker_id=str(interaction.user.id), blocked_id=str(partner.id)).first()
        if is_blocked:
            await interaction.followup.send("❗ 你已封鎖該用戶，無法配對。")
            return

    # 隨機動物暱稱
    animal = random.choice(ANIMALS)
    animal_channel_name = f"{animal}頻道"

    await interaction.followup.send(f"✅ 已排程配對頻道：`{animal_channel_name}` 將於 <t:{int(start_dt_utc.timestamp())}:t> 開啟")

    async def countdown():
        await asyncio.sleep((start_dt_utc - datetime.now(timezone.utc)).total_seconds())

        overwrites = {
            interaction.guild.default_role: discord.PermissionOverwrite(view_channel=False),
            interaction.user: discord.PermissionOverwrite(view_channel=True, connect=True),
            partner: discord.PermissionOverwrite(view_channel=True, connect=True),
        }

        category = discord.utils.get(interaction.guild.categories, name="語音頻道")
        vc = await interaction.guild.create_voice_channel(name=animal_channel_name, overwrites=overwrites, user_limit=limit, category=category)
        text_channel = await interaction.guild.create_text_channel(name="🔒匿名文字區", overwrites=overwrites, category=category)

        record = PairingRecord(
            user1_id=str(interaction.user.id),
            user2_id=str(partner.id),
            duration=minutes * 60,
            animal_name=animal
        )
        session.add(record)
        session.commit()

        active_voice_channels[vc.id] = {
            'text_channel': text_channel,
            'remaining': minutes * 60,
            'extended': 0,
            'record_id': record.id,
            'vc': vc
        }

        view = ExtendView(vc.id)
        await text_channel.send(f"🎉 語音頻道 `{animal_channel_name}` 已開啟！\n⏳ 可延長。", view=view)

        # 自動移動進頻道
        for user in [interaction.user, partner]:
            if user.voice and user.voice.channel:
                await user.move_to(vc)

        try:
            while active_voice_channels[vc.id]['remaining'] > 0:
                if active_voice_channels[vc.id]['remaining'] == 60:
                    await text_channel.send("⏰ 剩餘 1 分鐘。")
                await asyncio.sleep(1)
                active_voice_channels[vc.id]['remaining'] -= 1

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

            record.extended_times = active_voice_channels[vc.id]['extended']
            record.duration += record.extended_times * 600
            session.commit()

            active_voice_channels.pop(vc.id, None)

        except Exception as e:
            print(f"❌ 倒數錯誤: {e}")

    bot.loop.create_task(countdown())

# 封鎖動作
@bot.tree.command(name="viewblocklist", description="查看你封鎖的使用者", guild=discord.Object(id=GUILD_ID))
async def view_blocklist(interaction: discord.Interaction):
    with Session() as s:
        blocks = s.query(BlockRecord).filter(BlockRecord.blocker_id == str(interaction.user.id)).all()
        if not blocks:
            await interaction.response.send_message("📭 你尚未封鎖任何人。", ephemeral=True)
            return

        blocked_mentions = [f"<@{b.blocked_id}>" for b in blocks]
        await interaction.response.send_message(f"🔒 你封鎖的使用者：\n" + "\n".join(blocked_mentions), ephemeral=True)

# 解除封鎖
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

# --- /report 舉報功能 ---
@bot.tree.command(name="report", description="舉報不當行為", guild=discord.Object(id=GUILD_ID))
@app_commands.describe(member="被舉報的使用者", reason="舉報原因")
async def report(interaction: discord.Interaction, member: discord.Member, reason: str):
    admin = bot.get_channel(ADMIN_CHANNEL_ID)
    await interaction.response.send_message("✅ 舉報已提交，感謝你的協助。", ephemeral=True)
    if admin:
        await admin.send(f"🚨 舉報通知：<@{interaction.user.id}> 舉報 <@{member.id}>\n📄 理由：{reason}")

# --- /mystats 查詢自己 ---
@bot.tree.command(name="mystats", description="查詢自己的配對統計", guild=discord.Object(id=GUILD_ID))
async def mystats(interaction: discord.Interaction):
    records = session.query(PairingRecord).filter((PairingRecord.user1_id==str(interaction.user.id)) | (PairingRecord.user2_id==str(interaction.user.id))).all()
    count = len(records)
    ratings = [r.rating for r in records if r.rating]
    comments = [r.comment for r in records if r.comment]
    avg_rating = round(sum(ratings)/len(ratings), 1) if ratings else "無"
    await interaction.response.send_message(
        f"📊 你的配對紀錄：\n- 配對次數：{count} 次\n- 平均評分：{avg_rating} ⭐\n- 收到留言：{len(comments)} 則",
        ephemeral=True)

# --- /stats @user 管理員查詢 ---
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
    await interaction.response.send_message(
        f"📊 <@{member.id}> 的配對紀錄：\n- 配對次數：{count} 次\n- 平均評分：{avg_rating} ⭐\n- 收到留言：{len(comments)} 則",
        ephemeral=True)

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

# --- 啟動 Flask 在子執行緒 ---
def run_flask():
    app.run(host="0.0.0.0", port=5000)
@app.route("/create_vc", methods=["POST"])
def create_vc():
    data = request.get_json()
    customer_id = int(data["customer_id"])
    partner_id = int(data["partner_id"])
    start_time = datetime.fromisoformat(data["start_time"])
    duration = int(data["duration"])

    async def schedule_vc():
        await asyncio.sleep((start_time - datetime.utcnow()).total_seconds())
        guild = bot.get_guild(GUILD_ID)
        customer = guild.get_member(customer_id)
        partner = guild.get_member(partner_id)

        animal = random.choice(ANIMALS)
        animal_channel_name = f"{animal}頻道"

        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            customer: discord.PermissionOverwrite(view_channel=True, connect=True),
            partner: discord.PermissionOverwrite(view_channel=True, connect=True),
        }

        category = discord.utils.get(guild.categories, name="語音頻道")
        vc = await guild.create_voice_channel(name=animal_channel_name, overwrites=overwrites, user_limit=2, category=category)
        text_channel = await guild.create_text_channel(name="🔒匿名文字區", overwrites=overwrites, category=category)

        record = PairingRecord(
            user1_id=str(customer.id),
            user2_id=str(partner.id),
            duration=duration,
            animal_name=animal
        )
        session.add(record)
        session.commit()

        active_voice_channels[vc.id] = {
            'text_channel': text_channel,
            'remaining': duration,
            'extended': 0,
            'record_id': record.id,
            'vc': vc
        }

        view = ExtendView(vc.id)
        await text_channel.send(f"🎉 語音頻道 `{animal_channel_name}` 已開啟！\n⏳ 可延長。", view=view)

        if customer.voice and customer.voice.channel:
            await customer.move_to(vc)
        if partner.voice and partner.voice.channel:
            await partner.move_to(vc)

        try:
            while active_voice_channels[vc.id]['remaining'] > 0:
                if active_voice_channels[vc.id]['remaining'] == 60:
                    await text_channel.send("⏰ 剩餘 1 分鐘。")

                await asyncio.sleep(1)
                active_voice_channels[vc.id]['remaining'] -= 1

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

            record.extended_times = active_voice_channels[vc.id]['extended']
            record.duration += record.extended_times * 600
            session.commit()

            admin = bot.get_channel(ADMIN_CHANNEL_ID)
            if admin:
                await admin.send(
                    f"📋 配對紀錄：<@{record.user1_id}> × <@{record.user2_id}> | {record.duration//60} 分鐘 | 延長 {record.extended_times} 次"
                )

            active_voice_channels.pop(vc.id, None)
        except Exception as e:
            print(f"❌ Flask 排程錯誤: {e}")

    bot.loop.create_task(schedule_vc())
    return jsonify({"status": "ok"})

if __name__ == "__main__":
    # 啟動 Flask
    flask_thread = threading.Thread(target=run_flask)
    flask_thread.start()

    # 啟動 Discord bot（會阻塞直到關閉）
    bot.run(TOKEN)
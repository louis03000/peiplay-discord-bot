import asyncio
import discord
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv

# 載入環境變數
load_dotenv()

TOKEN = os.getenv("DISCORD_BOT_TOKEN")
GUILD_ID = int(os.getenv("DISCORD_GUILD_ID", "0"))
POSTGRES_CONN = os.getenv("POSTGRES_CONN")
ADMIN_CHANNEL_ID = int(os.getenv("ADMIN_CHANNEL_ID", "0"))

# 資料庫連接
engine = create_engine(POSTGRES_CONN)
Session = sessionmaker(bind=engine)

async def delete_booking_channels(booking_id: str):
    """刪除預約相關的 Discord 頻道"""
    try:
        # 初始化 Discord 客戶端
        intents = discord.Intents.default()
        intents.message_content = True
        client = discord.Client(intents=intents)
        
        await client.login(TOKEN)
        
        guild = client.get_guild(GUILD_ID)
        if not guild:
            print("❌ 找不到 Discord 伺服器")
            await client.close()
            return False
        
        # 從資料庫獲取頻道 ID
        with Session() as s:
            result = s.execute(
                text("SELECT \"discordTextChannelId\", \"discordVoiceChannelId\" FROM \"Booking\" WHERE id = :booking_id"),
                {"booking_id": booking_id}
            )
            row = result.fetchone()
            
            if not row:
                print(f"❌ 找不到預約 {booking_id} 的頻道資訊")
                await client.close()
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
                    print(f"✅ 已刪除文字頻道: {text_channel.name}")
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
                    print(f"✅ 已刪除語音頻道: {voice_channel.name}")
                else:
                    print(f"⚠️ 語音頻道 {voice_channel_id} 不存在")
            except Exception as voice_error:
                print(f"❌ 刪除語音頻道失敗: {voice_error}")
        
        # 清除資料庫中的頻道 ID
        try:
            with Session() as s:
                s.execute(
                    text("UPDATE \"Booking\" SET \"discordTextChannelId\" = NULL, \"discordVoiceChannelId\" = NULL WHERE id = :booking_id"),
                    {"booking_id": booking_id}
                )
                s.commit()
                print(f"✅ 已清除預約 {booking_id} 的頻道 ID")
        except Exception as db_error:
            print(f"❌ 清除頻道 ID 失敗: {db_error}")
        
        # 通知管理員
        try:
            admin_channel = guild.get_channel(ADMIN_CHANNEL_ID)
            if admin_channel and deleted_channels:
                await admin_channel.send(
                    f"🗑️ **預約頻道已刪除**\n"
                    f"預約ID: `{booking_id}`\n"
                    f"已刪除頻道: {', '.join(deleted_channels)}"
                )
        except Exception as notify_error:
            print(f"❌ 發送刪除通知失敗: {notify_error}")
        
        await client.close()
        return len(deleted_channels) > 0
        
    except Exception as error:
        print(f"❌ 刪除預約頻道失敗: {error}")
        try:
            await client.close()
        except:
            pass
        return False

if __name__ == "__main__":
    # 測試用
    import sys
    if len(sys.argv) > 1:
        booking_id = sys.argv[1]
        asyncio.run(delete_booking_channels(booking_id))

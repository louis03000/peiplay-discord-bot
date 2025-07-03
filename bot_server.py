from flask import Flask, request, jsonify
import os
import discord
import asyncio
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("DISCORD_BOT_TOKEN")
GUILD_ID = int(os.getenv("DISCORD_GUILD_ID", "0"))

intents = discord.Intents.default()
intents.members = True
intents.voice_states = True

bot = discord.Client(intents=intents)
app = Flask(__name__)

@app.route("/move_user", methods=["POST"])
def move_user():
    data = request.get_json()
    discord_id = int(data.get("discord_id"))
    vc_id = int(data.get("vc_id"))

    async def mover():
        await bot.wait_until_ready()
        guild = bot.get_guild(GUILD_ID)
        member = guild.get_member(discord_id)
        vc = guild.get_channel(vc_id)
        if member and vc:
            await member.move_to(vc)
            print(f"✅ 已將 {member} 移動到 {vc.name}")
        else:
            print("❌ 找不到成員或語音頻道")

    bot.loop.create_task(mover())
    return jsonify({"status": "ok"})

def start_flask():
    app.run(host="0.0.0.0", port=5000)

if __name__ == "__main__":
    import threading
    threading.Thread(target=start_flask, daemon=True).start()
    bot.run(TOKEN)
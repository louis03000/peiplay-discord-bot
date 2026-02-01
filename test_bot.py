#!/usr/bin/env python3
"""
Discord Bot 測試腳本
用於驗證環境設定和基本功能
"""

import os
import discord
from dotenv import load_dotenv

def test_environment():
    """測試環境變數"""
    print("🔍 測試環境變數...")
    
    load_dotenv()
    
    required_vars = [
        "DISCORD_BOT_TOKEN",
        "DISCORD_GUILD_ID", 
        "ADMIN_CHANNEL_ID",
        "POSTGRES_CONN"
    ]
    
    missing_vars = []
    for var in required_vars:
        value = os.getenv(var)
        if not value:
            missing_vars.append(var)
        else:
            print(f"✅ {var}: {'*' * len(value)}")
    
    if missing_vars:
        print(f"❌ 缺少環境變數: {', '.join(missing_vars)}")
        return False
    
    print("✅ 環境變數檢查通過")
    return True

def test_discord_connection():
    """測試 Discord 連接"""
    print("\n🔍 測試 Discord 連接...")
    
    try:
        intents = discord.Intents.default()
        bot = discord.Client(intents=intents)
        
        @bot.event
        async def on_ready():
            print(f"✅ Discord 連接成功: {bot.user}")
            await bot.close()
        
        bot.run(os.getenv("DISCORD_BOT_TOKEN"))
        return True
    except Exception as e:
        print(f"❌ Discord 連接失敗: {e}")
        return False

def test_database_connection():
    """測試資料庫連接"""
    print("\n🔍 測試資料庫連接...")
    
    try:
        from sqlalchemy import create_engine
        engine = create_engine(os.getenv("POSTGRES_CONN"))
        connection = engine.connect()
        connection.close()
        print("✅ 資料庫連接成功")
        return True
    except Exception as e:
        print(f"❌ 資料庫連接失敗: {e}")
        return False

def main():
    """主測試函數"""
    print("🎮 Discord Bot 測試工具")
    print("=" * 50)
    
    # 測試環境變數
    if not test_environment():
        print("\n❌ 環境設定有問題")
        return
    
    # 測試 Discord 連接
    if not test_discord_connection():
        print("\n❌ Discord 連接失敗")
        return
    
    # 測試資料庫連接
    if not test_database_connection():
        print("\n❌ 資料庫連接失敗")
        return
    
    print("\n🎉 所有測試通過！Bot 可以正常啟動")
    print("\n💡 下一步：")
    print("   1. 確保 Discord 伺服器中有「語音頻道」分類")
    print("   2. 執行: python bot.py")
    print("   3. 測試指令: /createvc")

if __name__ == "__main__":
    main()

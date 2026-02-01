#!/usr/bin/env python3
"""
PeiPlay Discord Bot 啟動腳本
"""

import os
import sys
import subprocess
from pathlib import Path
from dotenv import load_dotenv

def check_environment():
    """檢查環境設定"""
    print("🔍 檢查 Discord Bot 環境設定...")
    
    # 載入 .env 檔案
    load_dotenv()
    
    # 檢查 .env 檔案
    env_file = Path(".env")
    if not env_file.exists():
        print("❌ 找不到 .env 檔案")
        print("📝 請複製 discord_bot_env_example.env 為 .env 並填入正確的設定")
        return False
    
    # 檢查必要的環境變數
    required_vars = [
        "DISCORD_BOT_TOKEN",
        "DISCORD_GUILD_ID", 
        "ADMIN_CHANNEL_ID",
        "POSTGRES_CONN"
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"❌ 缺少必要的環境變數: {', '.join(missing_vars)}")
        print("📝 請在 .env 檔案中設定這些變數")
        return False
    
    print("✅ 環境設定檢查通過")
    return True

def check_dependencies():
    """檢查 Python 依賴"""
    print("🔍 檢查 Python 依賴...")
    
    required_packages = [
        "discord.py",
        "python-dotenv", 
        "sqlalchemy",
        "psycopg2-binary",
        "Flask"
    ]
    
    missing_packages = []
    for package in required_packages:
        try:
            __import__(package.replace("-", "_"))
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        print(f"❌ 缺少 Python 套件: {', '.join(missing_packages)}")
        print("📦 請執行: pip install -r requirements.txt")
        return False
    
    print("✅ 依賴檢查通過")
    return True

def start_bot():
    """啟動 Discord Bot"""
    print("🚀 啟動 Discord Bot...")
    
    try:
        # 使用 subprocess 啟動 bot
        result = subprocess.run([
            sys.executable, "bot.py"
        ], check=True)
    except subprocess.CalledProcessError as e:
        print(f"❌ Discord Bot 啟動失敗: {e}")
        return False
    except KeyboardInterrupt:
        print("\n⏹️ Discord Bot 已停止")
        return True
    
    return True

def main():
    """主函數"""
    print("🎮 PeiPlay Discord Bot 啟動器")
    print("=" * 50)
    
    # 檢查環境
    if not check_environment():
        print("\n❌ 環境設定有問題，請檢查上述錯誤")
        return
    
    # 檢查依賴
    if not check_dependencies():
        print("\n❌ 依賴檢查失敗，請安裝缺少的套件")
        return
    
    print("\n✅ 所有檢查通過，準備啟動 Discord Bot...")
    print("💡 提示：")
    print("   - 確保 Discord Bot 已加入您的伺服器")
    print("   - 確保 Bot 有管理頻道的權限")
    print("   - 確保伺服器中有「語音頻道」分類")
    print("   - 按 Ctrl+C 可以停止 Bot")
    print("\n" + "=" * 50)
    
    # 啟動 Bot
    start_bot()

if __name__ == "__main__":
    main()
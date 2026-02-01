"""
自動修復 bot.py 的腳本
請將此腳本放在 E:\python.12\discord-bot\ 目錄下執行
"""

import re
import os

BOT_FILE = r"E:\python.12\discord-bot\bot.py"
BACKUP_FILE = r"E:\python.12\discord-bot\bot.py.backup"

def fix_emoji_in_buttons(content):
    """修復按鈕中的 emoji 參數衝突"""
    # 移除 label 中包含 emoji 的按鈕的 emoji 參數
    # 匹配模式：@discord.ui.button(label="⭐...", ..., emoji="⭐")
    pattern = r'(@discord\.ui\.button\([^)]*label="[^"]*⭐[^"]*"[^)]*),\s*emoji="[^"]*"'
    
    def replace_func(match):
        return match.group(1) + ')'
    
    content = re.sub(pattern, replace_func, content)
    
    # 也處理單引號的情況
    pattern2 = r'(@discord\.ui\.button\([^)]*label=''[^']*⭐[^']*''[^)]*),\s*emoji=''[^']*'''
    content = re.sub(pattern2, replace_func, content)
    
    return content

def fix_countdown_function(content):
    """修復 countdown 函數中獲取用戶ID的部分"""
    # 查找創建 ManualRatingView 的地方
    pattern = r'(view\s*=\s*ManualRatingView\(record_id,\s*user1_id,\s*user2_id\))'
    
    replacement = '''# 從資料庫獲取正確的用戶ID
        with Session() as s:
            record = s.get(PairingRecord, record_id)
            if not record:
                print(f"❌ 找不到配對記錄: {record_id}")
                if text_channel and not text_channel.deleted:
                    await text_channel.delete()
                active_voice_channels.pop(vc_id, None)
                return
            
            # 確保從資料庫獲取正確的用戶ID
            user1_id = record.user1Id
            user2_id = record.user2Id
            print(f"🔍 從資料庫獲取用戶ID: user1_id={user1_id}, user2_id={user2_id}")
        
        \\1'''
    
    content = re.sub(pattern, replacement, content)
    
    return content

def main():
    print("🔧 開始修復 bot.py...")
    
    # 檢查文件是否存在
    if not os.path.exists(BOT_FILE):
        print(f"❌ 找不到文件: {BOT_FILE}")
        print("請確認文件路徑是否正確")
        return
    
    # 備份原文件
    print(f"📋 備份原文件到: {BACKUP_FILE}")
    with open(BOT_FILE, 'r', encoding='utf-8') as f:
        original_content = f.read()
    
    with open(BACKUP_FILE, 'w', encoding='utf-8') as f:
        f.write(original_content)
    
    # 修復內容
    print("🔨 修復 emoji 參數衝突...")
    fixed_content = fix_emoji_in_buttons(original_content)
    
    print("🔨 修復 countdown 函數...")
    fixed_content = fix_countdown_function(fixed_content)
    
    # 寫入修復後的文件
    print(f"💾 寫入修復後的文件...")
    with open(BOT_FILE, 'w', encoding='utf-8') as f:
        f.write(fixed_content)
    
    print("✅ 修復完成！")
    print(f"📝 原文件已備份到: {BACKUP_FILE}")
    print("⚠️  請檢查修復後的代碼，確認無誤後再運行 bot.py")

if __name__ == "__main__":
    main()


#!/usr/bin/env python3
"""
測試 Discord Bot 與 PeiPlay 資料庫的連接
"""

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

def test_database_connection():
    """測試資料庫連接"""
    print("🔍 測試 Discord Bot 資料庫連接...")
    
    # 載入環境變數
    load_dotenv()
    
    # 獲取資料庫連接字串
    postgres_conn = os.getenv("POSTGRES_CONN")
    if not postgres_conn:
        print("❌ 找不到 POSTGRES_CONN 環境變數")
        return False
    
    try:
        # 創建資料庫引擎
        engine = create_engine(postgres_conn)
        
        # 測試連接
        with engine.connect() as connection:
            print("✅ 資料庫連接成功")
            
            # 測試查詢 PeiPlay 資料表
            tables_to_test = ['User', 'Partner', 'Customer', 'Schedule', 'Booking']
            
            for table in tables_to_test:
                try:
                    result = connection.execute(text(f"SELECT COUNT(*) FROM \"{table}\""))
                    count = result.scalar()
                    print(f"✅ {table} 表: {count} 筆記錄")
                except Exception as e:
                    print(f"❌ {table} 表查詢失敗: {e}")
                    return False
            
            # 測試查詢有 Discord 名稱的用戶
            try:
                result = connection.execute(text('SELECT COUNT(*) FROM "User" WHERE discord IS NOT NULL'))
                discord_users = result.scalar()
                print(f"✅ 有設定 Discord 名稱的用戶: {discord_users} 人")
            except Exception as e:
                print(f"❌ 查詢 Discord 用戶失敗: {e}")
                return False
            
            # 測試查詢預約資料
            try:
                result = connection.execute(text('SELECT COUNT(*) FROM "Booking" WHERE status IN (\'CONFIRMED\', \'COMPLETED\')'))
                confirmed_bookings = result.scalar()
                print(f"✅ 已確認的預約: {confirmed_bookings} 筆")
            except Exception as e:
                print(f"❌ 查詢預約資料失敗: {e}")
                return False
            
            print("\n🎉 資料庫連接測試完成！")
            return True
            
    except Exception as e:
        print(f"❌ 資料庫連接失敗: {e}")
        return False

def main():
    """主函數"""
    print("🎮 Discord Bot 資料庫連接測試")
    print("=" * 50)
    
    if test_database_connection():
        print("\n✅ 所有測試通過！Bot 可以正常連接 PeiPlay 資料庫")
        print("\n💡 下一步：")
        print("   1. 確保 Discord Bot 設定正確")
        print("   2. 執行: python bot.py")
    else:
        print("\n❌ 資料庫連接測試失敗")
        print("\n🔧 請檢查：")
        print("   1. POSTGRES_CONN 環境變數是否正確")
        print("   2. 資料庫服務是否正在運行")
        print("   3. 資料庫權限是否足夠")

if __name__ == "__main__":
    main()
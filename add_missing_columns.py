import os
import psycopg2
from dotenv import load_dotenv

# 載入環境變數
load_dotenv()

# 獲取資料庫連接字串
POSTGRES_CONN = os.getenv("POSTGRES_CONN")

if not POSTGRES_CONN:
    print("找不到 POSTGRES_CONN 環境變數")
    exit(1)

try:
    # 連接到資料庫
    conn = psycopg2.connect(POSTGRES_CONN)
    cursor = conn.cursor()
    
    print("開始添加缺少的欄位...")
    
    # 添加 isInstantBooking 欄位
    try:
        cursor.execute("""
            ALTER TABLE "Booking" 
            ADD COLUMN "isInstantBooking" boolean DEFAULT false;
        """)
        print("O 已添加 isInstantBooking 欄位")
    except psycopg2.errors.DuplicateColumn:
        print("O isInstantBooking 欄位已存在")
    except Exception as e:
        print(f"X 添加 isInstantBooking 欄位失敗: {e}")
    
    # 添加 tenMinuteReminderShown 欄位
    try:
        cursor.execute("""
            ALTER TABLE "Booking" 
            ADD COLUMN "tenMinuteReminderShown" boolean DEFAULT false;
        """)
        print("O 已添加 tenMinuteReminderShown 欄位")
    except psycopg2.errors.DuplicateColumn:
        print("O tenMinuteReminderShown 欄位已存在")
    except Exception as e:
        print(f"X 添加 tenMinuteReminderShown 欄位失敗: {e}")
    
    # 提交變更
    conn.commit()
    print("\n所有變更已提交到資料庫")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"資料庫連接錯誤: {e}")
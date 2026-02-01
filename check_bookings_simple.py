import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import create_engine, text
from datetime import datetime, timezone

# 使用 Supabase 資料庫連接
DATABASE_URL = "postgresql://postgres.hxxqhdsrnjwqyignfrdy:peiplay2025sss920427@aws-0-ap-southeast-1.pooler.supabase.com:5432/postgres"
engine = create_engine(DATABASE_URL)

with engine.connect() as conn:
    result = conn.execute(text("SELECT b.id, s.\"startTime\" FROM \"Booking\" b JOIN \"Schedule\" s ON s.id = b.\"scheduleId\" WHERE b.status = 'CONFIRMED' ORDER BY s.\"startTime\" ASC"))
    
    print("已確認的預約:")
    now = datetime.now(timezone.utc)
    print(f"當前時間: {now}")
    print()
    
    for row in result:
        # 確保 startTime 有時區資訊
        start_time = row.startTime
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        
        time_diff = start_time - now
        print(f"預約 {row.id}: {start_time} (距離現在: {time_diff})")
    
    conn.close()

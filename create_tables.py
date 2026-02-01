from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# 資料庫連接
POSTGRES_CONN = "postgresql://postgres.hxxqhdsrnjwqyignfrdy:peiplay2025sss920427@aws-0-ap-southeast-1.pooler.supabase.com:5432/postgres"
engine = create_engine(POSTGRES_CONN)

# 創建表格
with engine.connect() as conn:
    # 創建 block_records 表格
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS block_records (
            id SERIAL PRIMARY KEY,
            blocker_id VARCHAR,
            blocked_id VARCHAR
        )
    """))
    
    # 創建 pairing_records 表格
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS pairing_records (
            id SERIAL PRIMARY KEY,
            user1_id VARCHAR,
            user2_id VARCHAR,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            extended_times INTEGER DEFAULT 0,
            duration INTEGER DEFAULT 0,
            rating INTEGER,
            comment VARCHAR,
            animal_name VARCHAR,
            booking_id VARCHAR
        )
    """))
    
    conn.commit()

print("✅ 資料庫表格創建完成")

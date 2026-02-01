#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from bot import Session
from sqlalchemy import text
from datetime import datetime, timezone, timedelta

def debug_bookings():
    """檢查預約狀態"""
    with Session() as s:
        # 查詢所有已確認的預約
        query = """
        SELECT 
            b.id, b.status, b."createdAt",
            c.name as customer_name, cu.discord as customer_discord,
            p.name as partner_name, pu.discord as partner_discord,
            s."startTime", s."endTime"
        FROM "Booking" b
        JOIN "Schedule" s ON s.id = b."scheduleId"
        JOIN "Customer" c ON c.id = b."customerId"
        JOIN "User" cu ON cu.id = c."userId"
        JOIN "Partner" p ON p.id = s."partnerId"
        JOIN "User" pu ON pu.id = p."userId"
        WHERE b.status IN ('CONFIRMED', 'COMPLETED')
        ORDER BY s."startTime" ASC
        """
        
        result = s.execute(text(query))
        
        print("🔍 已確認的預約列表：")
        print("=" * 80)
        
        now = datetime.now(timezone.utc)
        print(f"當前時間: {now}")
        print()
        
        for row in result:
            start_time = row.startTime
            time_diff = start_time - now
            
            print(f"預約ID: {row.id}")
            print(f"狀態: {row.status}")
            print(f"顧客: {row.customer_name} ({row.customer_discord})")
            print(f"夥伴: {row.partner_name} ({row.partner_discord})")
            print(f"開始時間: {start_time}")
            print(f"結束時間: {row.endTime}")
            print(f"距離現在: {time_diff}")
            
            # 檢查是否在 5 分鐘內
            if timedelta(minutes=-5) <= time_diff <= timedelta(minutes=5):
                print("🎯 這個預約應該被處理！")
            else:
                print("⏰ 不在處理時間範圍內")
            
            print("-" * 40)

if __name__ == "__main__":
    debug_bookings()

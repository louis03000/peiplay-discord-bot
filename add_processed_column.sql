-- 添加 processed 欄位到 Booking 表
ALTER TABLE "Booking" ADD COLUMN IF NOT EXISTS processed BOOLEAN DEFAULT false;

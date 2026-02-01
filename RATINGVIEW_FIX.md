# RatingView 修復說明

## 問題
錯誤訊息顯示：
```
TypeError: RatingView.__init__() takes 2 positional arguments but 4 were given
```

在 `countdown` 函數的第 3888 行，程式碼嘗試這樣呼叫：
```python
view = RatingView(record_id, user1_id, user2_id)
```

但 `RatingView.__init__()` 目前只接受 `booking_id` 一個參數。

## 解決方案

需要更新 `RatingView` 類別以支援配對記錄（pairing records）。有兩種方式：

### 方案 1：更新 RatingView 以支援兩種模式（推薦）

修改 `RatingView` 的 `__init__` 方法，讓它可以接受兩種參數模式：

```python
class RatingView(View):
    def __init__(self, record_id=None, user1_id=None, user2_id=None, booking_id=None):
        super().__init__(timeout=600)  # 10 分鐘超時
        
        # 支援配對記錄模式
        if record_id is not None:
            self.record_id = record_id
            self.user1_id = user1_id
            self.user2_id = user2_id
            self.booking_id = None
            self.is_pairing_record = True
        # 支援預約模式（向後兼容）
        elif booking_id is not None:
            self.booking_id = booking_id
            self.record_id = None
            self.is_pairing_record = False
        else:
            raise ValueError("必須提供 record_id 或 booking_id")
        
        self.ratings = {}  # 儲存用戶的評分
        self.submitted_users = set()  # 儲存已提交評價的用戶

    @discord.ui.button(label="⭐ 1星", style=discord.ButtonStyle.secondary, custom_id="rating_1")
    async def rate_1_star(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.handle_rating(interaction, 1)

    @discord.ui.button(label="⭐⭐ 2星", style=discord.ButtonStyle.secondary, custom_id="rating_2")
    async def rate_2_star(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.handle_rating(interaction, 2)

    @discord.ui.button(label="⭐⭐⭐ 3星", style=discord.ButtonStyle.secondary, custom_id="rating_3")
    async def rate_3_star(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.handle_rating(interaction, 3)

    @discord.ui.button(label="⭐⭐⭐⭐ 4星", style=discord.ButtonStyle.secondary, custom_id="rating_4")
    async def rate_4_star(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.handle_rating(interaction, 4)

    @discord.ui.button(label="⭐⭐⭐⭐⭐ 5星", style=discord.ButtonStyle.secondary, custom_id="rating_5")
    async def rate_5_star(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.handle_rating(interaction, 5)

    async def handle_rating(self, interaction: discord.Interaction, rating: int):
        user_id = interaction.user.id
        self.ratings[user_id] = rating
        
        if self.is_pairing_record:
            # 配對記錄模式：使用簡單的 RatingModal（只需要 record_id）
            from discord.ui import Modal, TextInput
            modal = RatingModalForPairing(self.record_id)
            await interaction.response.send_modal(modal)
        else:
            # 預約模式：使用原本的 RatingModal
            modal = RatingModal(rating, self.booking_id, self)
            await interaction.response.send_modal(modal)


# 為配對記錄創建專用的 RatingModal
class RatingModalForPairing(Modal, title="匿名評分與留言"):
    rating = TextInput(label="給予評分（1～5 星）", required=True)
    comment = TextInput(label="留下你的留言（選填）", required=False)

    def __init__(self, record_id):
        super().__init__()
        self.record_id = record_id

    async def on_submit(self, interaction: discord.Interaction):
        try:
            print(f"🔍 收到評價提交: record_id={self.record_id}, rating={self.rating}, comment={self.comment}")
            
            with Session() as s:
                record = s.get(PairingRecord, self.record_id)
                if not record:
                    print(f"❌ 找不到配對記錄: {self.record_id}")
                    await interaction.response.send_message("❌ 找不到配對記錄", ephemeral=True)
                    return
                
                user1_id = record.user1Id
                user2_id = record.user2Id
                
                record.rating = int(str(self.rating))
                record.comment = str(self.comment)
                s.commit()
            
            await interaction.response.send_message("✅ 感謝你的匿名評價！", ephemeral=True)

            if self.record_id not in pending_ratings:
                pending_ratings[self.record_id] = []
            
            rating_data = {
                'rating': int(str(self.rating)),
                'comment': str(self.comment),
                'user1': str(interaction.user.id),
                'user2': str(user2_id if str(interaction.user.id) == user1_id else user1_id)
            }
            pending_ratings[self.record_id].append(rating_data)
            print(f"✅ 評價已添加到待處理列表: {rating_data}")

            # 立即發送評價到管理員頻道
            await send_rating_to_admin(self.record_id, rating_data, user1_id, user2_id)

            evaluated_records.add(self.record_id)
            print(f"✅ 評價流程完成")
        except Exception as e:
            print(f"❌ 評分提交錯誤: {e}")
            import traceback
            traceback.print_exc()
            try:
                await interaction.response.send_message("❌ 提交失敗，請稍後再試", ephemeral=True)
            except:
                pass
```

### 方案 2：簡化版本（如果只需要配對記錄）

如果您的 `countdown` 函數只用於配對記錄，可以簡化為：

```python
class RatingView(View):
    def __init__(self, record_id, user1_id, user2_id):
        super().__init__(timeout=600)  # 10 分鐘超時
        self.record_id = record_id
        self.user1_id = user1_id
        self.user2_id = user2_id
        self.ratings = {}
        self.submitted_users = set()

    # ... 按鈕方法保持不變 ...

    async def handle_rating(self, interaction: discord.Interaction, rating: int):
        user_id = interaction.user.id
        self.ratings[user_id] = rating
        
        # 使用配對記錄專用的 RatingModal
        modal = RatingModalForPairing(self.record_id)
        await interaction.response.send_modal(modal)
```

## 需要檢查的項目

1. 確保 `RatingModalForPairing` 類別已定義（或使用現有的 `RatingModal` 如果它支援 `record_id`）
2. 確保 `PairingRecord` 模型已正確導入
3. 確保 `pending_ratings` 和 `evaluated_records` 變數已定義
4. 確保 `send_rating_to_admin` 函數已定義

## 在您的檔案中應用修復

請在 `E:\python.12\discord-bot\bot.py` 檔案中：

1. 找到 `class RatingView` 的定義（應該在第 2006 行附近）
2. 更新 `__init__` 方法以接受 `record_id, user1_id, user2_id` 參數
3. 更新 `handle_rating` 方法以使用正確的 Modal


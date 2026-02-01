"""
修復 RatingView 的程式碼片段

將以下程式碼替換您檔案中的 RatingView 類別定義
"""

# 修復後的 RatingView 類別（支援配對記錄）
class RatingView(View):
    def __init__(self, record_id=None, user1_id=None, user2_id=None, booking_id=None):
        """
        支援兩種模式：
        1. 配對記錄模式：record_id, user1_id, user2_id
        2. 預約模式：booking_id（向後兼容）
        """
        super().__init__(timeout=600)  # 10 分鐘超時
        
        # 判斷是哪種模式
        if record_id is not None:
            # 配對記錄模式
            self.record_id = record_id
            self.user1_id = user1_id
            self.user2_id = user2_id
            self.booking_id = None
            self.is_pairing_record = True
        elif booking_id is not None:
            # 預約模式（向後兼容）
            self.booking_id = booking_id
            self.record_id = None
            self.is_pairing_record = False
        else:
            # 如果只提供一個參數，假設是舊的 booking_id 格式（向後兼容）
            # 這允許舊代碼繼續工作
            import inspect
            frame = inspect.currentframe().f_back
            args = inspect.getargvalues(frame)
            if len([a for a in args.locals.values() if a is not None]) == 1:
                # 只有一個非 None 參數，假設是 booking_id
                self.booking_id = booking_id or record_id
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
            # 配對記錄模式：使用 RatingModal（只需要 record_id）
            # 假設您已經有一個 RatingModal 類別可以接受 record_id
            modal = RatingModal(self.record_id)
            await interaction.response.send_modal(modal)
        else:
            # 預約模式：使用原本的 RatingModal（需要 rating, booking_id, parent_view）
            modal = RatingModal(rating, self.booking_id, self)
            await interaction.response.send_modal(modal)


# 簡化版本（如果您的 countdown 函數確定只用於配對記錄）
# 使用這個版本如果確定不會有預約模式的呼叫
class RatingViewSimple(View):
    def __init__(self, record_id, user1_id, user2_id):
        super().__init__(timeout=600)  # 10 分鐘超時
        self.record_id = record_id
        self.user1_id = user1_id
        self.user2_id = user2_id
        self.ratings = {}
        self.submitted_users = set()

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
        
        # 使用配對記錄專用的 RatingModal
        modal = RatingModal(self.record_id)
        await interaction.response.send_modal(modal)


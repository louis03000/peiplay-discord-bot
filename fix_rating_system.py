"""
修復評價系統的完整代碼

請將以下代碼複製到您的 bot.py 文件中，替換現有的相關部分
"""

# ============================================
# 1. 修復 ManualRatingView - 移除 emoji 參數衝突
# ============================================

class ManualRatingView(discord.ui.View):
    def __init__(self, record_id, user1_id, user2_id):
        super().__init__(timeout=600)  # 10 分鐘超時
        self.record_id = record_id
        self.user1_id = user1_id
        self.user2_id = user2_id
        self.ratings = {}
        self.submitted_users = set()
    
    # 修復：移除 emoji 參數，因為 label 已經包含 emoji
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
    
    # 如果還有身份選擇按鈕，也移除 emoji 參數
    @discord.ui.button(label="👤 我是顧客", style=discord.ButtonStyle.primary, custom_id="role_customer")
    async def select_customer(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.selected_role = "customer"
        await interaction.response.send_message("✅ 您已選擇「顧客」身份", ephemeral=True)
    
    @discord.ui.button(label="👤 我是夥伴", style=discord.ButtonStyle.primary, custom_id="role_partner")
    async def select_partner(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.selected_role = "partner"
        await interaction.response.send_message("✅ 您已選擇「夥伴」身份", ephemeral=True)
    
    async def handle_rating(self, interaction: discord.Interaction, rating: int):
        user_id = interaction.user.id
        
        # 檢查是否已經評價過
        if user_id in self.submitted_users:
            await interaction.response.send_message("❗ 您已經提交過評價了。", ephemeral=True)
            return
        
        # 打開評價表單
        modal = RatingModal(self.record_id, rating)
        await interaction.response.send_modal(modal)


# ============================================
# 2. 修復 RatingModal - 支援評分參數
# ============================================

class RatingModal(discord.ui.Modal, title="匿名評分與留言"):
    rating_input = discord.ui.TextInput(
        label="評分（1-5星）",
        placeholder="請輸入 1-5",
        required=True,
        max_length=1
    )
    comment_input = discord.ui.TextInput(
        label="留言（選填）",
        placeholder="請輸入您的留言...",
        required=False,
        max_length=500,
        style=discord.TextStyle.paragraph
    )
    
    def __init__(self, record_id, rating=None):
        super().__init__()
        self.record_id = record_id
        self.pre_selected_rating = rating
        if rating:
            # 如果已經選擇了評分，預填並禁用評分輸入
            self.rating_input.default = str(rating)
            self.rating_input.required = False
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            # 使用預選的評分或從輸入獲取
            if self.pre_selected_rating:
                rating = self.pre_selected_rating
            else:
                try:
                    rating = int(self.rating_input.value)
                    if rating < 1 or rating > 5:
                        await interaction.response.send_message("❌ 評分必須在 1-5 之間", ephemeral=True)
                        return
                except ValueError:
                    await interaction.response.send_message("❌ 請輸入有效的數字（1-5）", ephemeral=True)
                    return
            
            comment = self.comment_input.value or ""
            
            print(f"🔍 收到評價提交: record_id={self.record_id}, rating={rating}, comment={comment}")
            
            # 保存到資料庫
            with Session() as s:
                record = s.get(PairingRecord, self.record_id)
                if not record:
                    print(f"❌ 找不到配對記錄: {self.record_id}")
                    await interaction.response.send_message("❌ 找不到配對記錄", ephemeral=True)
                    return
                
                # 獲取正確的用戶ID
                user1_id = record.user1Id
                user2_id = record.user2Id
                
                # 保存評價
                record.rating = rating
                record.comment = comment
                s.commit()
            
            await interaction.response.send_message("✅ 感謝你的匿名評價！", ephemeral=True)
            
            # 添加到待處理列表
            if self.record_id not in pending_ratings:
                pending_ratings[self.record_id] = []
            
            rating_data = {
                'rating': rating,
                'comment': comment,
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


# ============================================
# 3. 修復 countdown 函數 - 正確獲取用戶ID和檢查評價
# ============================================

async def countdown(vc_id, animal_channel_name, text_channel, vc, interaction, mentioned, record_id):
    try:
        print(f"🔍 開始倒數計時: vc_id={vc_id}, record_id={record_id}")
        
        # 檢查 record_id 是否有效
        if not record_id:
            print(f"❌ 警告: record_id 為 None，評價系統可能無法正常工作")
        
        # 移動用戶到語音頻道
        if mentioned:
            for user in mentioned:
                if user.voice and user.voice.channel:
                    await user.move_to(vc)
        
        view = ExtendView(vc.id)
        await text_channel.send(f"🎉 語音頻道 {vc.name} 已開啟！\n⏳ 可延長10分鐘 ( 為了您有更好的遊戲體驗，請到最後需要時再點選 ) 。", view=view)
        
        while active_voice_channels[vc_id]['remaining'] > 0:
            remaining = active_voice_channels[vc_id]['remaining']
            if remaining == 60:
                await text_channel.send("⏰ 剩餘 1 分鐘。")
            await asyncio.sleep(1)
            active_voice_channels[vc_id]['remaining'] -= 1
        
        await vc.delete()
        print(f"🎯 語音頻道已刪除，開始評價流程: record_id={record_id}")
        
        # 檢查 record_id 是否有效
        if not record_id:
            print(f"❌ record_id 為 None，無法顯示評價系統，刪除文字頻道")
            try:
                if text_channel and not text_channel.deleted:
                    await text_channel.delete()
                    print(f"✅ 已刪除文字頻道（無評價系統）: {text_channel.name}")
            except Exception as e:
                print(f"❌ 刪除文字頻道失敗: {e}")
            active_voice_channels.pop(vc_id, None)
            return
        
        # 從資料庫獲取正確的用戶ID
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
        
        # 顯示評價系統
        rating_system_sent = False
        try:
            if not text_channel or text_channel.deleted:
                print(f"⚠️ 文字頻道不存在或已刪除，無法顯示評價系統")
                active_voice_channels.pop(vc_id, None)
                return
            
            # 發送評價提示訊息
            embed = discord.Embed(
                title="⭐ 語音頻道已結束 - 請進行評價",
                description="感謝您使用 PeiPlay 服務！請花一點時間為您的夥伴進行匿名評價。",
                color=0xffd700
            )
            embed.add_field(
                name="📝 評價說明",
                value="• 點擊星星選擇評分(1-5星)\n• 選擇您的身份(顧客或夥伴)\n• 留言為選填項目\n• 評價完全匿名\n• 評價結果會回報給管理員",
                inline=False
            )
            embed.set_footer(text="評價有助於我們提供更好的服務品質")
            
            await text_channel.send(embed=embed)
            
            # 創建評價 View（使用正確的用戶ID）
            view = ManualRatingView(record_id, user1_id, user2_id)
            print(f"🔍 創建評價 View: record_id={record_id}, user1_id={user1_id}, user2_id={user2_id}")
            print(f"🔍 View 類型: {type(view).__name__}")
            print(f"🔍 View 按鈕數量: {len(view.children)}")
            
            await text_channel.send("📝 請使用下方按鈕進行評價：", view=view)
            rating_system_sent = True
            print(f"✅ 評價系統已成功顯示")
            
        except Exception as e:
            print(f"❌ 顯示評價系統失敗: {e}")
            import traceback
            traceback.print_exc()
            rating_system_sent = False
        
        # 如果評價系統沒有成功顯示，刪除文字頻道
        if not rating_system_sent:
            try:
                if text_channel and not text_channel.deleted:
                    await text_channel.delete()
                    print(f"✅ 已刪除文字頻道（評價系統顯示失敗）: {text_channel.name}")
            except Exception as e2:
                print(f"❌ 刪除文字頻道失敗: {e2}")
            active_voice_channels.pop(vc_id, None)
            return
        
        # 等待 10 分鐘讓用戶填寫評價
        print(f"⏰ 評價按鈕已發送，等待 600 秒後刪除文字頻道")
        await asyncio.sleep(600)  # 10 分鐘
        
        # 刪除文字頻道
        try:
            if text_channel and not text_channel.deleted:
                await text_channel.delete()
                print(f"🗑️ 文字頻道已刪除，評價流程結束")
        except Exception as e:
            print(f"❌ 刪除文字頻道失敗: {e}")
        
        # 更新記錄並發送到管理員頻道
        with Session() as s:
            record = s.get(PairingRecord, record_id)
            if record:
                record.extendedTimes = active_voice_channels.get(vc_id, {}).get('extended', 0)
                record.duration += record.extendedTimes * 600
                s.commit()
                
                # 再次從資料庫獲取正確的用戶ID（確保是最新的）
                user1_id = record.user1Id
                user2_id = record.user2Id
                duration = record.duration
                extended_times = record.extendedTimes
                booking_id = record.bookingId
                
                print(f"🔍 發送管理員訊息: user1_id={user1_id}, user2_id={user2_id}")
        
        # 發送到管理員頻道
        admin = bot.get_channel(ADMIN_CHANNEL_ID)
        if admin:
            try:
                # 獲取用戶顯示名稱
                try:
                    u1 = await bot.fetch_user(int(user1_id))
                    user1_display = u1.mention
                except:
                    user1_display = f"<@{user1_id}>"
                
                try:
                    u2 = await bot.fetch_user(int(user2_id))
                    user2_display = u2.mention
                except:
                    user2_display = f"<@{user2_id}>"
                
                header = f"📋 配對紀錄：{user1_display} × {user2_display} | {duration//60} 分鐘 | 延長 {extended_times} 次"
                if booking_id:
                    header += f" | 預約ID: {booking_id}"
                
                # 檢查是否有評價
                has_ratings = False
                feedback = "\n⭐ 評價回饋："
                
                # 檢查 pending_ratings
                if record_id in pending_ratings and len(pending_ratings[record_id]) > 0:
                    has_ratings = True
                    for r in pending_ratings[record_id]:
                        try:
                            from_user = await bot.fetch_user(int(r['user1']))
                            from_user_display = from_user.mention
                        except:
                            from_user_display = f"<@{r['user1']}>"
                        
                        try:
                            to_user = await bot.fetch_user(int(r['user2']))
                            to_user_display = to_user.mention
                        except:
                            to_user_display = f"<@{r['user2']}>"
                        
                        feedback += f"\n- 「{from_user_display} → {to_user_display}」：{r['rating']} ⭐"
                        if r.get('comment'):
                            feedback += f"\n  💬 {r['comment']}"
                    
                    del pending_ratings[record_id]
                
                # 也檢查資料庫中是否有評價
                with Session() as s:
                    db_record = s.get(PairingRecord, record_id)
                    if db_record and db_record.rating:
                        if not has_ratings:
                            has_ratings = True
                            feedback += f"\n- 評分：{db_record.rating} ⭐"
                            if db_record.comment:
                                feedback += f"\n  💬 {db_record.comment}"
                
                if has_ratings:
                    await admin.send(f"{header}{feedback}")
                else:
                    await admin.send(f"{header}\n⭐ 沒有收到任何評價。")
                    
            except Exception as e:
                print(f"❌ 推送管理區評價失敗：{e}")
                import traceback
                traceback.print_exc()
        
        active_voice_channels.pop(vc_id, None)
        
    except Exception as e:
        print(f"❌ 倒數錯誤: {e}")
        import traceback
        traceback.print_exc()


#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""修复 bot.py 中的所有缩进错误"""

import re

with open('bot.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

fixed_count = 0
errors = []

# 检查每一行的缩进
for i, line in enumerate(lines):
    # 检查是否有错误的缩进（16个空格的 else，应该是8个）
    if line.startswith('                else:'):
        lines[i] = '        else:\n'
        fixed_count += 1
        errors.append(f"Line {i+1}: Fixed else indentation")
    
    # 检查是否有错误的缩进（28个空格的 embed，可能是错误的）
    if line.startswith('                            embed = discord.Embed('):
        # 检查上下文，看看应该是什么缩进
        if i > 0 and 'await channel.send' in lines[i-1]:
            # 应该和上一行对齐
            lines[i] = '                    embed = discord.Embed(\n'
            fixed_count += 1
            errors.append(f"Line {i+1}: Fixed embed indentation")

if fixed_count > 0:
    with open('bot.py', 'w', encoding='utf-8') as f:
        f.writelines(lines)
    print(f"\n✅ 修复了 {fixed_count} 处缩进错误:")
    for err in errors:
        print(f"  - {err}")
else:
    print("没有找到需要修复的缩进错误")

# 尝试编译检查
import py_compile
try:
    py_compile.compile('bot.py', doraise=True)
    print("\n✅ 语法检查通过！")
except py_compile.PyCompileError as e:
    print(f"\n❌ 仍有语法错误: {e}")


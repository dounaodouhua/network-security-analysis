#!/usr/bin/env python3
"""
测试 DeepSeek API 连接
"""

import os
from openai import OpenAI

# 从环境变量获取 API Key
api_key = os.environ.get('DEEPSEEK_API_KEY')

if not api_key:
    print("❌ 错误: 未设置 DEEPSEEK_API_KEY 环境变量")
    print("\n请先设置环境变量:")
    print("  Windows (PowerShell): $env:DEEPSEEK_API_KEY='your-api-key'")
    print("  Windows (CMD): set DEEPSEEK_API_KEY=your-api-key")
    print("  Linux/Mac: export DEEPSEEK_API_KEY='your-api-key'")
    exit(1)

print(f"✓ API Key 已设置: {api_key[:10]}...")

# 创建客户端
client = OpenAI(
    api_key=api_key,
    base_url="https://api.deepseek.com"
)

print("\n正在测试 DeepSeek API 连接...")

try:
    # 发送测试请求
    response = client.chat.completions.create(
        model="deepseek-chat",
        messages=[
            {"role": "system", "content": "You are a helpful assistant"},
            {"role": "user", "content": "请用一句话介绍你自己"},
        ],
        stream=False
    )
    
    print("\n✅ DeepSeek API 连接成功！")
    print(f"\n模型响应: {response.choices[0].message.content}")
    print(f"\n使用的模型: {response.model}")
    print(f"Token 使用: {response.usage.total_tokens} (输入: {response.usage.prompt_tokens}, 输出: {response.usage.completion_tokens})")
    
except Exception as e:
    print(f"\n❌ DeepSeek API 连接失败: {e}")
    exit(1)


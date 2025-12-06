# DeepSeek API 配置说明

## 1. 获取 DeepSeek API Key

1. 访问 [DeepSeek 官网](https://platform.deepseek.com/)
2. 注册/登录账号
3. 进入 API Keys 页面
4. 创建新的 API Key 并复制

## 2. 设置环境变量

### Windows PowerShell
```powershell
$env:DEEPSEEK_API_KEY="your-api-key-here"
```

### Windows CMD
```cmd
set DEEPSEEK_API_KEY=your-api-key-here
```

### Linux/Mac
```bash
export DEEPSEEK_API_KEY="your-api-key-here"
```

### 永久设置（Windows）
1. 打开"系统属性" → "高级" → "环境变量"
2. 在"用户变量"中新建变量
   - 变量名: `DEEPSEEK_API_KEY`
   - 变量值: 你的 API Key
3. 点击"确定"保存

## 3. 测试 API 连接

运行测试脚本：
```bash
cd D:\python\network-security-analysis
D:\python\.venv\Scripts\python.exe test_deepseek.py
```

如果连接成功，会显示：
```
✅ DeepSeek API 连接成功！
模型响应: [DeepSeek 的回复]
```

## 4. 运行完整分析

设置好 API Key 后，运行分析：
```bash
D:\python\.venv\Scripts\python.exe run_analysis.py --visualize
```

系统会自动使用 DeepSeek 生成专业的安全分析报告。

## 5. DeepSeek vs OpenAI GPT

### DeepSeek 优势
- ✅ **价格更低**: 相比 GPT-4 便宜很多
- ✅ **中文支持好**: 专门优化了中文理解
- ✅ **无需翻墙**: 国内可直接访问
- ✅ **响应速度快**: 延迟更低

### 配置对比
| 项目 | DeepSeek | OpenAI GPT-4 |
|------|----------|--------------|
| API Base URL | `https://api.deepseek.com` | `https://api.openai.com/v1` |
| 模型名称 | `deepseek-chat` | `gpt-4-turbo-preview` |
| 价格 | ¥1/百万tokens | ¥70/百万tokens |
| 中文能力 | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ |

## 6. 报告生成位置

生成的报告会保存在：
- `reports/llm_report.md` - DeepSeek 生成的智能报告
- `reports/template_report.md` - 模板报告（备用）

## 7. 故障排除

### 问题 1: API Key 无效
```
❌ 错误: 未设置 DEEPSEEK_API_KEY 环境变量
```
**解决**: 确保已正确设置环境变量，重启终端后再试

### 问题 2: 连接超时
```
❌ DeepSeek API 连接失败: Connection timeout
```
**解决**: 检查网络连接，确保可以访问 api.deepseek.com

### 问题 3: 余额不足
```
❌ Error: Insufficient balance
```
**解决**: 登录 DeepSeek 平台充值

## 8. 成本估算

一次完整的网络安全分析报告生成：
- 输入 tokens: ~1000-2000
- 输出 tokens: ~1000-1500
- 总成本: 约 ¥0.002-0.005 (不到1分钱)

相比之下，使用 GPT-4 的成本约为 ¥0.14-0.35，贵了约 70 倍！


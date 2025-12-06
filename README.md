# 网络安全态势分析系统

基于Spark和大语言模型的网络入侵检测分析系统，专为UNSW-NB15数据集优化。

## ✅ 最新更新

- 已移除KDD99和CIC-IDS2017数据集支持
- 专注于UNSW-NB15数据集
- 自动适配UNSW-NB15列名（sbytes/dbytes等）
- 修复所有列名兼容性问题

## 快速开始

```bash
cd D:\python\network-security-analysis
D:\python\.venv\Scripts\python.exe run_analysis.py
```

## 运行模式

### 批处理模式（默认）
```bash
python run_analysis.py --mode batch
```

### 流处理模式
```bash
python run_analysis.py --mode streaming
```

### 可视化模式
```bash
python run_analysis.py --visualize
```
访问: http://localhost:8050

## 数据集

**UNSW-NB15** - 现代网络入侵检测数据集
- 记录数: 约257万条（示例数据1,900条）
- 特征数: 49个
- 攻击类型: 9种（Fuzzers, Analysis, Backdoor, DoS, Exploits, Generic, Reconnaissance, Shellcode, Worms）
- 📖 完整字段说明: 参见 [UNSW_NB15_FIELDS.md](./UNSW_NB15_FIELDS.md)
- 📊 字段元数据: `data/NUSW-NB15_features.csv`

## 输出结果

结果保存在 `results/` 目录：
- `analysis_report.html` - HTML格式报告
- `analysis_report.txt` - 文本格式报告
- `statistics.json` - 统计数据
- `anomalies.csv` - 检测到的异常
- `visualizations/` - 可视化图表

## 配置

### 基础配置

编辑 `config.py` 调整配置：
```python
DATA_PATH = "data/UNSW-NB15_1.csv"  # 数据路径
SPARK_EXECUTOR_MEMORY = "4g"        # Spark内存
VISUALIZATION_PORT = 8050            # 可视化端口
```

### DeepSeek API 配置（用于智能报告生成）

系统使用 DeepSeek API 生成专业的安全分析报告。

#### 方式 1: 环境变量（推荐）
```powershell
# Windows PowerShell
$env:DEEPSEEK_API_KEY="your-api-key-here"

# Linux/macOS
export DEEPSEEK_API_KEY="your-api-key-here"
```

#### 方式 2: 直接在 config.py 中配置
```python
LLM_API_KEY = "your-api-key-here"  # 默认值
LLM_MODEL = "deepseek-chat"
LLM_BASE_URL = "https://api.deepseek.com/v1"
```

📖 详细说明: 参见 [DEEPSEEK_SETUP.md](./DEEPSEEK_SETUP.md)

## 系统架构

1. **数据加载** (`data_loader.py`) - 加载和预处理UNSW-NB15数据
2. **数据预处理** (`data_preprocessor.py`) - 特征工程和标准化
3. **流量分析** (`traffic_analyzer.py`) - 网络流量分析和异常检测
4. **报告生成** (`report_generator.py`) - 生成分析报告
5. **可视化** (`visualizer.py`) - 交互式可视化仪表盘

## 注意事项

- 首次运行会自动生成示例数据
- 建议至少8GB RAM
- Windows上的Spark警告可以忽略


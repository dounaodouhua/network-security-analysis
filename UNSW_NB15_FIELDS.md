# UNSW-NB15 数据集字段说明

本文档详细说明了 UNSW-NB15 网络流量数据集的所有 49 个字段。

## 数据来源

参考文件：`data/NUSW-NB15_features.csv`

## 字段列表

### 基础连接信息 (1-6)

| 序号 | 字段名 | 类型 | 说明 |
|-----|--------|------|------|
| 1 | `srcip` | nominal | 源IP地址 |
| 2 | `sport` | integer | 源端口号 |
| 3 | `dstip` | nominal | 目标IP地址 |
| 4 | `dsport` | integer | 目标端口号 |
| 5 | `proto` | nominal | 传输协议（TCP、UDP等） |
| 6 | `state` | nominal | 连接状态：ACC, CLO, CON, ECO, ECR, FIN, INT, MAS, PAR, REQ, RST, TST, TXD, URH, URN, 或 (-) |

### 时长与字节统计 (7-13)

| 序号 | 字段名 | 类型 | 说明 |
|-----|--------|------|------|
| 7 | `dur` | Float | 记录总持续时间 |
| 8 | `sbytes` | Integer | 源到目标的传输字节数 |
| 9 | `dbytes` | Integer | 目标到源的传输字节数 |
| 10 | `sttl` | Integer | 源到目标的生存时间值 (TTL) |
| 11 | `dttl` | Integer | 目标到源的生存时间值 (TTL) |
| 12 | `sloss` | Integer | 源端重传或丢弃的数据包数 |
| 13 | `dloss` | Integer | 目标端重传或丢弃的数据包数 |

### 服务与负载 (14-18)

| 序号 | 字段名 | 类型 | 说明 |
|-----|--------|------|------|
| 14 | `service` | nominal | 服务类型：http, ftp, smtp, ssh, dns, ftp-data, irc 或 (-) |
| 15 | `sload` | Float | 源端的比特每秒 (bits per second) |
| 16 | `dload` | Float | 目标端的比特每秒 (bits per second) |
| 17 | `spkts` | integer | 源到目标的数据包计数 |
| 18 | `dpkts` | integer | 目标到源的数据包计数 |

### TCP 窗口与序列号 (19-24)

| 序号 | 字段名 | 类型 | 说明 |
|-----|--------|------|------|
| 19 | `swin` | integer | 源端TCP窗口广告值 |
| 20 | `dwin` | integer | 目标端TCP窗口广告值 |
| 21 | `stcpb` | integer | 源端TCP基本序列号 |
| 22 | `dtcpb` | integer | 目标端TCP基本序列号 |
| 23 | `smeansz` | integer | 源端传输的流数据包平均大小 |
| 24 | `dmeansz` | integer | 目标端传输的流数据包平均大小 |

### HTTP 特征 (25-26)

| 序号 | 字段名 | 类型 | 说明 |
|-----|--------|------|------|
| 25 | `trans_depth` | integer | 表示HTTP请求/响应事务的管道深度 |
| 26 | `res_bdy_len` | integer | 从服务器HTTP服务传输的数据的实际未压缩内容大小 |

### 抖动与时间 (27-32)

| 序号 | 字段名 | 类型 | 说明 |
|-----|--------|------|------|
| 27 | `sjit` | Float | 源端抖动（毫秒） |
| 28 | `djit` | Float | 目标端抖动（毫秒） |
| 29 | `stime` | Timestamp | 记录开始时间 |
| 30 | `ltime` | Timestamp | 记录结束时间 |
| 31 | `sintpkt` | Float | 源端数据包到达间隔时间（毫秒） |
| 32 | `dintpkt` | Float | 目标端数据包到达间隔时间（毫秒） |

### TCP 连接时间 (33-35)

| 序号 | 字段名 | 类型 | 说明 |
|-----|--------|------|------|
| 33 | `tcprtt` | Float | TCP连接建立往返时间，即 synack + ackdat |
| 34 | `synack` | Float | TCP连接建立时间：SYN 和 SYN_ACK 包之间的时间 |
| 35 | `ackdat` | Float | TCP连接建立时间：SYN_ACK 和 ACK 包之间的时间 |

### 二进制标志 (36, 39)

| 序号 | 字段名 | 类型 | 说明 |
|-----|--------|------|------|
| 36 | `is_sm_ips_ports` | Binary | 如果源和目标IP地址及端口号相等，则为1，否则为0 |
| 39 | `is_ftp_login` | Binary | 如果FTP会话通过用户名和密码访问，则为1，否则为0 |

### 连接统计特征 (37-38, 40-47)

| 序号 | 字段名 | 类型 | 说明 |
|-----|--------|------|------|
| 37 | `ct_state_ttl` | Integer | 根据源/目标TTL值的特定范围，每个状态的数量 |
| 38 | `ct_flw_http_mthd` | Integer | HTTP服务中具有Get和Post方法的流数量 |
| 40 | `ct_ftp_cmd` | integer | FTP会话中具有命令的流数量 |
| 41 | `ct_srv_src` | integer | 在最近100个连接中，包含相同服务和源地址的连接数量 |
| 42 | `ct_srv_dst` | integer | 在最近100个连接中，包含相同服务和目标地址的连接数量 |
| 43 | `ct_dst_ltm` | integer | 在最近100个连接中，相同目标地址的连接数量 |
| 44 | `ct_src_ltm` | integer | 在最近100个连接中，相同源地址的连接数量 |
| 45 | `ct_src_dport_ltm` | integer | 在最近100个连接中，相同源地址和目标端口的连接数量 |
| 46 | `ct_dst_sport_ltm` | integer | 在最近100个连接中，相同目标地址和源端口的连接数量 |
| 47 | `ct_dst_src_ltm` | integer | 在最近100个连接中，相同源和目标地址的连接数量 |

### 标签字段 (48-49)

| 序号 | 字段名 | 类型 | 说明 |
|-----|--------|------|------|
| 48 | `attack_cat` | nominal | 攻击类别名称：Fuzzers, Analysis, Backdoors, DoS, Exploits, Generic, Reconnaissance, Shellcode, Worms |
| 49 | `label` | binary | 0表示正常流量，1表示攻击流量 |

## 攻击类别说明

UNSW-NB15 数据集包含以下 9 种攻击类别：

1. **Fuzzers（模糊测试）**: 试图通过向目标发送大量随机数据来发现安全漏洞
2. **Analysis（分析攻击）**: 包括端口扫描、垃圾邮件和HTML文件渗透等
3. **Backdoors（后门）**: 绕过正常认证机制，秘密获取远程访问权限
4. **DoS（拒绝服务）**: 使目标系统无法提供正常服务
5. **Exploits（漏洞利用）**: 利用已知软件漏洞获取未授权访问
6. **Generic（通用攻击）**: 针对块密码的攻击技术
7. **Reconnaissance（侦察）**: 收集目标网络的信息以准备攻击
8. **Shellcode（Shell代码）**: 利用软件漏洞注入恶意代码
9. **Worms（蠕虫）**: 能够自我复制并在网络中传播的恶意软件

## 数据类型映射

在 PySpark 中的类型映射：

- `nominal` → `StringType()`
- `integer` → `IntegerType()` 或 `LongType()`（对于大数值如TCP序列号）
- `Float` → `DoubleType()`
- `Binary` → `IntegerType()`（0或1）
- `Timestamp` → `LongType()`（Unix时间戳）

## 使用示例

```python
from data_loader import DataLoader

# 加载数据
loader = DataLoader(spark)
df = loader.load_dataset()

# 查看基本统计
df.select('srcip', 'dstip', 'proto', 'service', 'attack_cat', 'label').show(10)

# 攻击分布统计
df.groupBy('attack_cat').count().orderBy('count', ascending=False).show()
```

## 参考资料

- 官方网站: https://research.unsw.edu.au/projects/unsw-nb15-dataset
- 数据集论文: Moustafa, N., & Slay, J. (2015). UNSW-NB15: a comprehensive data set for network intrusion detection systems


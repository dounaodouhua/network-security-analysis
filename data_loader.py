from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnan
from pyspark.sql.types import *
import logging


class DataLoader:
    """数据加载与理解模块 - UNSW-NB15数据集"""

    def __init__(self, spark):
        self.spark = spark
        self.logger = logging.getLogger(__name__)

    def load_dataset(self):
        """加载UNSW-NB15数据集"""
        from config import Config
        return self._load_unsw_nb15(Config.DATA_PATH)

    def _load_unsw_nb15(self, filepath):
        """加载UNSW-NB15数据集"""
        self.logger.info(f"加载UNSW-NB15数据集: {filepath}")

        # 检查文件是否存在
        import os
        if not os.path.exists(filepath):
            self.logger.warning(f"文件不存在: {filepath}")
            self.logger.info("尝试下载UNSW-NB15数据集...")
            filepath = self._download_unsw_nb15_dataset()

        # UNSW-NB15数据集CSV格式
        # 定义UNSW-NB15的列名（真实数据集没有表头）
        unsw_columns = [
            "srcip", "sport", "dstip", "dsport", "proto", "state", "dur", "sbytes", "dbytes",
            "sttl", "dttl", "sloss", "dloss", "service", "Sload", "Dload", "Spkts", "Dpkts",
            "swin", "dwin", "stcpb", "dtcpb", "smeansz", "dmeansz", "trans_depth", "res_bdy_len",
            "Sjit", "Djit", "Stime", "Ltime", "Sintpkt", "Dintpkt", "tcprtt", "synack", "ackdat",
            "is_sm_ips_ports", "ct_state_ttl", "ct_flw_http_mthd", "is_ftp_login", "ct_ftp_cmd",
            "ct_srv_src", "ct_srv_dst", "ct_dst_ltm", "ct_src_ltm", "ct_src_dport_ltm",
            "ct_dst_sport_ltm", "ct_dst_src_ltm", "attack_cat", "label"
        ]
        
        try:
            # 先尝试读取第一行判断是否有表头
            sample_df = self.spark.read.csv(filepath, header=False).limit(1)
            first_row = sample_df.collect()[0][0] if sample_df.count() > 0 else ""
            
            # 如果第一行包含"srcip"等列名，说明有表头
            has_header = "srcip" in str(first_row).lower()
            
            if has_header:
                # 有表头的情况（示例数据）
                df = self.spark.read.csv(filepath, header=True, inferSchema=True)
                # 标准化列名
                for col_name in df.columns:
                    new_col_name = col_name.strip().lower().replace(' ', '_').replace('-', '_')
                    if col_name != new_col_name:
                        df = df.withColumnRenamed(col_name, new_col_name)
            else:
                # 无表头的情况（真实UNSW-NB15数据）
                from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType
                
                # 定义schema（根据 NUSW-NB15_features.csv 完整字段说明）
                # 参考: data/NUSW-NB15_features.csv
                schema = StructType([
                    # 1. srcip - Source IP address (nominal)
                    StructField("srcip", StringType(), True),
                    # 2. sport - Source port number (integer)
                    StructField("sport", IntegerType(), True),
                    # 3. dstip - Destination IP address (nominal)
                    StructField("dstip", StringType(), True),
                    # 4. dsport - Destination port number (integer)
                    StructField("dsport", IntegerType(), True),
                    # 5. proto - Transaction protocol (nominal)
                    StructField("proto", StringType(), True),
                    # 6. state - Connection state (nominal)
                    StructField("state", StringType(), True),
                    # 7. dur - Record total duration (Float)
                    StructField("dur", DoubleType(), True),
                    # 8. sbytes - Source to destination transaction bytes (Integer)
                    StructField("sbytes", IntegerType(), True),
                    # 9. dbytes - Destination to source transaction bytes (Integer)
                    StructField("dbytes", IntegerType(), True),
                    # 10. sttl - Source to destination time to live value (Integer)
                    StructField("sttl", IntegerType(), True),
                    # 11. dttl - Destination to source time to live value (Integer)
                    StructField("dttl", IntegerType(), True),
                    # 12. sloss - Source packets retransmitted or dropped (Integer)
                    StructField("sloss", IntegerType(), True),
                    # 13. dloss - Destination packets retransmitted or dropped (Integer)
                    StructField("dloss", IntegerType(), True),
                    # 14. service - Service type: http, ftp, smtp, ssh, dns, etc. (nominal)
                    StructField("service", StringType(), True),
                    # 15. Sload - Source bits per second (Float)
                    StructField("sload", DoubleType(), True),
                    # 16. Dload - Destination bits per second (Float)
                    StructField("dload", DoubleType(), True),
                    # 17. Spkts - Source to destination packet count (integer)
                    StructField("spkts", IntegerType(), True),
                    # 18. Dpkts - Destination to source packet count (integer)
                    StructField("dpkts", IntegerType(), True),
                    # 19. swin - Source TCP window advertisement value (integer)
                    StructField("swin", IntegerType(), True),
                    # 20. dwin - Destination TCP window advertisement value (integer)
                    StructField("dwin", IntegerType(), True),
                    # 21. stcpb - Source TCP base sequence number (integer)
                    StructField("stcpb", LongType(), True),
                    # 22. dtcpb - Destination TCP base sequence number (integer)
                    StructField("dtcpb", LongType(), True),
                    # 23. smeansz - Mean of the flow packet size transmitted by the src (integer)
                    StructField("smeansz", IntegerType(), True),
                    # 24. dmeansz - Mean of the flow packet size transmitted by the dst (integer)
                    StructField("dmeansz", IntegerType(), True),
                    # 25. trans_depth - Pipelined depth into the connection of http request/response (integer)
                    StructField("trans_depth", IntegerType(), True),
                    # 26. res_bdy_len - Actual uncompressed content size from server's http service (integer)
                    StructField("res_bdy_len", IntegerType(), True),
                    # 27. Sjit - Source jitter in mSec (Float)
                    StructField("sjit", DoubleType(), True),
                    # 28. Djit - Destination jitter in mSec (Float)
                    StructField("djit", DoubleType(), True),
                    # 29. Stime - Record start time (Timestamp)
                    StructField("stime", LongType(), True),
                    # 30. Ltime - Record last time (Timestamp)
                    StructField("ltime", LongType(), True),
                    # 31. Sintpkt - Source interpacket arrival time in mSec (Float)
                    StructField("sintpkt", DoubleType(), True),
                    # 32. Dintpkt - Destination interpacket arrival time in mSec (Float)
                    StructField("dintpkt", DoubleType(), True),
                    # 33. tcprtt - TCP connection setup round-trip time (Float)
                    StructField("tcprtt", DoubleType(), True),
                    # 34. synack - TCP setup time between SYN and SYN_ACK packets (Float)
                    StructField("synack", DoubleType(), True),
                    # 35. ackdat - TCP setup time between SYN_ACK and ACK packets (Float)
                    StructField("ackdat", DoubleType(), True),
                    # 36. is_sm_ips_ports - If src/dst IPs and ports equal then 1 else 0 (Binary)
                    StructField("is_sm_ips_ports", IntegerType(), True),
                    # 37. ct_state_ttl - No. for each state according to specific TTL range (Integer)
                    StructField("ct_state_ttl", IntegerType(), True),
                    # 38. ct_flw_http_mthd - No. of flows with Get/Post methods in http service (Integer)
                    StructField("ct_flw_http_mthd", IntegerType(), True),
                    # 39. is_ftp_login - If ftp session accessed by user/password then 1 else 0 (Binary)
                    StructField("is_ftp_login", IntegerType(), True),
                    # 40. ct_ftp_cmd - No. of flows with a command in ftp session (integer)
                    StructField("ct_ftp_cmd", IntegerType(), True),
                    # 41. ct_srv_src - No. of connections with same service & src address in 100 conns (integer)
                    StructField("ct_srv_src", IntegerType(), True),
                    # 42. ct_srv_dst - No. of connections with same service & dst address in 100 conns (integer)
                    StructField("ct_srv_dst", IntegerType(), True),
                    # 43. ct_dst_ltm - No. of connections with same dst address in 100 conns (integer)
                    StructField("ct_dst_ltm", IntegerType(), True),
                    # 44. ct_src_ltm - No. of connections with same src address in 100 conns (integer)
                    StructField("ct_src_ltm", IntegerType(), True),
                    # 45. ct_src_dport_ltm - No. of connections with same src address & dst port in 100 conns (integer)
                    StructField("ct_src_dport_ltm", IntegerType(), True),
                    # 46. ct_dst_sport_ltm - No. of connections with same dst address & src port in 100 conns (integer)
                    StructField("ct_dst_sport_ltm", IntegerType(), True),
                    # 47. ct_dst_src_ltm - No. of connections with same src & dst address in 100 conns (integer)
                    StructField("ct_dst_src_ltm", IntegerType(), True),
                    # 48. attack_cat - Attack category name: Fuzzers, Analysis, Backdoors, DoS, Exploits, etc. (nominal)
                    StructField("attack_cat", StringType(), True),
                    # 49. Label - 0 for normal, 1 for attack records (binary)
                    StructField("label", IntegerType(), True)
                ])
                
                df = self.spark.read.csv(filepath, header=False, schema=schema, inferSchema=False)
            
            # 添加攻击类别（如果label列存在）
            if "label" in df.columns or "attack_cat" in df.columns:
                # UNSW-NB15使用attack_cat列表示攻击类型
                label_col = "attack_cat" if "attack_cat" in df.columns else "label"
                
                # 如果没有attack_category列，添加一个
                if "attack_category" not in df.columns:
                    from pyspark.sql.functions import when
                    df = df.withColumn("attack_category", 
                                      when(col(label_col).isNull() | (col(label_col) == "Normal"), "normal")
                                      .otherwise(col(label_col)))
            
            self.logger.info(f"成功加载UNSW-NB15数据集，共 {df.count()} 条记录")
            return df
            
        except Exception as e:
            self.logger.error(f"加载UNSW-NB15数据集失败: {e}")
            raise

    def _download_unsw_nb15_dataset(self):
        """下载UNSW-NB15数据集"""
        import urllib.request
        import os

        data_dir = "data"
        os.makedirs(data_dir, exist_ok=True)

        filepath = os.path.join(data_dir, "UNSW_NB15.csv")

        # 如果filepath是目录，先删除
        if os.path.isdir(filepath):
            self.logger.warning(f"检测到 {filepath} 是目录，正在删除...")
            try:
                import shutil
                shutil.rmtree(filepath)
                self.logger.info(f"已删除目录: {filepath}")
            except Exception as e:
                self.logger.error(f"删除目录失败: {e}")
                raise

        # UNSW-NB15数据集的官方下载链接
        urls = [
            "https://cloudstor.aarnet.edu.au/plus/s/2DhnLGDdEECo4ys/download?path=%2FUNSW-NB15%20-%20CSV%20Files&files=UNSW-NB15_1.csv",
        ]

        for url in urls:
            try:
                self.logger.info(f"从 {url} 下载数据集...")
                
                req = urllib.request.Request(
                    url,
                    headers={'User-Agent': 'Mozilla/5.0'}
                )
                
                with urllib.request.urlopen(req, timeout=60) as response:
                    with open(filepath, 'wb') as out_file:
                        out_file.write(response.read())

                self.logger.info(f"数据集已下载到: {filepath}")
                return filepath

            except Exception as e:
                self.logger.warning(f"从 {url} 下载失败: {e}")
                continue

        # 所有下载源都失败，创建示例数据用于测试
        self.logger.info("所有下载源均失败，创建示例数据用于测试...")
        self._create_unsw_sample_data(filepath)
        return filepath

    def _create_unsw_sample_data(self, filepath):
        """创建UNSW-NB15示例数据"""
        import random
        import os
        import shutil
        import csv
        
        # 如果filepath是目录，先删除
        if os.path.isdir(filepath):
            self.logger.warning(f"检测到 {filepath} 是目录，正在删除...")
            try:
                shutil.rmtree(filepath)
                self.logger.info(f"已删除目录: {filepath}")
            except Exception as e:
                self.logger.error(f"删除目录失败: {e}")
                raise
        
        # 如果文件已存在，删除它
        if os.path.exists(filepath):
            try:
                os.remove(filepath)
            except Exception as e:
                self.logger.error(f"删除文件失败: {e}")
                raise
        
        # UNSW-NB15特征列（简化版，包含主要特征）
        columns = [
            'srcip', 'sport', 'dstip', 'dsport', 'proto', 'state', 'dur', 'sbytes', 'dbytes',
            'sttl', 'dttl', 'sloss', 'dloss', 'service', 'Sload', 'Dload', 'Spkts', 'Dpkts',
            'swin', 'dwin', 'stcpb', 'dtcpb', 'smeansz', 'dmeansz', 'trans_depth',
            'res_bdy_len', 'Sjit', 'Djit', 'Stime', 'Ltime', 'Sintpkt', 'Dintpkt',
            'tcprtt', 'synack', 'ackdat', 'is_sm_ips_ports', 'ct_state_ttl', 'ct_flw_http_mthd',
            'is_ftp_login', 'ct_ftp_cmd', 'ct_srv_src', 'ct_srv_dst', 'ct_dst_ltm',
            'ct_src_ltm', 'ct_src_dport_ltm', 'ct_dst_sport_ltm', 'ct_dst_src_ltm',
            'attack_cat', 'label'
        ]
        
        # 攻击类型
        attack_types = {
            'Normal': 0,
            'Fuzzers': 1,
            'Analysis': 1,
            'Backdoor': 1,
            'DoS': 1,
            'Exploits': 1,
            'Generic': 1,
            'Reconnaissance': 1,
            'Shellcode': 1,
            'Worms': 1
        }
        
        # 生成示例数据
        data_rows = []
        
        # 生成1000条正常流量
        for i in range(1000):
            row = [
                f"192.168.{random.randint(1,254)}.{random.randint(1,254)}",  # srcip
                random.randint(1024, 65535),  # sport
                f"10.0.{random.randint(1,254)}.{random.randint(1,254)}",  # dstip
                random.choice([80, 443, 22, 21, 25]),  # dsport
                random.choice(['tcp', 'udp']),  # proto
                random.choice(['FIN', 'CON', 'INT']),  # state
                round(random.uniform(0.1, 100), 2),  # dur
                random.randint(100, 10000),  # sbytes
                random.randint(100, 10000),  # dbytes
                random.randint(32, 128),  # sttl
                random.randint(32, 128),  # dttl
                0,  # sloss
                0,  # dloss
                random.choice(['http', 'ftp', 'ssh', 'smtp', '-']),  # service
                round(random.uniform(0, 1000), 2),  # Sload
                round(random.uniform(0, 1000), 2),  # Dload
                random.randint(1, 100),  # Spkts
                random.randint(1, 100),  # Dpkts
                random.randint(100, 65535),  # swin
                random.randint(100, 65535),  # dwin
                random.randint(0, 1000000),  # stcpb
                random.randint(0, 1000000),  # dtcpb
                random.randint(40, 1500),  # smeansz
                random.randint(40, 1500),  # dmeansz
                random.randint(0, 5),  # trans_depth
                random.randint(0, 5000),  # res_bdy_len
                round(random.uniform(0, 10), 4),  # Sjit
                round(random.uniform(0, 10), 4),  # Djit
                random.randint(1000000000, 1700000000),  # Stime
                random.randint(1000000000, 1700000000),  # Ltime
                round(random.uniform(0, 1), 4),  # Sintpkt
                round(random.uniform(0, 1), 4),  # Dintpkt
                round(random.uniform(0, 1), 4),  # tcprtt
                round(random.uniform(0, 1), 4),  # synack
                round(random.uniform(0, 1), 4),  # ackdat
                0,  # is_sm_ips_ports
                random.randint(1, 10),  # ct_state_ttl
                random.randint(0, 5),  # ct_flw_http_mthd
                0,  # is_ftp_login
                random.randint(0, 5),  # ct_ftp_cmd
                random.randint(1, 20),  # ct_srv_src
                random.randint(1, 20),  # ct_srv_dst
                random.randint(1, 50),  # ct_dst_ltm
                random.randint(1, 50),  # ct_src_ltm
                random.randint(1, 30),  # ct_src_dport_ltm
                random.randint(1, 30),  # ct_dst_sport_ltm
                random.randint(1, 40),  # ct_dst_src_ltm
                'Normal',  # attack_cat
                0  # label
            ]
            data_rows.append(row)
        
        # 生成各种攻击流量
        attack_list = ['Fuzzers', 'Analysis', 'Backdoor', 'DoS', 'Exploits', 
                      'Generic', 'Reconnaissance', 'Shellcode', 'Worms']
        
        for attack_type in attack_list:
            for i in range(100):  # 每种攻击100条
                row = [
                    f"192.168.{random.randint(1,254)}.{random.randint(1,254)}",
                    random.randint(1024, 65535),
                    f"10.0.{random.randint(1,254)}.{random.randint(1,254)}",
                    random.choice([80, 443, 22, 21, 25, 3389]),
                    random.choice(['tcp', 'udp']),
                    random.choice(['FIN', 'CON', 'INT', 'REQ', 'RST']),
                    round(random.uniform(0.01, 200), 2),
                    random.randint(0, 50000),
                    random.randint(0, 50000),
                    random.randint(1, 255),
                    random.randint(1, 255),
                    random.randint(0, 10),
                    random.randint(0, 10),
                    random.choice(['http', 'ftp', 'ssh', 'smtp', 'dns', '-']),
                    round(random.uniform(0, 5000), 2),
                    round(random.uniform(0, 5000), 2),
                    random.randint(1, 500),
                    random.randint(1, 500),
                    random.randint(0, 65535),
                    random.randint(0, 65535),
                    random.randint(0, 2000000),
                    random.randint(0, 2000000),
                    random.randint(20, 1500),
                    random.randint(20, 1500),
                    random.randint(0, 10),
                    random.randint(0, 10000),
                    round(random.uniform(0, 50), 4),
                    round(random.uniform(0, 50), 4),
                    random.randint(1000000000, 1700000000),
                    random.randint(1000000000, 1700000000),
                    round(random.uniform(0, 5), 4),
                    round(random.uniform(0, 5), 4),
                    round(random.uniform(0, 5), 4),
                    round(random.uniform(0, 5), 4),
                    round(random.uniform(0, 5), 4),
                    random.randint(0, 1),
                    random.randint(1, 50),
                    random.randint(0, 20),
                    random.randint(0, 1),
                    random.randint(0, 20),
                    random.randint(1, 100),
                    random.randint(1, 100),
                    random.randint(1, 200),
                    random.randint(1, 200),
                    random.randint(1, 150),
                    random.randint(1, 150),
                    random.randint(1, 180),
                    attack_type,
                    1
                ]
                data_rows.append(row)
        
        # 随机打乱
        random.shuffle(data_rows)
        
        # 写入CSV文件
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(columns)  # 写入表头
            writer.writerows(data_rows)
        
        self.logger.info(f"已创建UNSW-NB15示例数据: {filepath} (共{len(data_rows)}条记录)")

    def get_data_statistics(self, df):
        """获取数据统计信息"""
        from pyspark.sql.functions import count, when

        # 基本统计
        total_count = df.count()

        # 攻击统计
        if "label" in df.columns:
            attack_count = df.filter(col("label") != 0).count()
            normal_count = df.filter(col("label") == 0).count()
            attack_ratio = attack_count / total_count if total_count > 0 else 0
        elif "attack_cat" in df.columns:
            attack_count = df.filter(col("attack_cat") != "Normal").count()
            normal_count = df.filter(col("attack_cat") == "Normal").count()
            attack_ratio = attack_count / total_count if total_count > 0 else 0
        else:
            attack_count = normal_count = attack_ratio = 0

        # 列信息
        columns = df.columns
        dtypes = df.dtypes

        # 缺失值统计
        missing_stats = {}
        for column in columns:
            missing_count = df.filter(col(column).isNull() | isnan(col(column))).count()
            missing_stats[column] = missing_count

        return {
            "count": total_count,
            "columns": columns,
            "dtypes": dtypes,
            "attack_count": attack_count,
            "normal_count": normal_count,
            "attack_ratio": attack_ratio,
            "missing_stats": missing_stats
        }

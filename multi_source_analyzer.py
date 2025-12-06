import geoip2.database
import requests
import json
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import logging


class MultiSourceAnalyzer:
    """多源数据关联分析"""

    def __init__(self, spark):
        self.spark = spark
        self.logger = logging.getLogger(__name__)

        # 初始化GeoIP数据库
        try:
            self.geoip_reader = geoip2.database.Reader('GeoLite2-City.mmdb')
        except:
            self.logger.warning("未找到GeoIP数据库文件")
            self.geoip_reader = None

    def enrich_with_geoip(self, df, ip_column="src_ip"):
        """使用GeoIP丰富地理位置信息"""
        if not self.geoip_reader:
            self.logger.warning("GeoIP数据库未初始化，跳过地理位置丰富")
            return df

        # 定义UDF获取地理位置
        @udf(returnType=StructType([
            StructField("country", StringType(), True),
            StructField("city", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True)
        ]))
        def get_location(ip):
            try:
                response = self.geoip_reader.city(ip)
                return (response.country.name,
                        response.city.name,
                        response.location.latitude,
                        response.location.longitude)
            except:
                return (None, None, None, None)

        # 添加地理位置信息
        df_with_geo = df.withColumn("location", get_location(col(ip_column)))

        # 展开location列
        df_expanded = df_with_geo \
            .withColumn("country", col("location.country")) \
            .withColumn("city", col("location.city")) \
            .withColumn("latitude", col("location.latitude")) \
            .withColumn("longitude", col("location.longitude")) \
            .drop("location")

        return df_expanded

    def enrich_with_whois(self, df, ip_column="src_ip"):
        """使用WHOIS信息丰富数据"""

        @udf(returnType=StructType([
            StructField("asn", StringType(), True),
            StructField("org", StringType(), True),
            StructField("isp", StringType(), True)
        ]))
        def get_whois_info(ip):
            try:
                # 使用ip-api.com获取WHOIS信息（免费版）
                response = requests.get(f"http://ip-api.com/json/{ip}", timeout=2)
                data = response.json()

                if data.get("status") == "success":
                    return (data.get("as", ""),
                            data.get("org", ""),
                            data.get("isp", ""))
                else:
                    return (None, None, None)
            except:
                return (None, None, None)

        # 添加WHOIS信息
        df_with_whois = df.withColumn("whois_info", get_whois_info(col(ip_column)))

        # 展开WHOIS信息
        df_expanded = df_with_whois \
            .withColumn("asn", col("whois_info.asn")) \
            .withColumn("organization", col("whois_info.org")) \
            .withColumn("isp", col("whois_info.isp")) \
            .drop("whois_info")

        return df_expanded

    def correlate_attack_patterns(self, df):
        """关联分析攻击模式"""
        from pyspark.sql.window import Window
        import pyspark.sql.functions as F

        # 按时间窗口和源IP分组
        window_spec = Window.partitionBy("src_ip").orderBy("timestamp")

        # 计算时间差和序列模式
        df_with_pattern = df \
            .withColumn("prev_time", F.lag("timestamp").over(window_spec)) \
            .withColumn("time_diff",
                        F.when(F.col("prev_time").isNotNull(),
                               F.unix_timestamp("timestamp") - F.unix_timestamp("prev_time"))
                        .otherwise(None)) \
            .withColumn("attack_sequence",
                        F.concat_ws("->",
                                    F.collect_list("attack_type").over(window_spec.rowsBetween(-5, 0))))

        # 识别频繁攻击序列
        frequent_sequences = df_with_pattern \
            .groupBy("attack_sequence") \
            .agg(F.count("*").alias("frequency"),
                 F.collect_set("src_ip").alias("source_ips")) \
            .filter(F.length("attack_sequence") > 10)  # 序列长度大于10

        return frequent_sequences

    def threat_intelligence_lookup(self, df, threat_feeds=None):
        """威胁情报查询"""
        if threat_feeds is None:
            threat_feeds = [
                "https://rules.emergingthreats.net/blockrules/compromised-ips.txt",
                "https://feodotracker.abuse.ch/downloads/ipblocklist.txt"
            ]

        # 加载威胁情报数据
        malicious_ips = set()

        for feed_url in threat_feeds:
            try:
                response = requests.get(feed_url, timeout=5)
                if response.status_code == 200:
                    # 解析IP列表（简化处理）
                    lines = response.text.split('\n')
                    for line in lines:
                        line = line.strip()
                        if line and not line.startswith('#'):
                            # 提取IP地址
                            parts = line.split()
                            if parts:
                                ip = parts[0]
                                if self._is_ip_address(ip):
                                    malicious_ips.add(ip)
            except Exception as e:
                self.logger.warning(f"无法加载威胁情报源 {feed_url}: {e}")

        # 广播恶意IP列表
        malicious_ips_bc = self.spark.sparkContext.broadcast(malicious_ips)

        # 定义UDF检查IP是否在威胁情报中
        @udf(returnType=BooleanType())
        def is_malicious_ip(ip):
            return ip in malicious_ips_bc.value

        # 标记恶意IP
        df_with_ti = df.withColumn("is_malicious_ip", is_malicious_ip(col("src_ip")))

        return df_with_ti

    def _is_ip_address(self, ip_str):
        """检查字符串是否为IP地址"""
        import re
        ip_pattern = r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$'
        return bool(re.match(ip_pattern, ip_str))
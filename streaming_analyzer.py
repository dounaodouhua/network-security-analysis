from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml import PipelineModel
import logging


class StreamingAnalyzer:
    """实时流量监控原型"""

    def __init__(self, spark):
        self.spark = spark
        self.logger = logging.getLogger(__name__)

        # 加载预训练模型
        self.model = self._load_model()

        # 定义schema
        self.schema = self._define_schema()

    def _define_schema(self):
        """定义流数据schema"""
        return StructType([
            StructField("timestamp", TimestampType(), True),
            StructField("src_ip", StringType(), True),
            StructField("dst_ip", StringType(), True),
            StructField("src_port", IntegerType(), True),
            StructField("dst_port", IntegerType(), True),
            StructField("protocol", StringType(), True),
            StructField("packet_size", IntegerType(), True),
            StructField("flag", StringType(), True),
            StructField("duration", DoubleType(), True),
            StructField("src_bytes", IntegerType(), True),
            StructField("dst_bytes", IntegerType(), True)
        ])

    def _load_model(self):
        """加载预训练的异常检测模型"""
        try:
            # 这里应该从文件系统加载实际训练好的模型
            # model_path = "models/anomaly_detection_model"
            # return PipelineModel.load(model_path)
            return None
        except:
            self.logger.warning("未找到预训练模型，将使用基于规则的检测")
            return None

    def start_streaming(self, host="localhost", port=9999):
        """启动流处理"""
        self.logger.info(f"启动流处理，监听 {host}:{port}")

        # 创建流DataFrame
        streaming_df = self.spark.readStream \
            .format("socket") \
            .option("host", host) \
            .option("port", port) \
            .load()

        # 解析JSON数据
        parsed_df = streaming_df.select(
            from_json(col("value"), self.schema).alias("data")
        ).select("data.*")

        # 特征工程
        processed_df = self._process_stream(parsed_df)

        # 异常检测
        if self.model:
            predictions_df = self.model.transform(processed_df)
        else:
            predictions_df = self._rule_based_detection(processed_df)

        # 输出结果
        query = predictions_df.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .start()

        # 同时写入Kafka或文件系统
        kafka_query = predictions_df.selectExpr("to_json(struct(*)) AS value") \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", "network_alerts") \
            .option("checkpointLocation", "/tmp/checkpoint") \
            .start()

        return query, kafka_query

    def _process_stream(self, df):
        """处理流数据"""
        # 添加时间窗口特征
        windowed_df = df.withWatermark("timestamp", "10 seconds") \
            .groupBy(
            window(col("timestamp"), "1 minute", "30 seconds"),
            col("src_ip")
        ).agg(
            count("*").alias("connection_count"),
            sum("src_bytes").alias("total_src_bytes"),
            sum("dst_bytes").alias("total_dst_bytes"),
            avg("duration").alias("avg_duration")
        )

        # 添加统计特征
        processed_df = windowed_df.withColumn(
            "bytes_per_connection",
            when(col("connection_count") > 0,
                 col("total_src_bytes") / col("connection_count")).otherwise(0)
        )

        return processed_df

    def _rule_based_detection(self, df):
        """基于规则的异常检测"""
        # 定义异常规则
        df_with_alerts = df.withColumn(
            "alert_level",
            when((col("connection_count") > 1000) & (col("avg_duration") < 0.1), "HIGH")
            .when((col("bytes_per_connection") > 10000) & (col("connection_count") > 100), "MEDIUM")
            .otherwise("LOW")
        ).withColumn(
            "alert_reason",
            when(col("alert_level") == "HIGH", "Possible DDoS attack detected")
            .when(col("alert_level") == "MEDIUM", "Suspicious traffic pattern")
            .otherwise("Normal")
        ).withColumn(
            "timestamp",
            current_timestamp()
        )

        return df_with_alerts

    def create_dashboard_stream(self):
        """创建流式仪表盘"""
        from pyspark.sql import functions as F

        # 模拟流数据源（实际应连接Kafka等）
        streaming_df = self.spark.readStream \
            .format("rate") \
            .option("rowsPerSecond", 10) \
            .load()

        # 添加模拟的网络数据
        network_stream = streaming_df \
            .withColumn("src_ip", F.expr("concat('192.168.', cast(rand()*255 as int), '.', cast(rand()*255 as int))")) \
            .withColumn("dst_port", F.expr("cast(rand()*65535 as int)")) \
            .withColumn("packet_size", F.expr("cast(rand()*1500 as int)")) \
            .withColumn("is_attack", F.expr("rand() < 0.05"))  # 5%是攻击

        # 聚合统计
        windowed_counts = network_stream \
            .withWatermark("timestamp", "10 seconds") \
            .groupBy(
            F.window(F.col("timestamp"), "1 minute"),
            F.col("is_attack")
        ).count()

        return windowed_counts
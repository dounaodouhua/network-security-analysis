from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.ml.feature import (
    StringIndexer, OneHotEncoder, VectorAssembler,
    StandardScaler, Imputer
)
from pyspark.ml import Pipeline
import logging


class DataPreprocessor:
    """数据预处理模块"""

    def __init__(self, spark):
        self.spark = spark
        self.logger = logging.getLogger(__name__)
        self.preprocessing_pipeline = None

    def preprocess(self, df):
        """执行完整的预处理流程"""
        self.logger.info("开始数据预处理")

        # 1. 数据清洗
        df_clean = self._clean_data(df)

        # 2. 特征工程
        df_features = self._feature_engineering(df_clean)

        # 3. 数据平衡处理
        df_balanced = self._handle_imbalance(df_features)

        return df_balanced

    def _clean_data(self, df):
        """数据清洗"""
        self.logger.info("数据清洗...")

        # 处理缺失值
        df_clean = df.dropna(subset=["label"])  # 删除标签缺失的行

        # 填充数值型特征的缺失值
        numeric_cols = [c for c, t in df.dtypes if t in ['int', 'double', 'float']]

        if numeric_cols:
            imputer = Imputer(
                inputCols=numeric_cols,
                outputCols=[f"{c}_imputed" for c in numeric_cols],
                strategy="mean"
            )
            df_clean = imputer.fit(df_clean).transform(df_clean)

            # 替换原始列
            for col_name in numeric_cols:
                df_clean = df_clean.withColumn(col_name, col(f"{col_name}_imputed"))
                df_clean = df_clean.drop(f"{col_name}_imputed")

        # 去除极端异常值（基于IQR）- 兼容不同数据集的列名
        outlier_cols = []
        if 'src_bytes' in df_clean.columns:
            outlier_cols.append('src_bytes')
        elif 'sbytes' in df_clean.columns:
            outlier_cols.append('sbytes')
            
        if 'dst_bytes' in df_clean.columns:
            outlier_cols.append('dst_bytes')
        elif 'dbytes' in df_clean.columns:
            outlier_cols.append('dbytes')
            
        if 'duration' in df_clean.columns:
            outlier_cols.append('duration')
        elif 'dur' in df_clean.columns:
            outlier_cols.append('dur')
        
        for col_name in outlier_cols:
            df_clean = self._remove_outliers(df_clean, col_name)

        return df_clean

    def _remove_outliers(self, df, column):
        """基于IQR去除异常值"""
        from pyspark.sql.window import Window
        import pyspark.sql.functions as F

        # 计算四分位数
        quantiles = df.approxQuantile(column, [0.25, 0.75], 0.01)
        Q1, Q3 = quantiles[0], quantiles[1]
        IQR = Q3 - Q1

        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR

        # 过滤异常值
        df_filtered = df.filter(
            (col(column) >= lower_bound) &
            (col(column) <= upper_bound)
        )

        return df_filtered

    def _feature_engineering(self, df):
        """特征工程"""
        self.logger.info("特征工程...")

        # 1. 创建时间特征（如果有时间戳）
        if 'timestamp' in df.columns:
            df = df.withColumn("hour", hour(col("timestamp")))
            df = df.withColumn("day_of_week", dayofweek(col("timestamp")))
            df = df.withColumn("is_weekend",
                               when(col("day_of_week").isin([1, 7]), 1).otherwise(0))

        # 2. 流量统计特征
        if all(col in df.columns for col in ['src_bytes', 'dst_bytes']):
            df = df.withColumn("total_bytes", col("src_bytes") + col("dst_bytes"))
            df = df.withColumn("bytes_ratio",
                               when(col("dst_bytes") > 0, col("src_bytes") / col("dst_bytes")).otherwise(0))

        # 3. 连接行为特征
        if all(col in df.columns for col in ['count', 'srv_count']):
            df = df.withColumn("connection_intensity", col("count") + col("srv_count"))

        # 4. 协议特征编码
        categorical_cols = ['protocol_type', 'service', 'flag']
        existing_cat_cols = [c for c in categorical_cols if c in df.columns]

        if existing_cat_cols:
            # 字符串索引化
            indexers = [
                StringIndexer(inputCol=col, outputCol=f"{col}_index", handleInvalid="keep")
                for col in existing_cat_cols
            ]

            # One-Hot编码
            encoders = [
                OneHotEncoder(inputCol=f"{col}_index", outputCol=f"{col}_encoded")
                for col in existing_cat_cols
            ]

            # 创建管道
            pipeline = Pipeline(stages=indexers + encoders)
            model = pipeline.fit(df)
            df = model.transform(df)

        # 5. 标签编码
        if 'label' in df.columns:
            label_indexer = StringIndexer(inputCol="label", outputCol="label_index")
            df = label_indexer.fit(df).transform(df)

        if 'attack_category' in df.columns:
            attack_indexer = StringIndexer(inputCol="attack_category", outputCol="attack_category_index")
            df = attack_indexer.fit(df).transform(df)

        return df

    def _handle_imbalance(self, df):
        """处理数据不平衡"""
        self.logger.info("处理数据不平衡...")

        if 'label_index' not in df.columns:
            return df

        # 统计各类样本数
        label_counts = df.groupBy("label_index").count().collect()
        label_counts_dict = {row['label_index']: row['count'] for row in label_counts}

        if len(label_counts_dict) < 2:
            return df

        # 找到少数类和多数类（使用Python内置函数）
        import builtins
        max_count = builtins.max(label_counts_dict.values())
        min_count = builtins.min(label_counts_dict.values())

        # 如果数据不平衡严重（比例>5:1），进行过采样
        if max_count / min_count > 5:
            self.logger.info(f"检测到数据不平衡（{max_count}:{min_count}），使用随机过采样...")
            # 对于大数据集，直接使用Spark的随机过采样，避免SMOTE的内存问题
            return self._random_oversample(df)

        return df

    def _random_oversample(self, df):
        """随机过采样"""
        from pyspark.sql.functions import col, count, rand

        # 计算每个类别的样本数
        label_stats = df.groupBy("label_index").agg(count("*").alias("count"))

        # 找到最大样本数
        max_count = label_stats.agg({"count": "max"}).collect()[0][0]

        # 对每个少数类进行过采样
        sampled_dfs = []

        for row in label_stats.collect():
            label_val = row["label_index"]
            label_count = row["count"]

            label_df = df.filter(col("label_index") == label_val)

            if label_count < max_count:
                # 计算需要采样的倍数
                sampling_ratio = max_count / label_count
                oversampled_df = label_df.sample(withReplacement=True, fraction=sampling_ratio, seed=42)
                # 确保不超过max_count
                oversampled_df = oversampled_df.limit(max_count)
            else:
                oversampled_df = label_df

            sampled_dfs.append(oversampled_df)

        # 合并所有DataFrame
        return sampled_dfs[0].union(sampled_dfs[1]) if len(sampled_dfs) > 1 else sampled_dfs[0]
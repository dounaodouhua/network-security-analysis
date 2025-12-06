from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
import logging
import json
import builtins
from datetime import datetime


class TrafficAnalyzer:
    """网络流量分析模块"""

    def __init__(self, spark):
        self.spark = spark
        self.logger = logging.getLogger(__name__)
    
    def _get_column_name(self, df, *alternatives):
        """获取存在的列名（支持多个备选名称）"""
        for alt in alternatives:
            if alt in df.columns:
                return alt
        return alternatives[0]  # 默认返回第一个

    def comprehensive_analysis(self, df):
        """执行全面分析"""
        self.logger.info("开始网络流量分析")

        results = {}

        # 1. 基本统计分析
        results['basic_stats'] = self._basic_statistical_analysis(df)

        # 2. 时空分布分析
        results['temporal_analysis'] = self._temporal_analysis(df)

        # 3. 协议分布分析
        results['protocol_analysis'] = self._protocol_analysis(df)

        # 4. 攻击特征分析
        results['attack_analysis'] = self._attack_pattern_analysis(df)

        # 5. 异常检测分析
        results['anomaly_detection'] = self._anomaly_detection(df)

        # 6. IP行为分析
        results['ip_analysis'] = self._ip_behavior_analysis(df)

        # 7. 流量模式分析
        results['traffic_patterns'] = self._traffic_pattern_analysis(df)

        # 汇总结果
        results['summary'] = self._generate_summary(results)

        return results

    def _basic_statistical_analysis(self, df):
        """基本统计分析"""
        self.logger.info("执行基本统计分析")

        stats = {}

        # 数据概览
        stats['total_records'] = df.count()
        stats['total_columns'] = len(df.columns)

        # 数值列统计
        numeric_cols = [c for c, t in df.dtypes if t in ['int', 'double', 'float', 'long']]

        if numeric_cols:
            numeric_stats = {}
            for col_name in numeric_cols[:10]:  # 只分析前10个数值列
                col_stats = df.select(
                    mean(col_name).alias('mean'),
                    stddev(col_name).alias('std'),
                    min(col_name).alias('min'),
                    max(col_name).alias('max'),
                    count(col_name).alias('count')
                ).collect()[0]

                numeric_stats[col_name] = {
                    'mean': float(col_stats['mean']) if col_stats['mean'] else 0,
                    'std': float(col_stats['std']) if col_stats['std'] else 0,
                    'min': float(col_stats['min']) if col_stats['min'] else 0,
                    'max': float(col_stats['max']) if col_stats['max'] else 0,
                    'count': int(col_stats['count'])
                }

            stats['numeric_stats'] = numeric_stats

        # 分类列统计
        categorical_cols = [c for c, t in df.dtypes if t == 'string']

        if categorical_cols:
            categorical_stats = {}
            for col_name in categorical_cols[:5]:  # 只分析前5个分类列
                value_counts = df.groupBy(col_name).count().orderBy(col('count').desc()).limit(10)
                categorical_stats[col_name] = value_counts.collect()

            stats['categorical_stats'] = categorical_stats

        return stats

    def _temporal_analysis(self, df):
        """时间分布分析"""
        self.logger.info("执行时间分布分析")

        temporal_stats = {}

        # 检查是否有时间相关列
        time_cols = [c for c in df.columns if 'time' in c.lower() or 'date' in c.lower()]

        if time_cols:
            time_col = time_cols[0]

            # 按小时分析
            if 'hour' in df.columns:
                hourly_dist = df.groupBy('hour').count().orderBy('hour').collect()
                temporal_stats['attack_hour_distribution'] = hourly_dist
            elif 'stime' in df.columns:
                # UNSW-NB15: 从 stime 时间戳提取小时
                from pyspark.sql.functions import from_unixtime, hour as spark_hour
                df_with_hour = df.withColumn('hour', spark_hour(from_unixtime(col('stime'))))
                hourly_dist = df_with_hour.groupBy('hour').count().orderBy('hour').collect()
                temporal_stats['attack_hour_distribution'] = hourly_dist

            # 按星期分析
            if 'day_of_week' in df.columns:
                daily_dist = df.groupBy('day_of_week').count().orderBy('day_of_week').collect()
                temporal_stats['daily_distribution'] = daily_dist

        # 流量随时间变化趋势
        if 'duration' in df.columns and 'total_bytes' in df.columns:
            # 模拟时间序列分析
            window_spec = Window.orderBy("duration")
            df_with_trend = df.withColumn("time_index", row_number().over(window_spec))

            # 计算移动平均
            window_size = 100
            df_with_trend = df_with_trend.withColumn(
                "bytes_moving_avg",
                avg("total_bytes").over(Window.rowsBetween(-window_size, window_size))
            )

            # 采样获取趋势数据
            trend_sample = df_with_trend.select("time_index", "bytes_moving_avg").sample(0.1).collect()
            temporal_stats['traffic_trend'] = trend_sample[:100]  # 取前100个点

        return temporal_stats

    def _protocol_analysis(self, df):
        """协议分布分析"""
        self.logger.info("执行协议分布分析")

        protocol_stats = {}

        # 协议类型分布 - 兼容 protocol_type 和 proto 列
        proto_col = None
        if 'protocol_type' in df.columns:
            proto_col = 'protocol_type'
        elif 'proto' in df.columns:
            proto_col = 'proto'
        
        if proto_col:
            protocol_dist = df.groupBy(proto_col).count().orderBy(col('count').desc()).collect()
            protocol_stats['protocol_distribution'] = protocol_dist

            # 协议与攻击类型关系
            if 'attack_category' in df.columns:
                protocol_attack = df.groupBy(proto_col, 'attack_category').count().orderBy(
                    col('count').desc()).collect()
                protocol_stats['protocol_attack_relation'] = protocol_attack

        # 服务类型分析
        if 'service' in df.columns:
            service_dist = df.groupBy('service').count().orderBy(col('count').desc()).limit(20).collect()
            protocol_stats['service_distribution'] = service_dist

        # 端口分析（如果有）
        port_cols = [c for c in df.columns if 'port' in c.lower()]
        for port_col in port_cols[:2]:  # 只分析前两个端口列
            port_dist = df.groupBy(port_col).count().orderBy(col('count').desc()).limit(10).collect()
            protocol_stats[f'{port_col}_distribution'] = port_dist

        return protocol_stats

    def _attack_pattern_analysis(self, df):
        """攻击模式分析"""
        attack_categories = {
            'normal.': 'normal',
            'back.': 'dos', 'land.': 'dos', 'neptune.': 'dos', 'pod.': 'dos', 'smurf.': 'dos', 'teardrop.': 'dos',
            'ftp_write.': 'r2l', 'guess_passwd.': 'r2l', 'imap.': 'r2l', 'multihop.': 'r2l',
            'phf.': 'r2l', 'spy.': 'r2l', 'warezclient.': 'r2l', 'warezmaster.': 'r2l',
            'buffer_overflow.': 'u2r', 'loadmodule.': 'u2r', 'perl.': 'u2r', 'rootkit.': 'u2r',
            'ipsweep.': 'probe', 'nmap.': 'probe', 'portsweep.': 'probe', 'satan.': 'probe'
        }

        # 添加攻击类别列
        attack_cat_udf = udf(lambda x: attack_categories.get(x, 'unknown'))
        df = df.withColumn("attack_category", attack_cat_udf(col("attack_type")))

        self.logger.info("执行攻击模式分析")

        attack_stats = {}

        if 'label' not in df.columns:
            return attack_stats

        # 攻击类型分布
        attack_dist = df.groupBy('label').count().orderBy(col('count').desc()).collect()
        attack_stats['attack_type_distribution'] = attack_dist

        # 攻击类别分布
        if 'attack_category' in df.columns:
            category_dist = df.groupBy('attack_category').count().orderBy(col('count').desc()).collect()
            attack_stats['attack_category_distribution'] = category_dist

        # 攻击特征统计
        attack_features = {}

        # 获取列名
        src_bytes_col = self._get_column_name(df, 'sbytes', 'src_bytes')
        dst_bytes_col = self._get_column_name(df, 'dbytes', 'dst_bytes')
        duration_col = self._get_column_name(df, 'dur', 'duration')
        
        # 分析DDoS攻击特征
        dos_attacks = ['neptune', 'smurf', 'pod', 'teardrop', 'back', 'land', 'DoS']
        if 'label' in df.columns or 'attack_cat' in df.columns:
            label_col = 'attack_cat' if 'attack_cat' in df.columns else 'label'
            dos_df = df.filter(col(label_col).isin(dos_attacks))
            if dos_df.count() > 0:
                dos_stats = dos_df.select(
                    mean(src_bytes_col).alias('avg_src_bytes'),
                    mean(dst_bytes_col).alias('avg_dst_bytes'),
                    mean(duration_col).alias('avg_duration'),
                    count('*').alias('count')
                ).collect()[0]

                attack_features['dos_characteristics'] = {
                    'avg_src_bytes': float(dos_stats['avg_src_bytes']) if dos_stats['avg_src_bytes'] else 0,
                    'avg_dst_bytes': float(dos_stats['avg_dst_bytes']) if dos_stats['avg_dst_bytes'] else 0,
                    'avg_duration': float(dos_stats['avg_duration']) if dos_stats['avg_duration'] else 0,
                    'count': int(dos_stats['count'])
                }

        # 分析扫描攻击特征
        probe_attacks = ['ipsweep', 'nmap', 'portsweep', 'satan', 'Reconnaissance', 'Analysis']
        if 'label' in df.columns or 'attack_cat' in df.columns:
            label_col = 'attack_cat' if 'attack_cat' in df.columns else 'label'
            probe_df = df.filter(col('label').isin(probe_attacks))
            if probe_df.count() > 0:
                probe_stats = probe_df.select(
                    mean('count').alias('avg_count'),
                    mean('srv_count').alias('avg_srv_count'),
                    mean('dst_host_count').alias('avg_dst_host_count'),
                    count('*').alias('count')
                ).collect()[0]

                attack_features['probe_characteristics'] = {
                    'avg_count': float(probe_stats['avg_count']) if probe_stats['avg_count'] else 0,
                    'avg_srv_count': float(probe_stats['avg_srv_count']) if probe_stats['avg_srv_count'] else 0,
                    'avg_dst_host_count': float(probe_stats['avg_dst_host_count']) if probe_stats[
                        'avg_dst_host_count'] else 0,
                    'count': int(probe_stats['count'])
                }

        attack_stats['attack_features'] = attack_features

        # 高危时间段分析
        if 'hour' in df.columns:
            attack_hours = df.filter(col('label') != 'normal').groupBy('hour').count().orderBy(
                col('count').desc()).collect()
            attack_stats['attack_hour_distribution'] = attack_hours

        return attack_stats

    def _anomaly_detection(self, df):
        """异常检测分析"""
        self.logger.info("执行异常检测分析")

        anomaly_stats = {}

        try:
            from pyspark.ml.feature import VectorAssembler
            from pyspark.ml.clustering import KMeans

            # 获取列名
            src_bytes_col = self._get_column_name(df, 'sbytes', 'src_bytes')
            dst_bytes_col = self._get_column_name(df, 'dbytes', 'dst_bytes')
            duration_col = self._get_column_name(df, 'dur', 'duration')

            # 选择特征进行聚类分析
            # 使用实际存在的列名
            feature_cols = [src_bytes_col, dst_bytes_col, duration_col]
            if 'count' in df.columns:
                feature_cols.append('count')
            if 'srv_count' in df.columns:
                feature_cols.append('srv_count')
            existing_feature_cols = [c for c in feature_cols if c in df.columns]

            if len(existing_feature_cols) >= 3:
                # 准备特征向量
                assembler = VectorAssembler(
                    inputCols=existing_feature_cols,
                    outputCol="features"
                )
                df_features = assembler.transform(df)

                # 使用KMeans进行异常检测
                kmeans = KMeans(k=2, seed=42, featuresCol="features")
                model = kmeans.fit(df_features)

                # 预测聚类
                predictions = model.transform(df_features)

                # 分析聚类结果
                cluster_stats = predictions.groupBy("prediction").agg(
                    count("*").alias("count"),
                    mean(src_bytes_col).alias("avg_src_bytes"),
                    mean(dst_bytes_col).alias("avg_dst_bytes")
                ).collect()

                anomaly_stats['cluster_analysis'] = cluster_stats

                # 识别可能的异常聚类
                # 假设数据量较小的聚类为异常
                cluster_counts = {row['prediction']: row['count'] for row in cluster_stats}
                if len(cluster_counts) == 2:
                    min_cluster = builtins.min(cluster_counts, key=cluster_counts.get)
                    anomaly_cluster = predictions.filter(col("prediction") == min_cluster)

                    anomaly_stats['anomaly_count'] = anomaly_cluster.count()
                    anomaly_stats['anomaly_ratio'] = anomaly_cluster.count() / df.count()

                    # 分析异常特征
                    anomaly_features = anomaly_cluster.select(
                        mean(src_bytes_col).alias("avg_src_bytes"),
                        mean(dst_bytes_col).alias("avg_dst_bytes"),
                        mean(duration_col).alias("avg_duration")
                    ).collect()[0]

                    anomaly_stats['anomaly_characteristics'] = {
                        'avg_src_bytes': float(anomaly_features['avg_src_bytes']) if anomaly_features[
                            'avg_src_bytes'] else 0,
                        'avg_dst_bytes': float(anomaly_features['avg_dst_bytes']) if anomaly_features[
                            'avg_dst_bytes'] else 0,
                        'avg_duration': float(anomaly_features['avg_duration']) if anomaly_features[
                            'avg_duration'] else 0
                    }

        except Exception as e:
            self.logger.warning(f"异常检测失败: {e}")
            anomaly_stats['error'] = str(e)

        return anomaly_stats

    def _ip_behavior_analysis(self, df):
        """IP行为分析"""
        self.logger.info("执行IP行为分析")

        ip_stats = {}
        
        # 获取列名
        src_bytes_col = self._get_column_name(df, 'sbytes', 'src_bytes')
        dst_bytes_col = self._get_column_name(df, 'dbytes', 'dst_bytes')

        # 查找IP相关列
        ip_cols = [c for c in df.columns if 'ip' in c.lower() or 'src' in c.lower() or 'dst' in c.lower()]

        # 源IP分析
        src_ip_cols = [c for c in ip_cols if 'src' in c.lower()]
        if src_ip_cols:
            src_ip_col = src_ip_cols[0]

            # 最活跃的源IP
            top_src_ips = df.groupBy(src_ip_col).agg(
                count("*").alias("connection_count"),
                sum(src_bytes_col).alias("total_src_bytes"),
                sum(dst_bytes_col).alias("total_dst_bytes")
            ).orderBy(col("connection_count").desc()).limit(10).collect()

            ip_stats['top_source_ips'] = top_src_ips

        # 目标IP分析
        dst_ip_cols = [c for c in ip_cols if 'dst' in c.lower()]
        if dst_ip_cols:
            dst_ip_col = dst_ip_cols[0]

            # 最受欢迎的目标IP
            top_dst_ips = df.groupBy(dst_ip_col).agg(
                count("*").alias("connection_count"),
                sum(dst_bytes_col).alias("total_dst_bytes")
            ).orderBy(col("connection_count").desc()).limit(10).collect()

            ip_stats['top_destination_ips'] = top_dst_ips

        # IP对分析（如果有源和目标IP）
        if src_ip_cols and dst_ip_cols:
            src_ip_col = src_ip_cols[0]
            dst_ip_col = dst_ip_cols[0]
            
            # 获取duration列名
            duration_col = self._get_column_name(df, 'dur', 'duration')

            # 最常见的IP对
            top_ip_pairs = df.groupBy(src_ip_col, dst_ip_col).agg(
                count("*").alias("connection_count"),
                mean(duration_col).alias("avg_duration")
            ).orderBy(col("connection_count").desc()).limit(10).collect()

            ip_stats['top_ip_pairs'] = top_ip_pairs

        return ip_stats

    def _traffic_pattern_analysis(self, df):
        """流量模式分析"""
        self.logger.info("执行流量模式分析")

        pattern_stats = {}

        # 流量大小分布 - 兼容 total_bytes 和 sbytes+dbytes
        total_bytes_col = None
        if 'total_bytes' in df.columns:
            total_bytes_col = 'total_bytes'
        elif 'sbytes' in df.columns and 'dbytes' in df.columns:
            # UNSW-NB15: 计算总字节数
            df = df.withColumn('total_bytes', col('sbytes') + col('dbytes'))
            total_bytes_col = 'total_bytes'
        
        if total_bytes_col:
            # 按流量大小分段
            df_with_segment = df.withColumn(
                "traffic_segment",
                when(col(total_bytes_col) < 1000, "小流量(<1KB)")
                .when(col(total_bytes_col) < 10000, "中流量(1-10KB)")
                .when(col(total_bytes_col) < 100000, "大流量(10-100KB)")
                .otherwise("超大流量(>100KB)")
            )

            segment_dist = df_with_segment.groupBy("traffic_segment").count().orderBy(col("count").desc()).collect()
            pattern_stats['traffic_segment_distribution'] = segment_dist

        # 连接模式分析
        if all(col in df.columns for col in ['count', 'srv_count']):
            # 高频率连接检测
            high_freq_threshold = df.approxQuantile("count", [0.95], 0.01)[0]
            high_freq_connections = df.filter(col("count") > high_freq_threshold).count()

            pattern_stats['high_frequency_connections'] = {
                'threshold': high_freq_threshold,
                'count': high_freq_connections,
                'percentage': high_freq_connections / df.count() if df.count() > 0 else 0
            }

        # 异常连接模式
        if all(col in df.columns for col in ['serror_rate', 'rerror_rate']):
            # 高错误率连接
            high_error_df = df.filter((col("serror_rate") > 0.5) | (col("rerror_rate") > 0.5))
            pattern_stats['high_error_connections'] = {
                'count': high_error_df.count(),
                'percentage': high_error_df.count() / df.count() if df.count() > 0 else 0
            }

        return pattern_stats

    def _generate_summary(self, analysis_results):
        """生成分析摘要"""
        self.logger.info("生成分析摘要")

        summary = {
            'analysis_timestamp': str(datetime.now()),
            'overview': {},
            'key_findings': [],
            'recommendations': []
        }

        # 提取关键信息
        if 'basic_stats' in analysis_results:
            basic = analysis_results['basic_stats']
            summary['overview']['total_records'] = basic.get('total_records', 0)
            summary['overview']['total_features'] = basic.get('total_columns', 0)

        if 'attack_analysis' in analysis_results:
            attack = analysis_results['attack_analysis']
            if 'attack_type_distribution' in attack:
                attack_types = attack['attack_type_distribution']
                summary['overview']['attack_types_count'] = len(attack_types)

                # 找到主要攻击类型
                if attack_types:
                    main_attack = attack_types[0]
                    # 兼容 PySpark Row 和字典
                    if isinstance(main_attack, dict):
                        attack_label = main_attack['label']
                        attack_count = main_attack['count']
                    else:
                        # PySpark Row 对象
                        attack_dict = main_attack.asDict()
                        attack_label = attack_dict['label']
                        attack_count = attack_dict['count']
                    
                    summary['key_findings'].append(
                        f"主要攻击类型: {attack_label} (占比: {attack_count / summary['overview']['total_records']:.2%})"
                    )

        if 'temporal_analysis' in analysis_results:
            temporal = analysis_results['temporal_analysis']
            if 'attack_hour_distribution' in temporal:
                attack_hours = temporal['attack_hour_distribution']
                if attack_hours:
                    # 使用 builtins.max 避免与 Spark SQL 的 max 冲突
                    # 转换为字典以便访问
                    hours_list = []
                    for row in attack_hours:
                        if isinstance(row, dict):
                            hours_list.append(row)
                        else:
                            # PySpark Row 对象
                            hours_list.append(row.asDict())
                    
                    if hours_list:
                        peak_hour = builtins.max(hours_list, key=lambda x: x['count'])
                        summary['key_findings'].append(
                            f"攻击高峰时段: {peak_hour['hour']}时 (攻击次数: {peak_hour['count']})"
                        )

        if 'protocol_analysis' in analysis_results:
            protocol = analysis_results['protocol_analysis']
            if 'protocol_distribution' in protocol:
                protocol_dist = protocol['protocol_distribution']
                if protocol_dist:
                    main_protocol = protocol_dist[0]
                    # 兼容 PySpark Row 和字典
                    if isinstance(main_protocol, dict):
                        proto_name = main_protocol.get('protocol_type', main_protocol.get('proto', '未知'))
                        proto_count = main_protocol['count']
                    else:
                        # PySpark Row 对象
                        proto_dict = main_protocol.asDict()
                        proto_name = proto_dict.get('protocol_type', proto_dict.get('proto', '未知'))
                        proto_count = proto_dict['count']
                    
                    summary['key_findings'].append(
                        f"主要协议: {proto_name} (占比: {proto_count / summary['overview']['total_records']:.2%})"
                    )

        # 生成建议
        if summary['key_findings']:
            summary['recommendations'] = [
                "1. 加强高峰时段的网络监控",
                "2. 对主要攻击类型部署相应的防护规则",
                "3. 对异常流量进行深度分析",
                "4. 定期更新攻击特征库"
            ]

        return summary
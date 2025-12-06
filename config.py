import os
from pyspark.sql import SparkSession
from pathlib import Path

class Config:
    # Spark配置
    SPARK_APP_NAME = "NetworkSecurityAnalysis"
    SPARK_MASTER = "local[*]"  # 生产环境改为spark://master:7077
    SPARK_EXECUTOR_MEMORY = "4g"
    SPARK_DRIVER_MEMORY = "4g"

    # 数据路径
    DATA_PATH = "data/UNSW-NB15_1.csv"

    # 大模型配置
    LLM_API_KEY = os.getenv("api", "sk-ca8ce42d044c4b87ac2c31bdd2ef4f17")  # DeepSeek API Key
    LLM_MODEL = "deepseek-chat"  # DeepSeek 模型
    LLM_BASE_URL = "https://api.deepseek.com/v1"  # DeepSeek API 地址

    # 新增KDD数据集路径
    KDD_DATA_PATH = "data/kddcup99/kddcup.data_10_percent"

    KDD99_TRAIN_PATH = "data/raw/kddcup99/kddcup.data_10_percent"
    KDD99_TEST_PATH = "data/raw/kddcup99/corrected"
    KDD99_NAMES_PATH = "data/raw/kddcup99/kddcup.names"
    KDD99_ATTACK_TYPES = "data/raw/kddcup99/training_attack_types"

    # 可视化配置
    VISUALIZATION_PORT = 8050
    THEME = "cyborg"  # dash主题

    # 特征配置
    TIME_WINDOW_SECONDS = 60  # 时间窗口大小（秒）
    TOP_N_IPS = 20  # 显示最多的IP数量


def create_spark_session():
    """创建Spark会话"""
    import sys
    
    # 设置 Python 可执行文件路径（Windows 兼容性）
    python_exe = sys.executable
    
    # 设置环境变量（必须在创建 Session 之前）
    os.environ['PYSPARK_PYTHON'] = python_exe
    os.environ['PYSPARK_DRIVER_PYTHON'] = python_exe
    
    # 停止已有的 Spark Session（如果存在）
    try:
        existing_spark = SparkSession.getActiveSession()
        if existing_spark:
            existing_spark.stop()
    except:
        pass
    
    spark = SparkSession.builder \
        .appName(Config.SPARK_APP_NAME) \
        .master(Config.SPARK_MASTER) \
        .config("spark.executor.memory", Config.SPARK_EXECUTOR_MEMORY) \
        .config("spark.driver.memory", Config.SPARK_DRIVER_MEMORY) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.pyspark.python", python_exe) \
        .config("spark.pyspark.driver.python", python_exe) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark
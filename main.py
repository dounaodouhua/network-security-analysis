#!/usr/bin/env python3
"""
网络安全态势分析系统
基于Spark和大语言模型的完整解决方案
"""

import sys
import logging
from datetime import datetime
from pathlib import Path

# 添加项目路径
sys.path.append(str(Path(__file__).parent))

from config import create_spark_session, Config
from data_loader import DataLoader
from data_preprocessor import DataPreprocessor
from traffic_analyzer import TrafficAnalyzer
from report_generator import ReportGenerator
from visualizer import NetworkVisualizer


class NetworkSecurityAnalysis:
    def __init__(self, dataset: str = "unsw_nb15"):
        self.dataset = dataset
        self.spark = self._init_spark()
        self.data_loader = DataLoader(self.spark, dataset)
        self.preprocessor = DataPreprocessor(self.spark)


        self.logger = self._setup_logging()

        # 初始化组件（保持不变）


        self.analyzer = TrafficAnalyzer(self.spark)
        self.report_gen = ReportGenerator()
        self.visualizer = NetworkVisualizer()

        # 存储中间结果
        self.results = {}

    def _setup_logging(self):
        """配置日志"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('network_analysis.log'),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger(__name__)

    def run_pipeline(self):
        self.logger.info(f"开始 {self.dataset} 数据集的网络安全态势分析")
        try:
            # 1. 数据加载
            df = self.data_loader.load_dataset()

            # 2. 数据预处理
            processed_df = self.preprocessor.preprocess(df, self.dataset)

            # 3. 分析（后续需调整analyzer兼容KDD99）
            analysis_results = self.analyzer.comprehensive_analysis(processed_df, self.dataset)

            # 4. 生成报告
            self.logger.info("步骤4: 生成分析报告")
            report = self.report_gen.generate_report(self.results)
            self.results['report'] = report

            # 5. 可视化展示
            self.logger.info("步骤5: 创建可视化")
            dashboard = self.visualizer.create_dashboard(self.results)
            self.results['dashboard'] = dashboard

            # 保存结果
            self._save_results()

            self.logger.info("分析完成！")

            return self.results
            # ... 其余流程保持不变 ...
        except Exception as e:
            self.logger.error(f"分析过程中发生错误: {str(e)}")
            raise

    def _save_results(self):
        """保存分析结果"""
        import json

        # 保存分析结果
        results_path = Path("results")
        results_path.mkdir(exist_ok=True)

        # 保存JSON格式的结果
        saveable_results = {
            'data_stats': self.results['data_stats'],
            'analysis_summary': self.results['analysis']['summary'],
            'report': self.results['report']
        }

        with open(results_path / f"analysis_{datetime.now():%Y%m%d_%H%M%S}.json", 'w') as f:
            json.dump(saveable_results, f, indent=2, default=str)

    def close(self):
        """清理资源"""
        self.spark.stop()
        self.logger.info("Spark会话已关闭")


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description="网络安全态势分析系统")
    parser.add_argument('--dataset', type=str, default='kdd99',
                        choices=['kdd99', 'unsw_nb15', 'cic_ids2017'],
                        help='选择数据集')
    parser.add_argument('--visualize', action='store_true',
                        help='启动可视化仪表盘')

    args = parser.parse_args()

    # 创建分析实例
    analyzer = NetworkSecurityAnalysis(args.dataset)

    try:
        # 运行分析
        results = analyzer.run_pipeline()

        # 输出关键结果
        print("\n" + "=" * 50)
        print("分析结果摘要")
        print("=" * 50)
        print(f"数据集: {args.dataset}")
        print(f"总样本数: {results['data_stats']['count']:,}")
        print(f"攻击样本数: {results['data_stats']['attack_count']:,}")
        print(f"攻击比例: {results['data_stats']['attack_ratio']:.2%}")

        if args.visualize:
            print("\n启动可视化仪表盘...")
            results['dashboard'].run(debug=False, port=Config.VISUALIZATION_PORT)

    finally:
        analyzer.close()


if __name__ == "__main__":
    main()
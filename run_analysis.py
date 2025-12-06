#!/usr/bin/env python3
"""
运行完整分析的脚本
"""

import sys
import argparse
from pathlib import Path

# 添加项目路径
sys.path.append(str(Path(__file__).parent))

from main import NetworkSecurityAnalysis


def run_analysis():
    """运行分析"""
    parser = argparse.ArgumentParser(description="运行网络安全分析 - UNSW-NB15数据集")
    parser.add_argument('--mode', type=str, default='batch',
                        choices=['batch', 'streaming', 'interactive'],
                        help='运行模式')
    parser.add_argument('--output', type=str, default='results',
                        help='输出目录')
    parser.add_argument('--visualize', action='store_true',
                        help='启动可视化')

    args = parser.parse_args()

    print(f"""
    ========================================
    网络安全态势分析系统 - UNSW-NB15
    ========================================
    数据集: UNSW-NB15
    模式: {args.mode}
    输出目录: {args.output}
    可视化: {args.visualize}
    ========================================
    """)

    # 创建分析器实例
    analyzer = NetworkSecurityAnalysis()

    try:
        if args.mode == 'batch':
            # 批处理分析
            print("开始批处理分析...")
            results = analyzer.run_pipeline()

            print("\n分析完成！")
            print(f"结果已保存到: {args.output}")

            if args.visualize:
                print("启动可视化仪表盘...")
                analyzer.results['dashboard'].run(debug=True, port=8050)

        elif args.mode == 'streaming':
            # 流处理分析
            print("开始流处理分析...")
            from streaming_analyzer import StreamingAnalyzer

            stream_analyzer = StreamingAnalyzer(analyzer.spark)
            query = stream_analyzer.start_streaming()

            print("流处理已启动，按Ctrl+C停止...")
            query.awaitTermination()

        elif args.mode == 'interactive':
            # 交互式分析
            print("启动交互式分析...")
            print("请在Jupyter Notebook中运行以下命令：")
            print("""
            from data_loader import DataLoader
            from data_preprocessor import DataPreprocessor
            from traffic_analyzer import TrafficAnalyzer

            # 加载数据
            loader = DataLoader(spark)
            df = loader.load_dataset()

            # 预处理
            preprocessor = DataPreprocessor(spark)
            processed_df = preprocessor.preprocess(df)

            # 分析
            analyzer = TrafficAnalyzer(spark)
            results = analyzer.comprehensive_analysis(processed_df)
            """)

    except KeyboardInterrupt:
        print("\n用户中断分析")
    except Exception as e:
        print(f"分析过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
    finally:
        analyzer.close()


if __name__ == "__main__":
    run_analysis()
import json
import logging
from datetime import datetime
from typing import Dict, Any
import openai
from config import Config


class ReportGenerator:
    """基于大语言模型的报告生成模块"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

        # 初始化OpenAI客户端（兼容DeepSeek）
        if Config.LLM_API_KEY:
            self.client = openai.OpenAI(
                api_key=Config.LLM_API_KEY,
                base_url=Config.LLM_BASE_URL
            )
        else:
            self.client = None
            self.logger.warning("未配置LLM API Key，将使用模板生成报告")

    def generate_report(self, analysis_results: Dict[str, Any]) -> str:
        """生成分析报告"""
        self.logger.info("生成分析报告")

        # 准备报告数据
        report_data = self._prepare_report_data(analysis_results)

        # 使用LLM生成报告或使用模板
        if self.client:
            return self._generate_with_llm(report_data)
        else:
            return self._generate_with_template(report_data)

    def _prepare_report_data(self, analysis_results):
        """准备报告数据"""
        report_data = {
            'metadata': {
                'generation_time': str(datetime.now()),
                'data_source': '网络安全分析系统',
                'report_id': f"REPORT_{datetime.now():%Y%m%d_%H%M%S}"
            },
            'analysis_summary': analysis_results.get('summary', {}),
            'detailed_analysis': {}
        }

        # 提取关键分析结果
        if 'analysis' in analysis_results:
            detailed = analysis_results['analysis']

            # 攻击分析
            if 'attack_analysis' in detailed:
                report_data['detailed_analysis']['attack_analysis'] = {
                    'attack_types': detailed['attack_analysis'].get('attack_type_distribution', []),
                    'attack_categories': detailed['attack_analysis'].get('attack_category_distribution', []),
                    'attack_features': detailed['attack_analysis'].get('attack_features', {})
                }

            # 协议分析
            if 'protocol_analysis' in detailed:
                report_data['detailed_analysis']['protocol_analysis'] = {
                    'protocol_distribution': detailed['protocol_analysis'].get('protocol_distribution', []),
                    'service_distribution': detailed['protocol_analysis'].get('service_distribution', [])
                }

            # 时间分析
            if 'temporal_analysis' in detailed:
                report_data['detailed_analysis']['temporal_analysis'] = {
                    'attack_hours': detailed['temporal_analysis'].get('attack_hour_distribution', [])
                }

            # IP分析
            if 'ip_analysis' in detailed:
                report_data['detailed_analysis']['ip_analysis'] = {
                    'top_source_ips': detailed['ip_analysis'].get('top_source_ips', []),
                    'top_destination_ips': detailed['ip_analysis'].get('top_destination_ips', [])
                }

        return report_data

    def _generate_with_llm(self, report_data):
        """使用LLM生成报告（支持DeepSeek/GPT）"""
        try:
            # 构建提示词
            prompt = self._build_prompt(report_data)

            response = self.client.chat.completions.create(
                model=Config.LLM_MODEL,
                messages=[
                    {"role": "system", "content": "你是一个专业的网络安全分析师，擅长撰写详细的安全分析报告。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7,
                max_tokens=2000,
                stream=False
            )

            report = response.choices[0].message.content
            self.logger.info(f"LLM报告生成成功 (模型: {Config.LLM_MODEL})")

            # 保存报告
            self._save_report(report, "llm_report.md")

            return report

        except Exception as e:
            self.logger.error(f"LLM报告生成失败: {e}")
            return self._generate_with_template(report_data)

    def _build_prompt(self, report_data):
        """构建提示词"""
        prompt = f"""基于以下网络安全分析数据，生成一份专业的安全态势分析报告：

# 分析数据
{json.dumps(report_data, indent=2, ensure_ascii=False)}

# 报告要求
请生成一份包含以下章节的详细报告：
1. 执行摘要 - 概述主要发现和结论
2. 攻击态势分析 - 分析攻击类型、频率和趋势
3. 流量特征分析 - 分析网络流量模式
4. 时间分布特征 - 分析攻击的时间规律
5. 风险评估 - 评估安全风险等级
6. 防护建议 - 提供具体的防护措施
7. 附录 - 详细数据表

报告语言：中文
报告风格：专业、客观、数据驱动
报告长度：1000-1500字

请直接输出报告内容，不要添加额外说明。"""

        return prompt

    def _generate_with_template(self, report_data):
        """使用模板生成报告"""
        template = self._load_template()

        # 填充模板
        summary = report_data['analysis_summary']
        detailed = report_data['detailed_analysis']

        # 填充数据
        report = template.replace("{{report_id}}", report_data['metadata']['report_id'])
        report = report.replace("{{generation_time}}", report_data['metadata']['generation_time'])

        # 填充摘要
        if 'overview' in summary:
            overview = summary['overview']
            report = report.replace("{{total_records}}", str(overview.get('total_records', 0)))
            report = report.replace("{{total_features}}", str(overview.get('total_features', 0)))

        # 填充关键发现
        key_findings = ""
        if 'key_findings' in summary:
            for finding in summary['key_findings']:
                key_findings += f"- {finding}\n"
        report = report.replace("{{key_findings}}", key_findings)

        # 填充建议
        recommendations = ""
        if 'recommendations' in summary:
            for rec in summary['recommendations']:
                recommendations += f"- {rec}\n"
        report = report.replace("{{recommendations}}", recommendations)

        # 填充详细分析
        detailed_analysis = ""
        if 'attack_analysis' in detailed:
            attack_data = detailed['attack_analysis']
            if 'attack_types' in attack_data:
                detailed_analysis += "## 攻击类型分布\n\n"
                for attack in attack_data['attack_types'][:10]:  # 前10种攻击
                    detailed_analysis += f"- {attack['label']}: {attack['count']}次\n"
                detailed_analysis += "\n"

        report = report.replace("{{detailed_analysis}}", detailed_analysis)

        # 保存报告
        self._save_report(report, "template_report.md")

        return report

    def _load_template(self):
        """加载报告模板"""
        template = """# 网络安全态势分析报告

## 报告信息
- **报告ID**: {{report_id}}
- **生成时间**: {{generation_time}}
- **数据源**: 网络安全分析系统

## 执行摘要

### 分析概览
- 总分析记录数: {{total_records}}
- 分析特征数: {{total_features}}

### 主要发现
{{key_findings}}

### 关键建议
{{recommendations}}

## 详细分析

{{detailed_analysis}}

## 结论

基于本次分析，当前网络环境面临多种安全威胁，建议立即采取相应防护措施。

---
*本报告由网络安全分析系统自动生成*
"""
        return template

    def _save_report(self, report, filename):
        """保存报告到文件"""
        import os

        reports_dir = "reports"
        os.makedirs(reports_dir, exist_ok=True)

        filepath = os.path.join(reports_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(report)

        self.logger.info(f"报告已保存到: {filepath}")
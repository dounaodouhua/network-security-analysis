import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from dash import Dash, dcc, html, Input, Output
import dash_bootstrap_components as dbc
import logging
from typing import Dict, Any


class NetworkVisualizer:
    """可视化模块"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.app = None

    def create_dashboard(self, analysis_results):
        """创建可视化仪表盘"""
        self.logger.info("创建可视化仪表盘")

        # 创建Dash应用
        self.app = Dash(__name__, external_stylesheets=[dbc.themes.CYBORG])

        # 准备数据
        figures = self._prepare_figures(analysis_results)

        # 布局
        self.app.layout = self._create_layout(figures, analysis_results)

        # 回调函数
        self._register_callbacks()

        return self.app

    def _prepare_figures(self, analysis_results):
        """准备图表数据"""
        figures = {}

        # 从分析结果中提取数据
        if 'analysis' in analysis_results:
            analysis = analysis_results['analysis']

            # 1. 攻击类型分布图
            if 'attack_analysis' in analysis:
                attack_data = analysis['attack_analysis']
                if 'attack_type_distribution' in attack_data:
                    figures['attack_dist'] = self._create_attack_distribution_chart(
                        attack_data['attack_type_distribution'])

                if 'attack_category_distribution' in attack_data:
                    figures['category_dist'] = self._create_category_chart(attack_data['attack_category_distribution'])

            # 2. 时间分布图
            if 'temporal_analysis' in analysis:
                temporal_data = analysis['temporal_analysis']
                if 'attack_hour_distribution' in temporal_data:
                    figures['time_dist'] = self._create_time_distribution_chart(
                        temporal_data['attack_hour_distribution'])

            # 3. 协议分布图
            if 'protocol_analysis' in analysis:
                protocol_data = analysis['protocol_analysis']
                if 'protocol_distribution' in protocol_data:
                    figures['protocol_dist'] = self._create_protocol_chart(protocol_data['protocol_distribution'])

            # 4. IP分析图
            if 'ip_analysis' in analysis:
                ip_data = analysis['ip_analysis']
                if 'top_source_ips' in ip_data:
                    figures['source_ips'] = self._create_ip_chart(ip_data['top_source_ips'], "Top Source IPs")

                if 'top_destination_ips' in ip_data:
                    figures['dest_ips'] = self._create_ip_chart(ip_data['top_destination_ips'], "Top Destination IPs")

            # 5. 流量模式图
            if 'traffic_patterns' in analysis:
                traffic_data = analysis['traffic_patterns']
                if 'traffic_segment_distribution' in traffic_data:
                    figures['traffic_pattern'] = self._create_traffic_pattern_chart(
                        traffic_data['traffic_segment_distribution'])

        # 6. 综合仪表图
        figures['metrics'] = self._create_metrics_panel(analysis_results)

        return figures

    def _create_attack_distribution_chart(self, attack_data):
        """创建攻击类型分布图"""
        if not attack_data or len(attack_data) == 0:
            # 返回空图表但带有提示信息
            fig = go.Figure()
            fig.add_annotation(
                text="暂无攻击类型数据",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=16, color="gray")
            )
            fig.update_layout(height=380, autosize=False)
            return fig

        # 准备数据 - 兼容字典和PySpark Row
        labels = []
        values = []
        for row in attack_data[:10]:
            if isinstance(row, dict):
                labels.append(str(row.get('label', '未知')))
                values.append(int(row.get('count', 0)))
            else:
                # PySpark Row 对象 - 使用字典访问方式
                try:
                    labels.append(str(row['label'] if 'label' in row.asDict() else '未知'))
                    values.append(int(row['count']))
                except:
                    labels.append('未知')
                    values.append(0)
        
        # 过滤掉值为0的数据
        filtered_data = [(l, v) for l, v in zip(labels, values) if v > 0]
        if not filtered_data:
            fig = go.Figure()
            fig.add_annotation(
                text="所有攻击类型计数为0",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=16, color="gray")
            )
            fig.update_layout(height=380, autosize=False)
            return fig
            
        labels, values = zip(*filtered_data)

        # 创建饼图
        fig = go.Figure(data=[go.Pie(
            labels=labels,
            values=values,
            hole=.3,
            textinfo='label+percent',
            marker=dict(colors=px.colors.qualitative.Set3)
        )])

        fig.update_layout(
            title_text="攻击类型分布",
            title_x=0.5,
            height=380,
            showlegend=True,
            autosize=False,
            margin=dict(l=20, r=20, t=40, b=20)
        )

        return fig

    def _create_category_chart(self, category_data):
        """创建攻击类别分布图"""
        if not category_data or len(category_data) == 0:
            fig = go.Figure()
            fig.add_annotation(
                text="暂无攻击类别数据",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=16, color="gray")
            )
            fig.update_layout(height=380, autosize=False)
            return fig

        # 兼容字典和PySpark Row
        labels = []
        values = []
        for row in category_data:
            if isinstance(row, dict):
                labels.append(str(row.get('attack_category', '未知')))
                values.append(int(row.get('count', 0)))
            else:
                # PySpark Row 对象
                try:
                    labels.append(str(row['attack_category'] if 'attack_category' in row.asDict() else '未知'))
                    values.append(int(row['count']))
                except:
                    labels.append('未知')
                    values.append(0)
        
        # 确保至少有一些数据
        if sum(values) == 0:
            fig = go.Figure()
            fig.add_annotation(
                text="所有攻击类别计数为0",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=16, color="gray")
            )
            fig.update_layout(height=380, autosize=False)
            return fig

        fig = go.Figure(data=[go.Bar(
            x=labels,
            y=values,
            marker_color='indianred',
            text=values,
            textposition='auto'
        )])

        fig.update_layout(
            title_text="攻击类别分布",
            title_x=0.5,
            xaxis_title="攻击类别",
            yaxis_title="数量",
            height=380,
            autosize=False,
            margin=dict(l=50, r=20, t=40, b=50),
            yaxis=dict(rangemode='tozero')
        )

        return fig

    def _create_time_distribution_chart(self, time_data):
        """创建时间分布图"""
        if not time_data or len(time_data) == 0:
            fig = go.Figure()
            fig.add_annotation(
                text="暂无时间分布数据",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=16, color="gray")
            )
            fig.update_layout(height=380, autosize=False)
            return fig

        # 准备数据 - 兼容字典和PySpark Row
        hours = []
        counts = []
        for row in time_data:
            if isinstance(row, dict):
                hours.append(int(row.get('hour', 0)))
                counts.append(int(row.get('count', 0)))
            else:
                # PySpark Row 对象
                try:
                    hours.append(int(row['hour']))
                    counts.append(int(row['count']))
                except:
                    hours.append(0)
                    counts.append(0)
        
        if sum(counts) == 0:
            fig = go.Figure()
            fig.add_annotation(
                text="所有时间段攻击计数为0",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=16, color="gray")
            )
            fig.update_layout(height=380, autosize=False)
            return fig

        # 创建线图
        fig = go.Figure()

        fig.add_trace(go.Scatter(
            x=hours,
            y=counts,
            mode='lines+markers',
            name='攻击次数',
            line=dict(color='firebrick', width=2),
            marker=dict(size=8)
        ))

        fig.update_layout(
            title_text="攻击时间分布",
            title_x=0.5,
            xaxis_title="小时",
            yaxis_title="攻击次数",
            height=380,
            xaxis=dict(tickmode='linear', dtick=1),
            autosize=False,
            margin=dict(l=50, r=20, t=40, b=50)
        )

        return fig

    def _create_protocol_chart(self, protocol_data):
        """创建协议分布图"""
        if not protocol_data or len(protocol_data) == 0:
            fig = go.Figure()
            fig.add_annotation(
                text="暂无协议分布数据",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=16, color="gray")
            )
            fig.update_layout(height=380, autosize=False)
            return fig

        # 兼容字典和PySpark Row，支持 protocol_type 和 proto 两种列名
        labels = []
        values = []
        for row in protocol_data:
            if isinstance(row, dict):
                proto = row.get('protocol_type', row.get('proto', '未知'))
                labels.append(str(proto))
                values.append(int(row.get('count', 0)))
            else:
                # PySpark Row 对象
                try:
                    row_dict = row.asDict()
                    proto = row_dict.get('protocol_type', row_dict.get('proto', '未知'))
                    labels.append(str(proto))
                    values.append(int(row_dict.get('count', 0)))
                except:
                    labels.append('未知')
                    values.append(0)
        
        if sum(values) == 0:
            fig = go.Figure()
            fig.add_annotation(
                text="所有协议类型计数为0",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=16, color="gray")
            )
            fig.update_layout(height=380, autosize=False)
            return fig

        fig = go.Figure(data=[go.Bar(
            x=labels,
            y=values,
            marker_color='teal',
            text=values,
            textposition='auto'
        )])

        fig.update_layout(
            title_text="协议类型分布",
            title_x=0.5,
            xaxis_title="协议类型",
            yaxis_title="数量",
            height=380,
            autosize=False,
            margin=dict(l=50, r=20, t=40, b=50),
            yaxis=dict(rangemode='tozero')
        )

        return fig

    def _create_ip_chart(self, ip_data, title):
        """创建IP分析图"""
        if not ip_data or len(ip_data) == 0:
            fig = go.Figure()
            fig.add_annotation(
                text="暂无IP分析数据",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=16, color="gray")
            )
            fig.update_layout(height=380, autosize=False)
            return fig

        # 提取IP和连接数 - 兼容不同的数据格式
        top_ips = []
        for row in ip_data[:10]:
            if isinstance(row, (list, tuple)) and len(row) >= 2:
                # 格式: (ip, count)
                top_ips.append((str(row[0]), int(row[1])))
            elif isinstance(row, dict):
                # 格式: {'ip': ..., 'count': ...}
                ip = str(row.get('ip', row.get('srcip', '未知')))
                count = int(row.get('count', 0))
                top_ips.append((ip, count))
            else:
                # PySpark Row 对象
                try:
                    row_dict = row.asDict()
                    ip = str(row_dict.get('ip', row_dict.get('srcip', '未知')))
                    count = int(row_dict.get('count', 0))
                    top_ips.append((ip, count))
                except:
                    top_ips.append(('未知', 0))
        
        if not top_ips:
            fig = go.Figure()
            fig.add_annotation(
                text="IP数据格式不正确",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=16, color="gray")
            )
            fig.update_layout(height=380, autosize=False)
            return fig
            
        ips, counts = zip(*top_ips)

        fig = go.Figure(data=[go.Bar(
            x=ips,
            y=counts,
            marker_color='orange',
            text=counts,
            textposition='auto'
        )])

        fig.update_layout(
            title_text=title,
            title_x=0.5,
            xaxis_title="IP地址",
            yaxis_title="连接数",
            height=380,
            xaxis_tickangle=-45,
            autosize=False,
            margin=dict(l=50, r=20, t=40, b=80),
            yaxis=dict(rangemode='tozero')
        )

        return fig

    def _create_traffic_pattern_chart(self, pattern_data):
        """创建流量模式图"""
        if not pattern_data or len(pattern_data) == 0:
            fig = go.Figure()
            fig.add_annotation(
                text="暂无流量模式数据",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=16, color="gray")
            )
            fig.update_layout(height=380, autosize=False)
            return fig

        # 兼容字典和PySpark Row
        segments = []
        counts = []
        for row in pattern_data:
            if isinstance(row, dict):
                segments.append(str(row.get('traffic_segment', '未知')))
                counts.append(int(row.get('count', 0)))
            else:
                # PySpark Row 对象
                try:
                    segments.append(str(row['traffic_segment'] if 'traffic_segment' in row.asDict() else '未知'))
                    counts.append(int(row['count']))
                except:
                    segments.append('未知')
                    counts.append(0)
        
        if sum(counts) == 0:
            fig = go.Figure()
            fig.add_annotation(
                text="所有流量分段计数为0",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=16, color="gray")
            )
            fig.update_layout(height=380, autosize=False)
            return fig

        # 动态颜色映射
        colors = ['green', 'blue', 'orange', 'red']
        marker_colors = colors[:len(segments)] if len(segments) <= len(colors) else colors * ((len(segments) // len(colors)) + 1)

        fig = go.Figure(data=[go.Bar(
            x=segments,
            y=counts,
            marker_color=marker_colors[:len(segments)],
            text=counts,
            textposition='auto'
        )])

        fig.update_layout(
            title_text="流量大小分布",
            title_x=0.5,
            xaxis_title="流量分段",
            yaxis_title="数量",
            height=380,
            autosize=False,
            margin=dict(l=50, r=20, t=40, b=50),
            yaxis=dict(rangemode='tozero')
        )

        return fig

    def _create_metrics_panel(self, analysis_results):
        """创建指标面板 - 使用卡片布局代替Indicator"""
        # 计算关键指标
        metrics = {}

        if 'data_stats' in analysis_results:
            stats = analysis_results['data_stats']
            metrics['总样本数'] = f"{stats.get('count', 0):,}"
            metrics['攻击样本数'] = f"{stats.get('attack_count', 0):,}"
            
            # 修复：正确计算攻击比例
            attack_ratio = stats.get('attack_ratio', 0)
            if attack_ratio == 0 and stats.get('count', 0) > 0 and stats.get('attack_count', 0) > 0:
                # 如果比例为0但有攻击数据，重新计算
                attack_ratio = stats.get('attack_count', 0) / stats.get('count', 1)
            metrics['攻击比例'] = f"{attack_ratio * 100:.2f}%"

        if 'analysis' in analysis_results:
            analysis = analysis_results['analysis']

            if 'anomaly_detection' in analysis:
                anomaly = analysis['anomaly_detection']
                if 'anomaly_ratio' in anomaly:
                    anomaly_ratio = anomaly.get('anomaly_ratio', 0)
                    metrics['异常流量比例'] = f"{anomaly_ratio * 100:.2f}%"

            if 'traffic_patterns' in analysis:
                traffic = analysis['traffic_patterns']
                if 'high_frequency_connections' in traffic:
                    hfc = traffic['high_frequency_connections']
                    metrics['高频连接比例'] = f"{hfc.get('percentage', 0) * 100:.2f}%"

        # 返回 metrics 字典供布局使用
        return metrics

    def _create_metrics_cards_OLD(self, analysis_results):
        """旧版本：创建指标卡片（保留作为备用）"""
        # 创建指标卡片
        fig = make_subplots(
            rows=2, cols=2,
            specs=[[{'type': 'indicator'}, {'type': 'indicator'}],
                   [{'type': 'indicator'}, {'type': 'indicator'}]]
        )

        metrics = self._create_metrics_panel(analysis_results)
        metrics_list = list(metrics.items())[:4]  # 取前4个指标

        for i, (name, value) in enumerate(metrics_list):
            row = i // 2 + 1
            col = i % 2 + 1

            fig.add_trace(
                go.Indicator(
                    mode="number",
                    value=value if isinstance(value, (int, float)) else 0,
                    number={'valueformat': '.0f' if isinstance(value, (int, float)) else ''},
                    title={"text": name},
                    domain={'row': row - 1, 'column': col - 1}
                ),
                row=row, col=col
            )

        fig.update_layout(
            height=380,
            grid={'rows': 2, 'columns': 2, 'pattern': "independent"},
            autosize=False,
            margin=dict(l=20, r=20, t=20, b=20)
        )

        return fig

    def _create_layout(self, figures, analysis_results):
        """创建仪表盘布局"""
        # 获取关键指标数据
        metrics = figures.get('metrics', {})
        
        # 创建指标卡片
        metric_cards = []
        card_style = {
            'height': '150px',
            'display': 'flex',
            'flexDirection': 'column',
            'justifyContent': 'center',
            'alignItems': 'center',
            'padding': '20px'
        }
        
        for title, value in metrics.items():
            card = dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H2(value, className="text-center", 
                               style={'color': '#00bc8c', 'fontWeight': 'bold', 'marginBottom': '10px'}),
                        html.H6(title, className="text-center text-muted", 
                               style={'marginBottom': '0'})
                    ], style=card_style)
                ], color="dark", outline=True)
            ], width=12, md=6, lg=3, className="mb-3")
            metric_cards.append(card)
        
        layout = dbc.Container([
            # 标题
            dbc.Row([
                dbc.Col([
                    html.H1("网络安全态势分析仪表盘", className="text-center my-4"),
                    html.Hr()
                ])
            ]),

            # 关键指标卡片
            html.H4("关键指标", className="text-center my-2"),
            dbc.Row(metric_cards, className="mb-4"),

            # 第一行图表
            dbc.Row([
                dbc.Col([
                    html.H4("攻击类型分布", className="text-center my-2"),
                    dcc.Graph(id='attack-dist-chart', figure=figures.get('attack_dist', go.Figure()),
                              config={'displayModeBar': False},
                              style={'height': '400px'})
                ], width=6),

                dbc.Col([
                    html.H4("攻击时间分布", className="text-center my-2"),
                    dcc.Graph(id='time-dist-chart', figure=figures.get('time_dist', go.Figure()),
                              config={'displayModeBar': False},
                              style={'height': '400px'})
                ], width=6)
            ], className="mb-4"),

            # 第二行图表
            dbc.Row([
                dbc.Col([
                    html.H4("协议分布", className="text-center my-2"),
                    dcc.Graph(id='protocol-chart', figure=figures.get('protocol_dist', go.Figure()),
                              config={'displayModeBar': False},
                              style={'height': '400px'})
                ], width=6),

                dbc.Col([
                    html.H4("攻击类别分布", className="text-center my-2"),
                    dcc.Graph(id='category-chart', figure=figures.get('category_dist', go.Figure()),
                              config={'displayModeBar': False},
                              style={'height': '400px'})
                ], width=6)
            ], className="mb-4"),

            # 第三行图表
            dbc.Row([
                dbc.Col([
                    html.H4("源IP分析", className="text-center my-2"),
                    dcc.Graph(id='source-ip-chart', figure=figures.get('source_ips', go.Figure()),
                              config={'displayModeBar': False},
                              style={'height': '400px'})
                ], width=6),

                dbc.Col([
                    html.H4("流量模式分析", className="text-center my-2"),
                    dcc.Graph(id='traffic-pattern-chart', figure=figures.get('traffic_pattern', go.Figure()),
                              config={'displayModeBar': False},
                              style={'height': '400px'})
                ], width=6)
            ], className="mb-4"),

            # 控制面板
            dbc.Row([
                dbc.Col([
                    html.H4("分析控制", className="text-center my-2"),
                    dbc.Card([
                        dbc.CardBody([
                            html.P("数据集选择:", className="card-text"),
                            dcc.Dropdown(
                                id='dataset-selector',
                                options=[
                                    {'label': 'KDD99', 'value': 'kdd99'},
                                    {'label': 'UNSW-NB15', 'value': 'unsw_nb15'},
                                    {'label': 'CIC-IDS2017', 'value': 'cic_ids2017'}
                                ],
                                value='kdd99',
                                className="mb-3"
                            ),
                            dbc.Button("重新分析", id='reanalyze-btn', color="primary", className="w-100")
                        ])
                    ])
                ], width=12, md=4),

                dbc.Col([
                    html.H4("实时监控", className="text-center my-2"),
                    dbc.Card([
                        dbc.CardBody([
                            html.Div(id='real-time-metrics', style={'minHeight': '100px'}),
                            dcc.Interval(
                                id='interval-component',
                                interval=30 * 1000,  # 30秒更新一次（减少刷新频率）
                                n_intervals=0,
                                disabled=False  # 可以设置为True来完全禁用
                            )
                        ])
                    ], style={'minHeight': '150px'})  # 固定卡片最小高度
                ], width=12, md=8)
            ]),

            # 报告显示
            dbc.Row([
                dbc.Col([
                    html.H4("分析报告", className="text-center my-2"),
                    dbc.Textarea(
                        id='report-display',
                        value=analysis_results.get('report', '报告生成中...'),
                        style={'height': '300px'},
                        readOnly=True
                    )
                ])
            ])
        ], fluid=True)

        return layout

    def _register_callbacks(self):
        """注册回调函数"""
        if not self.app:
            return

        @self.app.callback(
            Output('real-time-metrics', 'children'),
            Input('interval-component', 'n_intervals'),
            prevent_initial_call=False
        )
        def update_real_time_metrics(n):
            """更新实时指标"""
            # 这里可以连接实际的数据源
            import random
            from datetime import datetime

            metrics = [
                ("当前时间", datetime.now().strftime("%H:%M:%S")),
                ("实时连接数", f"{random.randint(1000, 5000):,}"),
                ("实时攻击数", str(random.randint(10, 100))),
                ("网络延迟", f"{random.randint(10, 100)}ms")
            ]

            cards = []
            for title, value in metrics:
                cards.append(
                    dbc.Col([
                        dbc.Card([
                            dbc.CardBody([
                                html.H5(value, className="card-title", style={'margin': '0'}),
                                html.P(title, className="card-text text-muted", style={'margin': '0', 'fontSize': '12px'})
                            ], style={'padding': '10px'})
                        ], className="text-center", style={'height': '70px'})
                    ], width=3)
                )

            return dbc.Row(cards, style={'margin': '0'})
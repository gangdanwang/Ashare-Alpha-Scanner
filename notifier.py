"""
通知模块：发送选股结果到邮箱和钉钉
样式参考 Arco Design 规范：https://arco.design/docs/spec/style-guideline
"""
import os
import smtplib
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime


# ── Arco Design 色彩规范 ──
# 主色 #165DFF，中性色系，功能色
ARCO = {
    "primary":       "#165DFF",
    "primary_light": "#E8F3FF",
    "bg_page":       "#F2F3F5",      # 页面背景
    "bg_card":       "#FFFFFF",      # 卡片背景
    "border":        "#E5E6EB",      # 边框/分割线
    "text_primary":  "#1D2129",      # 主文字
    "text_secondary":"#4E5969",      # 次级文字
    "text_tertiary": "#86909C",      # 辅助文字
    "success":       "#00B42A",      # 成功/上涨
    "danger":        "#F53F3F",      # 错误/下跌
    "warning":       "#FF7D00",      # 警告
    "shadow":        "0 2px 8px rgba(0,0,0,0.08)",  # 一级阴影
}


def load_env():
    """从 .env 文件加载环境变量"""
    env_path = os.path.join(os.path.dirname(__file__), '.env')
    if not os.path.exists(env_path):
        print("警告：.env 文件不存在，通知功能将无法使用")
        return {}
    env = {}
    with open(env_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, val = line.split('=', 1)
                env[key.strip()] = val.strip()
    return env


def send_email(subject: str, html_content: str):
    """发送邮件通知"""
    env = load_env()
    sender   = env.get('EMAIL_SENDER')
    receiver = env.get('EMAIL_RECEIVER')
    auth_code = env.get('EMAIL_AUTH_CODE')

    if not all([sender, receiver, auth_code]):
        print("邮件配置不完整，跳过邮件发送")
        return False
    if auth_code == 'your_qq_email_auth_code_here':
        print("请先在 .env 文件中配置 QQ 邮箱授权码")
        return False

    try:
        msg = MIMEMultipart('alternative')
        msg['From']    = sender
        msg['To']      = receiver
        msg['Subject'] = subject
        msg.attach(MIMEText(html_content, 'html', 'utf-8'))
        server = smtplib.SMTP_SSL('smtp.qq.com', 465, timeout=10)
        server.login(sender, auth_code)
        server.sendmail(sender, [receiver], msg.as_string())
        server.quit()
        print(f"✓ 邮件已发送至 {receiver}")
        return True
    except Exception as e:
        print(f"✗ 邮件发送失败: {e}")
        return False


def send_dingtalk(title: str, text: str):
    """发送钉钉机器人通知（支持加签验证）"""
    import hmac, hashlib, base64, urllib.parse, time as _time

    env    = load_env()
    token  = env.get('DINGTALK_ACCESS_TOKEN')
    secret = env.get('DINGTALK_SECRET', '')

    if not token:
        print("钉钉 token 未配置，跳过钉钉通知")
        return False

    url = f"https://oapi.dingtalk.com/robot/send?access_token={token}"
    if secret:
        timestamp = str(round(_time.time() * 1000))
        sign_str  = f"{timestamp}\n{secret}"
        sign = base64.b64encode(
            hmac.new(secret.encode('utf-8'), sign_str.encode('utf-8'), digestmod=hashlib.sha256).digest()
        ).decode('utf-8')
        url += f"&timestamp={timestamp}&sign={urllib.parse.quote_plus(sign)}"

    payload = {"msgtype": "markdown", "markdown": {"title": title, "text": text}}
    try:
        resp   = requests.post(url, json=payload, timeout=10)
        result = resp.json()
        if result.get('errcode') == 0:
            print("✓ 钉钉通知已发送")
            return True
        else:
            print(f"✗ 钉钉通知失败: {result.get('errmsg')}")
            return False
    except Exception as e:
        print(f"✗ 钉钉通知失败: {e}")
        return False


# ============================================================
# HTML 邮件模板（Arco Design 样式规范）
# 字体：PingFang SC / Microsoft YaHei / Helvetica Neue
# 主色：#165DFF，中性色文字，功能色区分涨跌
# 阴影：0 2px 8px rgba(0,0,0,0.08)（一级阴影）
# ============================================================

def _html_table(df, empty_text="暂无数据") -> str:
    """生成 Arco Design 风格的 HTML 表格"""
    if df is None or df.empty:
        return f"""
        <div style="
            padding: 24px;
            text-align: center;
            color: {ARCO['text_tertiary']};
            font-size: 14px;
            background: {ARCO['bg_page']};
            border-radius: 4px;
        ">{empty_text}</div>"""

    cols = df.columns.tolist()

    # 表头
    th_cells = "".join(f"""
        <th style="
            padding: 12px 16px;
            text-align: center;
            font-size: 14px;
            font-weight: 500;
            color: {ARCO['text_secondary']};
            background: {ARCO['bg_page']};
            border-bottom: 1px solid {ARCO['border']};
            white-space: nowrap;
        ">{c}</th>""" for c in cols)

    # 数据行
    rows_html = ""
    for i, (_, row) in enumerate(df.iterrows()):
        bg = ARCO['bg_card'] if i % 2 == 0 else "#FAFAFA"
        td_cells = ""
        for c in cols:
            val = row[c]
            # 涨跌幅、乖离率等数值用功能色区分正负
            color = ARCO['text_primary']
            if isinstance(val, (int, float)):
                if c in ('涨跌幅', 'vwap斜率(%)') and val > 0:
                    color = ARCO['success']
                elif c in ('涨跌幅', 'vwap斜率(%)') and val < 0:
                    color = ARCO['danger']
            td_cells += f"""
            <td style="
                padding: 12px 16px;
                text-align: center;
                font-size: 14px;
                color: {color};
                border-bottom: 1px solid {ARCO['border']};
                white-space: nowrap;
            ">{val}</td>"""
        rows_html += f'<tr style="background:{bg};">{td_cells}</tr>'

    return f"""
    <table style="
        width: 100%;
        border-collapse: collapse;
        border-radius: 4px;
        overflow: hidden;
        box-shadow: {ARCO['shadow']};
        font-family: 'PingFang SC','Microsoft YaHei','Helvetica Neue',Arial,sans-serif;
    ">
        <thead><tr>{th_cells}</tr></thead>
        <tbody>{rows_html}</tbody>
    </table>"""


def _section(title: str, count: int, table_html: str) -> str:
    """生成带标题的卡片区块"""
    badge = f"""<span style="
        display: inline-block;
        margin-left: 8px;
        padding: 1px 8px;
        background: {ARCO['primary_light']};
        color: {ARCO['primary']};
        border-radius: 10px;
        font-size: 12px;
        font-weight: 500;
        vertical-align: middle;
    ">{count} 只</span>"""

    return f"""
    <div style="margin-bottom: 32px;">
        <div style="
            display: flex;
            align-items: center;
            margin-bottom: 16px;
            padding-bottom: 12px;
            border-bottom: 2px solid {ARCO['primary']};
        ">
            <span style="
                font-size: 16px;
                font-weight: 500;
                color: {ARCO['text_primary']};
                font-family: 'PingFang SC','Microsoft YaHei',sans-serif;
            ">{title}</span>
            {badge}
        </div>
        {table_html}
    </div>"""


def build_email_html(date_str: str, df_stage1, df_stage2) -> str:
    """构建完整邮件 HTML"""
    s1_html = _html_table(df_stage1, "第一阶段无通过股票")
    s2_html = _html_table(df_stage2, "第二阶段无通过股票")

    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
</head>
<body style="
    margin: 0; padding: 0;
    background: {ARCO['bg_page']};
    font-family: 'PingFang SC','Microsoft YaHei','Helvetica Neue',Arial,sans-serif;
">
<div style="max-width: 800px; margin: 0 auto; padding: 32px 16px;">

    <!-- 页头 -->
    <div style="
        background: {ARCO['bg_card']};
        border-radius: 8px;
        padding: 24px 32px;
        margin-bottom: 24px;
        box-shadow: {ARCO['shadow']};
        border-left: 4px solid {ARCO['primary']};
    ">
        <div style="font-size: 20px; font-weight: 600; color: {ARCO['text_primary']};">
            {date_str} · 隔夜策略
        </div>
        <div style="margin-top: 6px; font-size: 13px; color: {ARCO['text_tertiary']};">
            运行时间：{datetime.now().strftime('%H:%M:%S')} &nbsp;|&nbsp;
            第一阶段：{len(df_stage1) if df_stage1 is not None and not df_stage1.empty else 0} 只 &nbsp;|&nbsp;
            第二阶段：{len(df_stage2) if df_stage2 is not None and not df_stage2.empty else 0} 只
        </div>
    </div>

    <!-- 内容卡片 -->
    <div style="
        background: {ARCO['bg_card']};
        border-radius: 8px;
        padding: 24px 32px;
        box-shadow: {ARCO['shadow']};
    ">
        {_section("第一阶段筛选", len(df_stage1) if df_stage1 is not None and not df_stage1.empty else 0, s1_html)}
        {_section("第二阶段精选", len(df_stage2) if df_stage2 is not None and not df_stage2.empty else 0, s2_html)}
    </div>

    <!-- 页脚 -->
    <div style="
        margin-top: 16px;
        text-align: center;
        font-size: 12px;
        color: {ARCO['text_tertiary']};
    ">
        本报告由 Alpha Scanner 自动生成，仅供参考，不构成投资建议
    </div>

</div>
</body>
</html>"""


def _md_table(df) -> str:
    """生成钉钉 Markdown 表格"""
    if df is None or df.empty:
        return "暂无数据\n"
    cols = df.columns.tolist()
    md  = "| " + " | ".join(cols) + " |\n"
    md += "| " + " | ".join(["---"] * len(cols)) + " |\n"
    for _, row in df.iterrows():
        md += "| " + " | ".join(str(row[c]) for c in cols) + " |\n"
    return md


def build_dingtalk_markdown(date_str: str, df_stage1, df_stage2) -> str:
    """构建钉钉 Markdown 消息"""
    n1 = len(df_stage1) if df_stage1 is not None and not df_stage1.empty else 0
    n2 = len(df_stage2) if df_stage2 is not None and not df_stage2.empty else 0

    md  = f"## {date_str} · 隔夜策略\n\n"
    md += f"> 第一阶段 **{n1}** 只 ｜ 第二阶段 **{n2}** 只\n\n"
    md += f"---\n\n"
    md += f"### 📋 第一阶段筛选\n\n"
    md += _md_table(df_stage1)
    md += f"\n---\n\n"
    md += f"### ⭐ 第二阶段精选\n\n"
    md += _md_table(df_stage2)
    md += f"\n> *本报告由 Alpha Scanner 自动生成，仅供参考*"
    return md


# ============================================================
# 对外接口
# ============================================================

def notify_results(df_stage1, df_stage2):
    """发送选股结果到邮箱和钉钉"""
    date_str = datetime.now().strftime("%Y年%m月%d日")
    subject  = f"{date_str}-隔夜策略"

    send_email(subject, build_email_html(date_str, df_stage1, df_stage2))
    send_dingtalk(subject, build_dingtalk_markdown(date_str, df_stage1, df_stage2))

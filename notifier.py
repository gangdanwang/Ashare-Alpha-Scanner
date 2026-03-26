"""
通知模块：发送选股结果到邮箱和钉钉
"""
import os
import smtplib
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime


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
    sender = env.get('EMAIL_SENDER')
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
        msg['From'] = sender
        msg['To'] = receiver
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

    env = load_env()
    token = env.get('DINGTALK_ACCESS_TOKEN')
    secret = env.get('DINGTALK_SECRET', '')   # 加签密钥，未开启加签则留空

    if not token:
        print("钉钉 token 未配置，跳过钉钉通知")
        return False

    url = f"https://oapi.dingtalk.com/robot/send?access_token={token}"

    # 若配置了加签密钥则附加签名
    if secret:
        timestamp = str(round(_time.time() * 1000))
        sign_str = f"{timestamp}\n{secret}"
        sign = base64.b64encode(
            hmac.new(secret.encode('utf-8'), sign_str.encode('utf-8'), digestmod=hashlib.sha256).digest()
        ).decode('utf-8')
        url += f"&timestamp={timestamp}&sign={urllib.parse.quote_plus(sign)}"

    payload = {
        "msgtype": "markdown",
        "markdown": {"title": title, "text": text},
    }

    try:
        resp = requests.post(url, json=payload, timeout=10)
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


def format_stage_table(df, stage_name: str) -> str:
    """将 DataFrame 格式化为 HTML 表格"""
    if df.empty:
        return f"<p><b>{stage_name}</b>：无数据</p>"
    
    html = f"<h3>{stage_name}</h3>"
    html += df.to_html(index=True, border=1, justify='center')
    return html


def format_stage_markdown(df, stage_name: str) -> str:
    """将 DataFrame 格式化为 Markdown 表格（钉钉用）"""
    if df.empty:
        return f"### {stage_name}\n无数据\n"
    
    md = f"### {stage_name}\n\n"
    cols = df.columns.tolist()
    md += "| " + " | ".join(cols) + " |\n"
    md += "| " + " | ".join(["---"] * len(cols)) + " |\n"
    
    for _, row in df.iterrows():
        md += "| " + " | ".join(str(row[c]) for c in cols) + " |\n"
    
    return md


def notify_results(df_stage1, df_stage2):
    """
    发送选股结果通知
    df_stage1: 第一阶段结果 DataFrame
    df_stage2: 第二阶段结果 DataFrame
    """
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    # ── 邮件通知 ──
    subject = f"A股选股结果 - {now}"
    html = f"""
    <html>
    <head>
        <meta charset="utf-8">
        <style>
            table {{ border-collapse: collapse; margin: 20px 0; }}
            th, td {{ padding: 8px 12px; text-align: center; }}
            th {{ background-color: #f0f0f0; }}
        </style>
    </head>
    <body>
        <h2>A股选股结果</h2>
        <p>运行时间：{now}</p>
        {format_stage_table(df_stage1, "第一阶段：粗筛通过")}
        {format_stage_table(df_stage2, "第二阶段：精选结果")}
    </body>
    </html>
    """
    send_email(subject, html)
    
    # ── 钉钉通知 ──
    title = f"A股选股 {now}"
    markdown = f"# A股选股结果\n\n**运行时间**：{now}\n\n"
    markdown += format_stage_markdown(df_stage1, "第一阶段：粗筛通过")
    markdown += "\n"
    markdown += format_stage_markdown(df_stage2, "第二阶段：精选结果")
    send_dingtalk(title, markdown)

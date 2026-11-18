from datetime import datetime,timezone
def now():
    """UTC ISO8601 时间戳；独立成模块以便无 SQLAlchemy 环境（验证网关）复用。"""
    return datetime.now(timezone.utc).isoformat()

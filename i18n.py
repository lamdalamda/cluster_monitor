"""给网页看的消息带中英两份，前端按当前语言显示。"""


def L(zh, en):
    return {"zh": zh, "en": en}


def as_l(m):
    """字符串（比如 ssh 原样的报错，或者旧缓存里的消息）当成两种语言一样"""
    if isinstance(m, dict) and "zh" in m:
        return m
    s = "" if m is None else str(m)
    return {"zh": s, "en": s}


def prefixed(prefix, m):
    return {k: prefix + v for k, v in as_l(m).items()}


def suffixed(m, zh, en):
    m = as_l(m)
    return {"zh": m["zh"] + zh, "en": m["en"] + en}

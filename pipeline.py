#!/usr/bin/env python3
"""消息归档管道 demo：去重 → 规则过滤 → 分类入库 → 生成日报。

用法：
    python3 pipeline.py data/events.jsonl

输入是 OneBot 格式的消息事件（一行一条 JSON），
输出：archive.db（SQLite）+ 日报-YYYYMMDD.txt
"""
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
RULES = json.loads((ROOT / "filter-rules.json").read_text(encoding="utf-8"))


def load_events(path):
    events = []
    with open(path, encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                print(f"  [跳过] 第 {line_no} 行不是合法 JSON")
    return events


def message_text(event):
    msg = event.get("message") or []
    if isinstance(msg, str):
        return msg
    parts = []
    for seg in msg:
        if seg.get("type") == "text":
            parts.append(seg.get("data", {}).get("text", ""))
    return "".join(parts).strip()


def classify(text):
    """返回命中的关键词列表。"""
    hits = []
    for kw in RULES["keywords"]:
        if kw in text:
            hits.append(kw)
    return hits


def is_ignored(event, text):
    group = event.get("group_name", "")
    sender = event.get("sender_name", "")
    for bad in RULES["ignoreGroups"]:
        if bad in group:
            return f"群名包含「{bad}」"
    for bad in RULES["ignoreSenders"]:
        if bad in sender:
            return f"发送者包含「{bad}」"
    if not text and not RULES.get("passMedia", False):
        return "非文本消息"
    return None


def dedupe(events):
    """按 message_id + 文本哈希去重，返回 (保留, 重复数)。"""
    seen = set()
    kept = []
    dup = 0
    for ev in events:
        mid = str(ev.get("message_id", ""))
        text = message_text(ev)
        key = (mid, hash(text))
        if key in seen:
            dup += 1
            continue
        seen.add(key)
        kept.append(ev)
    return kept, dup


def store(events):
    db = sqlite3.connect(ROOT / "archive.db")
    db.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            message_id TEXT PRIMARY KEY,
            ts TEXT, group_name TEXT, sender_name TEXT,
            text TEXT, keywords TEXT, important INTEGER
        )
    """)
    rows = []
    for ev in events:
        text = message_text(ev)
        kws = classify(text)
        rows.append((
            str(ev.get("message_id")), ev.get("time_str", ""), ev.get("group_name", ""),
            ev.get("sender_name", ""), text, ",".join(kws), 1 if kws else 0,
        ))
    db.executemany("INSERT OR REPLACE INTO messages VALUES (?,?,?,?,?,?,?)", rows)
    db.commit()
    return db


def daily_report(db, day):
    cur = db.execute(
        "SELECT group_name, sender_name, text, keywords FROM messages "
        "WHERE important=1 AND ts LIKE ? ORDER BY ts", (f"{day}%",)
    )
    items = cur.fetchall()
    lines = [f"消息日报 · {day}", "=" * 30, f"重要消息 {len(items)} 条", ""]
    for group, sender, text, kws in items:
        lines.append(f"- {group} / {sender}：{text}")
        lines.append(f"  命中：{kws}")
    out = ROOT / f"日报-{day}.txt"
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return out, len(items)


def main():
    src = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "data" / "events.jsonl"
    print(f"[1/4] 读取事件：{src}")
    raw = load_events(src)
    print(f"      共 {len(raw)} 条")

    print("[2/4] 去重")
    kept, dup = dedupe(raw)
    print(f"      去掉重复 {dup} 条，剩 {len(kept)} 条")

    print("[3/4] 过滤规则")
    filtered = []
    for ev in kept:
        text = message_text(ev)
        reason = is_ignored(ev, text)
        if reason:
            print(f"      跳过 {ev.get('message_id')}：{reason}")
            continue
        filtered.append(ev)
    print(f"      保留 {len(filtered)} 条")

    print("[4/4] 入库 + 生成日报")
    db = store(filtered)
    day = datetime.now().strftime("%Y%m%d")
    out, n = daily_report(db, day)
    print(f"      重要消息 {n} 条 -> {out.name}")
    print("完成。")


if __name__ == "__main__":
    main()
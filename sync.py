#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DSH 插件表每周同步（GitHub → Markdown/CSV/HTML + Notion）

数据流：
  awesome-dsh-plugin/awesome-dsh-plugin 的 README.zh.md（插件清单）
  + GitHub API（star / fork 热度）
      └─> 本脚本解析、抓取、比对
            ├─> dsh-plugins-table.md / dsh-plugins-table.csv（仓库内表格）
            ├─> docs/index.html（GitHub Pages 网页版）
            └─> Notion 数据库「DSH 插件总览」（新增/更新/归档，幂等）

用法：
  python3 sync.py                     # 全流程（需要 GITHUB_TOKEN；Notion 可选）
  python3 sync.py --no-notion         # 只更新仓库内表格 + 网页
  python3 sync.py --offline           # 用 data/heat.json 缓存热度，不请求 GitHub API
  python3 sync.py --readme-file x.md  # 用本地 README 文件（跳过下载）

环境变量：
  GITHUB_TOKEN    GitHub PAT（可选但强烈建议；无 token 时走慢速 topic 搜索）
  NOTION_API_KEY  Notion 集成令牌（设置后启用 Notion 同步）
  NOTION_DB_ID    Notion 数据库 ID（默认内置，可覆盖）
"""
import argparse
import csv
import datetime
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import OrderedDict, Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

DEFAULT_NOTION_DB = "645b8528889e465da4bc898654f898b6"
README_URL = "https://raw.githubusercontent.com/awesome-dsh-plugin/awesome-dsh-plugin/main/README.zh.md"
ROOT = os.path.dirname(os.path.abspath(__file__))
CACHE_PATH = os.path.join(ROOT, "data", "heat.json")

UA = "Mozilla/5.0 (dsh-table-sync)"


# ---------------------------------------------------------------- README ----
def fetch_readme(readme_file=None):
    """下载或读取最新 README.zh.md。"""
    if readme_file:
        return open(readme_file, encoding="utf-8").read()
    req = urllib.request.Request(README_URL, headers={"User-Agent": UA})
    for attempt in range(4):
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return r.read().decode("utf-8")
        except Exception as e:
            print(f"[readme] 下载失败（第 {attempt + 1} 次）: {e}", flush=True)
            time.sleep(3)
    raise RuntimeError("无法下载 README.zh.md")


def parse_readme(text):
    """解析 README → (分类统计, 条目列表)。"""
    counts = OrderedDict()
    entries = []
    cat = None
    for line in text.splitlines():
        s = line.strip()
        m = re.match(r"^###\s+(.+)$", s)
        if m:
            cat = m.group(1).strip()
            counts.setdefault(cat, 0)
            continue
        if s.startswith("- [") and cat:
            m2 = re.match(r"^- \[([^\]]+)\]\(([^)]+)\)\s*—\s*(.+)$", s)
            if m2:
                counts[cat] += 1
                entries.append({
                    "cat": cat,
                    "name": m2.group(1),
                    "url": m2.group(2),
                    "desc": m2.group(3),
                })
    # 源头去重：同一（归一化）URL 只保留首次出现的一条，
    # 避免 README 中同插件被重复列举时污染表格与 Notion（按完整 URL，不按 repo，
    # 以免误合并 monorepo 里的不同子插件）。
    seen_url = set()
    deduped = []
    for e in entries:
        k = norm_url(e["url"])
        if k in seen_url:
            continue
        seen_url.add(k)
        deduped.append(e)
    if len(deduped) != len(entries):
        print(f"[readme] 去重: {len(entries)} → {len(deduped)}", flush=True)
    return counts, deduped


def repo_of(url):
    m = re.search(r"https?://github\.com/([^/]+)/([^/)#?]+)", url)
    return f"{m.group(1)}/{m.group(2)}" if m else None


def norm_url(url):
    """归一化 URL 用于判重（去锚点/尾斜杠/.git/大小写差异）。

    注意：仍按「完整 URL」判重，不按 repo —— 因为 monorepo 里同一 repo
    下的不同子插件（URL 不同）是不同的插件，不能合并。
    """
    if not url:
        return None
    u = url.strip()
    u = u.split("#")[0]
    u = u.rstrip("/")
    u = re.sub(r"\.git$", "", u, flags=re.I)
    u = u.replace("www.", "")
    return u.lower()


# ---------------------------------------------------------------- 热度 ----
def load_cache():
    try:
        return json.load(open(CACHE_PATH, encoding="utf-8"))
    except Exception:
        return {}


def save_cache(cache):
    os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
    with open(CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=1)
    print(f"[heat] 缓存已写入 {CACHE_PATH}", flush=True)


def fetch_heat_topic_walk(repos, cache):
    """无 token 的慢速兜底：按 created 日期分桶翻页 topic:dsh-plugin。"""
    import datetime as dt

    data = dict(cache)
    base = "https://api.github.com/search/repositories"

    def api(q, page):
        url = base + "?" + urllib.parse.urlencode({"q": q, "per_page": 100, "page": page})
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        for _ in range(5):
            try:
                with urllib.request.urlopen(req, timeout=30) as r:
                    return json.loads(r.read().decode())
            except urllib.error.HTTPError as e:
                if e.code == 403:
                    wait = max(int(e.headers.get("X-RateLimit-Reset", 0)) - int(time.time()), 10)
                    print(f"  限速等待 {wait}s", flush=True)
                    time.sleep(wait)
                    continue
                return None
            except Exception:
                time.sleep(6)
        return None

    def absorb(d):
        for it in d.get("items", []):
            data[it["full_name"]] = {"stars": it.get("stargazers_count", 0), "forks": it.get("forks_count", 0)}

    def collect(q, label):
        d = api(q, 1)
        if d is None:
            return
        total = d.get("total_count", 0)
        absorb(d)
        page = 2
        while len(d.get("items", [])) == 100 and page * 100 < total:
            d = api(q, page)
            if d is None:
                break
            absorb(d)
            page += 1
            time.sleep(7)

    today = dt.date.today()
    d = dt.date(today.year, today.month, 1)
    # 本月按天分桶（当月热数据可能超 1000 上限）
    while d <= today:
        label = d.isoformat()
        collect(f"topic:dsh-plugin created:{label}..{label}", label)
        d += dt.timedelta(days=1)
        time.sleep(7)
    # 历史月份按月分桶
    y, m = today.year, today.month - 1
    while y >= 2026 and m >= 1:
        lo = dt.date(y, m, 1)
        hi = dt.date(y, m + 1, 1) - dt.timedelta(days=1)
        collect(f"topic:dsh-plugin created:{lo.isoformat()}..{hi.isoformat()}", lo.isoformat())
        m -= 1
        if m == 0:
            y -= 1
            m = 12
        time.sleep(7)
    collect("topic:dsh-plugin created:<2026-01-01", "pre-2026")
    return data


def fetch_heat(entries, cache, offline=False):
    """抓取所有精选仓库的 star/fork。有 token 走并发 core API；无 token 走 topic 搜索。"""
    repos = []
    for e in entries:
        r = repo_of(e["url"])
        if r and r not in repos:
            repos.append(r)
    data = dict(cache)
    token = os.environ.get("GITHUB_TOKEN", "")

    if offline:
        print(f"[heat] offline 模式：使用缓存 {len(data)} 条", flush=True)
        return data, data

    def gh_get(url):
        h = {"User-Agent": UA, "Accept": "application/vnd.github+json"}
        if token:
            h["Authorization"] = f"Bearer {token}"
        req = urllib.request.Request(url, headers=h)
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read().decode())

    if token:
        # 快速路径：逐仓库 core API（5000/小时）
        def fetch(repo):
            try:
                d = gh_get(f"https://api.github.com/repos/{repo}")
                return repo, {"stars": d.get("stargazers_count", 0), "forks": d.get("forks_count", 0)}
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    return repo, None
                if e.code == 403:
                    wait = max(int(e.headers.get("X-RateLimit-Reset", 0)) - int(time.time()), 10)
                    print(f"  限速等待 {wait}s", flush=True)
                    time.sleep(wait)
                    try:
                        d = gh_get(f"https://api.github.com/repos/{repo}")
                        return repo, {"stars": d.get("stargazers_count", 0), "forks": d.get("forks_count", 0)}
                    except Exception:
                        return repo, None
                return repo, None
            except Exception:
                return repo, None

        pending = [r for r in repos if r not in data or offline is False]
        print(f"[heat] 并发抓取 {len(repos)} 个仓库（token）", flush=True)
        with ThreadPoolExecutor(max_workers=8) as pool:
            futs = {pool.submit(fetch, r): r for r in repos}
            done = 0
            for fut in as_completed(futs):
                repo, info = fut.result()
                done += 1
                if info is not None:
                    data[repo] = info
                if done % 300 == 0:
                    print(f"  {done}/{len(repos)}", flush=True)
    else:
        print("[heat] 无 GITHUB_TOKEN，使用慢速 topic 搜索（约 20 分钟）", flush=True)
        data = fetch_heat_topic_walk(repos, data)

    save_cache(data)
    return data, data


# ---------------------------------------------------------------- 表格 ----
def esc(t):
    return t.replace("|", "\\|").replace("\n", " ")


def write_tables(entries, heat):
    today = datetime.date.today().isoformat()
    counts = OrderedDict()
    for e in entries:
        counts[e["cat"]] = counts.get(e["cat"], 0) + 1

    def heat_of(e):
        r = repo_of(e["url"])
        info = heat.get(r) if r else None
        return (info.get("stars") if info else None, info.get("forks") if info else None)

    out = []
    out.append("# DSH 插件分类总表（来源：awesome-dsh-plugin README.zh.md，含热度）\n")
    out.append(f"> 共收录 **{len(entries)}** 个插件，分为 **{len(counts)}** 个类别；热度（⭐ Stars / 🍴 Forks）取自 GitHub，更新于 {today}。\n")
    out.append("## 分类概览\n")
    out.append("| 分类 | 插件数 |")
    out.append("| --- | ---: |")
    for c, n in counts.items():
        out.append(f"| {c} | {n} |")
    out.append("")

    with_heat = [e for e in entries if heat_of(e)[0] is not None]
    if with_heat:
        out.append("## 🔥 热度 TOP 30（按 Star 数）\n")
        out.append("| # | 插件 | ⭐ Stars | 🍴 Forks | 链接 |")
        out.append("| ---: | --- | ---: | ---: | --- |")
        top = sorted(with_heat, key=lambda e: (heat_of(e)[0], heat_of(e)[1]), reverse=True)[:30]
        for i, e in enumerate(top, 1):
            s, f = heat_of(e)
            out.append(f"| {i} | `{esc(e['name'])}` | {s} | {f} | <{e['url']}> |")
        out.append("")

    out.append("## 插件明细\n")
    out.append("| 分类 | 插件 | ⭐ Stars | 🍴 Forks | 功能简介 | 链接 |")
    out.append("| --- | --- | ---: | ---: | --- | --- |")
    for e in entries:
        s, f = heat_of(e)
        out.append(f"| {esc(e['cat'])} | `{esc(e['name'])}` | {'—' if s is None else s} | {'—' if f is None else f} | {esc(e['desc'])} | <{e['url']}> |")

    md_path = os.path.join(ROOT, "dsh-plugins-table.md")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(out) + "\n")

    csv_path = os.path.join(ROOT, "dsh-plugins-table.csv")
    with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["分类", "插件", "Stars", "Forks", "功能简介", "链接"])
        for e in entries:
            s, f = heat_of(e)
            w.writerow([e["cat"], e["name"], "" if s is None else s, "" if f is None else f, e["desc"], e["url"]])
    print(f"[tables] 已写入 {md_path} / {csv_path}", flush=True)


# ---------------------------------------------------------------- HTML ----
def write_html(entries, heat):
    rows = []
    for e in entries:
        r = repo_of(e["url"])
        info = heat.get(r) if r else None
        rows.append({
            "name": e["name"], "cat": e["cat"], "desc": e["desc"], "url": e["url"],
            "stars": info.get("stars") if info else None,
            "forks": info.get("forks") if info else None,
        })
    payload = json.dumps(rows, ensure_ascii=False)
    html = HTML_TEMPLATE.replace("__DATA__", payload).replace("__DATE__", datetime.date.today().isoformat())
    os.makedirs(os.path.join(ROOT, "docs"), exist_ok=True)
    with open(os.path.join(ROOT, "docs", "index.html"), "w", encoding="utf-8") as f:
        f.write(html)
    print(f"[html] 已写入 {os.path.join(ROOT, 'docs', 'index.html')}", flush=True)


HTML_TEMPLATE = """<!doctype html>
<html lang="zh">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>DSH 插件总览 · 更新时间 __DATE__</title>
<style>
:root{--bg:#fafafa;--card:#fff;--fg:#1a1a1a;--muted:#8a8f98;--line:#e6e6e6;--accent:#4f7cff}
@media(prefers-color-scheme:dark){:root{--bg:#121212;--card:#1c1c1c;--fg:#e8e8e8;--muted:#8a8f98;--line:#2c2c2c}}
*{box-sizing:border-box}body{margin:0;font:14px/1.6 -apple-system,"PingFang SC","Microsoft YaHei",sans-serif;background:var(--bg);color:var(--fg)}
.wrap{max-width:1100px;margin:0 auto;padding:20px 16px 80px}
h1{font-size:20px;margin:8px 0 2px}.sub{color:var(--muted);font-size:12px;margin-bottom:16px}
.bar{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin-bottom:12px;position:sticky;top:0;background:var(--bg);padding:8px 0;z-index:5}
input,select{height:32px;padding:0 10px;border:1px solid var(--line);border-radius:8px;background:var(--card);color:var(--fg);font:inherit}
input{flex:1;min-width:200px}.count{color:var(--muted);font-size:12px}
.g{background:var(--card);border:1px solid var(--line);border-radius:10px;margin-bottom:10px;overflow:hidden}
.gh{padding:8px 14px;font-weight:600;font-size:13px;display:flex;justify-content:space-between;cursor:pointer;user-select:none}
.gh small{color:var(--muted);font-weight:400}
.row{display:grid;grid-template-columns:auto minmax(0,1fr) auto auto auto;gap:10px;align-items:center;padding:6px 14px;border-top:1px solid var(--line);cursor:pointer}
.row:hover{background:rgba(0,0,0,.04)}.dot{width:8px;height:8px;border-radius:50%}
.nm{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.nm b{font-weight:600}.dc{color:var(--muted);font-size:12px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.m{color:var(--muted);font-size:12px;text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap}
.empty{padding:40px;text-align:center;color:var(--muted)}
details.g summary{list-style:none}.g:not([open]) .row{display:none}
</style>
</head>
<body><div class="wrap">
<h1>📦 DSH 插件总览</h1>
<div class="sub">数据来源：awesome-dsh-plugin（GitHub Actions 每周自动同步） · 更新时间 __DATE__ · 点击行复制链接</div>
<div class="bar">
<input id="q" placeholder="搜索插件名称 / 描述…" autocomplete="off">
<select id="cat"><option value="">全部分类</option></select>
<select id="sort">
<option value="stars">按 Star 排序</option><option value="forks">按 Fork 排序</option>
<option value="name">按名称排序</option><option value="cat">按分类排序</option>
</select>
<span class="count" id="cnt"></span>
</div>
<div id="list"></div>
</div>
<script>
const DATA = __DATA__;
const COLORS = {code:"#4f7cff",docs:"#2e9e5b",config:"#8a63d2",data:"#d09b2e",image:"#e05b8d",media:"#d95f3b",web:"#2ea8b8",archive:"#7a8699",other:"#9aa0a6"};
const cats = [...new Set(DATA.map(d=>d.cat))];
const catSel = document.getElementById('cat');
cats.forEach(c=>{const o=document.createElement('option');o.value=c;o.textContent=c;catSel.appendChild(o)});
const fmt = n => n==null ? '—' : n>=1048576 ? (n/1048576).toFixed(1)+' MB' : n>=1024 ? (n/1024).toFixed(1)+' KB' : n+' B';
function render(){
  const q = document.getElementById('q').value.trim().toLowerCase();
  const c = document.getElementById('cat').value;
  const s = document.getElementById('sort').value;
  let rows = DATA.filter(d => (!c || d.cat===c) && (!q || d.name.toLowerCase().includes(q) || d.desc.toLowerCase().includes(q)));
  const key = d => s==='stars'? (d.stars??-1) : s==='forks'? (d.forks??-1) : s==='name'? d.name.toLowerCase() : d.cat;
  rows.sort((a,b)=> s==='name'||s==='cat' ? (key(a)<key(b)?-1:1) : (key(b)-key(a)));
  document.getElementById('cnt').textContent = rows.length + ' / ' + DATA.length;
  const groups = {};
  rows.forEach(d=>{(groups[d.cat]=groups[d.cat]||[]).push(d)});
  const list = document.getElementById('list'); list.innerHTML='';
  const order = cats.filter(c=>groups[c]);
  for (const g of order){
    const sec=document.createElement('details'); sec.className='g'; sec.open = groups[g].length<=30;
    sec.innerHTML = '<summary class="gh">'+g+'<small>'+groups[g].length+' 个</small></summary>';
    for (const d of groups[g]){
      const row=document.createElement('div'); row.className='row';
      row.title=d.url;
      const dot=COLORS[d.cat]||COLORS.other;
      row.innerHTML='<span class="dot" style="background:'+dot+'"></span><span class="nm"><b>'+d.name+'</b><div class="dc">'+d.desc+'</div></span><span class="m">'+(d.stars==null?'—':d.stars+' ⭐')+'</span><span class="m">'+(d.forks==null?'—':d.forks+' ⑂')+'</span><span class="m">'+d.cat+'</span>';
      row.onclick=()=>{navigator.clipboard.writeText(d.url); row.style.opacity=.5; setTimeout(()=>row.style.opacity=1,400)};
      sec.appendChild(row);
    }
    list.appendChild(sec);
  }
  if(!rows.length) list.innerHTML='<div class="empty">没有匹配的插件</div>';
}
['q','cat','sort'].forEach(id=>document.getElementById(id).addEventListener('input',render));
render();
</script></body></html>
"""


# ---------------------------------------------------------------- 二级分类 ----
# 自动打标规则：按「一级分类 + 名称/描述关键词」给出二级分类。
# 仅在 Notion 行原本没有二级分类时写入；已有人工维护值的一律保留不动。
SUBCAT_RULES = {
    "🎨 UI 增强": [
        ("文件浏览器/工作区", ["文件树", "文件浏览", "文件管理器", "工作区", "workspace", "explorer", "目录面板", "file tree"]),
        ("终端", ["terminal", "终端", "tui", "pty", "xterm", "shell", "控制台"]),
        ("导航/大纲/时间轴", ["导航", "大纲", "时间轴", "timeline", "outline", "rail", "目录条", "轮次", "跳转", "消息导航", "nav"]),
        ("折叠/精简视图", ["折叠", "fold", "collaps", "精简", "只看结果", "result-only", "hide", "隐藏", "compact", "收起"]),
        ("Mermaid/图表渲染", ["mermaid", "diagram", "图表", "渲染", "canvas", "diff 查看", "diff-view", "excalidraw", "画布", "grafo"]),
        ("桌宠/动画", ["桌宠", "宠物", "pet", "动画", "animation", "鲸鱼", "whale", "sprite", "live2d", "桌宠", "像素宠"]),
        ("文件上传/拖拽", ["上传", "拖拽", "drag", "drop", "文件引用", "file-mention", "附件", "粘贴", "upload"]),
        ("输入增强/提示词", ["输入", "composer", "提示词", "prompt", "历史", "快捷键", "hotkey", "回车", "enter", "粘贴板", "输入框"]),
        ("模型/技能选择器", ["选择器", "picker", "selector", "模型选择", "技能选择", "skill-picker"]),
        ("状态/监控面板", ["状态", "监控", "monitor", "hud", "sysmon", "面板", "仪表", "stats", "统计", "状态栏", "状态条", "看板"]),
        ("设置/外观微调", ["设置", "setting", "宽度", "字体大小", "字号", "customize", "自定义", "外观", "样式", "调整", "tweak"]),
        ("移动端适配", ["移动", "手机", "mobile", "窄屏", "触屏", "pwa", "ipad", "iphone"]),
        ("桌面壳/启动器", ["桌面", "desktop", "启动器", "launcher", "托盘", "tray", "窗口", "启动", "快捷方式", "shell"]),
    ],
    "💰 用量与计费": [
        ("余额/额度显示", ["余额", "balance", "额度", "quota", "配额", "credit", "钱包"]),
        ("用量热力图", ["热力图", "heatmap", "用量", "usage", "token 用量", "消耗", "统计"]),
        ("费用/成本统计", ["费用", "成本", "花费", "cost", "spend", "billing", "计费", "计价", "meter", "消费"]),
        ("用量报表", ["报表", "报告", "report", "汇总", "日报", "周报", "月报", "复盘"]),
    ],
    "🎭 主题与外观": [
        ("主题皮肤", ["主题", "theme", "皮肤", "skin", "配色", "catppuccin", "solarized", "换肤", "色板"]),
        ("壁纸/背景/玻璃", ["壁纸", "背景", "wallpaper", "glass", "玻璃", "液态", "磨砂", "frosted", "毛玻璃", "透明"]),
        ("字体", ["字体", "font", "字号"]),
    ],
    "🔌 模型与账号接入": [
        ("订阅/OAuth 接入", ["订阅", "subscription", "oauth", "登录", "账号", "codex", "chatgpt", "claude", "gemini", "登录态", "auth"]),
        ("多模型网关", ["路由", "router", "网关", "gateway", "回退", "fallback", "降级", "重试", "适配", "provider", "供应商", "中转"]),
    ],
    "🆔 身份与通信": [
        ("身份/账号", ["身份", "identity", "账号", "认证", "auth", "角色"]),
        ("通信/消息", ["通信", "消息", "message", "互通", "interconnect", "转发"]),
    ],
    "💬 会话与消息": [
        ("跨会话/互联/侧聊", ["跨会话", "侧聊", "侧会话", "互联", "互发", "互通", "relay", "room", "crosstalk", "桥", "bridge", "side"]),
        ("消息编辑/重发", ["编辑", "重发", "reroll", "撤回", "回退", "rewind", "删除", "delete", "撤销", "undo"]),
        ("会话搜索/标签/管理", ["搜索", "search", "标签", "tag", "会话", "session", "管理", "归档", "archive", "置顶", "pin", "重命名", "rename", "列表"]),
        ("会话导入/迁移", ["导入", "import", "迁移", "migrate", "转换", "迁移"]),
        ("上下文管理", ["上下文", "context", "压缩", "compact", "裁剪"]),
        ("会话控制/队列", ["队列", "queue", "暂停", "pause", "打断", "interrupt", "控制", "排队"]),
        ("会话报告/学习", ["报告", "report", "复盘", "学习", "讲解", "explain", "总结", "摘要"]),
        ("人格/角色扮演", ["人格", "persona", "角色扮演", "扮演", "女仆", "maid", "人格设定"]),
    ],
    "🧠 记忆": [
        ("记忆存储/索引", ["记忆", "memory", "召回", "recall", "remember", "记忆库", "知识库", "knowledge", "语义", "semantic", "向量", "vector", "回忆", "memoria", "记忆引擎", "长期记忆"]),
    ],
    "🛠️ 工具与能力": [
        ("网络/搜索", ["搜索", "search", "网页", "web", "网络", "fetch", "抓取", "rss", "tavily", "firecrawl", "searxng", "exa", "http", "http", "请求", "爬虫"]),
        ("运维/SSH/远程", ["ssh", "remote", "远程", "服务器", "server", "部署", "docker", "容器", "kubernetes", "jenkins", "运维", "ops", "k8s"]),
        ("文件/文档处理", ["文件", "pdf", "docx", "文档", "markdown", "csv", "excel", "office", "解析", "parse", "转换", "压缩包", "zip"]),
        ("代码/执行", ["代码", "code", "执行", "bash", "shell", "命令", "run", "编译", "compile", "测试", "test", "执行器", "脚本"]),
        ("数据/表格", ["数据", "data", "sql", "数据库", "database", "表格", "指标", "量化", "quant", "股票", "行情", "金融", "分析"]),
        ("安全/护栏/审计", ["安全", "security", "guard", "审计", "audit", "密钥", "secret", "扫描", "脱敏", "redact", "沙箱", "sandbox", "权限", "permission", "拦截", "闸门"]),
        ("知识库/笔记", ["知识库", "knowledge", "kb ", "笔记", "note", "obsidian", "zotero", "文献", "notion", "知识图谱"]),
        ("运维/SSH/远程", ["ssh", "remote", "远程", "服务器", "server", "部署", "docker", "容器", "kubernetes", "jenkins", "运维", "ops"]),
        ("GitHub/代码托管", ["github", "issue", "pr ", "release", "代码托管", "gitlab", "gitee", "repo"]),
        ("子代理/委派", ["subagent", "子代理", "委派", "delegate", "派发", "sub-agent", "外部 agent", "外部agent"]),
        ("插件管理", ["插件", "plugin", "安装", "卸载", "管理", "market"]),
        ("上下文/性能", ["上下文", "context", "压缩", "compactor", "性能", "performance", "缓存", "cache", "节省 token"]),
        ("自进化/自我改进", ["进化", "evolve", "自进化", "自我改进", "self-improve", "成长"]),
        ("媒体/视频生成", ["视频", "video", "媒体", "media", "ffmpeg", "lottie", "svg", "动画", "生成"]),
        ("语音/输入输出", ["语音", "voice", "speech", "tts", "asr", "识别", "朗读", "音频"]),
        ("日历/待办", ["日历", "calendar", "待办", "todo", "任务", "提醒", "schedule", "日程"]),
        ("图像/视觉工具", ["图片", "图像", "image", "视觉", "vision", "ocr", "截图", "screenshot", "看图", "识图"]),
    ],
    "🖼️ 视觉与多模态": [
        ("视觉理解/分析", ["视觉", "vision", "识别", "理解", "分析", "ocr", "读图", "看图", "describe", "描述", "识图"]),
        ("图像生成", ["生图", "图像生成", "image-gen", "imagegen", "dall-e", "绘图", "generate", "文生图"]),
        ("视频生成/编辑", ["视频生成", "veo", "生成视频", "视频编辑"]),
        ("图像编辑", ["图像编辑", "抠图", "retouch", "背景处理", "图片编辑"]),
        ("3D/模型", ["3d", "glb", "gltf", "pbr", "模型预览"]),
    ],
    "🎙️ 语音与音频": [
        ("语音输入", ["语音输入", "asr", "识别", "转写", "麦克风", "microphone", "whisper", "输入"]),
        ("语音输出/朗读", ["朗读", "播报", "speak", "tts", "配音", "音色", "语音回复", "输出"]),
        ("音频处理", ["音频", "audio", "音效", "sound", "音乐", "music"]),
    ],
    "📄 文档与渲染": [
        ("Markdown/预览", ["markdown", "md ", "预览", "preview", "渲染", "render", "图表", "mermaid"]),
        ("PDF/文档处理", ["pdf", "docx", "doc ", "文档", "转换", "convert", "导出", "export", "解析"]),
    ],
    "🧩 技能包": [
        ("技能选择器/管理", ["技能管理", "技能选择", "skill-manager", "技能", "skills", "管理", "扫描", "选择器"]),
        ("领域技能-编程", ["编程", "coding", "开发", "代码", "review", "测试", "debug", "排障", "插件开发", "git"]),
        ("领域技能-研究/通用", ["研究", "research", "学术", "论文", "数学", "math", "科研", "工作流", "通用", "写作"]),
        ("领域技能-产品/运营/视频", ["产品", "运营", "营销", "电商", "视频", "合同", "设计", "ui", "ux", "商业", "新媒体"]),
        ("领域技能-中医/健康", ["中医", "健康", "养生", "八字", "风水", "玄学", "命理", "五运六气"]),
        ("新手入门/趣味", ["入门", "新手", "starter", "趣味", "烂梗", "meme", "整活"]),
    ],
    "🔁 工作流与自动化": [
        ("定时/计划任务", ["定时", "计划", "cron", "schedule", "调度", "scheduler", "周期", "定时任务"]),
        ("自动化编排", ["编排", "orchestrat", "流水线", "pipeline", "dag", "自动化", "swarm", "团队", "team", "多智能体", "multi-agent", "子代理", "subagent", "并行", "workflow", "长任务"]),
        ("权限/审批自动化", ["审批", "approve", "权限", "auto-approve", "门禁", "allow", "permission"]),
        ("工程/科研工作流", ["工程", "科研", "科学", "science", "代码审查", "review", "spec", "需求", "规划", "plan", "任务规划", "交付", "验收"]),
        ("钩子/事件", ["钩子", "hook", "事件", "event", "生命周期", "lifecycle"]),
        ("容错/回退", ["容错", "回退", "fallback", "熔断", "高可用", "failover", "重试"]),
    ],
    "🔀 Git 与代码评审": [
        ("Git 工具", ["git", "commit", "分支", "branch", "worktree", "提交", "changelog", "diff", "stash"]),
        ("代码评审", ["评审", "审查", "review", "code review", "pr"]),
    ],
    "🔔 通知与集成": [
        ("通讯集成", ["微信", "weixin", "wechat", "飞书", "feishu", "lark", "钉钉", "dingtalk", "telegram", "slack", "discord", "qq", "通讯", "桥", "bridge", "channel", "机器人", "bot", "webhook", "im ", "双向"]),
        ("消息通知/提醒", ["通知", "提醒", "notify", "notification", "alert", "toast", "推送", "桌面", "desktop", "横幅"]),
        ("提示音/语音播报", ["提示音", "音效", "sound", "语音播报", "speak", "铃声", "ding", "音频"]),
        ("日历/会议/任务集成", ["日历", "calendar", "会议", "meeting", "ticktick", "滴答", "任务"]),
        ("IDE/外部桥接", ["vscode", "ide", "editor", "zed", "集成", "桥接"]),
        ("多智能体/子代理", ["子代理", "subagent", "智能体", "agent", "multi"]),
        ("钩子/事件", ["钩子", "hook", "事件", "event", "lifecycle"]),
    ],
    "🧑\u200d💻 开发与运行时": [
        ("诊断/健康检查", ["诊断", "doctor", "健康", "health", "体检", "检查", "修复", "救砖", "rescue", "启动守卫"]),
        ("安全/权限/审计", ["安全", "security", "guard", "审计", "audit", "密钥", "secret", "凭据", "credential", "权限", "permission", "沙箱", "sandbox", "门禁", "扫描", "vet", "体检", "投毒", "供应链"]),
        ("插件开发/生态", ["插件", "plugin", "模板", "template", "开发", "打包", "bundle", "manifest", "生态", "registry", "注册表", "热加载", "热重载", "hot-reload"]),
        ("更新/版本管理", ["更新", "update", "升级", "升级", "version", "版本", "checker", "发布"]),
        ("遥测/观测监控", ["遥测", "telemetry", "观测", "observability", "trace", "追踪", "opentelemetry", "prometheus", "指标", "metric", "监控", "日志", "log", "jsonl", "receipt", "埋点"]),
        ("重启/电源控制", ["重启", "restart", "电源", "power", "关机", "shutdown", "按钮"]),
        ("远程/局域网访问", ["远程", "remote", "局域网", "lan", "tunnel", "隧道", "访问", "gateway", "frp", "cloudflare", "内网", "公网", "认证"]),
        ("终端/tmux", ["终端", "terminal", "tty", "pty", "tmux", "shell", "web-shell"]),
        ("代码质量/测试", ["测试", "test", "lint", "质量", "代码审查", "tsc", "类型检查", "smell", "重构", "verify", "校验", "eval", "评测", "单测"]),
        ("凭证/存储后端", ["凭据", "credential", "vault", "keychain", "存储后端", "backend", "spill", "s3", "存储", "storage"]),
        ("Git 工具", ["git", "commit", "分支", "branch", "提交", "diff", "worktree", "stash"]),
    ],
    "🛒 插件市场与管理": [
        ("插件市场", ["市场", "market", "商店", "store", "hub", "marketplace", "workshop", "创意工坊", "商场"]),
        ("插件管理", ["管理", "manager", "启停", "开关", "toggle", "switch", "卸载", "安装", "inventory", "清单", "面板"]),
        ("更新/版本管理", ["更新", "update", "升级", "version", "版本"]),
    ],
    "🎮 娱乐": [
        ("趣味/整活", ["整活", "趣味", "meme", "表情", "emoji", "贴纸", "广告", "搞笑", "斗图", "烂梗"]),
        ("游戏/小游戏", ["游戏", "game", "五子棋", "象棋", "自走棋", "舒尔特", "小游戏", "摸鱼", "消消乐"]),
        ("桌宠/动画", ["桌宠", "宠物", "pet", "精灵", "sprite", "live2d", "养成", "收集", "宝可梦", "数码宝贝", "像素"]),
        ("角色卡/世界书", ["角色卡", "世界书", "酒馆", "sillytavern", "lorebook", "galgame", "角色扮演"]),
        ("股票/行情", ["股票", "行情", "stock", "同花顺", "盯盘", "投资", "持仓", "k线", "基金"]),
        ("媒体/影音", ["视频", "音乐", "b站", "bilibili", "抖音", "douyin", "播放器", "看片", "music", "网易云"]),
        ("像素/创作", ["像素", "aseprite", "pixel", "创作"]),
    ],
}

SUBCAT_FALLBACK = {
    "🎨 UI 增强": "其他 UI 增强",
    "💰 用量与计费": "其他 用量计费",
    "🎭 主题与外观": "其他 主题外观",
    "🔌 模型与账号接入": "其他 模型接入",
    "🆔 身份与通信": "其他 身份通信",
    "💬 会话与消息": "其他 会话消息",
    "🧠 记忆": "其他 记忆",
    "🛠️ 工具与能力": "其他 工具能力",
    "🖼️ 视觉与多模态": "其他 视觉多模态",
    "🎙️ 语音与音频": "其他 语音音频",
    "📄 文档与渲染": "其他 文档渲染",
    "🧩 技能包": "其他 技能包",
    "🔁 工作流与自动化": "其他 工作流自动化",
    "🔀 Git 与代码评审": "其他 Git评审",
    "🔔 通知与集成": "其他 通知集成",
    "🧑\u200d💻 开发与运行时": "其他 开发运行时",
    "🛒 插件市场与管理": "其他 插件市场",
    "🎮 娱乐": "其他 娱乐",
    "🌐 浏览器与网页": "其他 浏览器网页",
    "🔒 安全与权限": "其他 安全权限",
    "📱 远程与移动端": "其他 远程移动",
}


def tag_subcategory(e):
    """按一级分类 + 关键词自动给出二级分类；无命中返回该分类的兜底值。"""
    rules = SUBCAT_RULES.get(e["cat"])
    if not rules:
        return SUBCAT_FALLBACK.get(e["cat"], "")
    hay = f"{e['name']} {e['desc']}".lower()
    for sub, kws in rules:
        for kw in kws:
            if kw in hay:
                return sub
    return SUBCAT_FALLBACK.get(e["cat"], "")


# ---------------------------------------------------------------- Notion ----
def notion_call(method, path, body=None, key=None):
    cmd = ["curl", "-s", "--max-time", "25", "-X", method, f"https://api.notion.com/v1{path}",
           "-H", f"Authorization: Bearer {key}", "-H", "Notion-Version: 2022-06-28",
           "-H", "Content-Type: application/json"]
    if body is not None:
        cmd += ["-d", json.dumps(body, ensure_ascii=False)]
    for attempt in range(3):
        try:
            out = __import__("subprocess").run(cmd, capture_output=True, text=True, timeout=30).stdout
            if not out.strip():
                time.sleep(2)
                continue
            return json.loads(out)
        except Exception:
            time.sleep(2)
    return {"object": "error", "code": "hard_fail"}


def sync_notion(entries, heat, db_id, key):
    """与 Notion 数据库比对：新增/更新/归档 + 去重。幂等，可重复执行。"""
    import subprocess

    print(f"[notion] 开始同步数据库 {db_id}", flush=True)

    def query_all():
        rows = []
        cursor = None
        while True:
            body = {"page_size": 100}
            if cursor:
                body["start_cursor"] = cursor
            d = notion_call("POST", f"/databases/{db_id}/query", body, key)
            if d.get("object") != "list":
                raise RuntimeError(f"query failed: {d.get('code', d)}")
            rows.extend(d.get("results", []))
            if not d.get("has_more"):
                break
            cursor = d.get("next_cursor")
        return rows

    def link_of(r):
        p = r["properties"].get("链接")
        return norm_url(p["url"]) if p and p["type"] == "url" else None

    def props_body(e, sub=None):
        r = repo_of(e["url"])
        info = heat.get(r) if r else None
        s, f = (info.get("stars"), info.get("forks")) if info else (None, None)
        p = {
            "插件名称": {"title": [{"text": {"content": e["name"][:2000]}}]},
            "一级分类": {"select": {"name": e["cat"]}},
            "描述": {"rich_text": [{"text": {"content": e["desc"][:2000]}}]},
            # 必须写入「链接」：去重依赖此字段。此前漏写 → 链接为空 →
            # 判重失效 → 同一插件被反复新增（每天一条）。
            "链接": {"url": e["url"]},
        }
        if sub:
            p["二级分类"] = {"select": {"name": sub}}
        if s is not None:
            p["⭐ Stars"] = {"number": s}
        if f is not None:
            p["🍴 Forks"] = {"number": f}
        return p

    rows = query_all()
    print(f"[notion] 现有行 {len(rows)}", flush=True)
    # 去重：同一链接多行时归档多余行
    seen = {}
    dupes = []
    for r in rows:
        l = link_of(r)
        if l is None:
            continue
        if l in seen:
            dupes.append(r["id"])
        else:
            seen[l] = r["id"]
    for pid in dupes:
        notion_call("PATCH", f"/pages/{pid}", {"archived": True}, key)
    if dupes:
        print(f"[notion] 归档重复行 {len(dupes)}", flush=True)

    new_links = {norm_url(e["url"]) for e in entries}
    # seen / new_links 必须同构（都归一化），否则会误把正常记录判为"已下架"而归档
    to_archive = [l for l in seen if l not in new_links]
    # 差异比较：只更新真正变化的行（避免每周全量 PATCH）
    current = {}
    for r in rows:
        l = link_of(r)
        if l is None:
            continue
        p = r["properties"]
        def textv(prop):
            obj = p.get(prop)
            if not obj:
                return ""
            return "".join(x.get("plain_text", "") for x in obj.get(obj["type"], []) or [])
        current[l] = {
            "name": textv("插件名称"),
            "cat": p["一级分类"]["select"]["name"] if p["一级分类"]["select"] else None,
            "desc": textv("描述"),
            "stars": p.get("⭐ Stars", {}).get("number") if p.get("⭐ Stars") else None,
            "forks": p.get("🍴 Forks", {}).get("number") if p.get("🍴 Forks") else None,
            "sub": p["二级分类"]["select"]["name"] if p["二级分类"]["select"] else None,
        }

    def expected(e, cur):
        r = repo_of(e["url"])
        info = heat.get(r) if r else None
        s, f = (info.get("stars"), info.get("forks")) if info else (None, None)
        return {
            "name": e["name"], "cat": e["cat"], "desc": e["desc"], "stars": s, "forks": f,
            # 已有二级分类（人工维护）保持不动；为空则自动打标
            "sub": cur["sub"] if cur.get("sub") else tag_subcategory(e),
        }

    to_update = []
    to_create = []
    for e in entries:
        k = norm_url(e["url"])          # 与 seen / current 的 key 保持一致
        if k not in seen:
            to_create.append(e)
        elif current[k] != expected(e, current[k]):
            to_update.append(e)
    print(f"[notion] 差异: 更新 {len(to_update)} / 新增 {len(to_create)} / 归档 {len(to_archive)}", flush=True)

    lock = threading_local()
    bucket = {"tokens": 2.4, "last": time.monotonic()}

    def rate_wait():
        while True:
            with lock:
                now = time.monotonic()
                bucket["tokens"] = min(2.4, bucket["tokens"] + (now - bucket["last"]) * 2.4)
                bucket["last"] = now
                if bucket["tokens"] >= 1:
                    bucket["tokens"] -= 1
                    return
            time.sleep(0.05)

    def do_update(e):
        rate_wait()
        # 已有人工二级分类的行不覆盖；空行自动打标
        k = norm_url(e["url"])          # 与 seen / current 的 key 保持一致
        sub = None if current[k].get("sub") else tag_subcategory(e)
        return notion_call("PATCH", f"/pages/{seen[k]}", {"properties": props_body(e, sub)}, key)

    def do_create(e):
        rate_wait()
        return notion_call("POST", "/pages", {"parent": {"database_id": db_id}, "properties": props_body(e, tag_subcategory(e))}, key)

    def do_archive(l):
        rate_wait()
        return notion_call("PATCH", f"/pages/{seen[l]}", {"archived": True}, key)

    def run(tasks, fn, label):
        with ThreadPoolExecutor(max_workers=4) as pool:
            futs = [pool.submit(fn, t) for t in tasks]
            ok = 0
            for fut in as_completed(futs):
                r = fut.result()
                if r.get("object") == "page":
                    ok += 1
            print(f"[notion] {label}: {ok}/{len(tasks)}", flush=True)
            return ok

    run(to_archive, do_archive, "归档")
    run(to_create, do_create, "新增")
    run(to_update, do_update, "更新")
    print("[notion] 同步完成", flush=True)


def threading_local():
    import threading
    return threading.Lock()


# ---------------------------------------------------------------- main ----
def main():
    ap = argparse.ArgumentParser(description="DSH 插件表每周同步")
    ap.add_argument("--no-notion", action="store_true", help="跳过 Notion 同步")
    ap.add_argument("--no-pages", action="store_true", help="跳过 GitHub Pages 网页生成")
    ap.add_argument("--offline", action="store_true", help="热度使用本地缓存，不请求 GitHub")
    ap.add_argument("--readme-file", default=None, help="使用本地 README 文件")
    args = ap.parse_args()

    try:
        text = fetch_readme(args.readme_file)
        counts, entries = parse_readme(text)
        print(f"[readme] 共 {len(entries)} 条 / {len(counts)} 个分类", flush=True)
    except Exception as e:
        print(f"[readme] 失败: {e}", flush=True)
        sys.exit(1)

    cache = load_cache()
    try:
        heat, _ = fetch_heat(entries, cache, offline=args.offline)
    except Exception as e:
        print(f"[heat] 抓取失败，回退缓存: {e}", flush=True)
        heat = cache

    try:
        write_tables(entries, heat)
        if not args.no_pages:
            write_html(entries, heat)
    except Exception as e:
        print(f"[tables] 失败: {e}", flush=True)
        sys.exit(1)

    key = os.environ.get("NOTION_API_KEY", "")
    db_id = os.environ.get("NOTION_DB_ID", DEFAULT_NOTION_DB)
    if not args.no_notion and key:
        try:
            sync_notion(entries, heat, db_id, key)
        except Exception as e:
            print(f"[notion] 同步失败（不影响仓库表格）: {e}", flush=True)
    elif not args.no_notion:
        print("[notion] 未设置 NOTION_API_KEY，跳过 Notion 同步", flush=True)

    print("DONE", flush=True)


if __name__ == "__main__":
    main()

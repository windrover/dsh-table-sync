# DSH 插件表每日自动同步

把 [awesome-dsh-plugin](https://github.com/awesome-dsh-plugin/awesome-dsh-plugin) 的插件清单与
GitHub 热度（⭐ Star / 🍴 Fork）**每天自动同步**到三处：

1. **仓库内表格** —— `dsh-plugins-table.md` / `dsh-plugins-table.csv`（GitHub Actions 自动提交）
2. **GitHub Pages 网页** —— `docs/index.html`（带搜索 / 分类筛选 / 排序，自动部署）
3. **Notion 数据库** —— 自动新增 / 更新 / 归档（幂等，可重复执行）

数据流：

```
awesome-dsh-plugin README.zh.md（插件清单）
        +
GitHub API（每仓库 star / fork）
        │  sync.py（解析 → 抓取 → 比对）
        ├─→ dsh-plugins-table.md / .csv（提交回仓库）
        ├─→ docs/index.html（部署 GitHub Pages）
        └─→ Notion 数据库「DSH 插件总览」
```

## 目录结构

```
dsh-table-sync/
├── sync.py                     # 一键同步脚本（核心）
├── .github/workflows/sync.yml  # 每天自动执行的 Actions
├── data/heat.json              # 热度缓存（脚本自动维护并提交）
├── docs/index.html             # Pages 网页（脚本生成）
├── dsh-plugins-table.md        # 表格产物（脚本生成）
└── dsh-plugins-table.csv
```

## 本地一键运行

```sh
# 全流程（含 Notion 同步）
export GITHUB_TOKEN=ghp_xxx        # 可选但推荐（否则走慢速兜底）
export NOTION_API_KEY=ntn_xxx      # 设置后同步 Notion
python3 sync.py

# 常用变体
python3 sync.py --no-notion        # 只更新仓库表格 + 网页
python3 sync.py --offline          # 用 data/heat.json 缓存，不发 GitHub 请求
python3 sync.py --readme-file /path/README.zh.md   # 用本地清单文件
```

## 部署到 GitHub Actions（约 3 分钟）

1. **新建私有仓库**（如 `dsh-table-sync`），把本目录内容推上去：

   ```sh
   git init && git add -A && git commit -m init && git push
   ```

2. **创建 GitHub PAT**（供高限速抓取用，可选但推荐）：
   Settings → Developer settings → Personal access tokens → Generate new token (classic)
   权限只勾 **public_repo**（只需读取公开仓库），复制 `ghp_...`。

3. **创建 Notion 集成令牌**（同步 Notion 用）：
   https://www.notion.so/my-integrations → New integration → 记下 `ntn_...`，
   然后在 Notion 里打开「DSH 插件总览」数据库 → 右上角 **••• → Connections → 添加你的集成**。

4. **配置仓库 Secrets**：
   仓库 Settings → Secrets and variables → Actions → New repository secret：
   | 名称 | 值 |
   | --- | --- |
   | `GH_TOKEN` | 第 2 步的 `ghp_...` |
   | `NOTION_API_KEY` | 第 3 步的 `ntn_...` |
   | `NOTION_DB_ID` | 数据库 ID（可留空，脚本内置默认值） |

5. **开启 GitHub Pages**：
   仓库 Settings → Pages → Source 选 **GitHub Actions**。

6. **手动试跑一次**：Actions 页面 → dsh-table-sync → **Run workflow**。
   成功后仓库里会出现更新后的表格，Pages 网址为 `https://<用户名>.github.io/dsh-table-sync/`，
   Notion 数据库同步为最新。

之后**每天 00:00 UTC（北京时间 08:00）自动执行**，也可随时手动触发。

## 二级分类自动打标

Notion 数据库里的「二级分类」列（如 UI 增强下的 `文件浏览器/工作区`、`终端`、`导航/大纲/时间轴`…）
由脚本**按规则自动打标**：

- 规则 = 「一级分类 + 名称/描述关键词」匹配（规则表在 `sync.py` 的 `SUBCAT_RULES`，可按需增改）。
- **只填空值**：行原本没有二级分类时才写入自动标签；**已有人工维护的二级分类一律保留不动**。
- 未命中任何关键词的行落到该分类的兜底值（如 `其他 UI 增强`），保证 100% 有标签。
- 新增行在创建时即自动打标；旧行在下次同步时自动补上。

调整打标规则：编辑 `SUBCAT_RULES`（每个一级分类下的 `(二级分类名, [关键词列表])`，先匹配先得）即可。

## 安全性

- Token 全部存于 **Secrets / 环境变量**，不进代码、不进聊天。
- Notion 同步为**幂等**：重复运行只会补齐差异（新增/更新/归档重复行），不会重复建行。
- 脚本默认先保证仓库表格成功生成；Notion 失败只告警、不阻塞提交。
- GitHub PAT 权限最小化（public_repo 只读即可）；建议定期轮换。

## 频率调整

改 `.github/workflows/sync.yml` 里的 cron 即可，例如每天凌晨：

```yaml
schedule:
  - cron: '0 0 * * *'
```

cron 语法是 **UTC 时间**（北京时间 = UTC + 8）。

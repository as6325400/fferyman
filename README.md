# fferyman

Pluggable folder-mirror framework with SQLite-backed mappings.

監聽來源目錄的變化,用可擴充的演算法(plugin)把檔案/目錄鏡像到目標位置,並以 SQLite 紀錄所有 source→dest 對應、內容雜湊與重複狀態。適合資料夾同步、內容去重、備份、資料集整理等場景。

---

## 目錄

- [需求](#需求)
- [安裝](#安裝)
- [快速上手](#快速上手)
- [設定檔格式](#設定檔格式)
- [範例演算法](#範例演算法)
- [同步 Policy](#同步-policy)
- [日誌(Logging)](#日誌logging)
- [CLI 指令](#cli-指令)
- [生產部署(systemd)](#生產部署systemd)
- [撰寫自己的 Plugin](#撰寫自己的-plugin)
- [資料庫結構](#資料庫結構)
- [除錯](#除錯)
- [開發](#開發)

---

## 需求

- Python **3.11+**
- 相依套件(會自動安裝):`watchdog>=4.0`、`PyYAML>=6.0`
- Linux / macOS(Windows 未測試)

## 安裝

### 本地開發(editable install)

```bash
git clone <this-repo> fferyman
cd fferyman
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

安裝後會有 `fferyman` 這個指令可用。

### 生產環境

建議用獨立的 venv 或系統套件管理器:

```bash
python -m venv /opt/fferyman/venv
/opt/fferyman/venv/bin/pip install /path/to/fferyman-source
```

執行檔會在 `/opt/fferyman/venv/bin/fferyman`。

---

## 快速上手

1. **建立設定檔**

   複製範例:

   ```bash
   cp config/watch.example.yaml config/watch.yaml
   ```

   編輯 `config/watch.yaml`,把 `source` 跟 `dest` 改成你實際的路徑。

2. **檢查設定**

   ```bash
   fferyman doctor -c config/watch.yaml
   ```

   檢查:每個 watch 的演算法是否存在、source 是否可讀、dest 是否可建立、SQLite 是否可寫。

3. **啟動服務**

   ```bash
   fferyman run -c config/watch.yaml
   ```

   `run` 會先對每個 watch 做一次完整初始掃描(等同於呼叫一次 `scan`),**接著**啟動 watchdog 持續監聽。所以首次部署直接 `run` 就好,不必先手動 `scan`。前景執行,`Ctrl+C` 停止;生產環境請交給 systemd(見下文)。

   如果只想跑一次全量、不要常駐監聽(例如配 cron 輪詢、或 source 在 NFS 上 watchdog 不會觸發事件),才用 `scan`:

   ```bash
   fferyman scan -c config/watch.yaml
   ```

---

## 設定檔格式

設定檔是 YAML,範例:[config/watch.example.yaml](config/watch.example.yaml)

```yaml
database: ./fferyman.sqlite      # SQLite 檔案路徑(會自動建立)
plugins_dir: ./plugins           # (選填)外部 plugin 目錄
log_level: INFO                  # DEBUG / INFO / WARNING / ERROR

# 頂層 policy 預設(選填,watch 可覆蓋)
on_conflict: duplicate           # overwrite | duplicate | error
on_change:   version             # replace | version
on_delete:   keep_dest           # delete_dest | keep_dest | archive
duplicate_dir: duplicate
archive_dir:   archive

watches:
  - name: interns-share          # 必填,且必須唯一
    algorithm: flatten
    source: /weka/A/B/C          # 監聽來源
    dest:   /shared/B            # 鏡像目的地
    # 繼承頂層 policy

  - name: batch-dirs
    algorithm: mirror_matching_dirs
    source: /weka/A/B/C
    dest:   /shared/batches
    on_conflict: error           # 覆蓋頂層
    on_delete:   archive
    params:
      include: "batch_*"
      exclude: "tmp_*"
```

### 欄位

| 欄位 | 說明 |
|---|---|
| `database` | SQLite 檔案路徑。所有 watch 共用同一個 DB,內部依 `watch_name + algorithm` 做 scope 隔離 |
| `plugins_dir` | 選填。啟動時會把該目錄下的 `*.py` 當 plugin 自動載入 |
| `log_level` | Python logging 等級,字串 |
| `on_conflict` / `on_change` / `on_delete` | 同步 policy(見[同步 Policy](#同步-policy));可寫在頂層當預設、也可寫在 `watches[]` 覆蓋 |
| `duplicate_dir` / `archive_dir` | 撞名備份與封存的子資料夾名(預設 `duplicate` / `archive`) |
| `watches[].name` | watch 的唯一識別字,會出現在 log、DB 裡 |
| `watches[].algorithm` | 要用的演算法名稱(對應 mapper 的 `@algorithm("...")` 名字) |
| `watches[].source` | 監聽的來源目錄,必須已存在 |
| `watches[].dest` | 鏡像目的地,不存在會自動建立 |
| `watches[].params` | 傳給演算法函式的 kwargs;欄位由各演算法定義 |

### 路徑展開

`~` 會被展開為 home。相對路徑以「執行 fferyman 時的 CWD」為基準,在 systemd 下建議全部寫絕對路徑。

---

## 範例演算法

`examples/` 底下有兩個可以直接用的演算法,但**不會自動被載入** —— CLI 只會從設定檔裡的 `plugins_dir`、以及 Python entry points 載入演算法([src/fferyman/cli.py](src/fferyman/cli.py))。要用的話二選一:

- 把 `examples/*.py` 複製或 symlink 進 `plugins/`
- 或把設定檔 `plugins_dir` 指到 `./examples`

接著就可以在 `watches[].algorithm` 用下面這些名字。

### 1. `flatten`

把 source 樹底下的檔案全部打平到 `dest/<basename>`。**撞名、內容變更、來源刪除**的處理都由設定檔的 policy 決定。

範例(`on_conflict=duplicate`、`on_change=version`,預設值):
```
source/01/I       →  dest/I
source/02/I       →  dest/duplicate/I_2
source/03/I       →  dest/duplicate/I_3
```

同樣的演算法在 `on_conflict=overwrite` 下:
```
source/01/I       →  dest/I   (被後來的覆蓋)
source/02/I       →  dest/I   (再被覆蓋)
source/03/I       →  dest/I   (最終內容)
```

### 2. `mirror_matching_dirs`

把 source 第一層的目錄當作單位,整顆複製到 `dest/<dirname>/`。

| `params` 欄位 | 預設 | 說明 |
|---|---|---|
| `include` | `*` | 目錄名的 glob 白名單 |
| `exclude` | `""` | 目錄名的 glob 黑名單(空字串表示不排除) |

範例(`include: batch_*`、`exclude: tmp_*`):
```
source/batch_1/...  →  dest/batch_1/...
source/tmp_1/...    →  skip
source/other/...    →  skip
```

> 兩個演算法都只做 canonical target 映射。撞名 / 版本 / 刪除由 policy 管。

---

## 同步 Policy

fferyman 把同步過程中的**生命週期語意**抽成三個獨立 policy,在設定檔裡宣告,**不是 mapper 的責任**。

| Policy | 管什麼 | 選項 | 預設 |
|---|---|---|---|
| `on_conflict` | 新 source 的 canonical target 被別人佔時 | `overwrite` / `duplicate` / `error` | `duplicate` |
| `on_change` | 同 source 有新內容時 | `replace` / `version` | `version` |
| `on_delete` | source 消失時 dest 怎麼處理 | `delete_dest` / `keep_dest` / `archive` | `keep_dest` |

三個 policy 搭配起來能表達的行為:

- **嚴格鏡像**(兩邊保持一致):`overwrite` + `replace` + `delete_dest`
- **附加歸檔**(只進不出):`duplicate` + `version` + `keep_dest`(預設)
- **不可覆蓋 + 留歷史**:`error` + `version` + `archive`

Policy 可以寫在頂層當全域預設,也可以在個別 `watches[]` 內覆蓋(見上方設定檔範例)。

### Fingerprint 自動失效

每筆 DB 紀錄會帶一個 fingerprint:`<algo>@<revision>;<policy...>`。改 policy 或 `revision` 之後,fingerprint 一變,舊紀錄就被視為「過期」,下次 scan / reconcile 會:

- 內容也變了 → 套 `on_change`
- 內容沒變,只有 policy 變 → DB 就地更新 fingerprint,**不重複 copy**

完整說明:**[docs/policy.md](docs/policy.md)**。

---

## CLI 指令

所有指令都必須帶 `-c/--config`:

### `fferyman run -c <config>`
啟動時先對每個 watch 做一次完整初始掃描,接著啟動 watchdog 持續監聽。前景執行,`Ctrl+C` 停止。**首次部署用這個就好,不需要先 `scan`。**

### `fferyman scan -c <config>`
只做一次初始掃描,跑完就退出,不啟動 watchdog。適合:cron 輪詢式同步(source 在 NFS/CIFS 上 watchdog 不會觸發時)、資料搬遷後重建索引、CI/排程檢查。`scan` ⊂ `run`。

### `fferyman reconcile -c <config>`
做一次 scan,再把 DB 中所有「source 已消失但 mapping 還 active」的紀錄套 `on_delete`。用在:改 policy 或演算法 revision 之後、daemon 離線恢復、設定檔搬動、定期排程(每日跑一次)。詳見 [docs/policy.md](docs/policy.md#何時跑-fferyman-reconcile)。

### `fferyman list -c <config>`
列出目前註冊的演算法(附 `watch_mode` 跟 `revision`)以及設定檔裡的 watches(附 policy 摘要)。不做任何事。

### `fferyman doctor -c <config>`
健檢:演算法解析、source/dest 可達性、DB 可開。有問題時 exit code 非零。推薦接進 CI 或 systemd `ExecStartPre`。

---

## 生產部署(systemd)

專案提供範例 unit:[systemd/fferyman.service](systemd/fferyman.service)

### 部署步驟

1. 建立專用使用者(可選但推薦):

   ```bash
   sudo useradd -r -s /usr/sbin/nologin fferyman
   sudo mkdir -p /etc/fferyman /var/lib/fferyman
   sudo chown fferyman:fferyman /var/lib/fferyman
   ```

2. 安裝套件到系統 Python 或專用 venv:

   ```bash
   sudo python -m pip install /path/to/fferyman-source
   ```

3. 放設定檔:

   ```bash
   sudo cp config/watch.example.yaml /etc/fferyman/watch.yaml
   sudo $EDITOR /etc/fferyman/watch.yaml
   ```

   記得把 `database` 指向 `/var/lib/fferyman/fferyman.sqlite`,所有路徑用絕對路徑。

4. 安裝 unit:

   ```bash
   sudo cp systemd/fferyman.service /etc/systemd/system/
   sudo systemctl daemon-reload
   sudo systemctl enable --now fferyman
   ```

5. 查狀態 / log:

   ```bash
   systemctl status fferyman
   journalctl -u fferyman -f
   ```

### Unit 裡要注意的地方

範例 unit 內容([systemd/fferyman.service](systemd/fferyman.service)):

```ini
[Service]
ExecStart=/usr/bin/python -m fferyman run --config /etc/fferyman/watch.yaml
User=fferyman
ProtectSystem=strict
ReadWritePaths=/shared /var/lib/fferyman
```

- `ExecStart` 的 Python 路徑要改成你實際安裝的位置(系統 Python 或 venv 裡的 `python`)
- `ReadWritePaths` 必須列出:**所有** dest 目錄、SQLite 的所在目錄。沒列到 `ProtectSystem=strict` 會擋寫入。
- source 目錄只需要讀權限,不用列進 `ReadWritePaths`。

---

## 撰寫自己的 Plugin

一個演算法 = **一個函式**,只回答「這個 source 該去哪個 canonical dest?」。

```python
# plugins/my_algo.py
from fferyman import algorithm

@algorithm("flatten", revision=1)
def flatten(src, dest, **_):
    return dest / src.name
```

就這 3 行。事件監聽、hash、去重、撞名、內容變更、來源刪除、atomic 複製、DB 紀錄、log —— 全部由框架與 policy 處理,**不是演算法的事**。完整教學與更多範例(CAS、白名單、路徑重寫等)看 **[docs/writing-an-algorithm.md](docs/writing-an-algorithm.md)**。

---

## 日誌(Logging)

用 Python 標準 `logging`,由 [src/fferyman/cli.py](src/fferyman/cli.py) 在啟動時做 `basicConfig` 初始化,寫到 **stdout/stderr**。

### 格式與等級

格式固定為:

```
2026-04-15 14:03:21,105 INFO fferyman.interns-share: mirrored /weka/A/B/C/01/I -> /shared/B/I
```

等級由設定檔 `log_level` 控制(`DEBUG` / `INFO` / `WARNING` / `ERROR`,預設 `INFO`)。

### Logger 命名

| Logger 名稱 | 來源 |
|---|---|
| `fferyman.engine` | 事件派發、debounce、例外 |
| `fferyman.<watch_name>` | 各 watch 的 mirror 結果(成功 / 刪除 / 失敗) |

演算法是純函式、不直接寫 log —— copy 成功 / 失敗、軟刪除這些通通由 engine 記錄,自動帶 watch 名字。

### 取得 log

- **前景執行**:直接印在終端機
- **systemd**:進 journald
  ```bash
  journalctl -u fferyman -f           # 追 log
  journalctl -u fferyman --since today
  ```
- **寫到檔案**:systemd unit 改 `StandardOutput=append:/var/log/fferyman.log`,或用 shell redirect

### 目前沒有的

- 沒有 log rotation(靠 journald 或外部 logrotate)
- 沒有結構化 JSON log
- 沒有 audit trail(若要追「什麼時間複製了什麼」,查 [SQLite mappings 表](#資料庫結構),不要靠 log)

---

## 資料庫結構

SQLite 檔案裡有一張 `mappings` 表,主要欄位:

| 欄位 | 說明 |
|---|---|
| `id` | 自增 |
| `watch_name` | 對應設定檔 watch 的 name |
| `algorithm` | 演算法名 |
| `source_path` | 來源絕對路徑 |
| `dest_path` | 目的絕對路徑 |
| `content_hash` | SHA256(檔案)或 Merkle hash(目錄) |
| `is_duplicate` | 框架判定 target 落在 canonical 以外時為 `1`(撞名 / 版本旁分支) |
| `fingerprint` | `<algo>@<revision>;<policy...>`;policy 或 revision 改動會讓此欄位變化,舊紀錄因此失效 |
| `status` | `active` 或 `deleted`(軟刪除) |
| `source_inode`, `source_mtime` | 選填,engine 紀錄用 |

直接用 `sqlite3` 就能查:

```bash
sqlite3 /var/lib/fferyman/fferyman.sqlite \
  "SELECT watch_name, source_path, dest_path, is_duplicate, fingerprint
     FROM mappings WHERE status='active' LIMIT 20;"
```

---

## 除錯

- **看不到事件**:確認 `source` 真的是 `run` 的那個使用者可讀。watchdog 在某些網路 FS(NFS、CIFS)上不會觸發,這種情況改用 `scan` 搭 cron 輪詢。
- **doctor 全 OK 但 run 起不來**:看 `journalctl -u fferyman`,多半是 systemd 的 `ProtectSystem` / `ReadWritePaths` 擋到。
- **重複複製**:先 `sqlite3` 查 `source_path` 跟 `content_hash`,確認 DB 有沒有正確寫入。有可能是來源路徑的 inode 在切換儲存時變了。
- **想從零重建**:停 service、刪掉 SQLite、跑 `fferyman scan` 一次即可。目的地不需要清空 —— 依 `on_conflict` 走(預設 `duplicate` 會把既有檔案視為佔用並放進 `duplicate/`,這也是驗證方式)。
- **改了 policy 或演算法但沒更新到 dest**:跑 `fferyman reconcile`。只有 `scan` 會漏掉「source 已消失」的 mapping。
- **log 開細**:把 `log_level: DEBUG` 開起來。

---

## 開發

```bash
pip install -e ".[dev]"
pytest
```

測試位置:[tests/](tests/),涵蓋:

- 單元層 — `fsops` / `hashing` / `db`
- 整合層 — 兩個範例演算法的 scan / modify / delete / move
- Policy 矩陣 — `on_conflict` / `on_change` / `on_delete` 所有組合
- Target safety — mapper 回錯路徑的防守
- Fingerprint 失效與 `reconcile`
- 回歸測試 — 子檔案刪除誤判、跨邊界 move、revision retarget、file-mode 目錄、`duplicate_dir` traversal
- E2E — 真的跑 watchdog observer

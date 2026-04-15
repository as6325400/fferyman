# Sync Policy

fferyman 的同步行為由三個正交的 policy 決定。它們是**框架層級**的,寫在設定檔裡,不是 mapper 函式的責任。

| Policy | 管什麼 |
|---|---|
| `on_conflict` | **新 source** 的 canonical target 被**別人**佔走時怎麼辦 |
| `on_change` | **同 source** 有新內容時怎麼辦 |
| `on_delete` | **source 消失**時 dest 怎麼處理 |

---

## `on_conflict` — 撞名

情境:mapper 對某個 *新* source 回傳 `dest/a.txt`,但另一個 source 已經佔用那個路徑了。

| 值 | 行為 |
|---|---|
| `overwrite` | 覆蓋既有檔。DB 中舊 mapping 標 deleted,新 mapping 接手 |
| `duplicate`(預設) | 放到 `dest/<duplicate_dir>/a_2.txt`(同名遞增) |
| `error` | 拒絕、log error、跳過這個 source |

---

## `on_change` — 內容變更

情境:DB 裡已有 `sub/I` 的 mapping,但現在 `sub/I` 的 hash 變了(內容不同)。

| 值 | 行為 |
|---|---|
| `replace` | 把新內容寫到 canonical target(如果 canonical 被別人佔,再套 `on_conflict`);原 dest 若位置不同則刪掉,DB 舊 mapping 標 deleted |
| `version`(預設) | **保留原 dest 不動**,新版本放到 `dest/<duplicate_dir>/<name>_N` |

---

## `on_delete` — 來源消失

情境:`sub/I` 從 source 消失(DELETED event,或 reconcile 時發現已不存在)。

| 值 | 行為 |
|---|---|
| `keep_dest`(預設) | dest 留原樣,只在 DB 軟刪除 mapping |
| `delete_dest` | 刪掉 dest 檔案/目錄,DB 軟刪除 |
| `archive` | 把 dest 搬到 `dest/<archive_dir>/<rel_path>`,DB 軟刪除;目標已存在則 `_N` 遞增 |

---

## `hash_policy` — 何時算內容 hash

`hash_policy` 不改生命週期語意,但會改變 ingest 的 I/O 成本。

| 值 | 行為 |
|---|---|
| `always` | 每次 ingest 都先對 source 做完整內容 hash,再判斷要不要 copy |
| `metadata_fast_path`(預設) | regular file 先比 `inode + size + mtime_ns`; metadata 沒變就沿用 DB 內既有 hash,避免重讀整個檔案 |
| `copy_then_hash` | 第一次 regular-file ingest 先 copy 到 `dest/.fferyman-staging/`、對 staged copy 算 hash,再呼叫 mapper 決定正式 target。既能避免第一次 pre-hash,也不要求 hash-aware mapper 自己處理 `hash_=None` |

`copy_then_hash` 目前只套用在**第一次 regular-file ingest**。目錄單位、以及已經有 active mapping 的 source,仍然會走 pre-hash 路徑。

---

## 設定檔位置

可以放在**頂層**(所有 watches 的預設),或**每個 watch 內**(覆蓋頂層)。watch 層的優先。

```yaml
# 頂層預設
on_conflict: duplicate
on_change:   version
on_delete:   keep_dest
hash_policy: metadata_fast_path
duplicate_dir: duplicate
archive_dir:   archive

watches:
  - name: shared
    algorithm: flatten
    source: /src
    dest: /shared
    # 繼承頂層

  - name: critical
    algorithm: flatten
    source: /src
    dest: /critical
    on_conflict: error        # 覆蓋這一個
    on_delete:   archive
```

---

## Fingerprint 與自動失效

每筆 mapping 會記一個 **fingerprint**:

```
<algorithm_name>@<revision>;oc=...|oh=...|od=...|hp=...|dup=...|arc=...
```

只要演算法 `revision` 或任一 policy 欄位變動,fingerprint 就變。ingest 的快速跳過判斷是 `source + hash + fingerprint` 全部一致 —— fingerprint 一變,舊紀錄就不再算「已處理」,下次 scan / `reconcile` 會**重新呼叫 mapper** 並比對 canonical target:

| 狀況 | 動作 |
|---|---|
| 內容變了(hash 不同) | 套 `on_change`(replace / version) |
| 內容沒變、canonical target 也沒變 | 只更新 DB fingerprint,**不 copy** |
| 內容沒變、但 canonical target 變了(revision 改掉路徑邏輯等) | 套 `on_change` —— `replace` 搬到新位置並清舊 dest;`version` 在 duplicate 產生新位置 |

所以切 policy 或 bump revision 都是安全、可預期的:mapper 怎麼說,engine 就怎麼動。

---

## 何時跑 `fferyman reconcile`

`run` 本身的初始 scan 只會看 **source 上存在的檔案**。要處理「source 已經不存在但 DB 還有紀錄」的 mapping(例如:daemon 離線期間檔案被刪掉),需要 `reconcile`:

```bash
fferyman reconcile -c config.yaml
```

內部邏輯:
1. 做一次 `scan_once`(等同 `scan`)
2. 列舉 DB 所有 active mapping,對 source 不存在的那些觸發 `on_delete`

建議時機:
- 改了 policy 或 `revision` 之後
- Daemon 離線維護後重新啟動
- 設定檔被搬動(路徑變了)
- 定期 cron(每天一次)

跟 `scan` 的差別:`scan` 只看 source 上的檔,`reconcile` 還會處理 DB 裡的孤兒。

---

## 原子性保證

- **Copy 一致性**:預設優先用 `rclone copyto` 做 tmp 複製(若系統已安裝 `rclone`),否則 fallback 到 Python 內建 copy;最後仍然用 `os.replace`(檔案)或 tmp 目錄 + `os.rename`(目錄)。crash 中途不會在 dest 留下半檔。
- **寫入順序**:先 copy 成功、再更新 DB。crash 在兩者之間,下次 scan 會偵測到 dest 已有檔但 DB 無紀錄,會根據 `on_conflict` 處理。
- **Target safety**:mapper 若誤回 `dest` 之外或 `src` 本身的路徑,engine 會直接拒絕並 log error,不會執行 copy。
- **`duplicate_dir` / `archive_dir` 安全驗證**:不允許絕對路徑、路徑分隔符、`..` 等會逃出 `dest` 的值;`policy_from_dict` 以及 `Policy(...)` 直接建構時都會擋下(`ValueError`)。engine 在實際使用前還會再 resolve 一次確認。
- **失敗行為**:copy / rename 失敗會 log error,DB 不寫入,下次事件/scan 會重試(沒有自動 backoff;短時間內重複失敗會刷 log)。

---

## 目前**沒有**做的

實作上還是 MVP/骨架,以下屬於需求真的碰到才加:

- 事件佇列 / retry worker(目前失敗就 log,沒有指數 backoff / dead letter)
- 跨 watch 的原子性(兩個 watch 寫同一 dest 會互相覆蓋,engine 不會偵測)
- 多機 / 分散式 lock(同時兩個 daemon 吃同一 DB 會 race)
- Content diff(內容變更只看 hash 全部重 copy,沒有 rsync-style delta)
- SQLite 的 WAL rotation / vacuum(交給外部排程)

這些會在需要時以獨立 RFC 加入,不會偷渡進 mapper API。

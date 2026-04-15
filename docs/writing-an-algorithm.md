# 撰寫自己的演算法

一個 fferyman 演算法 = **一個函式**,回答一件事:「這個 source 該去哪個 *canonical* dest?」

就這樣。不用繼承、不用寫 event hook、不碰 DB、不做撞名處理、不管刪除 —— 所有生命週期和一致性問題都在框架 policy 層(見 [policy.md](policy.md))。

> 還不清楚框架全貌的話,先看 [../README.md](../README.md) 跟 [policy.md](policy.md)。

---

## 最小範例

```python
# plugins/my_algo.py
from fferyman import algorithm

@algorithm("flatten", revision=1)
def flatten(src, dest, **_):
    return dest / src.name
```

就這 3 行。撞名、版本、刪除都由設定檔的 policy 欄位決定,你不需要改演算法。

設定檔:
```yaml
watches:
  - name: demo
    algorithm: flatten
    source: /src
    dest:   /dst
    on_conflict: duplicate
    on_change:   version
    on_delete:   keep_dest
```

完成。

---

## 函式 signature

```python
@algorithm(name: str, *, watch_mode: str = "file", revision: int = 1)
def mapper(src: Path, dest: Path, **params) -> Path | None:
    ...
```

| 參數 | 意義 |
|---|---|
| `src` | 來源單位路徑(檔案或目錄,取決於 `watch_mode`) |
| `dest` | 設定檔宣告的目的地**根**目錄 |
| `**params` | YAML `params:` 區塊內容,以 kwargs 傳入;框架另外會傳 `hash_` |
| 回傳 | 目的地路徑(必須在 `dest` 底下),或 `None` 表示跳過 |

`hash_` 是框架算好的真實內容 hash。即使 watch 設成 `hash_policy=copy_then_hash`,engine 也會先把第一次同步的 regular file 複製到 staging 區、算完 hash 後才呼叫 mapper,所以 hash-aware mapper 不需要自己處理 `hash_=None`。

### `revision`

當你**改變 mapper 的目標選擇邏輯**(改路徑格式、改過濾條件等),把 `revision` +1。框架會把 revision 納入 fingerprint,舊紀錄自動失效,下次 scan / `reconcile` 會重新呼叫 mapper:

- 如果新 canonical target 跟舊的**一樣**,就只更新 DB fingerprint,不重複 copy。
- 如果新 canonical target **不同**,會依 `on_change` policy 處理 —— `replace` 會把檔案搬到新位置、清掉舊 dest;`version` 會保留舊 dest、把新位置當新版本。

所以:只改 `revision` 但 mapper 行為沒變 = 廉價 fingerprint 刷新;真的改了 target 計算 = 檔案會真的移動。

---

## `watch_mode`

| 值 | 顆粒度 | 函式拿到的 `src` | 子事件冒泡 | 自動 debounce |
|---|---|---|---|---|
| `"file"`(預設) | 每個**檔案** | 一定是檔案路徑 | — | 否 |
| `"dir:N"` | source 底下第 N 層的目錄 | 該目錄路徑 | ✓(第 N 層祖先) | ✓ |
| `"regex:<pat>"` | 任意深度、name 符合 regex 的目錄 | 該目錄路徑 | ✓(最近的匹配祖先) | ✓ |
| `"glob:<pat>"` | 符合 glob 的 path | 符合的 path | ✗ | 否 |
| `"custom"` | 全部事件都送 | 原 path(自己判斷) | ✗ | 否 |

**子事件冒泡** = 當單位目錄裡的子檔案變動,engine 會自動把事件對應回單位本身(重新 hash 整包)。  
**自動 debounce** = 多個事件在 `debounce_seconds`(預設 0.5 秒)內會被合併成一次 ingest,適合目錄內容分批寫入的情境。

### 什麼時候用哪個

- 要監聽**固定深度**的目錄 → `dir:N`
- 要監聽**任意深度、名字符合某格式**的目錄(例如 `XXXX-YYYYMMDD-HHMMSS`)→ `regex:<pat>`
- 要過濾個別檔案 → `glob:<pat>` 或 `file` + mapper 內判斷
- 極端 case、框架語意都不對 → `custom`(自己扛)

---

## 框架替你做的事(**不要**自己在 mapper 裡做)

| 框架做的 | 不要 |
|---|---|
| 事件監聽 (CREATED / MODIFIED / DELETED / MOVED) | 在 mapper 裡判斷事件類型 |
| SHA256 / Merkle hash / staged copy-then-hash | `hashlib` |
| 同 source + hash + fingerprint 已處理 → 跳過 | 自己查 DB |
| 撞名處理(依 `on_conflict`) | `target.exists()` 然後放 `duplicate/` |
| 內容變更處理(依 `on_change`) | 自己 mark_deleted 舊的 |
| 來源刪除處理(依 `on_delete`) | 自己 rm dest |
| atomic 複製(tmp + rename) | `shutil.copy2` 直接寫 |
| DB insert / update_fingerprint / mark_deleted | 自己碰 `MappingStore` |
| Target safety(擋 dest 外、擋 src 本身) | —— |
| Log(成功 / 跳過 / 失敗) | `print()` |

**mapper 的唯一職責**:給定 src,回答 canonical target 是哪個(不管目前檔案系統或 DB 狀態)。

---

## 能用的 helper

```python
from fferyman import algorithm, next_available_name
```

- `algorithm(name, *, watch_mode, revision)` — 裝飾器
- `next_available_name(parent, stem, suffix="")` — 多數 mapper **不會用到**,因為撞名已經由 policy 處理。只有在你想自己在 mapper 裡做特殊命名時才需要。

---

## 常見 mapper 範例(全部 1–5 行)

### 保留目錄結構

```python
@algorithm("preserve_tree")
def preserve_tree(src, dest, *, source_root, **_):
    return dest / src.relative_to(source_root)
```

### CAS(內容定址)

```python
@algorithm("cas", revision=1)
def cas(src, dest, *, hash_, **_):
    return dest / hash_[:2] / (hash_ + src.suffix)
```

框架會把算好的 hash 用 `hash_` kwarg 傳進來。

### 白名單過濾

```python
@algorithm("only_txt")
def only_txt(src, dest, **_):
    if src.suffix != ".txt":
        return None
    return dest / src.name
```

### 路徑改寫(`YYYY/MM/DD/file` → `YYYY-MM-DD_file`)

```python
@algorithm("date_flatten")
def date_flatten(src, dest, **_):
    parts = src.parts[-4:]
    if len(parts) < 4:
        return None
    y, m, d, name = parts
    return dest / f"{y}-{m}-{d}_{name}"
```

### 目錄層級過濾

```python
import fnmatch

@algorithm("batches_only", watch_mode="dir:1")
def batches_only(src, dest, *, include="batch_*", **_):
    if not fnmatch.fnmatch(src.name, include):
        return None
    return dest / src.name
```

### 任意深度、名字符合某 pattern 的目錄

用 `regex:` 模式處理「埋在不確定深度的路徑裡、但名字有固定格式」的同步單位:

```python
@algorithm(
    "sendout_flatten",
    watch_mode=r"regex:^[A-Za-z0-9]+-\d{8}-\d{6}$",
    revision=1,
)
def sendout_flatten(src, dest, **_):
    return dest / src.name
```

配合 YAML 的 `debounce_seconds` 可以處理「目錄內容分批寫入」的情況 —— 多個子檔案事件會 coalesce 成一次 ingest,避免抓到半成品。參考完整範例:[../examples/sendout_flatten.py](../examples/sendout_flatten.py)、[../examples/watch.sendout.example.yaml](../examples/watch.sendout.example.yaml)。

---

## 參數(params)

YAML 裡 `params:` 底下的欄位會以 kwargs 傳給函式。用 keyword-only 明宣:

```yaml
params:
  include: "batch_*"
  exclude: "tmp_*"
```

```python
@algorithm("matched", watch_mode="dir:1")
def matched(src, dest, *, include="*", exclude="", **_):
    if not fnmatch.fnmatch(src.name, include): return None
    if exclude and fnmatch.fnmatch(src.name, exclude): return None
    return dest / src.name
```

`**_` 放最後把沒用到的 kwargs 吃掉(包括框架傳的 `hash_`)。

---

## 載入方式

### 放 `plugins_dir`
```yaml
plugins_dir: ./plugins
```
把 `.py` 丟進去,啟動時自動掃描。

### Python entry point(給別人 `pip install`)
```toml
[project.entry-points."fferyman.algorithms"]
my_algo = "mypkg.algo:my_algo"
```

---

## 測試

```python
from pathlib import Path
from fferyman.core.db import Database
from fferyman.core.engine import Watch
from fferyman.core.policy import OnChange, OnConflict, OnDelete, Policy
from fferyman.core.registry import Registry


def test_my_algo(tmp_path):
    src = tmp_path / "src"; src.mkdir()
    dst = tmp_path / "dst"; dst.mkdir()
    (src / "a.txt").write_text("hi")

    reg = Registry()
    reg.load_from_directory(Path("plugins"))
    db = Database(tmp_path / "state.sqlite")

    watch = Watch(
        name="t",
        spec=reg.get("my_algo"),
        source=src, dest=dst,
        params={},
        store=db.scope("t", "my_algo"),
        policy=Policy(on_conflict=OnConflict.DUPLICATE),
    )
    watch.scan_once()

    assert (dst / "a.txt").read_text() == "hi"
    db.close()
```

更多範例看 [../tests/test_policies.py](../tests/test_policies.py) 跟 [../tests/test_flatten_example.py](../tests/test_flatten_example.py)。

---

## Checklist

- [ ] `@algorithm("unique_name", revision=N)` 名稱唯一,`revision` 有意識地管理
- [ ] `watch_mode` 符合單位顆粒度
- [ ] 函式只回傳 `Path` 或 `None`,從不丟例外(真的丟框架會 catch)
- [ ] 不用讀檔、不用 hash、不用查 DB、不用 `target.exists()` —— 框架處理
- [ ] 回傳 path **在 `dest` 底下**(target safety 會擋但多寫一道是好習慣)
- [ ] `**_` 放最後把未知 kwargs 吞掉
- [ ] `fferyman doctor -c config.yaml` 全綠

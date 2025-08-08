from __future__ import annotations

import argparse, html, json, pathlib, pickle, random, re, sys, time, warnings
from typing import List
import hashlib

import faiss
import numpy as np
import pandas as pd
import requests
import tiktoken
from bs4 import BeautifulSoup
from openai import OpenAI, OpenAIError
from tqdm import tqdm

# ─────────────────────────────────────────────────────────────────────────────
# 🔐 CONFIG (填自己的凭据)
# ─────────────────────────────────────────────────────────────────────────────
OPENAI_API_KEY = "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"
XKW_APP_ID     = "101891658823024500"
XKW_SECRET     = "UmUAkOUxxN0nK2nZWzZOOXDU3GuRZoZ4"
XKW_COOKIE     = (
    "xkw-device-id=8A94734F7C3E090D246AE4D0EF4412B6; "
    "SESSION=NDFjNTRjMGMtNDM1NC00N2NlLTg3NDgtYjdiYjY4N2Q2NDlj; "
    "acw_tc=0a47308617544638320526390ea1b008c267fae90b6bad4427858a4125075c; "
    "Hm_lvt_9cd71e5642aa75f2ab449541c4c00473=1754451485;"
)
XKW_GATE_ID    = "0fe0b215bf088ae079501835e52f5477"

EMBED_MODEL = "text-embedding-3-large"
EMBED_DIM   = 3072
BATCH_SIZE  = 64
TOKEN_LIMIT = 8000           # OpenAI 模型 8192 token，留余量
XKW_CHARMAX = 2000           # 学科网接口 text 字符上限
TOP_K       = 5
LOCAL_SEARCH_K = 10          # 本地检索取前 10，评估是否覆盖 XKW 的 5 条
SEARCH_K    = 15             # 检索 15，排除自身后应足够取满 10

# ─────────────────────────────────────────────────────────────────────────────
# INIT
# ─────────────────────────────────────────────────────────────────────────────
io   = OpenAI(api_key=OPENAI_API_KEY)
enc  = tiktoken.encoding_for_model(EMBED_MODEL)
HTML_RE = re.compile(r"<!--.*?-->|<[^>]+>", re.S)
warnings.filterwarnings("ignore", category=pd.errors.DtypeWarning)

# ─────────────────────────────────────────────────────────────────────────────
# 安全原子写工具（避免“半写入”撕裂导致 meta 与 emb/index 不一致）
# ─────────────────────────────────────────────────────────────────────────────
def _atomic_write_bytes(path: pathlib.Path, write_fn):
    tmp = pathlib.Path(str(path) + ".tmp")
    write_fn(tmp)
    tmp.replace(path)

def atomic_write_json(path: pathlib.Path, obj: dict):
    def _w(p): 
        with open(p, "w", encoding="utf-8") as f: json.dump(obj, f, indent=2, ensure_ascii=False)
    _atomic_write_bytes(path, _w)

def atomic_write_npy(path: pathlib.Path, arr: np.ndarray):
    def _w(p):
        # np.save 会在给定路径无 .npy 后缀时自动追加 .npy，为保证原子替换，需用文件句柄写入
        with open(p, 'wb') as f:
            np.save(f, arr)
    _atomic_write_bytes(path, _w)

def atomic_write_faiss(path: pathlib.Path, index: faiss.Index):
    def _w(p): faiss.write_index(index, str(p))
    _atomic_write_bytes(path, _w)

# ─────────────────────────────────────────────────────────────────────────────
# 文本处理
# ─────────────────────────────────────────────────────────────────────────────
def clean_html(raw: str) -> str:
    raw = html.unescape(str(raw or ""))
    # 先 soup 再正则，清得更干净
    try:
        txt = BeautifulSoup(raw, "lxml").get_text(" ")
    except Exception:
        txt = raw
    txt = HTML_RE.sub(" ", txt)
    return re.sub(r"\s+", " ", txt).strip()

def build_text(row: pd.Series, *, only_stem: bool = True) -> str:
    """生成提交 / 索引用文本。"""
    parts = [clean_html(row.get("content", ""))]
    if not only_stem:
        if row.get("type_id") in {1, 2}:
            parts.append(clean_html(row.get("options", "")))
        parts.append(clean_html(row.get("answer", "")))
        parts.append(clean_html(row.get("analysis", "")))
    text = " ".join(p for p in parts if p)
    return text[:XKW_CHARMAX]  # 与 XKW 对齐：字符上限 2000

# ─────────────────────────────────────────────────────────────────────────────
# Embedding helpers（含 L2 归一化）
# ─────────────────────────────────────────────────────────────────────────────
def embed(texts: List[str]) -> np.ndarray:
    vecs: List[List[float]] = []
    for i in range(0, len(texts), BATCH_SIZE):
        for retry in range(5):
            try:
                resp = io.embeddings.create(model=EMBED_MODEL,
                                            input=texts[i:i+BATCH_SIZE],
                                            encoding_format="float")
                vecs.extend([d.embedding for d in resp.data])
                break
            except OpenAIError as e:
                wait = 2 ** retry + random.random()
                print(f"⚠️  OpenAI error {e}; retry {wait:.1f}s")
                time.sleep(wait)
        else:
            raise RuntimeError("OpenAI embedding failed after retries")
    arr = np.asarray(vecs, dtype="float32")
    faiss.normalize_L2(arr)
    return arr

# ─────────────────────────────────────────────────────────────────────────────
# 学科网接口
# ─────────────────────────────────────────────────────────────────────────────
XKW_URL = "https://open.xkw.com/doc/api/xopqbm/questions/similar-recommend"

def call_xkw(text: str, k: int = 5, *, type_ids: list[int] | None = None) -> list[dict]:
    text = text[:XKW_CHARMAX].strip()
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Origin": "https://open.xkw.com",
        "Referer": "https://open.xkw.com/",
        "User-Agent": "Mozilla/5.0",
        "Request-Origion": "Knife4j",
        "knfie4j-gateway-request": XKW_GATE_ID,
        "Xop-App-Id": XKW_APP_ID,
        "secret": XKW_SECRET,
        "Cookie": XKW_COOKIE,
    }
    payload = {"text": text, "count": k}
    if type_ids:
        # 学科网支持按题型ID筛选；与本地的 type_id 对齐
        payload["type_ids"] = type_ids
    r = requests.post(XKW_URL, headers=headers, json=payload, timeout=15)
    r.raise_for_status()
    body = r.json()
    if body.get("code") != 2000000:
        # 若服务端不接受 type_ids，则回退不带过滤再试一次
        if type_ids:
            try:
                payload.pop("type_ids", None)
                r2 = requests.post(XKW_URL, headers=headers, json=payload, timeout=15)
                r2.raise_for_status()
                body2 = r2.json()
                if body2.get("code") == 2000000:
                    print("ℹ️  XKW 不接受 type_ids，已回退为不筛选模式")
                    return body2.get("data", [])[:k]
            except Exception:
                pass
        raise RuntimeError(body)
    return body.get("data", [])[:k]

def rows_from_xkw(resp: list[dict]) -> list[dict]:
    rows = []
    for d in resp:
        rows.append({
            "question_id": int(d["id"]),
            "type_id": d.get("type_id", 1),
            "content": clean_html(d.get("stem", "")),
            "options": clean_html(d.get("options", "")),
            "answer":  clean_html(d.get("answer", "")),
            "analysis":clean_html(d.get("analysis", "")),
        })
    return rows

# ─────────────────────────────────────────────────────────────────────────────
# CSV loader (auto header + options 列粗识别)
# ─────────────────────────────────────────────────────────────────────────────
def load_csv(path: pathlib.Path) -> pd.DataFrame:
    """Robust loader for both legacy CSV and new Doris-exported TSV.
    - Auto-detect delimiter (tab vs comma)
    - If header missing or not parsed, apply schema column names
    - Guarantee presence of key columns: question_id, content, options, answer, analysis
    """
    # Full schema from new table (order matters)
    schema_cols = [
        "question_id","type_id","region_id","section_id","grade_id","subject_id","term_id","version_id",
        "difficulty_id","tag_id","content","options","answer","answer_list","analysis","audio_url",
        "create_by","created_date","update_by","updated_date","source_id","org_path","is_delete","save_type",
        "school_id","visible","question_archive"
    ]

    # Peek first kb to guess delimiter
    with open(path, 'r', encoding='utf-8', errors='ignore') as f:
        head = f.read(2048)
    sep = '\t' if ('\t' in head and head.count('\t') >= head.count(',')) else ','

    # First try: with header
    try:
        df = pd.read_csv(path, header=0, sep=sep, on_bad_lines='skip', engine='python', quoting=3)
    except Exception:
        df = pd.read_csv(path, header=None, sep=sep, on_bad_lines='skip', engine='python', quoting=3)

    # If header didn't parse to include question_id, try without header and assign schema
    if "question_id" not in df.columns:
        try:
            df2 = pd.read_csv(path, header=None, sep=sep, on_bad_lines='skip', engine='python', quoting=3)
            # Assign schema names up to available columns
            cols = schema_cols[:df2.shape[1]]
            df2.columns = cols
            df = df2
        except Exception:
            # Fallback: try minimal mapping by position
            df.columns = [str(c) for c in df.columns]
            colmap_min = {0: "question_id", 3: "type_id", 10: "content"}
            df.rename(columns={i: n for i, n in colmap_min.items() if i in df.columns}, inplace=True)

    # Ensure key columns exist
    for col in ["question_id","content","options","answer","analysis"]:
        if col not in df.columns:
            df[col] = ""

    # Normalize types to strings for text fields to avoid NaN later
    for col in ["question_id","content","options","answer","analysis"]:
        try:
            df[col] = df[col].astype(str)
        except Exception:
            pass

    return df

# ─────────────────────────────────────────────────────────────────────────────
# Stable int64 ID for Faiss from question_id (VARCHAR-safe)
# ─────────────────────────────────────────────────────────────────────────────
def to_int64_id(qid_str: str) -> int:
    s = str(qid_str)
    if s.isdigit() and len(s) <= 18:
        try:
            return int(s)
        except Exception:
            pass
    h = hashlib.sha1(s.encode("utf-8")).digest()[:8]
    val = int.from_bytes(h, byteorder="big", signed=False)
    # 保证不为负，落入 int64 正区间
    val &= (1 << 63) - 1
    # 避免返回 0
    return val or 1

# ─────────────────────────────────────────────────────────────────────────────
# Pipeline
# ─────────────────────────────────────────────────────────────────────────────
def pipeline(dataset: pathlib.Path, save_enriched: bool = True):
    print(f"📥  Loading dataset: {dataset}")
    df = load_csv(dataset)
    original_count = len(df)
    print("题目数:", original_count)

    # 1️⃣ 随机抽题（保证 query_text 非空；优先 content 有效）
    rng_seed = int(time.time()) % 2**16
    candidates = df.copy()
    # content 有效优先
    try:
        mask = candidates['content'].astype(str).str.strip().str.len() > 5
        if mask.any():
            candidates = candidates[mask]
    except Exception:
        pass
    # 多次尝试获取非空文本
    sample_row = None
    query_text = ""
    for _ in range(20):
        row = candidates.sample(1, random_state=rng_seed).iloc[0]
        txt = build_text(row, only_stem=True)
        if not txt:
            txt = build_text(row, only_stem=False)
        if txt:
            sample_row = row
            query_text = txt
            break
        rng_seed += 1
    if sample_row is None:
        # 兜底：取第一行并尽量构造文本
        sample_row = df.iloc[0]
        query_text = build_text(sample_row, only_stem=False)
    src_questionid = sample_row["question_id"]
    print("🎲  Sampled", src_questionid)

    # 2️⃣ 调学科网（携带本地题型作为 hint）
    sample_type_id = None
    try:
        sample_type_id = int(sample_row.get('type_id')) if pd.notna(sample_row.get('type_id')) else None
    except Exception:
        sample_type_id = None
    type_ids_hint = [int(sample_type_id)] if sample_type_id is not None else None
    xkw_raw = call_xkw(query_text, TOP_K, type_ids=type_ids_hint)
    xkw_question_ids = [int(d["id"]) for d in xkw_raw]
    print("🔗  XKW Question IDs:", xkw_question_ids)

    # 3️⃣ 合并新题到数据集
    exist_set = set(df["question_id"].astype(str))
    new_rows  = [r for r in rows_from_xkw(xkw_raw) if str(r["question_id"]) not in exist_set]
    if new_rows:
        df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
        print(f"➕  Added {len(new_rows)} real questions → {len(df)} total")
        if save_enriched:
            base_stem = dataset.stem.replace("_enriched", "")
            target_path = dataset.parent / f"{base_stem}_enriched.csv"
            df.to_csv(target_path, index=False, encoding='utf-8')
            print(f"💾  Enriched dataset saved to: {target_path}")
            print(f"📊  Dataset growth: {original_count} → {len(df)} (+{len(new_rows)} questions)")
    else:
        print("ℹ️  No new questions to add (all exist in dataset)")

    # 4️⃣ 增量嵌入（question_id 作为 vectorID）
    emb_file  = pathlib.Path("ultimate_model_emb.npy")
    meta_file = pathlib.Path("ultimate_model_meta.json")
    index_file= pathlib.Path("ultimate_model.faiss")

    # 新建索引
    index: faiss.IndexIDMap = faiss.IndexIDMap(faiss.IndexFlatIP(EMBED_DIM))

    meta: dict | None = None
    meta_updated = False

    if emb_file.exists() and meta_file.exists() and index_file.exists():
        # 载入现有模型
        print("🔄  Loading existing model...")
        emb = np.load(emb_file)
        with open(meta_file, 'r', encoding='utf-8') as f:
            meta = json.load(f)
        index = faiss.read_index(str(index_file))

        existing_question_ids = list(meta.get('question_ids', []))  # list 保序
        existing_set = set(existing_question_ids)
        # 使用去重视图，避免同一 question_id 多行导致增量错位
        df_unique = df.drop_duplicates(subset=['question_id'], keep='first')
        current_ids  = df_unique['question_id'].astype(str).tolist()
        current_set  = set(current_ids)

        print(f"📊  Existing embeddings: {len(existing_question_ids)} questions")
        print(f"📊  Current dataset: {len(current_set)} questions")

        # 加载期一致性校验（即使没有增量也要检查）
        try:
            if len(existing_question_ids) != emb.shape[0]:
                raise RuntimeError(
                    f"meta/emb length mismatch: meta={len(existing_question_ids)}, emb={emb.shape[0]}"
                )
            if index.ntotal != emb.shape[0]:
                print("⚠️  Index count mismatch; rebuilding Faiss index from meta/emb …")
                rebuilt = faiss.IndexIDMap(faiss.IndexFlatIP(EMBED_DIM))
                id_arr = np.asarray([to_int64_id(q) for q in existing_question_ids], dtype='int64')
                rebuilt.add_with_ids(emb, id_arr)
                index = rebuilt
                # 原子落盘修复
                atomic_write_faiss(index_file, index)
        except Exception as e:
            print(f"❌  Consistency check failed: {e}")
            print("请删除 ultimate_model_* 文件后重建，或手动修复 meta/emb/index 一致性。")
            raise

        # 找出需要新增嵌入的题目（保持 df 顺序且去重）
        seen = set()
        missing_ids = []
        for qid in current_ids:
            if (qid not in existing_set) and (qid not in seen):
                missing_ids.append(qid)
                seen.add(qid)

        if missing_ids:
            print(f"🧠  Found {len(missing_ids)} NEW questions, generating embeddings...")

            # 以 missing_ids 的顺序重建缺失行，确保 new_emb ↔ append_ids 顺序一致
            df_map = df_unique.set_index(df_unique['question_id'].astype(str), drop=False)
            missing_rows = df_map.loc[missing_ids]

            new_texts = [build_text(row, only_stem=True) for _, row in missing_rows.iterrows()]
            new_emb   = embed(new_texts)

            # 断言数量一致（严格一一对应）
            assert new_emb.shape[0] == len(missing_ids), \
                f"new_emb({new_emb.shape[0]}) vs missing_ids({len(missing_ids)}) size mismatch"
            new_qids_arr = np.asarray([to_int64_id(q) for q in missing_rows['question_id'].astype(str).tolist()], dtype='int64')

            # 增量加到索引
            index.add_with_ids(new_emb, new_qids_arr)

            # 叠加到 emb（尾部），并按相同顺序 append 到 meta
            emb = np.vstack([emb, new_emb])

            meta_ids = existing_question_ids[:]  # old order
            meta_ids.extend(missing_ids)         # append in the SAME order
            meta = {
                'question_ids': meta_ids,
                'total_questions': len(meta_ids),
                'embedding_shape': emb.shape,
                'timestamp': time.time()
            }

            # 写盘前做强一致断言
            assert len(meta['question_ids']) == emb.shape[0] == index.ntotal, \
                f"inconsistent sizes after update: meta={len(meta['question_ids'])}, emb={emb.shape[0]}, faiss={index.ntotal}"

            # 原子落盘：先 emb / index / meta，三者用 .tmp → rename，避免撕裂
            atomic_write_npy(emb_file, emb)
            atomic_write_faiss(index_file, index)
            atomic_write_json(meta_file, meta)
            meta_updated = True

            print(f"✅  Incremental update: {len(existing_set)} + {len(missing_ids)} = {index.ntotal}")

        else:
            print("ℹ️  No new questions found, using existing model")
            # emb/index/meta 已载入
    else:
        # 首次运行：全量嵌入
        print("🧠  First run - generating embeddings for ALL questions...")
        emb = embed([build_text(r, only_stem=True) for _, r in df.iterrows()])
        qids = np.asarray([to_int64_id(q) for q in df['question_id'].astype(str).tolist()], dtype='int64')
        index.add_with_ids(emb, qids)

        meta = {
            'question_ids': [str(qid) for qid in df['question_id'].tolist()],
            'total_questions': emb.shape[0],
            'embedding_shape': emb.shape,
            'timestamp': time.time()
        }

        assert len(meta['question_ids']) == emb.shape[0] == index.ntotal, \
            f"inconsistent sizes at first run: meta={len(meta['question_ids'])}, emb={emb.shape[0]}, faiss={index.ntotal}"

        atomic_write_npy(emb_file, emb)
        atomic_write_faiss(index_file, index)
        atomic_write_json(meta_file, meta)
        meta_updated = True

        print(f"✅  Generated {emb.shape[0]} embeddings")

    print(f"🔍  Faiss index ready with {index.ntotal} questions")

    # 5️⃣ 本地检索并对齐评估
    print("🔍  Testing model with original query...")
    try:
        qvec = None
        if meta_file.exists():
            with open(meta_file, 'r', encoding='utf-8') as f:
                meta_now = json.load(f)
            if str(src_questionid) in meta_now['question_ids']:
                orig_idx = meta_now['question_ids'].index(str(src_questionid))
                # 切片 + 再做一次归一化（保险）
                qvec = emb[orig_idx:orig_idx+1].copy()
                faiss.normalize_L2(qvec)
        if qvec is None:
            # fallback：现算查询向量（调试阶段不在乎 token）
            qvec = embed([build_text(sample_row, only_stem=True)])
    except Exception as e:
        print(f"⚠️  Fallback to re-embedding query due to error: {e}")
        qvec = embed([build_text(sample_row, only_stem=True)])

    D, I = index.search(qvec, SEARCH_K)
    # 构建 faissID(int64) -> 原始 question_id(str) 的映射
    id64_to_qid = {}
    for q in df['question_id'].astype(str).tolist():
        id64_to_qid[to_int64_id(q)] = q
    src_id64 = to_int64_id(str(src_questionid))

    local_question_ids = []
    for qid in I[0]:
        if qid == -1 or int(qid) == src_id64:  # 排除自身
            continue
        local_question_ids.append(id64_to_qid.get(int(qid), str(qid)))
        if len(local_question_ids) == LOCAL_SEARCH_K:
            break
    print(f"🧠  Local Top {LOCAL_SEARCH_K} Question IDs:", local_question_ids)

    # 6️⃣ 对比验证（XKW Top-5 是否包含于本地 Top-10）
    xkw_question_ids_str = list(map(str, xkw_question_ids))
    xkw_set = set(xkw_question_ids_str)
    local_set = set(local_question_ids)

    matched_count = len(xkw_set & local_set)
    total_xkw = len(xkw_set)

    print(f"📊  匹配统计: {matched_count}/{total_xkw} 条学科网题目在本地 Top{LOCAL_SEARCH_K} 中")
    if matched_count == total_xkw:
        print("✅  完全召回 – 学科网所有题目都在本地 Top10 中，算法有效！")
        validation_success = True
    elif total_xkw and (matched_count >= total_xkw * 0.8):
        print(f"🟡  部分召回 – {matched_count}/{total_xkw}，召回率 {matched_count/total_xkw:.1%}")
        validation_success = True
    else:
        print(f"❌  召回不足 – {matched_count}/{total_xkw}，召回率 {matched_count/total_xkw:.1%}" if total_xkw else "❌  XKW 无返回")
        validation_success = False

    if matched_count < total_xkw:
        missing_ids = xkw_set - local_set
        print(f"   未召回的学科网ID: {list(missing_ids)}")
        overlapping_ids = xkw_set & local_set
        if overlapping_ids:
            print(f"   已召回的学科网ID: {list(overlapping_ids)}")

    # 7️⃣ 另存一份（pkl/feather/emb/faiss），但 **不重复写 meta**，避免覆盖步骤 4 的一致性
    if new_rows or not pathlib.Path("ultimate_model.pkl").exists():
        print("💾  Saving model bundle (pkl/feather/emb/faiss)...")

        model_data = {
            'df': df,
            'embeddings': emb,
            'index': index,
            'metadata': {
                'total_questions': len(df),
                'new_questions_added': len(new_rows) if new_rows else 0,
                'training_timestamp': time.time(),
                'base_dataset': str(dataset)
            }
        }
        with open("ultimate_model.pkl", "wb") as f:
            pickle.dump(model_data, f)

        # feather（若无 pyarrow 可改为 try/except）
        try:
            df_clean = df.copy()
            df_clean['question_id'] = df_clean['question_id'].astype(str)
            for col in df_clean.columns:
                if df_clean[col].dtype == 'object':
                    df_clean[col] = df_clean[col].astype(str)
            df_clean.to_feather("ultimate_model.feather")
        except Exception as e:
            print(f"ℹ️  Skip feather export: {e}")

        # 再落一次 emb/index（与 4 步保持一致版本）
        atomic_write_npy(emb_file, emb)
        atomic_write_faiss(index_file, index)

        # meta 本轮如已在步骤 4 更新过，就不再覆盖；若首次构建已写，也不必再写
        print("✅  Bundle saved. (meta already managed in step 4)")

    else:
        print("ℹ️  No new questions added, skipping bundle save")

    return {
        'original_count': original_count,
        'final_count': len(df),
        'new_questions': len(new_rows) if new_rows else 0,
        'validation_success': validation_success,
        'recall_rate': matched_count / total_xkw if total_xkw > 0 else 0.0,
        'matched_count': matched_count,
        'total_xkw_count': total_xkw,
        'matched_question_ids': list(xkw_set & local_set),
        'unmatched_question_ids': list(xkw_set - local_set)
    }

# ---------------------------------------------------------------------------
if __name__ == "__main__":
    p = argparse.ArgumentParser(description="ZXXK validation with dataset enrichment (C-fix hardened)")
    p.add_argument("csv", help="path to dataset CSV")
    p.add_argument("--no-save", action="store_true", help="skip saving enriched dataset")
    args = p.parse_args()
    pipeline(pathlib.Path(args.csv), save_enriched=not args.no_save)

#!/usr/bin/env python3
"""
Question Similarity Recommender (re‑worked)
=========================================
* Cleans HTML & entities
* Truncates long texts to 7.5 k tokens (≈ text‑embedding‑3‑large limit)
* Generates embeddings with exponential back‑off on rate‑limit errors
* L2‑normalises embeddings → inner‑product ≡ cosine similarity
* Uses **question_id** (not dataframe row) as Faiss vector ID
* Persists to three separate files (.feather / .npy / .faiss) for memory efficiency
* Compatible with Python ≥3.9

Install deps (one‑off):
    pip install pandas numpy tqdm faiss-cpu openai tiktoken beautifulsoup4 lxml

Run:
    export OPENAI_API_KEY="sk‑…"
    python question_similarity_recommender.py /path/to/questions.csv  # trains & demo
"""

from __future__ import annotations
import os, re, html, json, time, random, sys, warnings, pickle, pathlib
from typing import List, Dict, Optional

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm
import faiss
from openai import OpenAI, OpenAIError

try:
    import tiktoken
    _enc = tiktoken.encoding_for_model("text-embedding-3-large")
except ImportError:
    _enc = None
    warnings.warn("tiktoken not installed – token count truncation disabled", RuntimeWarning)

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _num_tokens(text: str) -> int:
    if _enc is None:
        return len(text.split())  # rough fallback
    return len(_enc.encode(text))

TOKEN_LIMIT = 8000  # hard limit 8192 – leave a buffer
EMBED_DIM   = 3072  # text‑embedding‑3‑large dimension

# ---------------------------------------------------------------------------
class QuestionSimilarityRecommender:
    """Semantic similarity recommender for exam questions."""

    def __init__(self, csv_path: str, api_key: Optional[str] = None):
        self.csv_path = pathlib.Path(csv_path)
        self.api_key  = api_key or os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise RuntimeError("OpenAI API key not provided. Set OPENAI_API_KEY env or pass api_key param.")
        self.client    = OpenAI(api_key=self.api_key)
        self.df: pd.DataFrame = pd.DataFrame()
        self.emb: np.ndarray  = np.empty((0, EMBED_DIM), dtype="float32")
        self.index: faiss.IndexIDMap | None = None

    # ---------------------------------------------------------------------
    # Data handling
    # ---------------------------------------------------------------------
    def _clean_html(self, text: str) -> str:
        text = html.unescape(str(text))
        text = re.sub(r"<!--.*?-->", " ", text, flags=re.S)
        soup = BeautifulSoup(text, "lxml")
        text = soup.get_text(" ")
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    def _make_text(self, row: pd.Series) -> str:
        parts = [self._clean_html(row.get("content", ""))]
        # only choice questions - check for various choice question type IDs
        type_id = row.get("type_id")
        if pd.notna(type_id) and (int(type_id) in {1, 2} or str(type_id).startswith('10')):
            options = row.get("options", "")
            if options:
                parts.append(self._clean_html(options))
        text = " ".join(p for p in parts if p)
        if _enc and _num_tokens(text) > TOKEN_LIMIT:
            cut = _enc.decode(_enc.encode(text)[:7500])
            text = cut + " [TRUNCATED]"
        return text

    def load_data(self) -> None:
        print("📊  Loading CSV data …")
        # 检查文件是否有header
        sample = pd.read_csv(self.csv_path, nrows=1)
        if 'question_id' in sample.columns:
            # 有header的CSV文件
            self.df = pd.read_csv(self.csv_path)
        else:
            # 没有header的CSV文件
            self.df = pd.read_csv(self.csv_path, header=None, engine="python", on_bad_lines="skip", quoting=3)

        # 如果没有header，需要重命名列
        if 'question_id' not in self.df.columns:
            colmap = {
                0: "question_id", 1: "subject_id", 3: "type_id", 5: "grade_id", 8: "difficulty_id",
                10: "content", 12: "answer", 13: "answer_list", 14: "analysis"
            }
            # rename columns if present
            self.df.rename(columns={i: name for i, name in colmap.items() if i < len(self.df.columns)}, inplace=True)
        # attempt to find options column (contains 'A.' etc.)
        for col in self.df.columns[::-1]:
            sample = str(self.df[col].iloc[0])
            if re.search(r"[A-D][\.、]", sample) or "①" in sample:
                self.df.rename(columns={col: "options"}, inplace=True)
                break
        print(f"✅  Loaded {len(self.df):,} rows – fields: {list(self.df.columns)[:15]}…")

    # ---------------------------------------------------------------------
    # Embeddings with back‑off
    # ---------------------------------------------------------------------
    def _embed_batch(self, texts: List[str]) -> List[List[float]]:
        for attempt in range(6):
            try:
                resp = self.client.embeddings.create(model="text-embedding-3-large", input=texts, encoding_format="float")
                return [x.embedding for x in resp.data]
            except OpenAIError as e:
                wait = 2 ** attempt + random.random()
                print(f"⚠️  OpenAI error ({e}); retry in {wait:.1f}s …")
                time.sleep(wait)
        raise RuntimeError("Failed to get embeddings after retries")

    def build_embeddings(self, batch_size: int = 64) -> None:
        texts, keep_rows = [], []
        for idx, row in self.df.iterrows():
            t = self._make_text(row)
            if t:
                texts.append(t)
                keep_rows.append(idx)
        self.df = self.df.loc[keep_rows].reset_index(drop=True)

        print(f"🧠  Generating embeddings for {len(texts):,} texts …")
        embs: List[List[float]] = []
        for i in tqdm(range(0, len(texts), batch_size)):
            embs.extend(self._embed_batch(texts[i:i + batch_size]))
        self.emb = np.asarray(embs, dtype="float32")
        faiss.normalize_L2(self.emb)
        print("✅  Embeddings shape:", self.emb.shape)

    # ---------------------------------------------------------------------
    # Faiss index
    # ---------------------------------------------------------------------
    def build_index(self) -> None:
        question_id_values = self.df["question_id"].fillna(-1).astype("int64").values
        index_flat = faiss.IndexFlatIP(EMBED_DIM)
        self.index = faiss.IndexIDMap(index_flat)
        self.index.add_with_ids(self.emb, question_id_values)
        print(f"🔍  Faiss index built – {self.index.ntotal:,} vectors")

    # ---------------------------------------------------------------------
    # Persistence
    # ---------------------------------------------------------------------
    def save(self, prefix: str = "question_sim") -> None:
        print("💾  Saving model files …")
        self.df.to_feather(f"{prefix}.feather")
        np.save(f"{prefix}_emb.npy", self.emb)
        faiss.write_index(self.index, f"{prefix}.faiss")
        print("✅  Saved:", f"{prefix}.feather", f"{prefix}_emb.npy", f"{prefix}.faiss")

    def load(self, prefix: str = "question_sim") -> None:
        print("📂  Loading model files …")
        self.df  = pd.read_feather(f"{prefix}.feather")
        self.emb = np.load(f"{prefix}_emb.npy")
        self.index = faiss.read_index(f"{prefix}.faiss")
        print("✅  Model loaded –", self.df.shape, self.emb.shape)

    # ---------------------------------------------------------------------
    # Query
    # ---------------------------------------------------------------------
    def _embed_query(self, text: str) -> np.ndarray:
        vec = np.asarray(self._embed_batch([text])[0], dtype="float32")
        faiss.normalize_L2(vec.reshape(1, -1))
        return vec

    def search(self, *, query_text: str | None = None, query_id: int | None = None,
               top_k: int = 10, filters: Optional[Dict[str, int]] = None) -> List[Dict]:
        if query_text is None and query_id is None:
            raise ValueError("Provide query_text or query_id")
        if query_text:
            qvec = self._embed_query(query_text).reshape(1, -1)
        else:
            row = self.df[self.df["question_id"] == query_id]
            if row.empty:
                raise ValueError("query_id not found")
            idx = row.index[0]
            qvec = self.emb[idx:idx + 1]
        D, I = self.index.search(qvec, top_k * 3)
        out = []
        for score, qid in zip(D[0], I[0]):
            if qid == -1 or (query_id is not None and qid == query_id):
                continue
            row = self.df[self.df["question_id"] == qid].iloc[0]
            if filters and any(row.get(k) != v for k, v in filters.items() if k in row):
                continue
            out.append({
                "question_id": int(qid),
                "similarity": float(score),
                "content": row.get("content", "")[:120],
                "type_id": row.get("type_id"),
                "grade_id": row.get("grade_id"),
                "subject_id": row.get("subject_id"),
            })
            if len(out) >= top_k:
                break
        return out

    # ---------------------------------------------------------------------
    # Incremental update
    # ---------------------------------------------------------------------
    def add_questions(self, new_questions: List[Dict], update_files: bool = True) -> int:
        """
        增量添加新题目到现有模型
        
        Parameters:
        -----------
        new_questions : List[Dict]
            新题目列表，每个dict包含question_id, content, options等字段
        update_files : bool, default True
            是否更新保存的模型文件
            
        Returns:
        --------
        int : 实际添加的题目数量
        """
        if not new_questions:
            return 0
            
        print(f"📥  Processing {len(new_questions)} new questions...")
        
        # 1. 去重：检查是否已存在
        existing_question_ids = set(self.df["question_id"].astype(str))
        unique_questions = [
            q for q in new_questions 
            if str(q.get("question_id", "")) not in existing_question_ids
        ]
        
        if not unique_questions:
            print("ℹ️  All questions already exist in dataset")
            return 0
            
        print(f"➕  Adding {len(unique_questions)} unique questions (filtered {len(new_questions) - len(unique_questions)} duplicates)")
        
        # 2. 转换为DataFrame并拼接
        new_df = pd.DataFrame(unique_questions)
        original_len = len(self.df)
        self.df = pd.concat([self.df, new_df], ignore_index=True)
        
        # 3. 为新题目生成嵌入
        new_texts = []
        for idx in range(original_len, len(self.df)):
            row = self.df.iloc[idx]
            text = self._make_text(row)
            if text:
                new_texts.append(text)
            else:
                # 如果文本为空，从DataFrame中移除这行
                self.df = self.df.drop(idx).reset_index(drop=True)
                
        if not new_texts:
            print("⚠️  No valid text found in new questions")
            return 0
            
        print(f"🧠  Generating embeddings for {len(new_texts)} new texts...")
        new_embs = []
        for i in tqdm(range(0, len(new_texts), 64)):
            new_embs.extend(self._embed_batch(new_texts[i:i + 64]))
        
        new_emb_array = np.asarray(new_embs, dtype="float32")
        faiss.normalize_L2(new_emb_array)
        
        # 4. 更新嵌入矩阵
        self.emb = np.vstack([self.emb, new_emb_array])
        
        # 5. 重建索引（包含新的向量）
        print("🔄  Rebuilding Faiss index with new vectors...")
        question_id_values = self.df["question_id"].fillna(-1).astype("int64").values
        index_flat = faiss.IndexFlatIP(EMBED_DIM)
        self.index = faiss.IndexIDMap(index_flat)
        self.index.add_with_ids(self.emb, question_id_values)
        
        # 6. 可选：更新保存的文件
        if update_files:
            self.save()
            
        added_count = len(unique_questions)
        print(f"✅  Successfully added {added_count} questions. Total: {len(self.df)}")
        return added_count

    def update_from_csv(self, csv_path: str, update_files: bool = True) -> int:
        """
        从CSV文件增量更新模型
        
        Parameters:
        -----------
        csv_path : str
            新数据的CSV文件路径
        update_files : bool, default True
            是否更新保存的模型文件
            
        Returns:
        --------
        int : 实际添加的题目数量
        """
        print(f"📂  Loading new data from {csv_path}")
        
        # 检查文件是否有header
        sample = pd.read_csv(csv_path, nrows=1)
        if 'question_id' in sample.columns:
            new_df = pd.read_csv(csv_path)
        else:
            new_df = pd.read_csv(csv_path, header=None, engine="python", on_bad_lines="skip", quoting=3)
            # 重命名列
            colmap = {
                0: "question_id", 1: "subject_id", 3: "type_id", 5: "grade_id", 8: "difficulty_id",
                10: "content", 12: "answer", 13: "answer_list", 14: "analysis"
            }
            new_df.rename(columns={i: name for i, name in colmap.items() if i < len(new_df.columns)}, inplace=True)
            
            # 尝试识别options列
            for col in new_df.columns[::-1]:
                sample = str(new_df[col].iloc[0])
                if re.search(r"[A-D][\.、]", sample) or "①" in sample:
                    new_df.rename(columns={col: "options"}, inplace=True)
                    break
        
        # 转换为字典列表
        new_questions = new_df.to_dict('records')
        return self.add_questions(new_questions, update_files)

    # ---------------------------------------------------------------------
    # End‑to‑end train
    # ---------------------------------------------------------------------
    def train(self):
        self.load_data()
        self.build_embeddings()
        self.build_index()

# ---------------------------------------------------------------------------
# CLI entry
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python question_similarity_recommender.py questions.csv [--demo]")
        sys.exit(1)

    csv_file = sys.argv[1]
    demo     = "--demo" in sys.argv

    rec = QuestionSimilarityRecommender(csv_file)
    rec.train()
    rec.save()

    if demo:
        rnd_id = int(rec.df["question_id"].sample(1).iloc[0])
        print("\nQuery ID:", rnd_id)
        similar = rec.search(query_id=rnd_id, top_k=5)
        print(json.dumps(similar, ensure_ascii=False, indent=2))

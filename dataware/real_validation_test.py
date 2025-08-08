#!/usr/bin/env python3
"""
真正的学科网验证测试
用新模型查原题目，看能否找到学科网的5个题目
"""

import pandas as pd
import pickle
import numpy as np
import faiss
from train_new_model import NewModelTrainer

def main():
    print("🧪 真正的学科网验证测试")
    print("="*60)
    
    # 1. 加载扩展数据集
    extended_df = pd.read_csv('/Users/zhuidexiaopengyou/Downloads/dataware_test/zxxk_extended_dataset.csv')
    print(f"📊 扩展数据集: {len(extended_df)} 个题目")
    
    # 2. 找到学科网题目
    zxxk_questions = extended_df[extended_df['question_id'].str.contains('zxxk', na=False)]
    print(f"🔍 学科网题目: {len(zxxk_questions)} 个")
    
    zxxk_ids = set(zxxk_questions['question_id'].values)
    print(f"📝 学科网题目ID: {list(zxxk_ids)}")
    
    # 3. 找到原始查询题目
    original_questions = extended_df[~extended_df['question_id'].str.contains('zxxk', na=False)]
    
    # 从最后几个题目中找到被用于查询的题目
    # 根据之前的验证日志，应该是数学题关于四边形和周长
    query_candidates = original_questions.tail(10)
    
    query_question = None
    for idx, row in query_candidates.iterrows():
        content = str(row['content']).lower()
        if '四边形' in content or '周长' in content or '角' in content or '边' in content:
            query_question = row
            break
    
    if query_question is None:
        # 如果没找到，就用最后一个
        query_question = original_questions.iloc[-1]
    
    print(f"\n📝 原始查询题目:")
    print(f"   ID: {query_question['question_id']}")
    print(f"   内容: {query_question['content'][:100]}...")
    
    # 4. 加载新训练的模型
    print(f"\n🧠 加载新训练的模型...")
    
    api_key = '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'
    trainer = NewModelTrainer(api_key, '/Users/zhuidexiaopengyou/Downloads/dataware_test/zxxk_extended_dataset.csv')
    
    # 加载模型数据
    with open('/Users/zhuidexiaopengyou/Downloads/dataware_test/zxxk_new_model.pkl', 'rb') as f:
        model_data = pickle.load(f)
    
    trainer.df = model_data['df']
    trainer.emb = model_data['emb']
    trainer.index = faiss.read_index('/Users/zhuidexiaopengyou/Downloads/dataware_test/zxxk_new_model_index.faiss')
    
    print(f"✅ 模型加载成功")
    print(f"   模型题目数: {len(trainer.df)}")
    print(f"   向量维度: {trainer.emb.shape}")
    print(f"   索引大小: {trainer.index.ntotal}")
    
    # 5. 用新模型搜索原始题目
    print(f"\n🔍 用新模型搜索原始题目...")
    
    query_text = trainer._make_text(query_question)
    print(f"查询文本: {query_text[:150]}...")
    
    # 搜索前20个最相似的题目
    results = trainer.search_similar(query_text, top_k=20)
    
    print(f"\n📋 搜索结果 (Top 20):")
    found_zxxk_count = 0
    found_zxxk_ranks = []
    
    for result in results:
        is_zxxk = result['question_id'] in zxxk_ids
        marker = "🎯" if is_zxxk else "  "
        
        if is_zxxk:
            found_zxxk_count += 1
            found_zxxk_ranks.append(result['rank'])
        
        print(f"{marker} {result['rank']:2d}. 相似度: {result['score']:.3f} - {result['content']}")
        if is_zxxk:
            print(f"     ✅ 这是学科网题目!")
    
    # 6. 验证结果分析
    print(f"\n🏆 验证结果分析:")
    print(f"   📊 学科网题目总数: {len(zxxk_ids)}")
    print(f"   ✅ 在Top20中找到: {found_zxxk_count} 个")
    print(f"   📈 召回率: {found_zxxk_count}/{len(zxxk_ids)} = {found_zxxk_count/len(zxxk_ids):.1%}")
    
    if found_zxxk_ranks:
        avg_rank = sum(found_zxxk_ranks) / len(found_zxxk_ranks)
        print(f"   📍 平均排名: {avg_rank:.1f}")
        print(f"   🎯 找到的排名: {found_zxxk_ranks}")
    
    # 判断验证是否成功
    success_threshold = 0.6  # 至少找到60%的学科网题目
    
    if found_zxxk_count / len(zxxk_ids) >= success_threshold:
        print(f"\n🎉 验证成功!")
        print(f"   ✅ 新模型能够找到学科网返回的相似题目")
        print(f"   ✅ 证明了语义相似度匹配的有效性")
        print(f"   ✅ 学科网API返回的题目确实与原题相似")
    else:
        print(f"\n❌ 验证失败!")
        print(f"   ❌ 新模型只找到了 {found_zxxk_count}/{len(zxxk_ids)} 个学科网题目")
        print(f"   ❌ 召回率低于阈值 {success_threshold:.1%}")
    
    print(f"\n" + "="*60)
    print(f"🏁 真正的验证测试完成!")
    
    return found_zxxk_count, len(zxxk_ids)

if __name__ == "__main__":
    main()
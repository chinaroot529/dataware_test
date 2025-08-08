#!/usr/bin/env python3
"""
智能题目推荐系统 - Web应用
提供API接口和Web界面
"""

from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
from question_similarity_recommender import QuestionSimilarityRecommender
import os
import json
import traceback
import pandas as pd
from datetime import datetime

app = Flask(__name__)
app.secret_key = 'recommend_secret_key_2025'
CORS(app, supports_credentials=True)

# 全局推荐系统实例
recommender = None
model_loaded = False

# 配置
API_KEY = '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'
CSV_FILE = '/Users/zhuidexiaopengyou/Downloads/dataware_test/questions_1000.csv'
MODEL_PATH = '/Users/zhuidexiaopengyou/Downloads/dataware_test/ultimate_model_961.pkl'

def init_recommender():
    """初始化推荐系统"""
    global recommender, model_loaded
    
    if recommender is not None:
        return True
    
    try:
        recommender = QuestionSimilarityRecommender(API_KEY, CSV_FILE)
        
        # 尝试加载已训练的模型
        if os.path.exists(MODEL_PATH):
            print(f"📂 加载已训练模型: {MODEL_PATH}")
            recommender.load_model(MODEL_PATH)
            model_loaded = True
            print("✅ 模型加载成功")
        else:
            print("⚠️ 未找到预训练模型，需要先训练")
            # 可以选择自动训练小样本模型
            print("🚀 开始训练小样本模型...")
            recommender.load_data()
            recommender.df = recommender.df.head(50)  # 使用50条数据快速训练
            texts = recommender.prepare_texts()
            recommender.embeddings = recommender.get_embeddings_batch(texts, batch_size=10)
            recommender.build_faiss_index(recommender.embeddings)
            recommender.save_model(MODEL_PATH)
            model_loaded = True
            print("✅ 快速训练完成")
        
        return True
        
    except Exception as e:
        print(f"❌ 推荐系统初始化失败: {e}")
        traceback.print_exc()
        return False

@app.route('/')
def index():
    """主页 - 重定向到推荐页面"""
    return render_template('recommend.html')

@app.route('/recommend')
def recommend_page():
    """推荐系统页面"""
    return render_template('recommend.html')

@app.route('/api/recommend/status', methods=['GET'])
def get_status():
    """获取系统状态"""
    try:
        if not model_loaded:
            success = init_recommender()
            if not success:
                return jsonify({
                    'success': False,
                    'message': '推荐系统未就绪'
                })
        
        total_questions = len(recommender.df) if recommender and recommender.df is not None else 0
        
        return jsonify({
            'success': True,
            'model_loaded': model_loaded,
            'total_questions': total_questions,
            'model_path': MODEL_PATH,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'获取状态失败: {str(e)}'
        })

@app.route('/api/recommend/search', methods=['POST'])
def search_similar():
    """搜索相似题目"""
    try:
        # 确保推荐系统已初始化
        if not model_loaded:
            success = init_recommender()
            if not success:
                return jsonify({
                    'success': False,
                    'message': '推荐系统未就绪，请稍后重试'
                })
        
        data = request.json
        if not data:
            return jsonify({
                'success': False,
                'message': '请求数据为空'
            })
        
        # 解析搜索参数
        search_mode = data.get('mode', 'id')
        top_k = data.get('top_k', 10)
        filters = data.get('filters', {})
        
        # 执行搜索
        if search_mode == 'id':
            query_id = data.get('query_id')
            if query_id is None:
                return jsonify({
                    'success': False,
                    'message': '缺少题目ID参数'
                })
            
            if query_id >= len(recommender.df):
                return jsonify({
                    'success': False,
                    'message': f'题目ID {query_id} 不存在，有效范围: 0-{len(recommender.df)-1}'
                })
            
            results = recommender.search_similar_questions(
                query_id=query_id,
                top_k=top_k,
                filters=filters if filters else None
            )
            
        else:  # text search
            query_text = data.get('query_text', '').strip()
            if not query_text:
                return jsonify({
                    'success': False,
                    'message': '缺少搜索文本'
                })
            
            results = recommender.search_similar_questions(
                query_text=query_text,
                top_k=top_k,
                filters=filters if filters else None
            )
        
        # 格式化结果
        formatted_results = []
        for result in results:
            formatted_result = {
                'question_id': int(result['question_id']) if pd.notna(result['question_id']) else 0,
                'similarity_score': float(result['similarity_score']),
                'content': str(result['content']) if result['content'] else '',
                'options': str(result.get('options', '')) if result.get('options') else '',
                'type_id': int(result.get('type_id')) if pd.notna(result.get('type_id')) else None,
                'grade_id': int(result.get('grade_id')) if pd.notna(result.get('grade_id')) else None,
                'subject_id': int(result.get('subject_id')) if pd.notna(result.get('subject_id')) else None,
                'difficulty_id': int(result.get('difficulty_id')) if pd.notna(result.get('difficulty_id')) else None,
                'analysis': str(result.get('analysis', '')) if result.get('analysis') else ''
            }
            formatted_results.append(formatted_result)
        
        return jsonify({
            'success': True,
            'results': formatted_results,
            'total': len(formatted_results),
            'search_params': {
                'mode': search_mode,
                'top_k': top_k,
                'filters': filters
            },
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        print(f"❌ 搜索错误: {e}")
        traceback.print_exc()
        return jsonify({
            'success': False,
            'message': f'搜索失败: {str(e)}'
        })

@app.route('/api/recommend/train', methods=['POST'])
def train_model():
    """训练推荐模型"""
    try:
        data = request.json or {}
        sample_size = data.get('sample_size', 100)
        
        global recommender, model_loaded
        
        print(f"🚀 开始训练推荐模型 (样本大小: {sample_size})")
        
        # 创建新的推荐系统实例
        recommender = QuestionSimilarityRecommender(API_KEY, CSV_FILE)
        
        # 加载数据
        recommender.load_data()
        
        # 限制样本大小
        if sample_size < len(recommender.df):
            recommender.df = recommender.df.head(sample_size)
        
        # 训练模型
        texts = recommender.prepare_texts()
        recommender.embeddings = recommender.get_embeddings_batch(texts, batch_size=10)
        recommender.build_faiss_index(recommender.embeddings)
        
        # 保存模型
        model_path = f"trained_model_{sample_size}.pkl"
        recommender.save_model(model_path)
        
        model_loaded = True
        
        return jsonify({
            'success': True,
            'message': f'模型训练完成，使用 {len(recommender.df)} 条数据',
            'model_path': model_path,
            'sample_size': len(recommender.df),
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        print(f"❌ 训练错误: {e}")
        traceback.print_exc()
        return jsonify({
            'success': False,
            'message': f'训练失败: {str(e)}'
        })

@app.route('/api/recommend/info', methods=['GET'])
def get_info():
    """获取推荐系统信息"""
    try:
        if not model_loaded:
            return jsonify({
                'success': False,
                'message': '推荐系统未加载'
            })
        
        # 获取数据统计
        type_distribution = recommender.df['type_id'].value_counts().to_dict() if recommender.df is not None else {}
        grade_distribution = recommender.df['grade_id'].value_counts().to_dict() if recommender.df is not None else {}
        subject_distribution = recommender.df['subject_id'].value_counts().to_dict() if recommender.df is not None else {}
        
        return jsonify({
            'success': True,
            'total_questions': len(recommender.df) if recommender.df is not None else 0,
            'embedding_dim': recommender.embedding_dim,
            'index_total': recommender.index.ntotal if recommender.index else 0,
            'distributions': {
                'type': type_distribution,
                'grade': grade_distribution,
                'subject': subject_distribution
            },
            'model_loaded': model_loaded,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'获取信息失败: {str(e)}'
        })

if __name__ == '__main__':
    print("🎓 智能题目推荐系统启动中...")
    print("="*50)
    
    # 初始化推荐系统
    if init_recommender():
        print("✅ 推荐系统初始化成功")
        print(f"📊 加载题目数量: {len(recommender.df) if recommender and recommender.df is not None else 0}")
        print(f"🔗 访问地址: http://localhost:5001")
        print("="*50)
        
        app.run(debug=True, host='0.0.0.0', port=5001)
    else:
        print("❌ 推荐系统初始化失败，请检查配置")
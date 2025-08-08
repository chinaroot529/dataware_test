#!/usr/bin/env python3
"""
闭环验证系统 - 完整版
====================
* 从学科网获取新题 → 持久化到数据集 → 增量更新模型 → 验证算法
* 解决token浪费问题，实现真正的数据积累和模型增长
* 支持版本管理和增量统计

使用方法:
    python closed_loop_validation.py questions.csv --iterations 5
"""

import argparse
import pathlib
import json
import time
from datetime import datetime
from typing import Dict, List

from train_ultimate_model import pipeline as validation_pipeline, load_csv
from question_similarity_recommender import QuestionSimilarityRecommender

class ClosedLoopValidator:
    """完整的闭环验证系统"""
    
    def __init__(self, base_dataset: str, api_key: str = None):
        self.base_dataset = pathlib.Path(base_dataset)
        self.api_key = api_key
        self.stats = {
            "start_time": datetime.now().isoformat(),
            "iterations": 0,
            "total_new_questions": 0,
            "total_api_calls": 0,
            "successful_matches": 0,
            "dataset_growth": []
        }
        
    def run_iteration(self, iteration: int) -> Dict:
        """运行一次完整的验证迭代"""
        print(f"\n{'='*60}")
        print(f"🔄  ITERATION {iteration + 1}")
        print(f"{'='*60}")
        
        iteration_start = time.time()
        
        # 1. 确定当前数据集路径
        if iteration == 0:
            current_dataset = self.base_dataset
        else:
            # 使用上一次enriched的结果
            current_dataset = self.base_dataset.parent / f"{self.base_dataset.stem}_enriched.csv"
            
        if not current_dataset.exists():
            current_dataset = self.base_dataset
            
        print(f"📂  Using dataset: {current_dataset}")
        
        # 2. 记录当前数据集大小
        df_before = load_csv(current_dataset)
        count_before = len(df_before)
        
        # 3. 运行验证pipeline（会自动保存enriched版本）
        try:
            validation_pipeline(current_dataset, save_enriched=True)
            self.stats["total_api_calls"] += 1
            
            # 4. 检查是否有新数据
            enriched_path = current_dataset.parent / f"{current_dataset.stem}_enriched.csv"
            if enriched_path.exists():
                df_after = load_csv(enriched_path)
                count_after = len(df_after)
                new_questions = count_after - count_before
                
                if new_questions > 0:
                    self.stats["total_new_questions"] += new_questions
                    print(f"📈  Dataset grew by {new_questions} questions")
                    
                    # 5. 更新推荐模型（如果有现有模型的话）
                    self._update_recommender_model(enriched_path)
                else:
                    print("📊  No new questions added this iteration")
                    
                # 记录增长统计
                self.stats["dataset_growth"].append({
                    "iteration": iteration + 1,
                    "before": count_before,
                    "after": count_after,
                    "added": new_questions,
                    "timestamp": datetime.now().isoformat()
                })
                
            iteration_time = time.time() - iteration_start
            print(f"⏱️  Iteration completed in {iteration_time:.1f}s")
            
            return {
                "success": True,
                "new_questions": new_questions,
                "time_taken": iteration_time
            }
            
        except Exception as e:
            print(f"❌  Iteration failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "time_taken": time.time() - iteration_start
            }
    
    def _update_recommender_model(self, enriched_path: pathlib.Path):
        """更新推荐模型（如果存在的话）"""
        model_files = [
            "question_sim.feather",
            "question_sim_emb.npy", 
            "question_sim.faiss"
        ]
        
        # 检查是否存在预训练模型
        if all((self.base_dataset.parent / f).exists() for f in model_files):
            try:
                print("🔄  Updating existing recommender model...")
                recommender = QuestionSimilarityRecommender(
                    csv_path=str(self.base_dataset), 
                    api_key=self.api_key
                )
                
                # 加载现有模型
                recommender.load("question_sim")
                
                # 增量更新
                added = recommender.update_from_csv(str(enriched_path), update_files=True)
                if added > 0:
                    print(f"✅  Model updated with {added} new questions")
                else:
                    print("ℹ️  No new questions to add to model")
                    
            except Exception as e:
                print(f"⚠️  Failed to update recommender model: {e}")
        else:
            print("ℹ️  No existing model found, skipping model update")
    
    def run_multiple_iterations(self, num_iterations: int = 3, delay_between: int = 2):
        """运行多次迭代"""
        print(f"🚀  Starting closed-loop validation with {num_iterations} iterations")
        print(f"📊  Base dataset: {self.base_dataset}")
        print(f"⏱️  Delay between iterations: {delay_between}s")
        
        results = []
        
        for i in range(num_iterations):
            result = self.run_iteration(i)
            results.append(result)
            self.stats["iterations"] += 1
            
            if result["success"]:
                self.stats["successful_matches"] += 1
                
            # 延迟（避免API限制）
            if i < num_iterations - 1:
                print(f"⏸️  Waiting {delay_between}s before next iteration...")
                time.sleep(delay_between)
        
        # 生成最终报告
        self._generate_report(results)
        
        return results
    
    def _generate_report(self, results: List[Dict]):
        """生成最终统计报告"""
        self.stats["end_time"] = datetime.now().isoformat()
        self.stats["total_time"] = sum(r.get("time_taken", 0) for r in results)
        
        print(f"\n{'='*60}")
        print(f"📊  FINAL REPORT")
        print(f"{'='*60}")
        print(f"🔢  Total iterations: {self.stats['iterations']}")
        print(f"✅  Successful iterations: {self.stats['successful_matches']}")
        print(f"📈  Total new questions discovered: {self.stats['total_new_questions']}")
        print(f"🌐  Total API calls made: {self.stats['total_api_calls']}")
        print(f"⏱️  Total time: {self.stats['total_time']:.1f}s")
        
        if self.stats["total_new_questions"] > 0:
            avg_new_per_iteration = self.stats["total_new_questions"] / self.stats["iterations"]
            print(f"📊  Average new questions per iteration: {avg_new_per_iteration:.1f}")
            
        # 保存详细报告
        report_path = self.base_dataset.parent / f"closed_loop_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump({
                "stats": self.stats,
                "iteration_results": results
            }, f, ensure_ascii=False, indent=2)
            
        print(f"📄  Detailed report saved to: {report_path}")
        
        # 数据集增长可视化
        if self.stats["dataset_growth"]:
            print(f"\n📈  Dataset Growth Timeline:")
            for growth in self.stats["dataset_growth"]:
                print(f"    Iteration {growth['iteration']}: {growth['before']} → {growth['after']} (+{growth['added']})")

def main():
    parser = argparse.ArgumentParser(description="Closed-loop validation system")
    parser.add_argument("dataset", help="Base dataset CSV file")
    parser.add_argument("--iterations", "-n", type=int, default=3, 
                       help="Number of validation iterations (default: 3)")
    parser.add_argument("--delay", "-d", type=int, default=2,
                       help="Delay between iterations in seconds (default: 2)")
    parser.add_argument("--api-key", help="OpenAI API key (or set OPENAI_API_KEY env var)")
    
    args = parser.parse_args()
    
    validator = ClosedLoopValidator(args.dataset, args.api_key)
    results = validator.run_multiple_iterations(args.iterations, args.delay)
    
    # 简单的成功/失败统计
    successful = sum(1 for r in results if r["success"])
    print(f"\n🎯  Final Success Rate: {successful}/{len(results)} ({successful/len(results)*100:.1f}%)")

if __name__ == "__main__":
    main()

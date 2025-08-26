"""
增强游戏王卡片嵌入系统
基于builder.py的数据结构，构建更合理的卡片嵌入系统
整合卡片描述、ID、属性、数值等多维信息
"""

import torch
import torch.nn as nn
import numpy as np
import json
import re
import os
import pickle
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path
from sentence_transformers import SentenceTransformer
from transformers import AutoTokenizer, AutoModel
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EnhancedCardEmbedding:
    """增强卡片嵌入系统 - 整合多维卡片信息"""

    def __init__(self, compressed_dim: int = 128):
        self.compressed_dim = compressed_dim

        # 文本嵌入模型
        logger.info("加载文本模型...")
        self.text_encoder = SentenceTransformer("all-MiniLM-L6-v2")  # 384维

        # 代码嵌入模型
        logger.info("加载代码模型...")
        self.code_tokenizer = AutoTokenizer.from_pretrained("microsoft/codebert-base")
        self.code_model = AutoModel.from_pretrained("microsoft/codebert-base")  # 768维

        # 压缩器
        self.text_compressor = None
        self.code_compressor = None
        self.attribute_scaler = None

        # 数据存储
        self.card_data = {}  # card_id -> 完整卡片信息
        self.card_embeddings = {}  # card_id -> final_embedding

        # 映射表 (从builder.py借鉴)
        self.race_map = {}
        self.attribute_map = {}
        self.type_map = {}

        logger.info(f"初始化完成，目标维度: {compressed_dim}")

    def load_cards_json(self, cards_file: str) -> Dict:
        """加载cards.json文件 - 仿照builder.py的_load_json_data"""
        try:
            with open(cards_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                logger.error(f"cards.json内容不是字典格式")
                return {}
            logger.info(f"成功加载 {len(data)} 张卡片数据")
            return data
        except (IOError, FileNotFoundError, json.JSONDecodeError) as e:
            logger.error(f"加载cards.json失败: {e}")
            return {}

    def extract_card_attributes(self, card_data: Dict) -> Dict:
        """提取卡片属性信息 - 基于builder.py的数据结构"""
        card_info = {
            "id": card_data.get("id"),
            "cid": card_data.get("cid"),
            # 多语言名称
            "cn_name": card_data.get("cn_name", ""),
            "sc_name": card_data.get("sc_name", ""),
            "md_name": card_data.get("md_name", ""),
            "nwbbs_n": card_data.get("nwbbs_n", ""),
            "cnocg_n": card_data.get("cnocg_n", ""),
            "jp_name": card_data.get("jp_name", ""),
            "en_name": card_data.get("en_name", ""),
            # 文本描述
            "desc": card_data.get("text", {}).get("desc", ""),
            "pdesc": card_data.get("text", {}).get("pdesc", ""),
            "types": card_data.get("text", {}).get("types", ""),
            # 数值属性
            "atk": card_data.get("data", {}).get("atk"),
            "def": card_data.get("data", {}).get("def"),
            "level": card_data.get("data", {}).get("level"),
            # 位掩码属性
            "race": card_data.get("data", {}).get("race", 0),
            "attribute": card_data.get("data", {}).get("attribute", 0),
            "type": card_data.get("data", {}).get("type", 0),
            "setcode": card_data.get("data", {}).get("setcode", 0),
        }
        return card_info

    def create_comprehensive_text(self, card_info: Dict) -> str:
        """创建综合文本描述 - 整合所有文本信息"""
        text_parts = []

        # 添加所有非空名称
        names = [
            card_info.get("cn_name", ""),
            card_info.get("sc_name", ""),
            card_info.get("md_name", ""),
            card_info.get("jp_name", ""),
            card_info.get("en_name", ""),
        ]
        unique_names = list(set([name for name in names if name.strip()]))
        if unique_names:
            text_parts.extend(unique_names)

        # 添加类型信息
        if card_info.get("types"):
            text_parts.append(card_info["types"])

        # 添加效果描述
        if card_info.get("desc"):
            text_parts.append(card_info["desc"])

        # 添加陀螺描述
        if card_info.get("pdesc"):
            text_parts.append(card_info["pdesc"])

        # 添加数值信息（如果有）
        numerical_info = []
        if card_info.get("atk") is not None:
            numerical_info.append(f"攻击力{card_info['atk']}")
        if card_info.get("def") is not None:
            numerical_info.append(f"守备力{card_info['def']}")
        if card_info.get("level") is not None:
            numerical_info.append(f"等级{card_info['level']}")

        if numerical_info:
            text_parts.append(" ".join(numerical_info))

        return " ".join(text_parts)

    def create_numerical_features(self, card_info: Dict) -> np.ndarray:
        """创建数值特征向量"""
        features = []

        # 基础数值 (标准化处理)
        atk = card_info.get("atk") if card_info.get("atk") is not None else -1
        def_ = card_info.get("def") if card_info.get("def") is not None else -1
        level = card_info.get("level") if card_info.get("level") is not None else 0

        features.extend([atk, def_, level])

        # 位掩码属性转换为多热向量
        # 种族 (取低16位，常见种族)
        race_bits = [(card_info.get("race", 0) >> i) & 1 for i in range(16)]
        features.extend(race_bits)

        # 属性 (取低8位)
        attr_bits = [(card_info.get("attribute", 0) >> i) & 1 for i in range(8)]
        features.extend(attr_bits)

        # 类型 (取低24位，常见类型)
        type_bits = [(card_info.get("type", 0) >> i) & 1 for i in range(24)]
        features.extend(type_bits)

        return np.array(features, dtype=np.float32)

    def extract_card_id_from_script(self, filename: str) -> Optional[int]:
        """从lua脚本文件名提取卡片ID"""
        match = re.search(r"c(\d+)\.lua", filename)
        return int(match.group(1)) if match else None

    def embed_text(self, text: str) -> np.ndarray:
        """嵌入综合文本"""
        if not text.strip():
            return np.zeros(384)

        # 限制文本长度避免超出模型限制
        if len(text) > 2000:
            text = text[:2000]

        embedding = self.text_encoder.encode(text)
        return embedding

    def embed_code(self, lua_code: str) -> np.ndarray:
        """嵌入Lua代码"""
        if not lua_code.strip():
            return np.zeros(768)

        processed_code = self._preprocess_lua(lua_code)

        inputs = self.code_tokenizer(
            processed_code,
            return_tensors="pt",
            max_length=512,
            truncation=True,
            padding=True,
        )

        with torch.no_grad():
            outputs = self.code_model(**inputs)
            embedding = outputs.last_hidden_state[:, 0, :].squeeze()

        return embedding.numpy()

    def _preprocess_lua(self, code: str) -> str:
        """预处理Lua代码"""
        # 移除注释
        code = re.sub(r"--.*", "", code)
        # 移除多余空白
        code = re.sub(r"\s+", " ", code)
        # 提取关键函数名
        functions = re.findall(r"function\s+\w+", code)
        if functions:
            code = " ".join(functions) + " " + code
        return code[:1500]

    def process_single_card(
        self, card_id: int, cards_data: Dict, script_dir: str = None
    ) -> Optional[Dict]:
        """处理单张卡片的完整信息"""
        # 查找卡片数据
        card_raw_data = None
        for key, data in cards_data.items():
            if data.get("id") == card_id:
                card_raw_data = data
                break

        if not card_raw_data:
            logger.warning(f"未找到卡片 {card_id} 的数据")
            return None

        # 提取卡片属性
        card_info = self.extract_card_attributes(card_raw_data)

        # 创建综合文本
        comprehensive_text = self.create_comprehensive_text(card_info)

        # 创建数值特征
        numerical_features = self.create_numerical_features(card_info)

        # 生成文本嵌入
        text_embedding = self.embed_text(comprehensive_text)

        # 处理lua脚本（如果有脚本目录）
        code_embedding = np.zeros(768)
        lua_code = ""
        if script_dir:
            script_path = os.path.join(script_dir, f"c{card_id}.lua")
            if os.path.exists(script_path):
                try:
                    with open(script_path, "r", encoding="utf-8", errors="ignore") as f:
                        lua_code = f.read()
                    code_embedding = self.embed_code(lua_code)
                except Exception as e:
                    logger.warning(f"读取脚本失败 {script_path}: {e}")

        return {
            "card_id": card_id,
            "card_info": card_info,
            "comprehensive_text": comprehensive_text,
            "numerical_features": numerical_features,
            "text_embedding": text_embedding,
            "code_embedding": code_embedding,
            "lua_code": lua_code,
        }

    def batch_process_cards(
        self, cards_file: str, script_dir: str = None, max_cards: int = None
    ) -> List[Dict]:
        """批量处理卡片"""
        # 加载卡片数据
        cards_data = self.load_cards_json(cards_file)
        if not cards_data:
            return []

        # 获取所有卡片ID
        card_ids = []
        for data in cards_data.values():
            if data.get("id"):
                card_ids.append(data["id"])

        if max_cards:
            card_ids = card_ids[:max_cards]

        logger.info(f"开始处理 {len(card_ids)} 张卡片")

        processed_cards = []
        for i, card_id in enumerate(card_ids):
            if i % 500 == 0:
                logger.info(f"处理进度: {i}/{len(card_ids)}")

            result = self.process_single_card(card_id, cards_data, script_dir)
            if result:
                processed_cards.append(result)
                # 同时存储到card_data中
                self.card_data[card_id] = result["card_info"]

        logger.info(f"成功处理 {len(processed_cards)} 张卡片")
        return processed_cards

    def train_compressors(self, processed_cards: List[Dict]):
        """训练所有压缩器"""
        logger.info("训练压缩器...")

        # 收集所有嵌入和特征
        text_embeddings = []
        code_embeddings = []
        numerical_features = []

        for card in processed_cards:
            text_embeddings.append(card["text_embedding"])
            code_embeddings.append(card["code_embedding"])
            numerical_features.append(card["numerical_features"])

        text_embeddings = np.array(text_embeddings)
        code_embeddings = np.array(code_embeddings)
        numerical_features = np.array(numerical_features)

        logger.info(f"文本嵌入形状: {text_embeddings.shape}")
        logger.info(f"代码嵌入形状: {code_embeddings.shape}")
        logger.info(f"数值特征形状: {numerical_features.shape}")

        # 训练文本压缩器
        text_dim = min(self.compressed_dim // 2, text_embeddings.shape[1])
        self.text_compressor = PCA(n_components=text_dim)
        self.text_compressor.fit(text_embeddings)
        text_variance = np.sum(self.text_compressor.explained_variance_ratio_)

        # 训练代码压缩器
        code_dim = min(self.compressed_dim // 2, code_embeddings.shape[1])
        self.code_compressor = PCA(n_components=code_dim)
        self.code_compressor.fit(code_embeddings)
        code_variance = np.sum(self.code_compressor.explained_variance_ratio_)

        # 训练数值特征标准化器
        self.attribute_scaler = StandardScaler()
        self.attribute_scaler.fit(numerical_features)

        logger.info(f"文本压缩信息保留: {text_variance:.3f}")
        logger.info(f"代码压缩信息保留: {code_variance:.3f}")

    def create_final_embedding(
        self,
        text_embedding: np.ndarray,
        code_embedding: np.ndarray,
        numerical_features: np.ndarray,
    ) -> np.ndarray:
        """创建最终的多维卡片嵌入"""
        # 压缩语义嵌入
        text_compressed = self.text_compressor.transform(text_embedding.reshape(1, -1))[
            0
        ]
        code_compressed = self.code_compressor.transform(code_embedding.reshape(1, -1))[
            0
        ]

        # 标准化数值特征
        numerical_normalized = self.attribute_scaler.transform(
            numerical_features.reshape(1, -1)
        )[0]

        # 权重融合（可调整权重）
        text_weight = 0.5
        code_weight = 0.3
        numerical_weight = 0.2

        # 计算加权嵌入
        semantic_embedding = np.concatenate(
            [text_compressed * text_weight, code_compressed * code_weight]
        )

        # 数值特征降维到合适大小
        numerical_dim = min(32, len(numerical_normalized))
        numerical_reduced = numerical_normalized[:numerical_dim] * numerical_weight

        # 最终融合
        final_embedding = np.concatenate([semantic_embedding, numerical_reduced])

        return final_embedding

    def build_embedding_database(self, processed_cards: List[Dict]):
        """构建最终嵌入数据库"""
        logger.info("构建嵌入数据库...")

        for card in processed_cards:
            card_id = card["card_id"]

            final_embedding = self.create_final_embedding(
                card["text_embedding"],
                card["code_embedding"],
                card["numerical_features"],
            )

            self.card_embeddings[card_id] = {
                "embedding": final_embedding,
                "card_info": card["card_info"],
                "comprehensive_text": card["comprehensive_text"],
            }

        logger.info(f"嵌入数据库构建完成: {len(self.card_embeddings)} 张卡片")
        if self.card_embeddings:
            sample_embedding = list(self.card_embeddings.values())[0]["embedding"]
            logger.info(f"最终嵌入维度: {len(sample_embedding)}")

    def get_card_embedding(self, card_id: int) -> Optional[np.ndarray]:
        """获取卡片嵌入向量"""
        if card_id in self.card_embeddings:
            return self.card_embeddings[card_id]["embedding"]
        return None

    def get_card_info(self, card_id: int) -> Optional[Dict]:
        """获取卡片完整信息"""
        if card_id in self.card_embeddings:
            return self.card_embeddings[card_id]["card_info"]
        return None

    def find_similar_cards(
        self, card_id: int, top_k: int = 5, similarity_type: str = "cosine"
    ) -> List[Tuple]:
        """找相似卡片"""
        target_emb = self.get_card_embedding(card_id)
        if target_emb is None:
            return []

        similarities = []
        for other_id, data in self.card_embeddings.items():
            if other_id != card_id:
                other_emb = data["embedding"]

                if similarity_type == "cosine":
                    sim = np.dot(target_emb, other_emb) / (
                        np.linalg.norm(target_emb) * np.linalg.norm(other_emb)
                    )
                elif similarity_type == "euclidean":
                    sim = -np.linalg.norm(target_emb - other_emb)
                else:
                    sim = np.dot(target_emb, other_emb)

                card_info = data["card_info"]
                card_name = (
                    card_info.get("cn_name")
                    or card_info.get("en_name")
                    or f"Card_{other_id}"
                )

                similarities.append((other_id, sim, card_name, card_info))

        similarities.sort(key=lambda x: x[1], reverse=True)
        return similarities[:top_k]

    def search_by_text(self, query: str, top_k: int = 10) -> List[Tuple]:
        """根据文本查询相似卡片"""
        query_embedding = self.embed_text(query)
        if self.text_compressor is None:
            logger.error("压缩器未训练，无法进行搜索")
            return []

        query_compressed = self.text_compressor.transform(
            query_embedding.reshape(1, -1)
        )[0]

        similarities = []
        for card_id, data in self.card_embeddings.items():
            card_emb = data["embedding"]
            # 只比较文本部分
            text_dim = len(query_compressed)
            card_text_emb = card_emb[:text_dim]

            sim = np.dot(query_compressed, card_text_emb) / (
                np.linalg.norm(query_compressed) * np.linalg.norm(card_text_emb)
            )

            card_info = data["card_info"]
            card_name = (
                card_info.get("cn_name")
                or card_info.get("en_name")
                or f"Card_{card_id}"
            )

            similarities.append((card_id, sim, card_name, data["comprehensive_text"]))

        similarities.sort(key=lambda x: x[1], reverse=True)
        return similarities[:top_k]

    def save_database(self, filepath: str):
        """保存完整数据库"""
        save_data = {
            "card_embeddings": self.card_embeddings,
            "card_data": self.card_data,
            "text_compressor": self.text_compressor,
            "code_compressor": self.code_compressor,
            "attribute_scaler": self.attribute_scaler,
            "compressed_dim": self.compressed_dim,
            "race_map": self.race_map,
            "attribute_map": self.attribute_map,
            "type_map": self.type_map,
        }

        with open(filepath, "wb") as f:
            pickle.dump(save_data, f)

        logger.info(f"数据库已保存到: {filepath}")
        logger.info(f"包含 {len(self.card_embeddings)} 张卡片的嵌入数据")

    def load_database(self, filepath: str):
        """加载数据库"""
        with open(filepath, "rb") as f:
            save_data = pickle.load(f)

        self.card_embeddings = save_data.get("card_embeddings", {})
        self.card_data = save_data.get("card_data", {})
        self.text_compressor = save_data.get("text_compressor")
        self.code_compressor = save_data.get("code_compressor")
        self.attribute_scaler = save_data.get("attribute_scaler")
        self.compressed_dim = save_data.get("compressed_dim", 128)
        self.race_map = save_data.get("race_map", {})
        self.attribute_map = save_data.get("attribute_map", {})
        self.type_map = save_data.get("type_map", {})

        logger.info(f"数据库已加载: {len(self.card_embeddings)} 张卡片")


def main():
    """主函数 - 构建增强卡片嵌入数据库"""

    # 配置路径
    cards_file = "./cards.json"  # cards.json文件路径
    script_dir = "./lua_scripts"  # lua脚本目录（可选）
    output_file = "enhanced_yugioh_embeddings.pkl"

    # 1. 初始化系统
    embedding_system = EnhancedCardEmbedding(compressed_dim=256)

    # 2. 批量处理卡片
    processed_cards = embedding_system.batch_process_cards(
        cards_file=cards_file,
        script_dir=script_dir,
        max_cards=5000,  # 限制处理数量用于测试，实际使用时可以去掉
    )

    if not processed_cards:
        logger.error("没有成功处理任何卡片！")
        return

    # 3. 训练压缩器
    embedding_system.train_compressors(processed_cards)

    # 4. 构建嵌入数据库
    embedding_system.build_embedding_database(processed_cards)

    # 5. 保存数据库
    embedding_system.save_database(output_file)

    # 6. 验证效果
    verify_enhanced_embeddings(embedding_system)


def verify_enhanced_embeddings(embedding_system):
    """验证增强嵌入效果"""
    logger.info("=== 验证增强嵌入效果 ===")

    # 随机选择几张卡片测试
    card_ids = list(embedding_system.card_embeddings.keys())[:5]

    for card_id in card_ids:
        card_info = embedding_system.get_card_info(card_id)
        embedding = embedding_system.get_card_embedding(card_id)

        card_name = (
            card_info.get("cn_name") or card_info.get("en_name") or f"Card_{card_id}"
        )

        logger.info(f"\n卡片 {card_id}: {card_name}")
        logger.info(
            f"  攻击/守备/等级: {card_info.get('atk')}/{card_info.get('def')}/{card_info.get('level')}"
        )
        logger.info(f"  嵌入维度: {embedding.shape}")
        logger.info(f"  描述: {card_info.get('desc', '')[:100]}...")

        # 找相似卡片
        similar_cards = embedding_system.find_similar_cards(card_id, top_k=3)
        logger.info(f"  相似卡片:")
        for sim_id, sim_score, sim_name, sim_info in similar_cards:
            logger.info(f"    {sim_name} (ID:{sim_id}, 相似度:{sim_score:.3f})")

    # 测试文本搜索
    logger.info(f"\n=== 文本搜索测试 ===")
    test_queries = ["青眼白龙", "魔法师", "战士"]

    for query in test_queries:
        logger.info(f"\n搜索: '{query}'")
        results = embedding_system.search_by_text(query, top_k=3)
        for card_id, score, name, desc in results:
            logger.info(f"  {name} (ID:{card_id}, 相关度:{score:.3f})")

    logger.info("=== 验证完成 ===")


def quick_test():
    """快速测试已有数据库"""
    embedding_system = EnhancedCardEmbedding()
    embedding_system.load_database("enhanced_yugioh_embeddings.pkl")

    # 测试几个卡片
    test_ids = list(embedding_system.card_embeddings.keys())[:3]

    for card_id in test_ids:
        card_info = embedding_system.get_card_info(card_id)
        embedding = embedding_system.get_card_embedding(card_id)

        if card_info and embedding is not None:
            name = card_info.get("cn_name") or f"Card_{card_id}"
            print(f"卡片 {name} (ID:{card_id}) - 嵌入维度: {embedding.shape}")
        else:
            print(f"卡片 {card_id} 数据缺失")


if __name__ == "__main__":
    # 首次运行：构建数据库
    main()

    # 后续使用：快速测试
    # quick_test()

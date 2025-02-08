import yaml
from typing import Dict, Any, List
from vanna.ollama import Ollama
from vanna.chromadb import ChromaDB_VectorStore

class MyVanna(ChromaDB_VectorStore, Ollama):
    def __init__(self, config: Dict[str, Any] = None):
        # 显式调用两个父类的初始化
        ChromaDB_VectorStore.__init__(self, config=config)
        Ollama.__init__(self, config=config)
        
    def _validate_data_structure(self, data: Dict[str, Any]) -> None:
        """验证YAML数据结构"""
        structure = {
            'ddls': {'id': str, 'ddl': str},
            'documentation': {'id': str, 'content': str},
            'sqls': {'id': str, 'query': str}
        }
        
        for section, required_fields in structure.items():
            if section in data:
                for idx, item in enumerate(data[section]):
                    if not isinstance(item, dict):
                        raise ValueError(f"{section}[{idx}] 应为字典类型")
                    for field, field_type in required_fields.items():
                        if field not in item:
                            raise ValueError(f"{section}[{idx}] 缺少字段: {field}")
                        if not isinstance(item[field], field_type):
                            raise TypeError(f"{section}[{idx}].{field} 类型错误，应为 {field_type}")

    def _load_training_data(self, file_path: str) -> Dict[str, List[Dict]]:
        """加载训练数据"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f) or {}
                self._validate_data_structure(data)
                return data
        except FileNotFoundError:
            raise FileNotFoundError(f"训练文件 {file_path} 不存在")
        except yaml.YAMLError as e:
            raise ValueError(f"YAML解析错误: {str(e)}")

    def add_training_data_from_file(self, file_path: str) -> None:
        """加载训练数据"""
        data = self._load_training_data(file_path)
        
        # 训练DDL
        for ddl_item in data.get('ddls', []):
            self.train(ddl=ddl_item['ddl'])
        
        # 训练文档
        for doc_item in data.get('documentation', []):
            self.train(documentation=doc_item['content'])
        
        # 训练SQL
        for sql_item in data.get('sqls', []):
            self.train(sql=sql_item['query'])

def main():
    # 确保配置包含所有必需参数
    config = {
        'model': 'deepseek-r1:14b',          # 模型名称
        'ollama_host': 'http://127.0.0.1:11434',  # Ollama服务地址
        'chromadb_settings': {       # ChromaDB配置
            'persist_directory': './chroma_db',
            'anonymized_telemetry': False
        },
        'ollama_timeout': 300.0      # 增加超时时间
    }
    
    try:
        vn = MyVanna(config=config)
        
        # 加载训练数据
        vn.add_training_data_from_file('training_data.yaml')
        print("训练数据加载完成")
        
        # 示例查询
        question = "请查询 my_table1 中 name 为 'Alice' 的记录"
        response = vn.ask(question=question)
        print("生成的SQL:\n", response)
        
    except Exception as e:
        print(f"运行错误: {str(e)}")

if __name__ == "__main__":
    main()

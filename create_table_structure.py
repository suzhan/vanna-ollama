import yaml
from sqlalchemy import create_engine, MetaData
from sqlalchemy.schema import CreateTable

def format_ddl(ddl):
    """
    简单格式化 ddl：
    - 尝试将列定义部分分行显示
    - 添加分号结尾
    """
    if "(" in ddl and ")" in ddl:
        head, rest = ddl.split("(", 1)
        inside, tail = rest.rsplit(")", 1)
        columns = inside.split(",")
        formatted_columns = ",\n    ".join(col.strip() for col in columns)
        ddl = f"{head.strip()} (\n    {formatted_columns}\n){tail.strip()}"
    ddl = ddl.strip()
    if not ddl.endswith(';'):
        ddl += ';'
    return ddl

def generate_ddls_from_db(connection_string):
    """
    根据数据库连接字符串，反射所有表并生成符合要求格式的DDL列表。
    """
    engine = create_engine(connection_string)
    metadata = MetaData()
    metadata.reflect(bind=engine)
    ddls = []
    for table_name, table in metadata.tables.items():
        ddl = str(CreateTable(table).compile(engine))
        # 如果未包含 IF NOT EXISTS，则加入
        if "IF NOT EXISTS" not in ddl:
            ddl = ddl.replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS ")
        ddl = format_ddl(ddl)
        ddls.append({
            'id': table_name,
            'ddl': ddl
        })
    return ddls

if __name__ == '__main__':
    # 请根据实际情况修改数据库连接字符串，支持 MySQL、PostgreSQL 等
    connection_string = "mysql+pymysql://user:password@host/dbname"
    ddls = generate_ddls_from_db(connection_string)
    data = {'ddls': ddls}
    with open('generated_training_data.yaml', 'w', encoding='utf-8') as f:
        yaml.dump(data, f, allow_unicode=True, sort_keys=False)
    print("生成的DDL已写入 generated_training_data.yaml")

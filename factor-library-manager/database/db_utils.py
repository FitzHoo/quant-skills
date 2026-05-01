"""
因子库数据库工具
================

用于将因子数据写入 PostgreSQL 数据库 (aigenfactor)
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from typing import Dict, List, Optional
from pathlib import Path
import yaml
from datetime import datetime


# 数据库默认配置
DEFAULT_DB_CONFIG = {
    "host": "2.tcp.nas.cpolar.cn",
    "port": 14983,
    "user": "postgres",
    "password": "",  # 需要用户提供
    "database": "aigenfactor"
}


def get_db_engine(db_config: dict = None) -> Engine:
    """
    获取数据库连接引擎

    Parameters:
    -----------
    db_config: 数据库配置，包含 host, port, user, password, database

    Returns:
    --------
    Engine: SQLAlchemy 数据库引擎
    """
    if db_config is None:
        db_config = DEFAULT_DB_CONFIG

    connection_string = (
        f"postgresql://{db_config['user']}:{db_config['password']}"
        f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    )

    engine = create_engine(
        connection_string,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        echo=False
    )

    return engine


def create_database_if_not_exists(admin_config: dict, db_name: str = "aigenfactor"):
    """
    创建数据库（如果不存在）

    需要管理员权限连接到默认的 postgres 数据库
    """
    conn_str = (
        f"postgresql://{admin_config['user']}:{admin_config['password']}"
        f"@{admin_config['host']}:{admin_config['port']}/postgres"
    )

    engine = create_engine(conn_str)
    with engine.connect() as conn:
        # 检查数据库是否存在
        result = conn.execute(
            text(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")
        )
        if result.fetchone() is None:
            conn.execute(text(f"CREATE DATABASE {db_name}"))
            conn.commit()
            print(f"数据库 {db_name} 创建成功")
        else:
            print(f"数据库 {db_name} 已存在")


def init_database_tables(engine: Engine):
    """
    初始化数据库表结构
    """
    sql_path = Path(__file__).parent / "init_db.sql"
    if sql_path.exists():
        with open(sql_path, 'r', encoding='utf-8') as f:
            sql_script = f.read()

        # 分割 SQL 语句并执行
        with engine.connect() as conn:
            # 跳过 CREATE DATABASE 语句（因为已经连接到目标数据库）
            statements = sql_script.split(';')
            for stmt in statements:
                stmt = stmt.strip()
                if stmt and not stmt.startswith('--') and 'CREATE DATABASE' not in stmt:
                    try:
                        conn.execute(text(stmt))
                    except Exception as e:
                        # 忽略已存在的错误
                        if 'already exists' not in str(e):
                            print(f"执行SQL出错: {e}")
            conn.commit()

        print("数据库表结构初始化完成")
    else:
        print(f"SQL文件不存在: {sql_path}")


def write_factor_metadata(engine: Engine, factor_data: dict) -> bool:
    """
    写入因子元信息到 factor_metadata 表

    Parameters:
    -----------
    engine: 数据库引擎
    factor_data: 因子元信息字典，包含：
        - factor_name: 因子名称
        - full_name: 完整名称
        - abbreviation: 缩写
        - style: 风格分类
        - data_source: 数据源
        - frequency: 频率
        - broker: 来源券商
        - report_title: 研报标题
        - report_date: 研报日期
        - formula: 计算公式
        - params: 参数配置 (dict)
        - tags: 标签列表
        - notes: 备注

    Returns:
    --------
    bool: 是否写入成功
    """
    with engine.connect() as conn:
        # 检查是否已存在
        result = conn.execute(
            text("SELECT id FROM factor_metadata WHERE factor_name = :name"),
            {"name": factor_data['factor_name']}
        )
        if result.fetchone() is not None:
            # 更新
            update_sql = """
            UPDATE factor_metadata SET
                full_name = :full_name,
                abbreviation = :abbreviation,
                style = :style,
                data_source = :data_source,
                frequency = :frequency,
                broker = :broker,
                report_title = :report_title,
                report_date = :report_date,
                formula = :formula,
                params = :params,
                tags = :tags,
                notes = :notes,
                updated_at = NOW()
            WHERE factor_name = :factor_name
            """
            conn.execute(text(update_sql), {
                **factor_data,
                'params': str(factor_data.get('params', {})),
                'tags': factor_data.get('tags', [])
            })
        else:
            # 插入
            insert_sql = """
            INSERT INTO factor_metadata (
                factor_name, full_name, abbreviation, style, data_source,
                frequency, broker, report_title, report_date, formula,
                params, tags, notes
            ) VALUES (
                :factor_name, :full_name, :abbreviation, :style, :data_source,
                :frequency, :broker, :report_title, :report_date, :formula,
                :params::jsonb, :tags, :notes
            )
            """
            conn.execute(text(insert_sql), {
                **factor_data,
                'params': str(factor_data.get('params', {})),
                'tags': factor_data.get('tags', [])
            })

        conn.commit()

    return True


def write_factor_values(
    engine: Engine,
    factor_name: str,
    df: pd.DataFrame,
    date_col: str = 'date',
    stock_col: str = 'stock_code',
    value_col: str = 'factor_value',
    version: str = 'v1.0'
) -> int:
    """
    写入因子值到 factor_values 表

    Parameters:
    -----------
    engine: 数据库引擎
    factor_name: 因子名称
    df: 因子值DataFrame，包含日期、股票代码、因子值
    date_col: 日期列名
    stock_col: 股票代码列名
    value_col: 因子值列名
    version: 版本号

    Returns:
    --------
    int: 写入的记录数
    """
    # 准备数据
    records = []
    for _, row in df.iterrows():
        records.append({
            'trade_date': row[date_col],
            'stock_code': row[stock_col],
            'factor_name': factor_name,
            'factor_value': row[value_col],
            'version': version
        })

    # 使用 COPY 或批量插入
    with engine.connect() as conn:
        # 先删除旧数据
        conn.execute(
            text("DELETE FROM factor_values WHERE factor_name = :name AND version = :ver"),
            {"name": factor_name, "ver": version}
        )

        # 批量插入
        if len(records) > 0:
            # 使用 pandas to_sql
            df_insert = pd.DataFrame(records)
            df_insert.to_sql(
                'factor_values',
                engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )

        conn.commit()

    return len(records)


def write_factor_performance(engine: Engine, factor_name: str, perf_data: dict, version: str = 'v1.0') -> bool:
    """
    写入因子表现数据到 factor_performance 表

    Parameters:
    -----------
    engine: 数据库引擎
    factor_name: 因子名称
    perf_data: 表现数据字典
    version: 版本号

    Returns:
    --------
    bool: 是否写入成功
    """
    data = {
        'factor_name': factor_name,
        'version': version,
        **perf_data
    }

    with engine.connect() as conn:
        # 检查是否已存在
        result = conn.execute(
            text("SELECT id FROM factor_performance WHERE factor_name = :name AND version = :ver"),
            {"name": factor_name, "ver": version}
        )

        columns = [
            'backtest_start', 'backtest_end', 'universe', 'rebalance_freq',
            'rank_ic_mean', 'rank_icir', 'ic_positive_ratio',
            'long_short_return', 'long_short_volatility', 'max_drawdown', 'sharpe_ratio',
            'group0_return', 'group1_return', 'group2_return', 'group3_return', 'group4_return',
            'benchmark_ic', 'benchmark_return', 'verified_at'
        ]

        if result.fetchone() is not None:
            # 更新
            set_clause = ', '.join([f"{col} = :{col}" for col in columns])
            update_sql = f"UPDATE factor_performance SET {set_clause} WHERE factor_name = :factor_name AND version = :version"
            conn.execute(text(update_sql), data)
        else:
            # 插入
            col_names = ', '.join(columns)
            placeholders = ', '.join([f":{col}" for col in columns])
            insert_sql = f"""
            INSERT INTO factor_performance (factor_name, version, {col_names})
            VALUES (:factor_name, :version, {placeholders})
            """
            conn.execute(text(insert_sql), data)

        conn.commit()

    return True


def register_factor_to_db(
    db_config: dict,
    factor_yaml_path: str,
    performance_yaml_path: str,
    factor_csv_path: str = None,
    init_db: bool = False
) -> Dict:
    """
    一键注册因子到数据库

    Parameters:
    -----------
    db_config: 数据库配置
    factor_yaml_path: factor.yaml 文件路径
    performance_yaml_path: performance.yaml 文件路径
    factor_csv_path: 因子值 CSV 文件路径（可选）
    init_db: 是否初始化数据库表

    Returns:
    --------
    Dict: 注册结果
    """
    result = {
        'success': True,
        'metadata': False,
        'performance': False,
        'values': 0,
        'errors': []
    }

    try:
        engine = get_db_engine(db_config)

        if init_db:
            init_database_tables(engine)

        # 读取 factor.yaml
        with open(factor_yaml_path, 'r', encoding='utf-8') as f:
            factor_data = yaml.safe_load(f)

        # 写入元信息
        write_factor_metadata(engine, factor_data)
        result['metadata'] = True

        # 读取 performance.yaml
        with open(performance_yaml_path, 'r', encoding='utf-8') as f:
            perf_data = yaml.safe_load(f)

        # 写入表现数据
        write_factor_performance(
            engine,
            factor_data['factor_name'],
            perf_data.get('metrics', {}),
            perf_data.get('version', 'v1.0')
        )
        result['performance'] = True

        # 写入因子值（如果提供了CSV路径）
        if factor_csv_path:
            df = pd.read_csv(factor_csv_path)
            count = write_factor_values(
                engine,
                factor_data['factor_name'],
                df,
                version=perf_data.get('version', 'v1.0')
            )
            result['values'] = count

    except Exception as e:
        result['success'] = False
        result['errors'].append(str(e))

    return result


# 命令行入口
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='因子库数据库工具')
    parser.add_argument('--init-db', action='store_true', help='初始化数据库表')
    parser.add_argument('--host', default='2.tcp.nas.cpolar.cn', help='数据库主机')
    parser.add_argument('--port', type=int, default=14983, help='数据库端口')
    parser.add_argument('--user', default='postgres', help='数据库用户')
    parser.add_argument('--password', required=True, help='数据库密码')
    parser.add_argument('--database', default='aigenfactor', help='数据库名称')

    args = parser.parse_args()

    db_config = {
        'host': args.host,
        'port': args.port,
        'user': args.user,
        'password': args.password,
        'database': args.database
    }

    if args.init_db:
        engine = get_db_engine(db_config)
        init_database_tables(engine)
        print("数据库初始化完成")
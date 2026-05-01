"""
因子库数据库工具
================

用于将因子数据写入 PostgreSQL 数据库 (aigenfactor)

配置管理：
---------
- 默认配置从 .env 文件读取（推荐）
- 也可通过 db_config 字典显式传入
"""

import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from typing import Dict, List, Optional
from pathlib import Path
import yaml
from datetime import datetime

# 尝试加载 dotenv（可选依赖）
try:
    from dotenv import load_dotenv
    load_dotenv()  # 自动加载 .env 文件
except ImportError:
    pass  # dotenv 未安装时，直接读取系统环境变量


# ============================================================================
# 默认数据库配置（从环境变量读取）
# ============================================================================

def get_default_db_config() -> dict:
    """
    从环境变量获取默认数据库配置

    环境变量名称：
    - DB_HOST: 数据库主机
    - DB_PORT: 数据库端口
    - DB_USER: 用户名
    - DB_PASSWORD: 密码
    - DB_NAME: 数据库名

    Returns:
    --------
    dict: 数据库配置字典
    """
    return {
        "host": os.getenv('DB_HOST', 'localhost'),
        "port": int(os.getenv('DB_PORT', '5432')),
        "user": os.getenv('DB_USER', 'postgres'),
        "password": os.getenv('DB_PASSWORD', ''),
        "database": os.getenv('DB_NAME', 'aigenfactor')
    }


def get_db_engine(db_config: dict = None) -> Engine:
    """
    获取数据库连接引擎

    Parameters:
    -----------
    db_config: 数据库配置（可选），包含 host, port, user, password, database
        如果不传入，自动使用.env默认配置

    Returns:
    --------
    Engine: SQLAlchemy 数据库引擎

    Example:
    --------
    >>> # 使用默认配置（从.env读取）
    >>> engine = get_db_engine()

    >>> # 显式传入配置
    >>> db_config = {'host': 'localhost', 'port': 5432, ...}
    >>> engine = get_db_engine(db_config)
    """
    if db_config is None:
        db_config = get_default_db_config()

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
        - factor_name: 因子名称（英文格式，必填）
        - full_name: 完整名称（中文）
        - abbreviation: 缩写
        - style: 风格分类（英文：sentiment/value/momentum/quality/growth/technical）
        - data_source: 数据源（英文：shareholder/financial/market/alternative）
        - frequency: 频率（monthly/weekly/daily）
        - broker: 来源券商（中文）
        - report_title: 研报标题（必填）
        - report_date: 研报日期
        - notes: 备注（必填）

    Returns:
    --------
    bool: 是否写入成功
    """
    # 确保必填字段存在
    required_fields = ['factor_name', 'report_title', 'notes']
    for field in required_fields:
        if field not in factor_data or not factor_data[field]:
            raise ValueError(f"必填字段 {field} 缺失或为空")

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
                notes = :notes
            WHERE factor_name = :factor_name
            """
            conn.execute(text(update_sql), {
                'factor_name': factor_data['factor_name'],
                'full_name': factor_data.get('full_name', ''),
                'abbreviation': factor_data.get('abbreviation', ''),
                'style': factor_data.get('style', ''),
                'data_source': factor_data.get('data_source', ''),
                'frequency': factor_data.get('frequency', 'monthly'),
                'broker': factor_data.get('broker', ''),
                'report_title': factor_data['report_title'],
                'report_date': factor_data.get('report_date'),
                'notes': factor_data['notes']
            })
        else:
            # 插入
            insert_sql = """
            INSERT INTO factor_metadata (
                factor_name, full_name, abbreviation, style, data_source,
                frequency, broker, report_title, report_date, notes
            ) VALUES (
                :factor_name, :full_name, :abbreviation, :style, :data_source,
                :frequency, :broker, :report_title, :report_date, :notes
            )
            """
            conn.execute(text(insert_sql), {
                'factor_name': factor_data['factor_name'],
                'full_name': factor_data.get('full_name', ''),
                'abbreviation': factor_data.get('abbreviation', ''),
                'style': factor_data.get('style', ''),
                'data_source': factor_data.get('data_source', ''),
                'frequency': factor_data.get('frequency', 'monthly'),
                'broker': factor_data.get('broker', ''),
                'report_title': factor_data['report_title'],
                'report_date': factor_data.get('report_date'),
                'notes': factor_data['notes']
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
    version: str = 'v1.0',
    chunk_size: int = 50000
) -> int:
    """
    写入因子值到 factor_values 表（批量插入优化）

    Parameters:
    -----------
    engine: 数据库引擎
    factor_name: 因子名称
    df: 因子值DataFrame，包含日期、股票代码、因子值
    date_col: 日期列名（支持YYYY-MM-DD或YYYYMMDD格式）
    stock_col: 股票代码列名
    value_col: 因子值列名
    version: 版本号
    chunk_size: 批量插入块大小

    Returns:
    --------
    int: 写入的记录数

    Notes:
    -----
    - factor_value 保留4位小数
    - trade_date 格式为 DATE (YYYY-MM-DD)
    - stock_code 格式为 .SZ/.SH
    """
    # 标准化日期格式
    df_copy = df.copy()
    if df_copy[date_col].dtype == 'object':
        # YYYYMMDD 字符串转为 YYYY-MM-DD
        df_copy[date_col] = pd.to_datetime(df_copy[date_col], format='%Y%m%d').dt.strftime('%Y-%m-%d')
    else:
        df_copy[date_col] = pd.to_datetime(df_copy[date_col]).dt.strftime('%Y-%m-%d')

    # 因子值保留4位小数
    df_copy[value_col] = df_copy[value_col].round(4)

    # 准备插入数据
    df_insert = df_copy[[date_col, stock_col, value_col]].copy()
    df_insert['factor_name'] = factor_name
    df_insert['version'] = version
    df_insert.columns = ['trade_date', 'stock_code', 'factor_value', 'factor_name', 'version']

    # 排序：按日期升序、股票代码升序
    df_insert = df_insert.sort_values(['trade_date', 'stock_code'])

    with engine.connect() as conn:
        # 先删除旧数据
        conn.execute(
            text("DELETE FROM factor_values WHERE factor_name = :name AND version = :ver"),
            {"name": factor_name, "ver": version}
        )
        conn.commit()

    # 批量插入（使用chunk_size分块）
    if len(df_insert) > 0:
        df_insert.to_sql(
            'factor_values',
            engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=chunk_size
        )

    return len(df_insert)


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

    # 获取默认配置
    default_config = get_default_db_config()

    parser = argparse.ArgumentParser(description='因子库数据库工具')
    parser.add_argument('--init-db', action='store_true', help='初始化数据库表')
    parser.add_argument('--host', default=default_config['host'], help='数据库主机（默认从.env读取）')
    parser.add_argument('--port', type=int, default=default_config['port'], help='数据库端口（默认从.env读取）')
    parser.add_argument('--user', default=default_config['user'], help='数据库用户（默认从.env读取）')
    parser.add_argument('--password', default=default_config['password'], help='数据库密码（默认从.env读取）')
    parser.add_argument('--database', default=default_config['database'], help='数据库名称（默认从.env读取）')

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
    else:
        # 测试连接
        engine = get_db_engine(db_config)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM factor_metadata"))
            count = result.fetchone()[0]
            print(f"数据库连接成功，当前因子数量: {count}")
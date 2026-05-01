"""
重新初始化 aigenfactor 数据库
- factor_value 默认保留4位小数
- factor_name 放在 factor_value 后面
- 无 version 字段
"""
import pandas as pd
from sqlalchemy import create_engine, text
from io import StringIO
import psycopg2
from pathlib import Path
import sys

# 添加研报解析目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# 从 utils 模块读取数据库配置（安全化）
from utils.db_config import get_db_config, get_sqlalchemy_url

def recreate_tables(engine):
    """重建表结构"""
    print("\n正在重建表结构...")

    with engine.connect() as conn:
        # 删除旧表
        conn.execute(text("DROP TABLE IF EXISTS factor_values CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS factor_metadata CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS factor_performance CASCADE"))
        conn.commit()

        # 因子元信息表（更新后的结构）
        conn.execute(text("""
            CREATE TABLE factor_metadata (
                id SERIAL PRIMARY KEY,
                factor_name VARCHAR(100) NOT NULL UNIQUE,
                full_name VARCHAR(200),
                abbreviation VARCHAR(20),
                style VARCHAR(20),
                data_source VARCHAR(20),
                frequency VARCHAR(10),
                broker VARCHAR(50),
                report_title VARCHAR(200) NOT NULL,
                report_date DATE,
                notes TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))

        # 因子值表 - 列顺序：trade_date, stock_code, factor_value, factor_name
        conn.execute(text("""
            CREATE TABLE factor_values (
                id BIGSERIAL PRIMARY KEY,
                trade_date DATE NOT NULL,
                stock_code VARCHAR(20) NOT NULL,
                factor_value NUMERIC(12, 4),
                factor_name VARCHAR(100) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))

        # 创建索引
        conn.execute(text("CREATE INDEX idx_fv_date ON factor_values(trade_date)"))
        conn.execute(text("CREATE INDEX idx_fv_stock ON factor_values(stock_code)"))
        conn.execute(text("CREATE INDEX idx_fv_factor ON factor_values(factor_name)"))

        # 因子表现表
        conn.execute(text("""
            CREATE TABLE factor_performance (
                id SERIAL PRIMARY KEY,
                factor_name VARCHAR(100) NOT NULL UNIQUE,
                backtest_start DATE,
                backtest_end DATE,
                universe VARCHAR(50),
                rebalance_freq VARCHAR(20),
                rank_ic_mean NUMERIC(8, 4),
                rank_icir NUMERIC(8, 4),
                ic_positive_ratio NUMERIC(8, 4),
                long_short_return NUMERIC(8, 4),
                long_short_volatility NUMERIC(8, 4),
                max_drawdown NUMERIC(8, 4),
                sharpe_ratio NUMERIC(8, 4),
                turnover NUMERIC(8, 4),
                group0_return NUMERIC(8, 4),
                group1_return NUMERIC(8, 4),
                group2_return NUMERIC(8, 4),
                group3_return NUMERIC(8, 4),
                group4_return NUMERIC(8, 4),
                benchmark_ic NUMERIC(8, 4),
                benchmark_return NUMERIC(8, 4),
                verified_at DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))

        conn.commit()
        print("[OK] 表结构重建完成")

def write_metadata(engine, data):
    """写入因子元信息（更新后的表结构）"""
    factor_name = data['name']
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO factor_metadata (factor_name, full_name, abbreviation, style, data_source,
                frequency, broker, report_title, report_date, notes)
            VALUES (:factor_name, :full_name, :abbreviation, :style, :data_source,
                :frequency, :broker, :report_title, :report_date, :notes)
            ON CONFLICT (factor_name) DO UPDATE SET
                full_name = EXCLUDED.full_name,
                style = EXCLUDED.style,
                notes = EXCLUDED.notes
        """), {**data, 'factor_name': factor_name})
        conn.commit()

def write_values_bulk(engine, factor_name, csv_path):
    """批量写入因子值（保留4位小数）"""
    # 读取 CSV
    df = pd.read_csv(csv_path)

    # factor_value 保留4位小数
    df['factor'] = df['factor'].round(4)
    df['factor_name'] = factor_name

    # 删除旧数据
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM factor_values WHERE factor_name = :name"),
                     {"name": factor_name})
        conn.commit()

    # 使用 psycopg2 的 copy_from 批量写入（从 .env 读取配置）
    db_config = get_db_config(database='aigenfactor')
    conn_str = (
        f"host={db_config['host']} port={db_config['port']} "
        f"dbname={db_config['database']} user={db_config['user']} password={db_config['password']}"
    )
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()

    # 准备数据 - 列顺序：trade_date, stock_code, factor_value, factor_name
    output = StringIO()
    df_to_write = df[['date', 'stock_code', 'factor', 'factor_name']].copy()
    df_to_write.to_csv(
        output, sep='\t', header=False, index=False, date_format='%Y-%m-%d',
        float_format='%.4f'
    )
    output.seek(0)

    # COPY 命令
    cur.copy_from(output, 'factor_values',
                  columns=('trade_date', 'stock_code', 'factor_value', 'factor_name'))
    conn.commit()
    cur.close()
    conn.close()

    return len(df)

def write_performance(engine, factor_name, perf):
    """写入因子表现"""
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO factor_performance (factor_name, backtest_start, backtest_end,
                universe, rebalance_freq, rank_ic_mean, rank_icir, ic_positive_ratio,
                long_short_return, max_drawdown, sharpe_ratio)
            VALUES (:factor_name, :backtest_start, :backtest_end,
                :universe, :rebalance_freq, :rank_ic_mean, :rank_icir, :ic_positive_ratio,
                :long_short_return, :max_drawdown, :sharpe_ratio)
            ON CONFLICT (factor_name) DO UPDATE SET
                rank_ic_mean = EXCLUDED.rank_ic_mean,
                long_short_return = EXCLUDED.long_short_return
        """), {**perf, 'factor_name': factor_name})
        conn.commit()

def main():
    print("=" * 60)
    print("Factor Library Database")
    print("=" * 60)

    # 连接数据库（从 .env 读取配置）
    engine = create_engine(get_sqlalchemy_url(database='aigenfactor'))

    # 重建表结构
    recreate_tables(engine)

    # 注册因子（示例数据）
    factors = [
        {
            'name': 'HolderNumChange_SNC',
            'full_name': '股东户数变动',
            'abbreviation': 'SNC',
            'style': 'sentiment',
            'data_source': 'shareholder',
            'frequency': 'monthly',
            'broker': '开源证券',
            'report_title': '扎堆效应：股东户数变动',
            'report_date': '2022-11-22',
            'notes': '股东户数变动因子，反映投资者扎堆效应',
            'csv_path': 'C:/Users/HooH/Documents/金工研报/研报解析/03_复现项目/扎堆效应_股东户数变动_复现/output/factors/snc_factor.csv',
            'performance': {
                'backtest_start': '2016-01-01',
                'backtest_end': '2026-04-30',
                'universe': '全市场',
                'rebalance_freq': 'monthly',
                'rank_ic_mean': 9.90,
                'rank_icir': 3.99,
                'ic_positive_ratio': 88.5,
                'long_short_return': 33.0,
                'max_drawdown': 3.19,
                'sharpe_ratio': 4.07
            }
        },
        {
            'name': 'PerCapitaRatio_PCRC',
            'full_name': '人均持股变动',
            'abbreviation': 'PCRC',
            'style': 'sentiment',
            'data_source': 'shareholder',
            'frequency': 'monthly',
            'broker': '开源证券',
            'report_title': '扎堆效应：股东户数变动',
            'report_date': '2022-11-22',
            'notes': '人均持股变动因子，反映投资者扎堆效应',
            'csv_path': 'C:/Users/HooH/Documents/金工研报/研报解析/03_复现项目/扎堆效应_股东户数变动_复现/output/factors/pcrc_factor.csv',
            'performance': {
                'backtest_start': '2016-01-01',
                'backtest_end': '2026-04-30',
                'universe': '全市场',
                'rebalance_freq': 'monthly',
                'rank_ic_mean': 9.73,
                'rank_icir': 3.98,
                'ic_positive_ratio': 86.5,
                'long_short_return': 31.1,
                'max_drawdown': 2.83,
                'sharpe_ratio': 3.93
            }
        }
    ]

    for f in factors:
        print(f"\n--- {f['name']} ---")

        write_metadata(engine, f)
        print("  [OK] Metadata")

        count = write_values_bulk(engine, f['name'], f['csv_path'])
        print(f"  [OK] Values: {count}")

        write_performance(engine, f['name'], f['performance'])
        print("  [OK] Performance")

    # 验证
    print("\n" + "=" * 60)
    print("Verification")
    print("=" * 60)

    with engine.connect() as conn:
        # 检查表结构
        result = conn.execute(text("""
            SELECT column_name, data_type, ordinal_position
            FROM information_schema.columns
            WHERE table_name = 'factor_values'
            ORDER BY ordinal_position
        """))
        print("\nfactor_values columns:")
        for row in result:
            print(f"  {row[2]}. {row[0]}: {row[1]}")

        # 检查数据样例
        result = conn.execute(text("""
            SELECT trade_date, stock_code, factor_value, factor_name
            FROM factor_values
            WHERE factor_name = 'holder_change_snc'
            LIMIT 5
        """))
        print("\nSample data:")
        for row in result:
            print(f"  {row[0]} | {row[1]} | {row[2]} | {row[3]}")

        # 统计
        result = conn.execute(text("SELECT factor_name, COUNT(*) FROM factor_values GROUP BY factor_name"))
        print("\nRow counts:")
        for row in result:
            print(f"  - {row[0]}: {row[1]}")

    print("\n[OK] Done!")

if __name__ == "__main__":
    main()
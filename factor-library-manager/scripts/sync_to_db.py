"""
因子数据库同步脚本（统一版）
==============================

从任意复现项目读取因子数据，同步写入 aigenfactor 数据库。

核心改进：
1. 列名自动适配（解决字段不稳定问题）
2. 增强的风格分类映射
3. 因子名称规范化
4. 文件查找支持多种命名变体

使用方式:
    python sync_to_db.py --project "03_复现项目/{项目名}_复现"
    python sync_to_db.py --project "03_复现项目/{项目名}_复现" --dry-run
    python sync_to_db.py --project "03_复现项目/{项目名}_复现" --month-end-only
    python sync_to_db.py --factor "HolderNumChange_SNC"
    python sync_to_db.py --all  # 同步所有已注册因子

参数:
    --project: 复现项目路径（相对于研报解析目录）
    --factor: 因子名称（从已注册因子库查找）
    --all: 同步所有已注册因子
    --dry-run: 仅打印信息，不写入数据库
    --month-end-only: 仅写入月末数据（减少写入量）

纪律:
    - 只写入 aigenfactor 数据库，严禁写入 postgres
    - 因子值保留4位小数
    - 按 date、code 升序排序
"""

import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path
from datetime import datetime
import yaml
import argparse
import sys
import re

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from utils.db_config import get_db_config

ROOT_PATH = Path(__file__).parent.parent.parent

# ============================================================================
# 增强的映射表
# ============================================================================

# 风格分类映射（增强版）
STYLE_MAPPING = {
    '情绪': 'sentiment',
    '价值': 'value',
    '动量': 'momentum',
    '质量': 'quality',
    '成长': 'growth',
    '技术': 'technical',
    '反转': 'reversal',
    '波动': 'volatility',
    # 英文变体
    'sentiment': 'sentiment',
    'value': 'value',
    'momentum': 'momentum',
    'quality': 'quality',
    'growth': 'growth',
    'technical': 'technical',
    'reversal': 'reversal',
    'volatility': 'volatility',
    # 部分匹配
    '技术形态': 'technical',
    '技术指标': 'technical',
    '量价': 'market',
}

# 数据源映射
DATA_SOURCE_MAPPING = {
    '股东': 'shareholder',
    '财务': 'financial',
    '行情': 'market',
    '另类': 'alternative',
    # 英文变体
    'shareholder': 'shareholder',
    'financial': 'financial',
    'market': 'market',
    'alternative': 'alternative',
}

# ============================================================================
# 列名适配器（核心功能）
# ============================================================================

def normalize_factor_value_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    因子值列名适配

    支持的列名:
    - factor_value（标准）
    - factor（旧项目）
    - value（个别项目）
    - factor_val（变体）
    """
    VALUE_COLUMNS = ['factor_value', 'factor', 'value', 'factor_val', 'factor_v']

    for col in VALUE_COLUMNS:
        if col in df.columns:
            if col != 'factor_value':
                df['factor_value'] = df[col]
            return df

    raise ValueError(f"因子值列不存在，期望: {VALUE_COLUMNS}, 实际: {df.columns.tolist()}")


def normalize_ic_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    IC列名适配

    支持的列名:
    - rank_ic（标准）
    - ic（旧项目）
    - RankIC（大写）
    - spearman_ic（变体）
    """
    IC_COLUMNS = ['rank_ic', 'ic', 'RankIC', 'rank_ic_mean', 'spearman_ic', 'spearman_rank_ic']

    for col in IC_COLUMNS:
        if col in df.columns:
            if col != 'rank_ic':
                df['rank_ic'] = df[col]
            return df

    raise ValueError(f"IC列不存在，期望: {IC_COLUMNS}, 实际: {df.columns.tolist()}")


def normalize_group_columns(df: pd.DataFrame, n_groups: int = 5) -> pd.DataFrame:
    """
    分组收益列名适配

    支持的格式:
    - group_0, group_1, ..., long_short（标准）
    - group0, group1, ..., long_short（旧项目）
    - g0, g1, ..., ls（变体）
    """
    # 分组列名映射
    for i in range(n_groups):
        possible_cols = [
            f'group_{i}',      # 标准
            f'group{i}',       # 旧项目
            f'g{i}',           # 简写
            f'Group{i}',       # 大写
        ]
        target_col = f'group_{i}'
        for col in possible_cols:
            if col in df.columns:
                if col != target_col:
                    df[target_col] = df[col]
                break

    # 多空列名映射
    LS_COLUMNS = ['long_short', 'long_short_return', 'ls', 'ls_return', 'L/S']
    for col in LS_COLUMNS:
        if col in df.columns:
            if col != 'long_short':
                df['long_short'] = df[col]
            break

    return df


def normalize_style(style_input: str) -> str:
    """
    风格分类规范化

    输入可以是中文、英文或部分匹配
    返回英文标准格式
    """
    if not style_input:
        return 'sentiment'  # 默认

    style_input = style_input.strip().lower()

    # 直接匹配
    if style_input in STYLE_MAPPING:
        return STYLE_MAPPING[style_input]

    # 部分匹配
    for key, value in STYLE_MAPPING.items():
        if key.lower() in style_input or style_input in key.lower():
            return value

    # 无法识别，返回默认
    print(f"[WARN] 无法识别风格: {style_input}, 使用默认 sentiment")
    return 'sentiment'


# ============================================================================
# 文件查找（支持多种命名变体）
# ============================================================================

def find_factor_file(output_path: Path, factor_key: str, custom_file: str = None) -> Path:
    """
    因子值文件查找

    搜索顺序:
    1. config.yaml 中指定的自定义文件
    2. {factor_key}_factor.csv（标准）
    3. {factor_key}.csv（简化）
    4. {factor_key}_factor_full.csv（完整版）
    5. 小写变体
    """
    candidates = []

    if custom_file:
        candidates.append(output_path / custom_file)

    candidates.extend([
        output_path / f"{factor_key}_factor.csv",
        output_path / f"{factor_key}.csv",
        output_path / f"{factor_key}_factor_full.csv",
        output_path / f"{factor_key.lower()}_factor.csv",
        output_path / f"{factor_key.lower()}.csv",
    ])

    for path in candidates:
        if path.exists():
            return path

    return None


def find_ic_file(ic_dir: Path, factor_key: str, custom_file: str = None) -> Path:
    """IC文件查找"""
    candidates = []

    if custom_file:
        candidates.append(ic_dir / custom_file)

    candidates.extend([
        ic_dir / f"{factor_key}_ic.csv",
        ic_dir / f"{factor_key}.csv",
        ic_dir / f"{factor_key}_ic_full.csv",
        ic_dir / f"{factor_key.lower()}_ic.csv",
        ic_dir / f"{factor_key.lower()}.csv",
    ])

    for path in candidates:
        if path.exists():
            return path

    return None


def find_group_file(group_dir: Path, factor_key: str, custom_file: str = None) -> Path:
    """分组收益文件查找"""
    candidates = []

    if custom_file:
        candidates.append(group_dir / custom_file)

    candidates.extend([
        group_dir / f"{factor_key}_groups.csv",
        group_dir / f"{factor_key}.csv",
        group_dir / f"{factor_key}_groups_full.csv",
        group_dir / f"{factor_key.lower()}_groups.csv",
        group_dir / f"{factor_key.lower()}.csv",
    ])

    for path in candidates:
        if path.exists():
            return path

    return None


# ============================================================================
# 数据库连接
# ============================================================================

def get_connection():
    """获取数据库连接"""
    return psycopg2.connect(**get_db_config(database='aigenfactor'))


def clear_existing_factors(conn, factor_names):
    """清理已存在的因子数据"""
    cursor = conn.cursor()
    for factor_name in factor_names:
        cursor.execute(f"DELETE FROM factor_metadata WHERE factor_name = '{factor_name}'")
        cursor.execute(f"DELETE FROM factor_values WHERE factor_name = '{factor_name}'")
        cursor.execute(f"DELETE FROM factor_performance WHERE factor_name = '{factor_name}'")
    conn.commit()
    print(f"[OK] 已清理 {len(factor_names)} 个因子旧数据")


# ============================================================================
# 因子信息解析
# ============================================================================

def parse_factor_info_from_project(project_path):
    """
    从复现项目解析因子信息

    Returns:
        dict: {file_key: {factor_name, full_name, ...}}
    """
    config_file = project_path / 'config.yaml'
    if not config_file.exists():
        print(f"[!] config.yaml 不存在: {config_file}")
        return {}

    with open(config_file, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    factors_info = {}
    factors_config = config.get('factors', {})

    # 项目元信息
    project_info = config.get('project', {})
    broker = project_info.get('source', 'Unknown')
    report_title = project_info.get('report_title', '')
    report_date = project_info.get('report_date', '')

    for factor_key, fc in factors_config.items():
        # 解析风格（使用增强映射）
        style_cn = fc.get('type', '').replace('因子', '')
        style = normalize_style(style_cn)

        # 解析数据源
        data_source = 'market'  # 默认
        tables = config.get('tables', {})
        if 'holder_number' in tables or 'ashareholdernumber' in tables:
            data_source = 'shareholder'

        # 构建因子名
        factor_name_key = fc.get('name', factor_key)
        abbr = factor_key.upper()[:3] if len(factor_key) <= 3 else factor_key.upper()

        # 从已注册的 factor.yaml 读取更准确的信息
        registered_factor_path = ROOT_PATH / '04_因子注册' / 'factors' / f"{factor_name_key.replace(' ', '_')}"
        if registered_factor_path.exists():
            factor_yaml = registered_factor_path / 'factor.yaml'
            if factor_yaml.exists():
                with open(factor_yaml, 'r', encoding='utf-8') as f:
                    reg_config = yaml.safe_load(f)
                factor_name = reg_config.get('name', factor_name_key)
                abbr = reg_config.get('abbreviation', abbr)
                style = reg_config.get('classification', {}).get('style', style)
                data_source = reg_config.get('classification', {}).get('data_source', data_source)
        else:
            factor_name = f"{factor_name_key}_{abbr}"

        factors_info[factor_key] = {
            'factor_name': factor_name,
            'full_name': fc.get('name', factor_name_key),
            'abbreviation': abbr,
            'style': style,
            'data_source': data_source,
            'frequency': 'monthly',
            'broker': broker,
            'report_title': report_title,
            'report_date': report_date,
            'description': fc.get('description', ''),
            # 自定义文件名支持
            'factor_file': fc.get('factor_file', None),
            'ic_file': fc.get('ic_file', None),
            'group_file': fc.get('group_file', None),
        }

    return factors_info


# ============================================================================
# 数据插入
# ============================================================================

def insert_metadata(conn, factors_info):
    """插入因子元信息（更新后的表结构）"""
    cursor = conn.cursor()

    for factor_key, factor_info in factors_info.items():
        cursor.execute("""
            INSERT INTO factor_metadata
            (factor_name, full_name, abbreviation, style, data_source, frequency,
             broker, report_title, report_date, notes, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            factor_info['factor_name'],
            factor_info['full_name'],
            factor_info['abbreviation'],
            factor_info['style'],
            factor_info['data_source'],
            factor_info['frequency'],
            factor_info['broker'],
            factor_info['report_title'],
            factor_info['report_date'],
            factor_info['description'],  # notes 字段
            datetime.now()
        ))

    conn.commit()
    print(f"[OK] 元信息已写入: {len(factors_info)} 条")


def insert_factor_values(conn, project_path, factors_info, chunk_size=50000, month_end_only=False):
    """插入因子值 - 使用列名适配器"""
    cursor = conn.cursor()
    output_path = project_path / 'output' / 'factors'

    if not output_path.exists():
        print(f"[!] 因子值目录不存在: {output_path}")
        return

    total_count = 0

    for factor_key, factor_info in factors_info.items():
        factor_file = find_factor_file(output_path, factor_key, factor_info.get('factor_file'))

        if factor_file is None:
            print(f"[!] {factor_key} 因子值文件不存在，跳过")
            continue

        print(f"读取 {factor_file.name}...")
        df = pd.read_csv(factor_file)

        # 列名适配（核心改进）
        df = normalize_factor_value_column(df)

        # 处理日期格式
        df['date'] = pd.to_datetime(df['date'])

        # 月末数据筛选（可选）
        if month_end_only:
            trading_days = sorted(df['date'].unique())
            month_end_map = {}
            for d in trading_days:
                key = (d.year, d.month)
                if key not in month_end_map or d > month_end_map[key]:
                    month_end_map[key] = d
            month_ends = list(month_end_map.values())
            df = df[df['date'].isin(month_ends)]
            print(f"  [月末模式] 从 {len(trading_days)} 个交易日筛选到 {len(month_ends)} 个月末")

        df['trade_date'] = df['date'].dt.strftime('%Y-%m-%d')

        # 按date和stock_code排序
        df = df.sort_values(['date', 'stock_code'])

        # 保留4位小数
        df['factor_value'] = df['factor_value'].round(4)

        factor_name = factor_info['factor_name']

        # 准备数据
        data = [(row['trade_date'], row['stock_code'], row['factor_value'], factor_name)
                for row in df.to_dict('records')]

        # 批量写入
        execute_values(
            cursor,
            "INSERT INTO factor_values (trade_date, stock_code, factor_value, factor_name) VALUES %s",
            data,
            page_size=chunk_size
        )
        conn.commit()

        total_count += len(data)
        print(f"[OK] {factor_name}: {len(data)} 条因子值")

    print(f"[OK] 因子值总计: {total_count} 条")


def insert_performance(conn, project_path, factors_info):
    """插入因子表现数据 - 使用列名适配器"""
    cursor = conn.cursor()
    backtest_path = project_path / 'output' / 'backtest'

    if not backtest_path.exists():
        print(f"[!] 回测目录不存在: {backtest_path}")
        return

    ic_dir = backtest_path / 'ic_series'
    group_dir = backtest_path / 'group_returns'

    for factor_key, factor_info in factors_info.items():
        ic_file = find_ic_file(ic_dir, factor_key, factor_info.get('ic_file'))

        if ic_file is None:
            print(f"[!] {factor_key} IC文件不存在，跳过")
            continue

        ic_df = pd.read_csv(ic_file)

        # IC列名适配（核心改进）
        ic_df = normalize_ic_column(ic_df)

        # 计算指标
        rank_ic_mean = float(ic_df['rank_ic'].mean() * 100)
        rank_icir = float(ic_df['rank_ic'].mean() / ic_df['rank_ic'].std()) if ic_df['rank_ic'].std() > 0 else 0
        ic_positive_ratio = float((ic_df['rank_ic'] > 0).sum() / len(ic_df) * 100)

        group_file = find_group_file(group_dir, factor_key, factor_info.get('group_file'))

        group_returns = {}
        long_short_return = 0

        if group_file:
            group_df = pd.read_csv(group_file)

            # 分组列名适配（核心改进）
            group_df = normalize_group_columns(group_df)

            # 计算年化收益（月频 * 12）
            for i in range(5):
                col = f'group_{i}'
                if col in group_df.columns:
                    group_returns[f'group{i}'] = float(group_df[col].mean() * 12 * 100)

            # 多空收益
            if 'long_short' in group_df.columns:
                long_short_return = float(group_df['long_short'].mean() * 12 * 100)
            else:
                long_short_return = group_returns.get('group4', 0) - group_returns.get('group0', 0)

        factor_name = factor_info['factor_name']
        verified_at = datetime.now().strftime('%Y-%m-%d')
        backtest_start = ic_df['date'].min()
        backtest_end = ic_df['date'].max()

        cursor.execute("""
            INSERT INTO factor_performance
            (factor_name, backtest_start, backtest_end, universe, rebalance_freq,
             rank_ic_mean, rank_icir, ic_positive_ratio, long_short_return,
             group0_return, group1_return, group2_return, group3_return, group4_return, verified_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            factor_name, backtest_start, backtest_end, '全市场', '月频',
            round(rank_ic_mean, 2), round(rank_icir, 2), round(ic_positive_ratio, 2),
            round(long_short_return, 2),
            round(group_returns.get('group0', 0), 2), round(group_returns.get('group1', 0), 2),
            round(group_returns.get('group2', 0), 2), round(group_returns.get('group3', 0), 2),
            round(group_returns.get('group4', 0), 2), verified_at
        ))
        conn.commit()

        print(f"[OK] {factor_name}: IC={rank_ic_mean:.2f}%, ICIR={rank_icir:.2f}, L/S={long_short_return:.2f}%")


# ============================================================================
# 同步流程
# ============================================================================

def sync_project(project_path, dry_run=False, month_end_only=False):
    """同步单个复现项目"""
    print(f"\n{'='*60}")
    print(f"同步项目: {project_path.name}")
    print("="*60)

    # 解析因子信息
    factors_info = parse_factor_info_from_project(project_path)

    if not factors_info:
        print("[!] 未找到因子配置")
        return

    print(f"发现因子: {list(factors_info.keys())}")

    # dry-run 模式
    if dry_run:
        print("\n[dry-run] 仅打印信息，不写入数据库")
        for factor_key, info in factors_info.items():
            print(f"  - {info['factor_name']}: {info['full_name']}")
            if info.get('factor_file'):
                print(f"    自定义因子文件: {info['factor_file']}")
            if info.get('ic_file'):
                print(f"    自定义IC文件: {info['ic_file']}")
            if info.get('group_file'):
                print(f"    自定义分组文件: {info['group_file']}")
        print("="*60)
        return

    # 连接数据库
    conn = get_connection()

    try:
        # 1. 清理旧数据
        print("\n[1] 清理已存在因子数据...")
        factor_names = [fi['factor_name'] for fi in factors_info.values()]
        clear_existing_factors(conn, factor_names)

        # 2. 元信息
        print("\n[2] 插入因子元信息...")
        insert_metadata(conn, factors_info)

        # 3. 因子值
        print("\n[3] 插入因子值...")
        if month_end_only:
            print("[月末模式] 仅写入月末数据")
        insert_factor_values(conn, project_path, factors_info, month_end_only=month_end_only)

        # 4. 表现数据
        print("\n[4] 插入因子表现数据...")
        insert_performance(conn, project_path, factors_info)

        print("="*60)
        print("[OK] 同步完成!")
        print("="*60)

    except Exception as e:
        print(f"[ERROR] {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


def sync_factor_by_name(factor_name, dry_run=False, month_end_only=False):
    """根据因子名查找并同步"""
    factor_dir = ROOT_PATH / '04_因子注册' / 'factors' / factor_name
    if not factor_dir.exists():
        print(f"[!] 因子未注册: {factor_name}")
        return

    factor_yaml = factor_dir / 'factor.yaml'
    if not factor_yaml.exists():
        print(f"[!] factor.yaml 不存在")
        return

    with open(factor_yaml, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    reproduction_path = config.get('source', {}).get('reproduction_path', '')
    if not reproduction_path:
        print(f"[!] 未找到复现项目路径")
        return

    project_path = ROOT_PATH / reproduction_path
    sync_project(project_path, dry_run=dry_run, month_end_only=month_end_only)


def sync_all_factors(dry_run=False, month_end_only=False):
    """同步所有已注册因子"""
    factors_dir = ROOT_PATH / '04_因子注册' / 'factors'

    if not factors_dir.exists():
        print("[!] 因子库目录不存在")
        return

    factor_names = [d.name for d in factors_dir.iterdir() if d.is_dir() and (d / 'factor.yaml').exists()]

    print(f"发现 {len(factor_names)} 个已注册因子")

    for factor_name in factor_names:
        try:
            sync_factor_by_name(factor_name, dry_run=dry_run, month_end_only=month_end_only)
        except Exception as e:
            print(f"[ERROR] {factor_name}: {e}")
            continue


# ============================================================================
# 主函数
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='因子数据库同步')
    parser.add_argument('--project', help='复现项目路径（相对于研报解析目录）')
    parser.add_argument('--factor', help='因子名称（从已注册因子库查找）')
    parser.add_argument('--all', action='store_true', help='同步所有已注册因子')
    parser.add_argument('--dry-run', action='store_true', help='仅打印信息，不写入数据库')
    parser.add_argument('--month-end-only', action='store_true', help='仅写入月末数据')
    args = parser.parse_args()

    print("="*60)
    print("因子数据库同步")
    print("纪律: 只写入aigenfactor，严禁修改postgres")
    print("列名适配: 自动处理 factor/factor_value, ic/rank_ic, group0/group_0")
    print("="*60)

    if args.all:
        sync_all_factors(dry_run=args.dry_run, month_end_only=args.month_end_only)
    elif args.factor:
        sync_factor_by_name(args.factor, dry_run=args.dry_run, month_end_only=args.month_end_only)
    elif args.project:
        project_path = ROOT_PATH / args.project
        if not project_path.exists():
            print(f"[!] 项目路径不存在: {project_path}")
            sys.exit(1)
        sync_project(project_path, dry_run=args.dry_run, month_end_only=args.month_end_only)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == '__main__':
    main()
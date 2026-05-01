"""
因子库数据验证脚本
==================

验证 INDEX.md 与数据库的一致性，检查空值问题。

使用方式:
    python verify_factor_data.py

检查项:
1. 数据库 factor_performance 表是否有 NULL 值
2. 因子目录是否完整（与 INDEX.md 一致）
3. performance.yaml 文件是否存在且格式正确
"""

import sys
from pathlib import Path
import yaml
import psycopg2

# 添加研报解析目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utils.db_config import get_db_config

ROOT_PATH = Path(__file__).parent.parent.parent


def check_database_null_values():
    """检查数据库中的 NULL 值"""
    print("\n[1] 检查数据库 NULL 值...")

    conn = psycopg2.connect(**get_db_config(database='aigenfactor'))
    cursor = conn.cursor()

    # 查询所有因子表现数据
    cursor.execute("""
        SELECT factor_name, rank_ic_mean, rank_icir, ic_positive_ratio,
               long_short_return, group0_return, group4_return
        FROM factor_performance
        ORDER BY rank_ic_mean DESC
    """)
    rows = cursor.fetchall()

    null_issues = []
    valid_count = 0

    for row in rows:
        factor_name = row[0]
        values = {
            'rank_ic_mean': row[1],
            'rank_icir': row[2],
            'ic_positive_ratio': row[3],
            'long_short_return': row[4],
            'group0_return': row[5],
            'group4_return': row[6]
        }

        # 检查 NULL 值
        null_fields = [k for k, v in values.items() if v is None]
        if null_fields:
            null_issues.append({
                'factor': factor_name,
                'null_fields': null_fields
            })
        else:
            valid_count += 1

    conn.close()

    if null_issues:
        print(f"  [!] 发现 {len(null_issues)} 个因子有 NULL 值:")
        for issue in null_issues:
            print(f"      - {issue['factor']}: {issue['null_fields']}")
    else:
        print(f"  [OK] 所有 {valid_count} 个因子数据完整，无 NULL 值")

    return null_issues


def check_factor_directories():
    """检查因子目录完整性"""
    print("\n[2] 检查因子目录...")

    factors_dir = ROOT_PATH / '04_因子注册' / 'factors'

    if not factors_dir.exists():
        print("  [!] 因子目录不存在")
        return False

    # 获取所有因子目录
    factor_dirs = [d.name for d in factors_dir.iterdir() if d.is_dir()]

    # 获取数据库中的因子名称
    conn = psycopg2.connect(**get_db_config(database='aigenfactor'))
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT factor_name FROM factor_performance")
    db_factors = [row[0] for row in cursor.fetchall()]
    conn.close()

    # 检查一致性
    missing_dirs = [f for f in db_factors if f not in factor_dirs]
    extra_dirs = [d for d in factor_dirs if d not in db_factors]

    if missing_dirs:
        print(f"  [!] 缺少因子目录: {missing_dirs}")
    if extra_dirs:
        print(f"  [!] 多余因子目录: {extra_dirs}")

    if not missing_dirs and not extra_dirs:
        print(f"  [OK] 因子目录与数据库一致，共 {len(factor_dirs)} 个")

    return len(missing_dirs) == 0


def check_performance_yaml():
    """检查 performance.yaml 文件"""
    print("\n[3] 检查 performance.yaml...")

    factors_dir = ROOT_PATH / '04_因子注册' / 'factors'

    issues = []
    valid_count = 0

    for factor_dir in factors_dir.iterdir():
        if not factor_dir.is_dir():
            continue

        perf_file = factor_dir / 'performance.yaml'
        if not perf_file.exists():
            issues.append({
                'factor': factor_dir.name,
                'issue': 'performance.yaml 不存在'
            })
            continue

        try:
            with open(perf_file, 'r', encoding='utf-8') as f:
                perf = yaml.safe_load(f)

            # 检查必要字段
            metrics = perf.get('metrics', {})
            required_fields = ['rank_ic_mean', 'rank_icir', 'long_short_return']
            missing_fields = [f for f in required_fields if f not in metrics]

            if missing_fields:
                issues.append({
                    'factor': factor_dir.name,
                    'issue': f"缺少字段: {missing_fields}"
                })
            else:
                valid_count += 1

        except Exception as e:
            issues.append({
                'factor': factor_dir.name,
                'issue': f"读取失败: {e}"
            })

    if issues:
        print(f"  [!] 发现 {len(issues)} 个问题:")
        for issue in issues:
            print(f"      - {issue['factor']}: {issue['issue']}")
    else:
        print(f"  [OK] 所有 {valid_count} 个 performance.yaml 格式正确")

    return len(issues) == 0


def check_index_md_format():
    """检查 INDEX.md 格式"""
    print("\n[4] 检查 INDEX.md 格式...")

    index_file = ROOT_PATH / '04_因子注册' / 'INDEX.md'
    if not index_file.exists():
        print("  [!] INDEX.md 不存在")
        return False

    with open(index_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # 检查是否有 "-" 作为空值的情况
    lines = content.split('\n')
    issues = []

    for i, line in enumerate(lines):
        # 检查表格行中的 "-" 空值
        if '|' in line and ' - ' in line:
            # 排除表头分隔行
            if '---' in line:
                continue
            # 排除正常用法（如 "v1.0"）
            if ' - ' in line and not line.strip().startswith('|'):
                continue
            # 检查是否是空值显示
            parts = line.split('|')
            for part in parts:
                if part.strip() == '-':
                    issues.append({
                        'line': i + 1,
                        'content': line.strip()[:80]
                    })

    if issues:
        print(f"  [!] 发现 {len(issues)} 处可能的空值显示 '-'")
        for issue in issues[:5]:
            print(f"      - 第 {issue['line']} 行: {issue['content']}")
        if len(issues) > 5:
            print(f"      ... 还有 {len(issues) - 5} 处")
    else:
        print("  [OK] INDEX.md 无空值显示问题")

    return len(issues) == 0


def main():
    print("=" * 60)
    print("因子库数据验证")
    print("=" * 60)

    results = {
        'database': check_database_null_values(),
        'directories': check_factor_directories(),
        'yaml': check_performance_yaml(),
        'index': check_index_md_format()
    }

    print("\n" + "=" * 60)
    print("验证结果汇总")
    print("=" * 60)

    all_passed = all([
        len(results['database']) == 0,
        results['directories'],
        results['yaml'],
        results['index']
    ])

    if all_passed:
        print("[OK] 所有检查通过，数据格式一致")
    else:
        print("[!] 存在问题，请修复后再继续")

    return all_passed


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
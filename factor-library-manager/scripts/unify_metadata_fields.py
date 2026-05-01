"""
统一 factor_metadata 表命名规则
================================

1. frequency: 统一使用英文 "monthly"
2. style: 英文风格分类（与 INDEX.md 一致）
3. 已删除 formula/params/tags 列（表结构已更新）
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utils.db_config import get_db_config
import psycopg2

# 更新数据
UPDATE_DATA = {
    'LargeFlowStrength_S3': {'frequency': 'monthly', 'style': 'sentiment'},
    'SmallFlowStrength_S3': {'frequency': 'monthly', 'style': 'sentiment'},
    'LargeResidualFlow': {'frequency': 'monthly', 'style': 'sentiment'},
    'SmallResidualFlow': {'frequency': 'monthly', 'style': 'sentiment'},
    'LargeResidualReverse': {'frequency': 'monthly', 'style': 'reversal'},
    'SmallResidualReverse': {'frequency': 'monthly', 'style': 'reversal'},
    'LongMomentum_LM1': {'frequency': 'monthly', 'style': 'momentum'},
    'LongMomentum_LM2': {'frequency': 'monthly', 'style': 'momentum'},
    'HolderNumChange_SNC': {'frequency': 'monthly', 'style': 'sentiment'},
    'PerCapitaRatio_PCRC': {'frequency': 'monthly', 'style': 'sentiment'},
    'Amplitude_AMP': {'frequency': 'monthly', 'style': 'volatility'},
    'HighAmplitude_VH': {'frequency': 'monthly', 'style': 'volatility'},
    'IdealAmplitude_VI': {'frequency': 'monthly', 'style': 'volatility'},
    'LowAmplitude_VL': {'frequency': 'monthly', 'style': 'volatility'},
    'ActiveBuyPos_ABP': {'frequency': 'monthly', 'style': 'sentiment'},
    'ActiveBuyNeg_ABN': {'frequency': 'monthly', 'style': 'sentiment'},
    'PriceConvergence_PCF': {'frequency': 'monthly', 'style': 'reversal'},
    'VolumeConvergence_VCF': {'frequency': 'monthly', 'style': 'reversal'},
    'AmountConvergence_ACF': {'frequency': 'monthly', 'style': 'reversal'},
    'TurnoverRateConvergence_TRCF': {'frequency': 'monthly', 'style': 'reversal'},
}


def check_table_structure():
    """查看当前表结构"""
    print("=" * 60)
    print("检查 factor_metadata 表结构")
    print("=" * 60)

    conn = psycopg2.connect(**get_db_config(database='aigenfactor'))
    cursor = conn.cursor()

    cursor.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'factor_metadata'
        ORDER BY ordinal_position
    """)
    print("\n当前表结构:")
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]}")

    conn.close()


def update_data():
    """更新数据：统一使用英文命名"""
    print("\n" + "=" * 60)
    print("更新数据内容")
    print("规则: frequency=monthly, style=英文风格分类")
    print("=" * 60)

    conn = psycopg2.connect(**get_db_config(database='aigenfactor'))
    cursor = conn.cursor()

    updated_count = 0

    for factor_name, data in UPDATE_DATA.items():
        print(f"\n更新 {factor_name}:")
        print(f"  frequency: {data['frequency']}")
        print(f"  style: {data['style']}")

        cursor.execute("""
            UPDATE factor_metadata
            SET frequency = %s,
                style = %s
            WHERE factor_name = %s
        """, (
            data['frequency'],
            data['style'],
            factor_name
        ))

        if cursor.rowcount > 0:
            updated_count += 1
            print(f"  [OK] 已更新")
        else:
            print(f"  [!] 未找到记录")

    conn.commit()
    conn.close()

    print("\n" + "=" * 60)
    print(f"[OK] 更新完成，共更新 {updated_count} 条记录")
    print("=" * 60)


def verify_result():
    """验证更新结果"""
    print("\n验证最终结果:")

    conn = psycopg2.connect(**get_db_config(database='aigenfactor'))
    cursor = conn.cursor()

    # 查看数据
    cursor.execute("""
        SELECT factor_name, abbreviation, full_name, style, frequency, data_source, broker
        FROM factor_metadata
        ORDER BY created_at
    """)
    print("\n数据内容:")
    print("| Factor | Abbr | Full | Style | Freq | DataSource | Broker |")
    print("|--------|------|------|-------|------|------------|--------|")
    for row in cursor.fetchall():
        print(f"| {row[0][:20]} | {row[1]} | {row[2]} | {row[3]} | {row[4]} | {row[5]} | {row[6]} |")

    conn.close()


if __name__ == '__main__':
    check_table_structure()
    update_data()
    verify_result()
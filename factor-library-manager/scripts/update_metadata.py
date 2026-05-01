"""
更新 factor_metadata 表数据
===========================

使数据库数据与 INDEX.md 严格匹配

规则：
- abbreviation: 与 INDEX.md 一致（3字母缩写）
- full_name: 简洁直观，不超过10字，不标注方向，单数形式（中文）
- broker: 中文（开源证券）
- data_source: market/shareholder
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utils.db_config import get_db_config
import psycopg2

# 更新数据映射（按 INDEX.md 顺序）
UPDATE_DATA = {
    'LargeFlowStrength_S3': {
        'abbreviation': 'LFS',
        'full_name': '大单资金强度',
        'style': 'sentiment',
        'data_source': 'market',
        'broker': '开源证券',
    },
    'SmallFlowStrength_S3': {
        'abbreviation': 'SFS',
        'full_name': '小单资金强度',
        'style': 'sentiment',
        'data_source': 'market',
        'broker': '开源证券',
    },
    'LargeResidualFlow': {
        'abbreviation': 'LRF',
        'full_name': '大单残余流',
        'style': 'sentiment',
        'data_source': 'market',
        'broker': '开源证券',
    },
    'SmallResidualFlow': {
        'abbreviation': 'SRF',
        'full_name': '小单残余流',
        'style': 'sentiment',
        'data_source': 'market',
        'broker': '开源证券',
    },
    'LargeResidualReverse': {
        'abbreviation': 'LRR',
        'full_name': '大单残余反转',
        'style': 'reversal',
        'data_source': 'market',
        'broker': '开源证券',
    },
    'SmallResidualReverse': {
        'abbreviation': 'SRR',
        'full_name': '小单残余反转',
        'style': 'reversal',
        'data_source': 'market',
        'broker': '开源证券',
    },
    'LongMomentum_LM1': {
        'abbreviation': 'LM1',
        'full_name': '长端动量1.0',
        'style': 'momentum',
        'data_source': 'market',
        'broker': '开源证券',
    },
    'LongMomentum_LM2': {
        'abbreviation': 'LM2',
        'full_name': '长端动量2.0',
        'style': 'momentum',
        'data_source': 'market',
        'broker': '开源证券',
    },
    'HolderNumChange_SNC': {
        'abbreviation': 'SNC',
        'full_name': '股东户数变动',
        'style': 'sentiment',
        'data_source': 'shareholder',
        'broker': '开源证券',
    },
    'PerCapitaRatio_PCRC': {
        'abbreviation': 'PCRC',
        'full_name': '人均持股变动',
        'style': 'sentiment',
        'data_source': 'shareholder',
        'broker': '开源证券',
    },
    'Amplitude_AMP': {
        'abbreviation': 'AMP',
        'full_name': '传统振幅',
        'style': 'volatility',
        'data_source': 'market',
        'broker': '开源证券',
    },
    'HighAmplitude_VH': {
        'abbreviation': 'VH',
        'full_name': '高价振幅',
        'style': 'volatility',
        'data_source': 'market',
        'broker': '开源证券',
    },
    'IdealAmplitude_VI': {
        'abbreviation': 'VI',
        'full_name': '理想振幅',
        'style': 'volatility',
        'data_source': 'market',
        'broker': '开源证券',
    },
    'LowAmplitude_VL': {
        'abbreviation': 'VL',
        'full_name': '低价振幅',
        'style': 'volatility',
        'data_source': 'market',
        'broker': '开源证券',
    },
    'ActiveBuyPos_ABP': {
        'abbreviation': 'ABP',
        'full_name': '主动买入正向',
        'style': 'sentiment',
        'data_source': 'market',
        'broker': '开源证券',
    },
    'ActiveBuyNeg_ABN': {
        'abbreviation': 'ABN',
        'full_name': '主动买入负向',
        'style': 'sentiment',
        'data_source': 'market',
        'broker': '开源证券',
    },
    'PriceConvergence_PCF': {
        'abbreviation': 'PCF',
        'full_name': '价格收敛',
        'style': 'reversal',
        'data_source': 'market',
        'broker': '开源证券',
    },
    'VolumeConvergence_VCF': {
        'abbreviation': 'VCF',
        'full_name': '成交量收敛',
        'style': 'reversal',
        'data_source': 'market',
        'broker': '开源证券',
    },
    'AmountConvergence_ACF': {
        'abbreviation': 'ACF',
        'full_name': '成交额收敛',
        'style': 'reversal',
        'data_source': 'market',
        'broker': '开源证券',
    },
    'TurnoverRateConvergence_TRCF': {
        'abbreviation': 'TRCF',
        'full_name': '换手率收敛',
        'style': 'reversal',
        'data_source': 'market',
        'broker': '开源证券',
    },
}


def update_metadata():
    """更新 factor_metadata 表"""
    print("=" * 60)
    print("更新 factor_metadata 表数据")
    print("规则: 与 INDEX.md 严格匹配")
    print("=" * 60)

    conn = psycopg2.connect(**get_db_config(database='aigenfactor'))
    cursor = conn.cursor()

    updated_count = 0

    for factor_name, data in UPDATE_DATA.items():
        print(f"\n更新 {factor_name}:")
        print(f"  abbr: {data['abbreviation']}")
        print(f"  full: {data['full_name']}")
        print(f"  broker: {data['broker']}")

        cursor.execute("""
            UPDATE factor_metadata
            SET abbreviation = %s,
                full_name = %s,
                style = %s,
                data_source = %s,
                broker = %s
            WHERE factor_name = %s
        """, (
            data['abbreviation'],
            data['full_name'],
            data['style'],
            data['data_source'],
            data['broker'],
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


def verify_update():
    """验证更新结果"""
    print("\n验证更新结果:")

    conn = psycopg2.connect(**get_db_config(database='aigenfactor'))
    cursor = conn.cursor()

    cursor.execute("""
        SELECT factor_name, abbreviation, full_name, style, data_source, broker
        FROM factor_metadata
        ORDER BY created_at
    """)
    rows = cursor.fetchall()

    print(f"\n| Factor Name | Abbr | Full Name | Style | DataSource | Broker |")
    print("|-------------|------|-----------|-------|------------|--------|")
    for row in rows:
        print(f"| {row[0]} | {row[1]} | {row[2]} | {row[3]} | {row[4]} | {row[5]} |")

    conn.close()


if __name__ == '__main__':
    update_metadata()
    verify_update()
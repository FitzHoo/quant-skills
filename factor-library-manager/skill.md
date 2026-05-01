---
name: factor-library-manager
description: 管理金融工程因子库，提供因子注册、查询、对比、版本管理和报告生成功能。用于解决因子分类混乱、表现数据缺失记录、版本无记录等问题。触发关键词：因子库、注册因子、查询因子、因子对比、因子版本、因子报告。支持从复现项目自动提取因子信息并入库，按多维度检索因子，对比多个因子表现，记录参数调整历史，生成月度汇总报告。
---

# 因子库管理器 (Factor Library Manager)

系统化管理已复现的量化因子，解决分类混乱、表现缺失、版本无记录问题。

---

## 📁 因子库目录结构

```
04_因子注册/
│
├── INDEX.md                    # 因子总索引（核心入口）
│
├── factors/                    # 因子存放目录
│   └── {因子名称}/
│       ├── factor.yaml         # 因子元信息（分类、标签）
│       ├── performance.yaml    # 表现指标记录
│       ├── versions/           # 版本历史
│       │   ├── v1.0/
│       │   ├── v1.1/
│       │   └── CHANGELOG.md
│       └── latest/             # 当前使用版本（指向最新）
│
├── classification/             # 分类体系定义
│   ├── 风格分类.yaml           # 情绪/价值/动量/质量/成长/技术
│   ├── 数据源分类.yaml         # 股东/财务/行情/另类
│   └── 研报来源.yaml           # 券商来源索引
│
├── templates/                  # 模板文件
│   ├── factor_template.yaml
│   ├── performance_template.yaml
│   └── changelog_template.md
│
└── reports/                    # 定期报告
    ├── monthly_summary.md
    └── factor_comparison.md
```

---

## 🚀 核心功能

### 功能 1：因子注册 (factor-register)

将复现完成的因子注册到因子库。

**触发条件**：
- 用户说："把这个因子加入因子库"
- 用户说："注册因子 XXX"
- 复现完成后提示注册

**执行流程**：

```
1. 定位复现项目目录
   - 搜索 03_复现项目/{因子名}_复现/
   - 或用户指定路径

2. 读取复现数据
   - config.yaml → 提取参数、数据源、来源信息
   - output/backtest/ → 提取 IC、分组收益等表现数据
   - 复现结果对比.md → 提取研报基准值

3. 创建因子目录
   - mkdir 04_因子注册/factors/{因子名}/
   - mkdir 04_因子注册/factors/{因子名}/versions/v1.0/

4. 生成元信息文件
   - factor.yaml（分类、来源、依赖）
   - performance.yaml（表现指标）
   - params.yaml（参数配置）

5. 复制代码文件
   - 将 code/ 目录复制到 v1.0/

6. 创建版本记录
   - CHANGELOG.md 初始版本条目

7. 更新总索引
   - INDEX.md 添加因子条目
   - 更新分类统计

8. **[自动] 同步数据库**
   - **直接调用统一脚本**: `python 04_因子注册/scripts/sync_to_db.py --project "03_复现项目/{项目名}_复现"`
   - 连接 aigenfactor 数据库
   - 清理已存在的因子数据
   - 写入 factor_metadata 表（元信息）
   - 写入 factor_values 表（因子值，批量插入）
   - 写入 factor_performance 表（表现数据）
   - 确保数据格式一致性
```

**⚠️ 重要：数据库同步使用统一脚本**
- INDEX.md 更新完成后，**直接调用统一脚本同步数据库**
- **禁止在复现项目 code 目录创建新的写入脚本**
- 统一脚本位置：`04_因子注册/scripts/sync_to_db.py`
- 调用方式：`python sync_to_db.py --project "03_复现项目/{项目名}_复现"`

---

### 数据库同步通用脚本

**位置**: `04_因子注册/scripts/sync_to_db.py`

**使用方式**：
```bash
# 同步指定复现项目
python 04_因子注册/scripts/sync_to_db.py --project "03_复现项目/{项目名}_复现"

# 预检查模式（不写入数据库）
python 04_因子注册/scripts/sync_to_db.py --project "03_复现项目/{项目名}_复现" --dry-run

# 同步指定因子
python 04_因子注册/scripts/sync_to_db.py --factor "HolderNumChange_SNC"

# 同步所有已注册因子
python 04_因子注册/scripts/sync_to_db.py --all
```

**脚本功能**：
1. 自动解析 `config.yaml` 提取因子信息
2. 自动扫描 `output/factors/` 读取因子值
3. **列名自动适配**（解决字段不稳定问题）
4. 自动计算 IC、ICIR、多空收益
5. 批量写入数据库（chunk_size=50000）

**列名适配规则**：
| 数据类型 | 支持的列名 | 标准化输出 |
|----------|------------|------------|
| 因子值 | `factor_value`, `factor`, `value`, `factor_val` | `factor_value` |
| IC | `rank_ic`, `ic`, `RankIC`, `spearman_ic` | `rank_ic` |
| 分组收益 | `group_0`, `group0`, `g0`, `Group0` | `group_0` |
| 多空收益 | `long_short`, `long_short_return`, `ls`, `L/S` | `long_short` |

---

### 数据库同步流程

```python
import psycopg2

conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()

# 检查元信息
cursor.execute('SELECT factor_name, style, data_source FROM factor_metadata WHERE factor_name = ?', (factor_name,))

# 检查因子值数量
cursor.execute('SELECT COUNT(*) FROM factor_values WHERE factor_name = ?', (factor_name,))

# 检查表现数据
cursor.execute('SELECT rank_ic_mean, rank_icir, long_short_return FROM factor_performance WHERE factor_name = ?', (factor_name,))
```

---

### 数据格式一致性要求

所有因子写入数据库时必须遵循以下格式：

| 字段 | 格式 | 示例 |
|------|------|------|
| trade_date | DATE (YYYY-MM-DD) | 2010-02-26 |
| stock_code | VARCHAR (.SZ/.SH) | 000001.SZ |
| factor_value | DOUBLE (4位小数) | -1.3663 |
| factor_name | VARCHAR (Name_Abbr) | HolderNumChange_SNC |
| style | VARCHAR (英文) | sentiment/volatility/momentum |
| data_source | VARCHAR (英文) | market/shareholder |

**排序规则**: 按 trade_date 升序、stock_code 升序`

**输出示例**：
```
✅ 因子注册成功！

因子名称: 扎堆效应_SNC
存储路径: 04_因子注册/factors/扎堆效应_SNC/

已创建文件:
- factor.yaml (元信息)
- performance.yaml (表现记录)
- versions/v1.0/ (初始版本)
- CHANGELOG.md (版本记录)

数据库写入:
- factor_metadata: 1 条记录
- factor_values: 1020 条记录 (2015-01 至 2023-12)
- factor_performance: 1 条记录

INDEX.md 已更新，因子总数: 1
```

---

### 功能 2：因子查询 (factor-query)

按多维度检索因子信息。

**支持的查询方式**：

```bash
# 按名称查询（模糊匹配）
/factor-query 股东户数
"查找 股东户数 因子"

# 按分类查询
/factor-query 情绪类
"列出所有 情绪类 因子"
"列出 动量 因子"

# 按数据源查询
/factor-query 股东数据
"列出使用 股东数据 的因子"

# 按表现查询
/factor-query IC>5%
"IC > 5% 的因子有哪些"
"多空收益最高的因子"

# 按来源查询
/factor-query 开源证券
"开源证券 的研报复现了哪些因子"

# 综合查询
/factor-query 情绪类 AND IC>3%
```

**执行流程**：

```
1. 解析查询条件
   - 名称关键词 → 模糊匹配 factor.yaml 的 name/abbreviation
   - 分类条件 → 匹配 classification.style/data_source
   - 表现条件 → 匹配 performance.yaml 的 metrics
   - 来源条件 → 匹配 source.broker

2. 扫描因子库
   - 遍历 04_因子注册/factors/ 目录
   - 加载每个因子的 factor.yaml 和 performance.yaml

3. 筛选匹配结果
   - 应用查询条件过滤

4. 输出结果表格
   - 按指定列排序
   - 格式化输出
```

**输出示例**：
```
查询条件: 情绪类因子

找到 3 个匹配因子：

| 因子名称 | RankIC | ICIR | 多空收益 | 来源券商 | 版本 |
|----------|--------|------|----------|----------|------|
| 扎堆效应_股东户数变动 | 4.32% | 2.15 | 13.2% | 开源证券 | v1.2 |
| 机构持股变动 | 3.8% | 1.92 | 11.5% | 华泰证券 | v1.0 |
| 北向资金净流入 | 5.1% | 2.45 | 15.8% | 国泰君安 | v1.1 |

详细信息请查看: 04_因子注册/factors/{因子名}/
```

---

### 功能 3：因子对比 (factor-compare)

对比多个因子的表现。

**使用方式**：
```
/factor-compare 扎堆效应 机构持股变动 北向资金
"对比 扎堆效应 和 机构持股变动"
```

**执行流程**：

```
1. 加载指定因子数据
   - 读取 factor.yaml 和 performance.yaml

2. 生成对比表格
   - 核心指标对比（IC、ICIR、收益、回撤）
   - 分类对比（风格、数据源、频率）
   - 适用场景对比（股票池、调仓频率）

3. 如有因子值数据，计算相关性矩阵
   - 读取 output/factors/*.csv
   - 计算截面相关性

4. 输出对比报告
```

**输出示例**：
```markdown
## 因子对比报告

### 表现指标对比

| 指标 | 扎堆效应 | 机构持股变动 | 北向资金 |
|------|----------|--------------|----------|
| RankIC均值 | 4.32% | 3.8% | 5.1% |
| ICIR | 2.15 | 1.92 | 2.45 |
| 多空年化收益 | 13.2% | 11.5% | 15.8% |
| 最大回撤 | 3.2% | 4.1% | 2.8% |
| 换手率 | 24% | 35% | 18% |

### 分类对比

| 维度 | 扎堆效应 | 机构持股变动 | 北向资金 |
|------|----------|--------------|----------|
| 风格 | 情绪 | 情绪 | 情绪 |
| 数据源 | 股东 | 股东 | 另类 |
| 调仓频率 | 月频 | 月频 | 周频 |

### 适用场景

| 因子 | 推荐股票池 | 中性化方式 |
|------|-----------|-----------|
| 扎堆效应 | 全市场、中证1000 | 行业+市值 |
| 机构持股变动 | 沪深300、中证500 | 行业+市值 |
| 北向资金 | 沪深300 | 无需中性化 |

### 因子相关性

（如有因子值数据则显示）

| 因子 | 扎堆效应 | 机构持股 | 北向资金 |
|------|----------|----------|----------|
| 扎堆效应 | 1.00 | 0.65 | 0.32 |
| 机构持股 | 0.65 | 1.00 | 0.48 |
| 北向资金 | 0.32 | 0.48 | 1.00 |

建议：扎堆效应与北向资金相关性较低，适合组合使用。
```

---

### 功能 4：版本管理 (factor-version)

记录因子参数调整历史。

**使用方式**：
```
# 创建新版本
/factor-version 扎堆效应 --new --reason "参数优化" --changes "gap=5"

# 查看版本历史
/factor-version 扎堆效应 --history

# 对比版本
/factor-version 扎堆效应 --compare v1.0 v1.2

# 回退版本
/factor-version 扎堆效应 --rollback v1.0
```

**执行流程（创建新版本）**：

```
1. 读取当前版本信息
   - factor.yaml 获取 current_version

2. 计算新版本号
   - v1.2 → v1.3（小调整）
   - 或 v2.0（重大变更）

3. 创建版本目录
   - mkdir versions/v{新版本}/
   - 复制当前代码

4. 更新参数配置
   - params.yaml 应用变更

5. 记录变更日志
   - 更新 CHANGELOG.md
   - 记录变更内容、原因、效果

6. 更新元信息
   - factor.yaml 更新 current_version
   - 更新 last_updated

7. 重新计算表现（可选）
   - 运行回测获取新表现
   - 更新 performance.yaml
```

---

### 功能 5：报告生成 (factor-report)

生成因子库汇总报告。

**使用方式**：
```
/factor-report --monthly
/factor-report --comparison
"生成因子库月度报告"
```

**月度报告内容**：

```markdown
# 因子库月度报告

**报告日期**: 2026-04-10
**因子总数**: 8
**本月新增**: 2

---

## 分类统计

| 分类 | 数量 | 占比 |
|------|------|------|
| 情绪 | 3 | 37.5% |
| 动量 | 2 | 25.0% |
| 价值 | 2 | 25.0% |
| 质量 | 1 | 12.5% |

---

## 表现汇总

| 因子 | RankIC | ICIR | 多空收益 | 最大回撤 | 版本 |
|------|--------|------|----------|----------|------|
| 北向资金净流入 | 5.1% | 2.45 | 15.8% | 2.8% | v1.1 |
| 扎堆效应_股东户数变动 | 4.32% | 2.15 | 13.2% | 3.2% | v1.2 |
| ... | ... | ... | ... | ... | ... |

---

## 表现排名

### IC Top 3
1. 北向资金净流入 (5.1%)
2. 扎堆效应_股东户数变动 (4.32%)
3. 动量反转 (4.1%)

### 收益 Top 3
1. 北向资金净流入 (15.8%)
2. 扎堆效应_股东户数变动 (13.2%)
3. ...

---

## 本月更新

| 因子 | 更新内容 | 日期 |
|------|----------|------|
| 扎堆效应 | v1.2: 参数优化 gap=5 | 2026-04-10 |
| ... | ... | ... |

---

## 本月新增

| 因子 | 来源券商 | RankIC |
|------|----------|--------|
| 北向资金净流入 | 国泰君安 | 5.1% |
| ROE稳定性 | 华泰证券 | 3.5% |
```

---

## 📝 文件格式

### factor.yaml 模板

```yaml
# 因子元信息
name: "{因子名称}"
full_name: "{完整名称}"
abbreviation: "{缩写}"

# 分类
classification:
  style: "{style}"              # sentiment/value/momentum/quality/growth/technical/reversal
  style_cn: "{中文风格}"         # 情绪/价值/动量/质量/成长/技术/反转（用于展示）
  data_source: "{数据源}"      # 股东/财务/行情/另类
  frequency: "{频率}"          # 月频/周频/日频

# 来源
source:
  broker: "{券商}"
  report_title: "{研报标题}"
  report_date: "{研报日期}"
  report_path: "{解析文档路径}"

# 依赖
dependencies:
  data_fields:
    - "{字段1}"
    - "{字段2}"
  tables:
    - "{表1}"

# 当前版本
current_version: "v1.0"

# 标签
tags:
  - "{标签1}"
  - "{标签2}"

# 创建信息
created_at: "{日期}"
created_by: "Claude"
last_updated: "{日期}"

# 备注
notes: ""
```

### performance.yaml 模板

```yaml
# 回测表现
version: "v1.0"
backtest_period: "{起始} - {结束}"
rebalance_freq: "{频率}"
universe: "{股票池}"

# 核心指标
metrics:
  rank_ic_mean: {值}           # 单位：%
  rank_icir: {值}
  ic_positive_ratio: {值}      # 单位：%
  long_short_return: {值}      # 单位：%
  long_short_volatility: {值}
  max_drawdown: {值}           # 单位：%
  sharpe_ratio: {值}

# 分组表现
group_returns:
  group0: {值}
  group1: {值}
  group2: {值}
  group3: {值}
  group4: {值}

# 适用场景
suitability:
  recommended_universe: ["{股票池}"]
  recommended_freq: "{频率}"
  neutralization: "{中性化方式}"

# 数据来源
data_source: "{复现项目路径}"
verified_at: "{日期}"
```

### CHANGELOG.md 模板

```markdown
# {因子名称}版本变更记录

## v1.0 ({日期})

初始版本，复现研报原始参数

### 参数配置
- {参数1} = {值}
- {参数2} = {值}

### 表现数据
- RankIC: {值}%
- ICIR: {值}
- 多空收益: {值}%
```

---

## 🔄 与其他 Skill 的协作

### 与 report-parser 协作

```
report-parser 解析研报 → 02_解析输出/{因子名}.md
                    ↓
         用户可参考解析文档进行复现
```

### 与 factor-reproducer 协作

```
factor-reproducer 复现因子 → 03_复现项目/{因子名}_复现/
                           ↓
              factor-library-manager 注册因子入库
                           ↓
              04_因子注册/factors/{因子名}/
```

---

## 📋 使用示例

### 示例 1：注册因子

```
用户: 把扎堆效应因子加入因子库

执行:
1. 搜索 03_复现项目/扎堆效应_复现/
2. 读取 config.yaml、output/backtest/
3. 创建 04_因子注册/factors/扎堆效应_股东户数变动/
4. 生成 factor.yaml、performance.yaml
5. 更新 INDEX.md

输出: ✅ 因子注册成功！因子总数: 1
```

### 示例 2：查询因子

```
用户: 列出所有情绪类因子

执行:
1. 遍历 04_因子注册/factors/
2. 筛选 classification.style == "情绪"
3. 输出表格

输出: 找到 3 个情绪类因子...
```

### 示例 3：对比因子

```
用户: 对比扎堆效应和机构持股变动

执行:
1. 加载两个因子的 factor.yaml、performance.yaml
2. 生成对比表格
3. 如有数据则计算相关性

输出: 因子对比报告...
```

---

## ⚠️ 注意事项

1. **因子库位置**: 默认在 `04_因子注册/` 目录，与 `研报解析/` 同级
2. **版本命名**: 小调整递增小版本号(v1.1)，重大变更递增大版本号(v2.0)
3. **表现数据来源**: 优先从复现项目 output/ 读取，其次从解析文档提取
4. **INDEX.md 维护**: 每次注册/更新因子后自动更新总索引
5. **分类标准**: 参考 classification/ 目录下的 yaml 定义，**统一使用英文风格分类**
6. **[!!] 数据库操作纪律**: **严禁修改 postgres 库，只允许读取。所有写入操作必须写入 aigenfactor 数据库**
7. **[!!] 默认同步数据库**: **INDEX.md 更新完成后，自动执行数据库同步，无需用户额外确认**
8. **因子值精度**: 存入数据库时默认保留 **4位小数**，避免浮点精度问题
9. **排序规则**: 因子入库时默认按 **date升序、code升序** 排序，便于查询和一致性
10. **批量写入**: 使用 `execute_values` 批量插入，chunk_size=50000，提升效率
11. **脚本位置**: 数据库同步脚本位于 `04_因子注册/scripts/sync_to_db.py`（通用脚本）

---

## 风格分类映射表

| English | Chinese | 描述 |
|---------|---------|------|
| sentiment | 情绪 | 反映市场参与者情绪和行为 |
| value | 价值 | 基于基本面估值指标 |
| momentum | 动量 | 反映价格趋势和惯性 |
| quality | 质量 | 反映公司财务质量 |
| growth | 成长 | 反映公司成长潜力 |
| technical | 技术 | 基于技术指标和形态 |
| reversal | 反转 | 反映价格反转信号 |

---

## 🚀 快速开始

1. 确保因子库目录结构已创建
2. 复现完因子后调用统一脚本注册：
   ```bash
   python 04_因子注册/scripts/sync_to_db.py --project "03_复现项目/{项目名}_复现"
   ```
3. 使用 `/factor-query {条件}` 查询因子
4. 使用 `/factor-compare {因子1} {因子2}` 对比
5. 定期使用 `/factor-report --monthly` 生成报告

**⚠️ 注意：严禁在复现项目 code 目录创建新的数据库写入脚本，统一使用 `04_因子注册/scripts/sync_to_db.py`**

---

## 🗄️ 数据库存储（默认自动同步）

**⚠️ 重要变更**: 因子注册后**默认自动同步**到 aigenfactor 数据库，无需用户额外确认。

### 自动同步触发条件

- INDEX.md 更新完成 → **直接调用统一脚本**：`python 04_因子注册/scripts/sync_to_db.py --project "03_复现项目/{项目名}_复现"`
- **禁止检查或创建复现项目 code 目录下的 write_to_db.py**
- **禁止在复现项目创建新的数据库写入脚本**

### 数据库配置

- **数据库名**: `aigenfactor`（**严禁写入 postgres**）
- **连接配置**:
  ```python
  DB_CONFIG = {
      'host': '2.tcp.nas.cpolar.cn',
      'port': 14983,
      'user': 'wind_user',
      'password': '2024_user_wind',
      'database': 'aigenfactor'  # 只写入此库
  }
  ```

### 表结构设计

#### factor_metadata（因子元信息表）

| 字段 | 类型 | 说明 |
|------|------|------|
| factor_name | VARCHAR | 因子名称（主键）|
| full_name | VARCHAR | 完整名称 |
| abbreviation | VARCHAR | 缩写 |
| style | VARCHAR | 风格（英文）|
| data_source | VARCHAR | 数据源 |
| frequency | VARCHAR | 调仓频率 |
| broker | VARCHAR | 券商来源 |
| report_title | VARCHAR | 研报标题 |
| report_date | DATE | 研报日期 |
| created_at | TIMESTAMP | 创建时间 |

#### factor_values（因子值表）

| 字段 | 类型 | 说明 |
|------|------|------|
| trade_date | DATE | 交易日期 |
| stock_code | VARCHAR | 股票代码 |
| factor_value | DOUBLE | 因子值（4位小数）|
| factor_name | VARCHAR | 因子名称（外键）|

**查询示例**：
```sql
-- 获取某日期的因子截面
SELECT * FROM factor_values WHERE trade_date = '2023-12-29';

-- 获取某因子的时间序列
SELECT trade_date, factor_value FROM factor_values 
WHERE stock_code = '000001.SZ' AND factor_name = 'HolderNumChange_SNC';

-- 获取某因子所有数据量
SELECT COUNT(*) FROM factor_values WHERE factor_name = 'LongMomentum_LM2';
```

#### factor_performance（因子表现表）

| 字段 | 类型 | 说明 |
|------|------|------|
| factor_name | VARCHAR | 因子名称 |
| backtest_start | DATE | 回测起始 |
| backtest_end | DATE | 回测结束 |
| rank_ic_mean | DECIMAL | IC均值（%）|
| rank_icir | DECIMAL | ICIR（月度）|
| ic_positive_ratio | DECIMAL | IC为正占比（%）|
| long_short_return | DECIMAL | 多空收益（%）|
| group0_return ~ group4_return | DECIMAL | 分组收益（%）|
| verified_at | DATE | 验证日期 |

---

### 数据库同步工作流

```
因子注册 → INDEX.md更新 → 直接调用统一脚本
                           ↓
                 python 04_因子注册/scripts/sync_to_db.py --project "03_复现项目/{项目名}_复现"
                           ↓
                 1. 连接 aigenfactor 数据库
                           ↓
                 2. 清理已存在的因子数据
                           ↓
                 3. 批量写入 factor_metadata
                           ↓
                 4. 批量写入 factor_values (chunk_size=50000)
                           ↓
                 5. 写入 factor_performance
                           ↓
                 6. 验证写入结果
                           ↓
                 7. 输出同步报告
```

**⚠️ 严禁在复现项目 code 目录创建新的数据库写入脚本**

---

### 已入库因子示例（2026-04-15）

| 因子名 | 记录数 | IC | ICIR | L/S |
|--------|--------|----|----|-----|
| HolderNumChange_SNC | 454,647 | 5.59% | 0.58 | 16.17% |
| PerCapitaRatio_PCRC | 454,647 | 5.49% | 0.58 | 15.00% |
| LongMomentum_LM1 | 328,271 | 3.75% | 0.35 | 11.43% |
| LongMomentum_LM2 | 326,286 | 6.96% | 0.62 | 17.78% |
| HighAmplitude_VH | 325,234 | 9.58% | 0.71 | 17.01% |
| IdealAmplitude_VI | 325,234 | 8.29% | 1.08 | 21.39% |
| Amplitude_AMP | 327,643 | 7.70% | 0.45 | 9.85% |
| LowAmplitude_VL | 325,234 | -2.27% | -0.14 | 3.08% |

**factor_values 总记录数**: 3,641,268 条
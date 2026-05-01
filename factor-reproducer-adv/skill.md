---
name: factor-reproducer-adv
description: Advanced factor reproduction skill using adapter pattern with MultiFactorModel integration. Use for reproducing factor strategies with lower deviation through unified deviation control mechanisms.
---

# 金融工程因子复现器（高级版）

基于适配器模式，复用 MultiFactorModel 模块，实现低偏差因子复现。

---

## 📁 工作目录结构

复现工作在以下目录结构中进行：

```
研报解析/
│
├── 02_解析输出/                    # 输入来源（report-parser输出）
│   └── {标题简称}/
│       └── {标题简称}.md           # 解析文档
│
├── 03_复现项目/
│   ├── code_template/             # 适配层（框架提供）
│   │   ├── factor_adapter.py      # 适配层核心
│   │   ├── config_parser.py       # 配置解析
│   │   ├── deviation_checker.py   # 偏差检查
│   │   ├── chart_generator.py     # 图表生成
│   │   └── __init__.py
│   │
│   └── {标题简称}_复现/
│       ├── config.yaml            # 复现配置
│       ├── code/
│       │   ├── factor_definition.py   # 因子计算
│       │   └── run_backtest.py        # 执行脚本
│       └── output/
│           ├── factors/
│           ├── backtest/
│           └── figures/
│
└── MultiFactorModel/              # 外部依赖
    ├── factor_analysis.py         # FactorTest
    └── data_preprocessing.py      # FilterAbnormalStock
```

---

## 🔄 复现工作流程

### Step 1: 输入验证

**检查解析文档完整性**，必须包含：

| 必需内容 | 检查项 |
|----------|--------|
| **数据源说明** | 数据源表格存在 |
| **因子计算步骤** | 步骤完整，输入输出清晰 |
| **计算公式** | 公式代码块存在 |
| **参数值** | 默认参数已标注 |
| **回测表现数据** | RankIC、多空收益等数值 |

---

### Step 2: 项目初始化

创建复现项目目录：

```bash
mkdir -p 03_复现项目/{标题简称}_复现/{code,output/{factors,backtest,figures}}
```

---

### Step 3: 生成 config.yaml

从解析文档提取配置。

---

### Step 4: 生成 factor_definition.py

翻译研报计算步骤为函数。

---

### Step 5: 生成 run_backtest.py

配置驱动执行脚本。

---

### Step 6: 用户执行

```bash
python run_backtest.py
```

---

## 📊 偏差控制机制

| 偏差来源 | 适配层处理 |
|----------|------------|
| 时间对齐 | align_time() 交易日月末映射 |
| 负向因子方向 | run_backtest() IC均值自动判断 |
| 股票池过滤 | FilterAbnormalStock 类 |
| 偏差检查 | DeviationChecker 配置驱动 |

---

## ⚠️ 关键注意事项

### 1. 数据格式
因子数据必须为 MultiIndex DataFrame，index 为 (date, code)。

### 2. 因子方向自动判断
根据 IC 均值判断：IC<0 为负向因子。

### 3. 股票池过滤
调用 MultiFactorModel.data_preprocessing.FilterAbnormalStock 类。

---

## 📦 依赖要求

| 模块 | 依赖 | 说明 |
|------|------|------|
| ConfigParser | yaml | 无外部依赖 |
| DeviationChecker | pandas, numpy | 无外部依赖 |
| FactorAdapter | **statsmodels** | MultiFactorModel 依赖 |

**安装依赖**：
```bash
pip install statsmodels pandas numpy pyyaml
```

---

*本skill使用适配器模式复用MultiFactorModel模块*
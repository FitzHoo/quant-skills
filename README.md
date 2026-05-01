# Quant Skills for Claude Code

量化金融工程技能集合，用于Claude Code CLI。

## 安装方法

### 方式一：克隆到用户skills目录

```bash
# 克隆仓库
git clone https://github.com/HooH/quant-skills.git

# 复制skill到Claude配置目录
cp -r quant-skills/report-parser ~/.claude/skills/
```

### 方式二：直接引用（推荐）

在Claude Code中使用Skill工具时，可以通过路径引用：

```
/userSettings:report-parser
```

## Skills列表

### report-parser

**描述**: 解析金融工程研报（PDF格式）为结构化markdown文档，优化因子复现所需的可操作性信息。

**使用场景**:
- 用户提到"解析研报"、"分析PDF"、"提取因子信息"
- 处理金融工程、量化策略相关PDF文档

**调用方式**:
```
/report-parser {PDF路径}
```

**输出**:
- 结构化markdown文档（包含数据源、因子计算步骤、回测结果等）
- 自动生成config.yaml配置（用于因子复现）

### factor-reproducer-adv

**描述**: 高级因子复现器，基于适配器模式复用MultiFactorModel模块，实现低偏差因子复现。

**使用场景**:
- 用户提到"复现因子"、"因子复现"、"回测因子"
- 基于report-parser解析结果进行因子实现

**调用方式**:
```
/factor-reproducer-adv {解析文档路径}
```

**特点**:
- 适配器模式，复用现有回测框架
- 偏差控制机制，确保复现结果接近研报
- 自动生成复现报告和图表

### factor-library-manager

**描述**: 因子库管理器，系统化管理已复现的量化因子，解决分类混乱、表现缺失、版本无记录问题。

**使用场景**:
- 用户提到"注册因子"、"因子入库"、"管理因子"
- 复现完成后将因子加入因子库

**调用方式**:
```
/factor-library-manager 注册因子
/factor-query {查询条件}
/factor-compare {因子1} {因子2}
```

**功能**:
- 因子注册（自动同步数据库）
- 因子查询（多维度检索）
- 因子对比（表现相关性分析）
- 版本管理（参数调整记录）
- 报告生成（月度汇总）

## 配置说明

数据库连接使用 `.env` 文件配置，避免密码泄露。

```bash
# 复制模板文件
cp factor-library-manager/database/.env.example .env

# 编辑配置（填写实际数据库信息）
# DB_HOST=localhost
# DB_PORT=5432
# DB_USER=your_username
# DB_PASSWORD=your_password
# DB_NAME=aigenfactor
```

**安装依赖**（可选，用于自动加载.env）：
```bash
pip install python-dotenv
```

**注意**：`.env` 文件已在 `.gitignore` 中排除，不会被提交到仓库。

## 同步更新

```bash
# 更新本地skills
cd quant-skills
git pull

# 同步到Claude配置目录
cp -r report-parser ~/.claude/skills/
```

## 目录结构

```
quant-skills/
├── README.md               # 本文件
├── report-parser/          # 研报解析skill
│   └── skill.md            # Skill定义文件
├── factor-reproducer-adv/  # 高级因子复现skill
│   └── skill.md            # Skill定义文件
├── factor-library-manager/ # 因子库管理skill
│   ├── skill.md            # Skill定义文件
│   ├── classification/     # 分类体系定义
│   ├── database/           # 数据库配置
│   └── templates/          # 模板文件
└── [其他skills]/           # 未来可添加更多
```

## 贡献

欢迎添加新的量化金融技能。每个skill需要：
1. `skill.md` - 核心定义文件
2. 符合Claude Code skill格式规范

---

**作者**: FitzHoo
**更新日期**: 2026-05-01

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
├── README.md           # 本文件
├── report-parser/      # 研报解析skill
│   ├── skill.md        # Skill定义文件
│   └── evals/          # 评估测试（可选）
└── [其他skills]/       # 未来可添加更多
```

## 贡献

欢迎添加新的量化金融技能。每个skill需要：
1. `skill.md` - 核心定义文件
2. 符合Claude Code skill格式规范

---

**作者**: HooH
**更新日期**: 2026-05-01更新日期: 2026-05-01

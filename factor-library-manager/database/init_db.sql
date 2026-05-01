-- 因子库数据库初始化脚本
-- 数据库名: aigenfactor

-- ============================================
-- 1. 创建数据库（需要管理员权限）
-- ============================================
-- CREATE DATABASE aigenfactor;

-- ============================================
-- 2. 因子元信息表
-- ============================================
CREATE TABLE IF NOT EXISTS factor_metadata (
    id SERIAL PRIMARY KEY,
    factor_name VARCHAR(100) NOT NULL UNIQUE,      -- 因子名称（英文格式，如HolderNumChange_SNC）
    full_name VARCHAR(200),                         -- 完整名称（中文）
    abbreviation VARCHAR(20),                       -- 缩写
    style VARCHAR(20),                              -- 风格分类（英文）：sentiment/value/momentum/quality/growth/technical
    data_source VARCHAR(20),                        -- 数据源（英文）：shareholder/financial/market/alternative
    frequency VARCHAR(10),                          -- 频率：monthly/weekly/daily
    broker VARCHAR(50),                             -- 来源券商（中文）
    report_title VARCHAR(200) NOT NULL,             -- 研报标题（必填）
    report_date DATE,                               -- 研报日期
    notes TEXT NOT NULL,                            -- 备注（必填）
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE factor_metadata IS '因子元信息表';
COMMENT ON COLUMN factor_metadata.factor_name IS '因子唯一标识（英文格式：Name_Abbr）';
COMMENT ON COLUMN factor_metadata.style IS '风格分类（英文）：sentiment/value/momentum/quality/growth/technical';
COMMENT ON COLUMN factor_metadata.data_source IS '数据源（英文）：shareholder/financial/market/alternative';

-- ============================================
-- 3. 因子值表（核心表）
-- ============================================
CREATE TABLE IF NOT EXISTS factor_values (
    id BIGSERIAL PRIMARY KEY,
    trade_date DATE NOT NULL,                       -- 交易日期
    stock_code VARCHAR(20) NOT NULL,                -- 股票代码
    factor_name VARCHAR(100) NOT NULL,              -- 因子名称
    factor_value DOUBLE PRECISION,                  -- 因子值
    version VARCHAR(10) DEFAULT 'v1.0',             -- 版本号
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- 唯一约束：同一日期、股票、因子、版本只能有一条记录
    CONSTRAINT uk_factor_value UNIQUE (trade_date, stock_code, factor_name, version)
);

-- 创建索引加速查询
CREATE INDEX IF NOT EXISTS idx_factor_values_date ON factor_values(trade_date);
CREATE INDEX IF NOT EXISTS idx_factor_values_stock ON factor_values(stock_code);
CREATE INDEX IF NOT EXISTS idx_factor_values_factor ON factor_values(factor_name);
CREATE INDEX IF NOT EXISTS idx_factor_values_version ON factor_values(version);

COMMENT ON TABLE factor_values IS '因子值表（长格式）';
COMMENT ON COLUMN factor_values.trade_date IS '交易日期（调仓日）';
COMMENT ON COLUMN factor_values.stock_code IS '股票代码（Wind格式）';
COMMENT ON COLUMN factor_values.factor_name IS '因子名称，关联factor_metadata';
COMMENT ON COLUMN factor_values.factor_value IS '因子值（已标准化/中性化）';
COMMENT ON COLUMN factor_values.version IS '版本号，支持参数调整后的版本管理';

-- ============================================
-- 4. 因子表现表
-- ============================================
CREATE TABLE IF NOT EXISTS factor_performance (
    id SERIAL PRIMARY KEY,
    factor_name VARCHAR(100) NOT NULL,              -- 因子名称
    version VARCHAR(10) DEFAULT 'v1.0',             -- 版本号
    backtest_start DATE,                            -- 回测起始日期
    backtest_end DATE,                              -- 回测结束日期
    universe VARCHAR(50),                           -- 股票池
    rebalance_freq VARCHAR(20),                     -- 调仓频率
    rank_ic_mean DOUBLE PRECISION,                  -- RankIC均值（%）
    rank_icir DOUBLE PRECISION,                     -- RankICIR
    ic_positive_ratio DOUBLE PRECISION,             -- IC为正占比（%）
    long_short_return DOUBLE PRECISION,             -- 多空年化收益（%）
    long_short_volatility DOUBLE PRECISION,         -- 多空年化波动（%）
    max_drawdown DOUBLE PRECISION,                  -- 最大回撤（%）
    sharpe_ratio DOUBLE PRECISION,                  -- 夏普比率
    turnover DOUBLE PRECISION,                      -- 换手率（%）
    group0_return DOUBLE PRECISION,                 -- 第0组年化收益
    group1_return DOUBLE PRECISION,                 -- 第1组年化收益
    group2_return DOUBLE PRECISION,                 -- 第2组年化收益
    group3_return DOUBLE PRECISION,                 -- 第3组年化收益
    group4_return DOUBLE PRECISION,                 -- 第4组年化收益
    benchmark_ic DOUBLE PRECISION,                  -- 研报基准IC
    benchmark_return DOUBLE PRECISION,              -- 研报基准收益
    verified_at DATE,                               -- 验证日期
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uk_factor_performance UNIQUE (factor_name, version)
);

COMMENT ON TABLE factor_performance IS '因子表现统计表';

-- ============================================
-- 5. 因子版本变更记录表
-- ============================================
CREATE TABLE IF NOT EXISTS factor_changelog (
    id SERIAL PRIMARY KEY,
    factor_name VARCHAR(100) NOT NULL,              -- 因子名称
    version VARCHAR(10) NOT NULL,                   -- 版本号
    change_type VARCHAR(20),                        -- 变更类型：param_tune/logic_update/bug_fix
    change_description TEXT,                        -- 变更描述
    old_params JSONB,                               -- 旧参数
    new_params JSONB,                               -- 新参数
    old_performance JSONB,                          -- 旧表现
    new_performance JSONB,                          -- 新表现
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE factor_changelog IS '因子版本变更记录表';

-- ============================================
-- 6. 视图：因子值宽格式（便于矩阵计算）
-- ============================================
CREATE OR REPLACE VIEW v_factor_matrix AS
SELECT
    trade_date,
    stock_code,
    MAX(CASE WHEN factor_name = '扎堆效应_SNC' THEN factor_value END) AS snc,
    MAX(CASE WHEN factor_name = '扎堆效应_PCRC' THEN factor_value END) AS pcrc,
    MAX(CASE WHEN factor_name = '长端动量' THEN factor_value END) AS long_mom
    -- 新增因子时需要手动添加列
FROM factor_values
WHERE version = 'v1.0'  -- 默认取最新版本
GROUP BY trade_date, stock_code
ORDER BY trade_date, stock_code;

COMMENT ON VIEW v_factor_matrix IS '因子值宽格式视图，用于矩阵计算';

-- ============================================
-- 7. 常用查询函数
-- ============================================

-- 获取指定日期的因子值截面
CREATE OR REPLACE FUNCTION get_factor截面(p_date DATE, p_factor VARCHAR DEFAULT NULL)
RETURNS TABLE(
    stock_code VARCHAR,
    factor_name VARCHAR,
    factor_value DOUBLE PRECISION
) AS $$
BEGIN
    RETURN QUERY
    SELECT fv.stock_code, fv.factor_name, fv.factor_value
    FROM factor_values fv
    WHERE fv.trade_date = p_date
      AND fv.version = 'v1.0'
      AND (p_factor IS NULL OR fv.factor_name = p_factor)
    ORDER BY fv.stock_code;
END;
$$ LANGUAGE plpgsql;

-- 获取因子时间序列
CREATE OR REPLACE FUNCTION get_factor_ts(p_stock VARCHAR, p_factor VARCHAR)
RETURNS TABLE(
    trade_date DATE,
    factor_value DOUBLE PRECISION
) AS $$
BEGIN
    RETURN QUERY
    SELECT fv.trade_date, fv.factor_value
    FROM factor_values fv
    WHERE fv.stock_code = p_stock
      AND fv.factor_name = p_factor
      AND fv.version = 'v1.0'
    ORDER BY fv.trade_date;
END;
$$ LANGUAGE plpgsql;
-- RisingWave optimized schema for blockchain data
CREATE TABLE IF NOT EXISTS block_meta (
    id          VARCHAR NOT NULL PRIMARY KEY,
    at          TIMESTAMP WITH TIME ZONE,
    number      BIGINT,
    hash        VARCHAR,
    parent_hash VARCHAR,
    timestamp   TIMESTAMP WITH TIME ZONE
);

-- Cursor table for Substreams state management
CREATE TABLE IF NOT EXISTS cursors (
    id         VARCHAR NOT NULL PRIMARY KEY,
    cursor     VARCHAR,
    block_num  BIGINT,
    block_id   VARCHAR
);

-- Optional: Create a materialized view for real-time analytics
-- This demonstrates RisingWave's streaming capabilities
CREATE MATERIALIZED VIEW IF NOT EXISTS block_stats AS
SELECT 
    DATE_TRUNC('hour', timestamp) as hour,
    COUNT(*) as block_count,
    MIN(number) as min_block,
    MAX(number) as max_block,
    AVG(EXTRACT(EPOCH FROM (timestamp - LAG(timestamp) OVER (ORDER BY number)))) as avg_block_time
FROM block_meta
GROUP BY DATE_TRUNC('hour', timestamp); 
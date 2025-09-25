CREATE TABLE IF NOT EXISTS block_meta (
    "id" VARCHAR(64),
    "at" VARCHAR(64),
    "number" UInt64,
    "hash" VARCHAR(64),
    "parent_hash" VARCHAR(64),
    "timestamp" VARCHAR(64),
) ENGINE = MergeTree PRIMARY KEY ("id");

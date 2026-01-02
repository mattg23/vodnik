
CREATE TABLE series (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    storage_type TEXT NOT NULL,
    block_len INTEGER NOT NULL,
    block_res TEXT NOT NULL,
    sample_len INTEGER NOT NULL,
    sample_res TEXT NOT NULL,
    first INTEGER NOT NULL,
    last INTEGER NOT NULL,
    labels TEXT NOT NULL
);


CREATE TABLE blocks (
    series_id INTEGER NOT NULL,
    block_id INTEGER NOT NULL,

    count_non_missing INTEGER NOT NULL,
    count_valid INTEGER NOT NULL,

    sum_val BLOB,       -- Serialized Accumulator (i128/u128/f64)
    min_val NUMERIC,    
    max_val NUMERIC,

    fst_valid_val NUMERIC,
    fst_valid_q INTEGER, 
    fst_valid_offset INTEGER,

    lst_valid_val NUMERIC,
    lst_valid_q INTEGER,
    lst_valid_offset INTEGER,

    fst_val NUMERIC,
    fst_q INTEGER,
    fst_offset INTEGER,

    lst_val NUMERIC,
    lst_q INTEGER,
    lst_offset INTEGER,

    qual_acc_or INTEGER NOT NULL,
    qual_acc_and INTEGER NOT NULL,

    -- Storage Pointer & System Meta
    object_key TEXT NOT NULL, 
    created_at INTEGER NOT NULL DEFAULT (unixepoch()),

    PRIMARY KEY (series_id, block_id)
) WITHOUT ROWID;

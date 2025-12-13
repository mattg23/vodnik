
CREATE TABLE series (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    storage_type TEXT NOT NULL,
    block_len INTEGER NOT NULL,
    block_res TEXT NOT NULL,
    first INTEGER NOT NULL,
    last INTEGER NOT NULL,
    labels TEXT NOT NULL
);

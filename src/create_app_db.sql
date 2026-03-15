CREATE TABLE events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    api_token TEXT NOT NULL,
    name TEXT,
    date TEXT,
    owner TEXT NOT NULL,
    is_local BOOLEAN DEFAULT 1,
    CONSTRAINT events_unique0 UNIQUE (api_token)
);

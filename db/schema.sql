-- Enable foreign key constraints
PRAGMA foreign_keys = ON;

-- Profiles table
CREATE TABLE IF NOT EXISTS processed_profiles (
    twitter_handle TEXT PRIMARY KEY COLLATE NOCASE,
    first_discovered_date TEXT NOT NULL,
    last_updated_date TEXT NOT NULL,
    notion_page_id TEXT,
    category TEXT CHECK(category IN ('Project', 'Profile')),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Source relationships table
CREATE TABLE IF NOT EXISTS source_relationships (
    twitter_handle TEXT COLLATE NOCASE,
    discovered_by_handle TEXT COLLATE NOCASE,
    discovery_date TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (twitter_handle, discovered_by_handle),
    FOREIGN KEY (twitter_handle) REFERENCES processed_profiles(twitter_handle)
);

-- Indices
CREATE INDEX idx_profiles_category ON processed_profiles(category);
CREATE INDEX idx_profiles_last_updated ON processed_profiles(last_updated_date);
CREATE INDEX idx_relationships_discovered_by ON source_relationships(discovered_by_handle); 

import argparse
import sqlite3
from pathlib import Path


def _needs_migration(conn: sqlite3.Connection) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='processed_profiles'")
    row = cur.fetchone()
    create_sql = (row[0] if row and row[0] else "") or ""
    return "collate nocase" not in create_sql.lower()


def migrate(db_path: Path) -> None:
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    try:
        if not _needs_migration(conn):
            print(f"Already migrated: {db_path}")
            return

        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM processed_profiles")
        old_profile_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM source_relationships")
        old_relationship_count = cur.fetchone()[0]

        print(f"Migrating to case-insensitive handles (COLLATE NOCASE): {db_path}")

        cur.execute("PRAGMA foreign_keys = OFF")
        cur.execute("BEGIN IMMEDIATE")

        cur.execute("DROP TABLE IF EXISTS processed_profiles_tmp")
        cur.execute("DROP TABLE IF EXISTS source_relationships_tmp")

        cur.execute(
            """
            CREATE TABLE processed_profiles_tmp (
                twitter_handle TEXT PRIMARY KEY COLLATE NOCASE,
                first_discovered_date TEXT NOT NULL,
                last_updated_date TEXT NOT NULL,
                notion_page_id TEXT,
                category TEXT CHECK(category IN ('Project', 'Profile')),
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        cur.execute(
            """
            INSERT INTO processed_profiles_tmp (
                twitter_handle,
                first_discovered_date,
                last_updated_date,
                notion_page_id,
                category,
                created_at
            )
            WITH canonical AS (
                SELECT
                    lower(twitter_handle) AS handle_key,
                    first_discovered_date,
                    last_updated_date,
                    notion_page_id,
                    category,
                    created_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY lower(twitter_handle)
                        ORDER BY
                            (notion_page_id IS NOT NULL) DESC,
                            first_discovered_date ASC,
                            last_updated_date DESC,
                            rowid DESC
                    ) AS rn
                FROM processed_profiles
            ),
            agg AS (
                SELECT
                    handle_key,
                    MIN(first_discovered_date) AS first_discovered_date,
                    MAX(last_updated_date) AS last_updated_date,
                    CASE
                        WHEN SUM(CASE WHEN lower(COALESCE(category, '')) = 'profile' THEN 1 ELSE 0 END) > 0 THEN 'Profile'
                        WHEN SUM(CASE WHEN lower(COALESCE(category, '')) = 'project' THEN 1 ELSE 0 END) > 0 THEN 'Project'
                        ELSE NULL
                    END AS category,
                    MIN(created_at) AS created_at
                FROM canonical
                GROUP BY handle_key
            ),
            pick AS (
                SELECT handle_key, notion_page_id
                FROM canonical
                WHERE rn = 1
            )
            SELECT
                agg.handle_key AS twitter_handle,
                agg.first_discovered_date,
                agg.last_updated_date,
                pick.notion_page_id,
                agg.category,
                agg.created_at
            FROM agg
            LEFT JOIN pick USING(handle_key)
            """
        )

        cur.execute(
            """
            CREATE TABLE source_relationships_tmp (
                twitter_handle TEXT COLLATE NOCASE,
                discovered_by_handle TEXT COLLATE NOCASE,
                discovery_date TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (twitter_handle, discovered_by_handle),
                FOREIGN KEY (twitter_handle) REFERENCES processed_profiles_tmp(twitter_handle)
            )
            """
        )

        cur.execute(
            """
            INSERT INTO source_relationships_tmp (
                twitter_handle,
                discovered_by_handle,
                discovery_date,
                created_at
            )
            SELECT
                lower(sr.twitter_handle) AS twitter_handle,
                lower(sr.discovered_by_handle) AS discovered_by_handle,
                MIN(sr.discovery_date) AS discovery_date,
                MIN(sr.created_at) AS created_at
            FROM source_relationships sr
            GROUP BY lower(sr.twitter_handle), lower(sr.discovered_by_handle)
            """
        )

        cur.execute("DROP TABLE source_relationships")
        cur.execute("DROP TABLE processed_profiles")
        cur.execute("ALTER TABLE processed_profiles_tmp RENAME TO processed_profiles")
        cur.execute("ALTER TABLE source_relationships_tmp RENAME TO source_relationships")

        cur.execute("CREATE INDEX IF NOT EXISTS idx_profiles_category ON processed_profiles(category)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_profiles_last_updated ON processed_profiles(last_updated_date)")
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_relationships_discovered_by ON source_relationships(discovered_by_handle)"
        )

        conn.commit()

        cur.execute("SELECT COUNT(*) FROM processed_profiles")
        new_profile_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM source_relationships")
        new_relationship_count = cur.fetchone()[0]

        print(
            "Migration complete. "
            f"profiles: {old_profile_count} -> {new_profile_count}, "
            f"relationships: {old_relationship_count} -> {new_relationship_count}"
        )
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate twitter_handles to be case-insensitive in SQLite DB.")
    parser.add_argument(
        "--db",
        default=str(Path("db") / "twitter_profiles.db"),
        help="Path to SQLite DB (default: db/twitter_profiles.db)",
    )
    args = parser.parse_args()
    migrate(Path(args.db))


if __name__ == "__main__":
    main()


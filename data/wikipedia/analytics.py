import duckdb

con = duckdb.connect()

# Read the space-separated file with explicit column names
con.execute(
    """
CREATE TABLE pageviews AS
SELECT * FROM read_csv(
    '/Users/marlenebargou/Downloads/hourly-pageviews-20251001-000000.tsv',  -- your file name
    delim=' ',
    columns={'project': 'VARCHAR', 'page_title': 'VARCHAR', 'views': 'INTEGER', 'bytes': 'INTEGER'},
    header=False
);
"""
)

# Check sample rows
print(con.execute("SELECT * FROM pageviews LIMIT 5").fetchdf())

# Count how many rows total
print(con.execute("SELECT COUNT(*) FROM pageviews").fetchone())

# Example: which project codes exist and how many pages per project
print(
    con.execute(
        """
SELECT project, COUNT(*) AS pages
FROM pageviews
GROUP BY project
ORDER BY pages DESC
LIMIT 10;
"""
    ).fetchdf()
)

# Example: most viewed pages overall
print(
    con.execute(
        """
SELECT project, page_title, SUM(views) AS total_views
FROM pageviews
GROUP BY project, page_title
ORDER BY total_views DESC
LIMIT 10;
"""
    ).fetchdf()
)

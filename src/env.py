import os

studentdb_dsn = os.getenv(
    "STUDENTDB_DSN",
    "host=127.0.0.1 dbname=studentdb user=student password=student"
)

sparkifydb_dsn = os.getenv(
    "SPARKIFYDB_DSN",
    "host=127.0.0.1 dbname=sparkifydb user=student password=student"
)

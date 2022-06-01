import os
from dotenv import load_dotenv

load_dotenv('.env.development')

print(os.environ.get("CLICKHOUSE_DB_URL"))
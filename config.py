import os



DB_IP = os.getenv('DB_IP')

PGUSER = os.getenv('PGUSER')
PGPASSWORD = os.getenv('PGPASSWORD')
NAME_DATABASE = os.getenv('NAME_DATABASE')

POSTGRES_URI = f"postgresql://{PGUSER}:{PGPASSWORD}@{DB_IP}/{NAME_DATABASE}"
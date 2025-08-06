import pyodbc
import os
from dotenv import load_dotenv

load_dotenv("env/claves.env")

server = os.getenv("MSSQL_SERVER")
database = os.getenv("MSSQL_DB")
username = os.getenv("MSSQL_USER")
password = os.getenv("MSSQL_PASSWORD")

try:
    conn_str = f"DRIVER={{ODBC Driver 18 for SQL Server}}; SERVER={server}; DATABASE={database};UID={username};PWD={password}; TrustServerCertificate=yes;"
    conn = pyodbc.connect(conn_str)
    print("✅ Conexión exitosa a la base de datos")
    conn.close()
except Exception as e:
    print("❌ Error de conexión:", e)

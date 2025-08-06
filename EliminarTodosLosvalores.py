import pyodbc
import os
from dotenv import load_dotenv

load_dotenv("env/claves.env")

server = os.getenv("MSSQL_SERVER")
database = os.getenv("MSSQL_DB")
username = os.getenv("MSSQL_USER")
password = os.getenv("MSSQL_PASSWORD")

conn_str = f"DRIVER={{ODBC Driver 18 for SQL Server}}; SERVER={server}; DATABASE={database};UID={username};PWD={password}; TrustServerCertificate=yes;"
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

tablas = ["ActividadesCobros", "Pagos", "Deudas", "Cuentas", "Clientes"]

for tabla in tablas:
    try:
        cursor.execute(f"DELETE FROM [pruebabanco].[dbo].[{tabla}];")
        print(f"✅ Registros eliminados de {tabla}")
    except Exception as e:
        print(f"⚠ No se pudo borrar {tabla}: {e}")


# Confirmar cambios
conn.commit()

print("✅ Se eliminaron todos los registros de las tablas en pruebabanco")

# Cerrar conexión
cursor.close()
conn.close()

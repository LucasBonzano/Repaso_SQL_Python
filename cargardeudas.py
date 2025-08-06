import os
import pandas as pd
import pyodbc
from dotenv import load_dotenv
from datetime import datetime

load_dotenv("env/claves.env")

server = os.getenv("MSSQL_SERVER")
database = os.getenv("MSSQL_DB")
username = os.getenv("MSSQL_USER")
password = os.getenv("MSSQL_PASSWORD")

conn_str = f"DRIVER={{ODBC Driver 18 for SQL Server}}; SERVER={server}; DATABASE={database};UID={username};PWD={password}; TrustServerCertificate=yes;"
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

deudas = pd.read_csv("deudas.txt", sep="|")

for index, row in deudas.iterrows():
    cuenta_id = int(row["CuentaID"])
    monto_original = float(row["MontoOriginal"])
    saldo_pendiente = float(row["SaldoPendiente"])
    fecha_creacion = row["FechaCreacion"]
    fecha_vencimiento = row["FechaVencimiento"]
    estado = row["Estado"]

    print(f"\n🔎 Verificando cuenta {cuenta_id}...")

    # Verificar si la cuenta existe
    cursor.execute("SELECT CuentaID FROM Cuentas WHERE CuentaID = ?", cuenta_id)
    cuenta = cursor.fetchone()

    if not cuenta:
        print(f"❌ La cuenta {cuenta_id} no existe. Deuda no cargada.")
        continue

    # Insertar la deuda
    cursor.execute("""
        INSERT INTO Deudas (CuentaID, MontoOriginal, SaldoPendiente, FechaCreacion, FechaVencimiento, Estado)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (cuenta_id, monto_original, saldo_pendiente, fecha_creacion, fecha_vencimiento, estado))

    print(f"✅ Deuda insertada para cuenta {cuenta_id} (Monto: {monto_original})")

# Guardar cambios
conn.commit()
conn.close()
print("\n✅ Carga de deudas finalizada.")

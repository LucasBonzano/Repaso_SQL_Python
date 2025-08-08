import os
import pandas as pd
import pyodbc
from dotenv import load_dotenv
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP

load_dotenv("env/claves.env")

server = os.getenv("MSSQL_SERVER")
database = os.getenv("MSSQL_DB")
username = os.getenv("MSSQL_USER")
password = os.getenv("MSSQL_PASSWORD")

conn_str = f"DRIVER={{ODBC Driver 18 for SQL Server}}; SERVER={server}; DATABASE={database};UID={username};PWD={password}; TrustServerCertificate=yes;"
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# ✅ Lista para registrar cuentas modificadas
cuentas_modificadas = []

try:
    df = pd.read_csv('cargarsaldo.txt', sep='|', header=None, names=['DNI', 'MontoPagado', 'SucursalID', 'MedioPago'])
except Exception as e:
    print(f"❌ Error al leer el archivo: {e}")
    exit()

for _, row in df.iterrows():
    try:
        dni = str(row['DNI']).strip()
        monto = Decimal(str(row['MontoPagado'])).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        sucursal_id = int(row['SucursalID'])
        tipo_pago = int(row['MedioPago'])

        cursor.execute("SELECT ClienteID FROM Clientes WHERE DNI = ?", dni)
        cliente = cursor.fetchone()
        if not cliente:
            print(f"❌ Cliente con DNI {dni} no encontrado.")
            continue

        cliente_id = cliente.ClienteID

        cursor.execute("""
            SELECT CuentaID, Saldo FROM Cuentas
            WHERE ClienteID = ? AND Estado = 'ACTIVA'
            ORDER BY FechaApertura ASC
        """, cliente_id)
        cuenta = cursor.fetchone()
        if not cuenta:
            print(f"❌ No se encontró cuenta activa para el cliente DNI {dni}.")
            continue

        cuenta_id = cuenta.CuentaID

        saldo_actual = Decimal(str(cuenta.Saldo)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        nuevo_saldo = saldo_actual + monto

        cursor.execute("""
            UPDATE Cuentas SET Saldo = ?
            WHERE CuentaID = ?
        """, nuevo_saldo, cuenta_id)

        cursor.execute("""
            INSERT INTO cargas_saldos (tipo_pago, sucursal_id, numero_cuenta, monto, fecha, dni_cliente)
            VALUES (?, ?, ?, ?, ?, ?)
        """, tipo_pago, sucursal_id, cuenta_id, monto, datetime.now(), dni)

        print(f"✅ Carga exitosa: DNI {dni} → CuentaID {cuenta_id} → +{monto}")
        cuentas_modificadas.append({
            "DNI": dni,
            "CuentaID": cuenta_id,
            "NuevoSaldo": str(nuevo_saldo),
            "Fecha": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })

        conn.commit()

    except Exception as e:
        print(f"⚠️ Error procesando línea '{row.to_dict()}': {e}")

# ✅ Guardar los resultados en un archivo .txt
if cuentas_modificadas:
    output_file = 'cuentas_modificadas.txt'
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("DNI|CuentaID|NuevoSaldo|Fecha\n")
        for item in cuentas_modificadas:
            f.write(f"{item['DNI']}|{item['CuentaID']}|{item['NuevoSaldo']}|{item['Fecha']}\n")

    print(f"\n📁 Archivo generado: {output_file}")
else:
    print("\nℹ️ No se modificó ninguna cuenta.")

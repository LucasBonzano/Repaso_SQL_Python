import os
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

cursor.execute("SELECT ClienteID, Nombre, Apellido FROM Clientes")
clientes = cursor.fetchall()
print(clientes)

for cliente_id, nombre, apellido in clientes:
    print(f"\n👤 Procesando cliente: {nombre} {apellido} (ID {cliente_id})")

    cursor.execute("""
        SELECT CuentaID, Saldo
        FROM Cuentas
        WHERE ClienteID = ? AND Estado = 'ACTIVA' AND Saldo > 0
    """, cliente_id)
    cuenta = cursor.fetchone()

    if not cuenta:
        print("❌ No tiene cuenta activa con saldo")
        continue

    cuenta_id, saldo_cuenta = cuenta
    print(f"✅ Cuenta encontrada ID {cuenta_id} con saldo {saldo_cuenta}")

    cursor.execute("""
        SELECT DeudaID, SaldoPendiente
        FROM Deudas
        WHERE CuentaID = ? AND Estado = 'PENDIENTE'
        ORDER BY FechaCreacion ASC
    """, cuenta_id)
    deudas = cursor.fetchall()

    if not deudas:
        print("✅ No tiene deudas pendientes")
        continue

    for deuda_id, saldo_pendiente in deudas:
        if saldo_cuenta <= 0:
            print("⚠️ Saldo agotado, no se pueden seguir pagando deudas")
            break

        print(f"🔎 Deuda {deuda_id} con saldo pendiente {saldo_pendiente}")

        if saldo_cuenta >= saldo_pendiente:
            # Pago total
            saldo_cuenta -= saldo_pendiente
            cursor.execute("""
                UPDATE Deudas
                SET SaldoPendiente = 0, Estado = 'CANCELADA'
                WHERE DeudaID = ?
            """, deuda_id)
            resultado = f"Deuda {deuda_id} cancelada (pagado {saldo_pendiente})"
        else:
            # Pago parcial
            nuevo_saldo = saldo_pendiente - saldo_cuenta
            cursor.execute("""
                UPDATE Deudas
                SET SaldoPendiente = ?, Estado = 'PENDIENTE'
                WHERE DeudaID = ?
            """, (nuevo_saldo, deuda_id))
            resultado = f"Deuda {deuda_id} pago parcial {saldo_cuenta}, queda {nuevo_saldo}"
            saldo_cuenta = 0

        # Actualizar saldo de la cuenta
        cursor.execute("""
            UPDATE Cuentas
            SET Saldo = ?
            WHERE CuentaID = ?
        """, (saldo_cuenta, cuenta_id))

        # Registrar pago
        monto_pagado = saldo_pendiente if "cancelada" in resultado else (saldo_pendiente - nuevo_saldo)
        cursor.execute("""
            INSERT INTO Pagos (DeudaID, FechaPago, MontoPagado, MedioPago, SucursalID)
            VALUES (?, ?, ?, ?, ?)
        """, (deuda_id, datetime.now(), monto_pagado, "Débito automático", 1))

        # Registrar actividad
        cursor.execute("""
            INSERT INTO ActividadesCobro (DeudaID, FechaActividad, TipoActividad, Resultado, Notas, UsuarioResponsable)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (deuda_id, datetime.now(), "Cobro", "Procesado", resultado, "ScriptPython"))

        print(f"✅ {resultado}")

# Confirmar cambios
conn.commit()
conn.close()
print("\n✅ Procesamiento de todas las deudas finalizado.")

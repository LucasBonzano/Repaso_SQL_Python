import os
import random
from datetime import datetime, timedelta
import pyodbc
from faker import Faker
from dotenv import load_dotenv

fake = Faker("es_ES")

load_dotenv("env/claves.env")

server = os.getenv("MSSQL_SERVER")
database = os.getenv("MSSQL_DB")
username = os.getenv("MSSQL_USER")
password = os.getenv("MSSQL_PASSWORD")

conn_str = f"DRIVER={{ODBC Driver 18 for SQL Server}}; SERVER={server}; DATABASE={database};UID={username};PWD={password}; TrustServerCertificate=yes;"
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

for _ in range(10):
    dni = fake.random_int(min=20000000, max=45000000)
    nombre = fake.first_name()
    apellido = fake.last_name()
    email = fake.email()
    telefono = fake.phone_number()
    direccion = fake.address()
    fecha_alta = fake.date_this_decade()
    estado = random.choice([1, 0])

    cursor.execute("""
        INSERT INTO Clientes (DNI, Nombre, Apellido, Email, Telefono, Direccion, FechaAlta, Estado)
        OUTPUT INSERTED.ClienteID
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (dni, nombre, apellido, email, telefono, direccion, fecha_alta, estado))
    cliente_id = cursor.fetchone()[0]

    # Crear cuenta para el cliente
    numero_cuenta = fake.iban()[:20]
    tipo_cuenta = random.choice(["CAJA DE AHORRO", "CUENTA CORRIENTE"])
    fecha_apertura = fake.date_this_decade()
    saldo = round(random.uniform(1000, 50000), 2)
    estado_cuenta = "ACTIVA"

    cursor.execute("""
        INSERT INTO Cuentas (ClienteID, NumeroCuenta, TipoCuenta, FechaApertura, Saldo, Estado)
        OUTPUT INSERTED.CuentaID
        VALUES (?, ?, ?, ?, ?, ?)
    """, (cliente_id, numero_cuenta, tipo_cuenta, fecha_apertura, saldo, estado_cuenta))
    cuenta_id = cursor.fetchone()[0]

    # Crear deuda para la cuenta
    monto_original = round(random.uniform(500, 10000), 2)
    saldo_pendiente = monto_original
    fecha_creacion = datetime.now() - timedelta(days=random.randint(0, 365))
    fecha_vencimiento = fecha_creacion + timedelta(days=30)
    estado_deuda = "PENDIENTE"

    cursor.execute("""
        INSERT INTO Deudas (CuentaID, MontoOriginal, SaldoPendiente, FechaCreacion, FechaVencimiento, Estado)
        OUTPUT INSERTED.DeudaID
        VALUES (?, ?, ?, ?, ?, ?)
    """, (cuenta_id, monto_original, saldo_pendiente, fecha_creacion, fecha_vencimiento, estado_deuda))
    deuda_id = cursor.fetchone()[0]

    cursor.execute("""
        INSERT INTO ActividadesCobro (DeudaID, FechaActividad, TipoActividad, Resultado, Notas, UsuarioResponsable)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (deuda_id, datetime.now(), "Creación", "Pendiente", "Deuda generada", "ScriptCarga"))

conn.commit()
conn.close()

print("✅ Datos cargados exitosamente en la base de datos.")

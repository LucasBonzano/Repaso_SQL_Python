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
    cursor = conn.cursor()
    cursor.execute('''
    IF NOT EXISTS (
        SELECT * FROM sysobjects WHERE name='cargas_saldos' AND xtype='U'
    )
    CREATE TABLE [pruebabanco].[dbo].[cargas_saldos] (
        id INT IDENTITY(1,1) PRIMARY KEY,
        tipo_pago INT NOT NULL,
        sucursal_id INT NOT NULL,
        dni_cliente VARCHAR(15) NOT NULL,
        numero_cuenta INT NOT NULL,
        monto DECIMAL(12,2) NOT NULL,
        fecha DATETIME DEFAULT GETDATE(),
        CONSTRAINT fk_cliente FOREIGN KEY (dni_cliente) REFERENCES Clientes(DNI),
        CONSTRAINT fk_cuenta FOREIGN KEY (numero_cuenta) REFERENCES cuentas(CuentaID)
    )
    ''')

    conn.commit()
    print("Tabla 'cargas_saldos' creada exitosamente con claves foráneas.")

    cursor.close()
    conn.close()

except Exception as e:
    print("❌ Error de conexión:", e)
    cursor.close()
    conn.close()
import csv
import re

# Rutas fijas de archivos (ajustar si hace falta)
INPUT_FILE = "INPUT0822.txt"
CONVENIOS_FILE = "convenios.csv"
OUTPUT_FILE = "OUTPUT0822.txt"


def parse_fixed_width(line):
    return {
        "ente": line[0:3].strip(),
        "denominacion": line[3:23].strip(),
        "cantidad": int(line[23:29]),
        "importe": int(line[29:47]),
        "signo_ajuste": line[47],
        "ajuste": int(line[48:63]),
        "adherido": line[64],  # 'B' o 'N'
        "comision_emisor": int(line[65:81]),
        "signo_comision_adherente": line[81],
        "comision_adherente": int(line[82:98]),
        "iva_comision_emisor": int(line[98:114]),
        "signo_iva_comision_adherente": line[114],
        "iva_comision_adherente": int(line[115:131]),
        "percepcion": int(line[131:147]),
        "retencion_iva": int(line[147:163]),
        "retencion_ganancias": int(line[163:179]),
        "retencion_iibb": int(line[179:195]),
        "signo_neto": line[195],
        "neto": int(line[196:214]),
    }

def cargar_convenios(csv_path):
    convenios = {}

    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        sample = f.read(2048)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        except csv.Error:
            dialect = csv.excel 

        reader = csv.DictReader(f, dialect=dialect)

        if not reader.fieldnames:
            raise ValueError("El CSV no tiene encabezados.")

        norm_map = {h: h.strip().lower().lstrip("\ufeff") for h in reader.fieldnames}
        required = {"ente_id", "sucursal", "cuenta"}
        got = set(norm_map.values())

        if not required.issubset(got):
            raise KeyError(
                f"Encabezados encontrados: {sorted(got)}. "
                f"Se requieren: {sorted(required)}"
            )

        for row in reader:

            r = {norm_map[k]: (row[k] or "").strip() for k in row}
            ente = r["ente_id"]
            sucursal = re.sub(r"\D", "", r["sucursal"]).zfill(3)[:3]
            cuenta = re.sub(r"\D", "", r["cuenta"]).zfill(11)[:11]

            if ente:
                convenios[ente] = (sucursal, cuenta)

    return convenios



def generar_lineas_salida(registro, sucursal, cuenta):
    conceptos = [
        ("BRUTO", registro["importe"], "+"),
        ("AJUSTE", registro["ajuste"], registro["signo_ajuste"]),
        ("COMISION_EMISOR", registro["comision_emisor"], "-"),
        ("IVA_COMISION_EMISOR", registro["iva_comision_emisor"], "-"),
        ("COMISION_ADHERENTE", registro["comision_adherente"], "-"),
        ("IVA_COMISION_ADHEREN", registro["iva_comision_adherente"], registro["signo_iva_comision_adherente"]),
        ("PERCEPCION", registro["percepcion"], "+"),
        ("RETENCION_IVA", registro["retencion_iva"], "-"),
        ("RETENCION_GANANCIAS", registro["retencion_ganancias"], "-"),
        ("RETENCION_IIBB", registro["retencion_iibb"], "-"),
        ("NETO", registro["neto"], "+"),
    ]

    lineas = []
    for concepto, importe, signo in conceptos:
        if importe != 0:
            linea = (
                f"{registro['ente']:<3}"         
                f"{registro['denominacion']:<20}"
                f"{registro['adherido']:<1}"     
                f"{sucursal:>03}"                
                f"{cuenta:>011}"                 
                f"{concepto:<20}"                
                f"{signo:<1}"                    
                f"{importe:016d}"                
            )
            lineas.append(linea)
    return lineas


def main():
    convenios = cargar_convenios(CONVENIOS_FILE)

    with open(INPUT_FILE, encoding="utf-8") as fin, open(OUTPUT_FILE, "w", encoding="utf-8") as fout:
        for line in fin:
            registro = parse_fixed_width(line)

            sucursal, cuenta = convenios.get(registro["ente"], ("000", "00000000000"))

            lineas = generar_lineas_salida(registro, sucursal, cuenta)
            for l in lineas:
                fout.write(l + "\n")


if __name__ == "__main__":
    main()

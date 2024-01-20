import subprocess
import json
import psycopg2
from dotenv import load_dotenv
from decimal import Decimal
import os

def obtener_ip_docker(nombre_contenedor):
    try:
        resultado = subprocess.run(
            ["docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", nombre_contenedor],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        return resultado.stdout.strip()
    except subprocess.CalledProcessError as e:
        print("Error al obtener la IP del contenedor:", e)
        return None

# Cargar las variables de entorno desde .env
load_dotenv("../.env")

# Obtener la direcciÃƒÂ³n IP del contenedor de PostgreSQL
db_host = obtener_ip_docker("postgresql")
if not db_host:
    raise Exception("No se pudo obtener la direcciÃƒÂ³n IP del contenedor de PostgreSQL.")

# El resto de las variables
#db_name = os.getenv("DB_OFFLINE")
db_name = "fierrodb"
db_user = "root"
#db_password = os.getenv("POSTGRES_PASSWORD")
db_password= "<23456"

# Cambiar al directorio donde se encuentra consulta.sql
os.chdir("/home/taller/offline-autoservice/tlp_json")

# Leer la consulta SQL desde el archivo
with open("consulta.sql", "r") as file:
    consulta_sql = file.read()

# ConexiÃƒÂ³n a la base de datos
conn = psycopg2.connect(
    host=db_host,
    database=db_name,
    user=db_user,
    password=db_password)

# Crear un cursor
cur = conn.cursor()

# Ejecutar la consulta
cur.execute(consulta_sql)

# Obtener los resultados
rows = cur.fetchall()


# Nombres de columnas (opcional, para un JSON más legible)
columnas = [desc[0] for desc in cur.description]
resultados = [
    {clave: (int(valor) if isinstance(valor, Decimal) else valor) for clave, valor in zip(columnas, row)}
    for row in rows
]

# Número de registros por archivo
registros_por_archivo = 40000

# Cálculo del número total de archivos necesario
total_registros = len(resultados)
cantidad_archivos = total_registros // registros_por_archivo + (1 if total_registros % registros_por_archivo else 0)

# Función para dividir la lista de resultados en varias partes
def dividir_resultados(lista, n):
    for i in range(0, len(lista), n):
        yield lista[i:i + n]

# Divide los resultados en la cantidad necesaria de partes
partes = list(dividir_resultados(resultados, registros_por_archivo))

# Convertir a JSON y guardar cada parte en un archivo separado
for i, parte in enumerate(partes, 1):
    resultado_json = json.dumps(parte, ensure_ascii=False, indent=4)
    with open(f"resultados_parte{i}.json", "w", encoding='utf-8') as f:
        f.write(resultado_json)

# Cerrar la conexión
cur.close()
conn.close()

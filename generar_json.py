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

# Nombres de columnas (opcional, para un JSON mÃƒÂ¡s legible)
columnas = [desc[0] for desc in cur.description]
resultados = [
    {clave: (float(valor) if isinstance(valor, Decimal) else valor) for clave, valor in dict(zip(columnas, row)).items()}
    for row in rows
]

# Convertir a JSON
resultado_json = json.dumps(resultados)

# Guardar en un archivo
with open("resultados.json", "w") as f:
    f.write(resultado_json)

# Cerrar la conexiÃƒÂ³n
cur.close()
conn.close()

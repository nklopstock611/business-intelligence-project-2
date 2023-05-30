import psycopg2

# Establecer la conexión con la base de datos PostgreSQL
conn = psycopg2.connect(
    database="db-asma",
    user="nico",
    password="!¿proyecto-asma?!",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

cursor.execute("""
    SELECT dl.idlocalidad, dl.localidad, denf.enf_resp, dl.longitud, dl.latitud, SUM(count)
    FROM tablahechos as th JOIN dimlocalidad as dl
                            ON th.idlocalidad = dl.idlocalidad
                            JOIN DimEnfRespiratorias as denf
                            ON denf.idenfermedad = th.idenfrespiratorias
    GROUP BY dl.idlocalidad, dl.localidad, denf.enf_resp
    ORDER BY CAST(SUBSTRING(dl.idlocalidad, 2, 10) as int);
""")

localidades = cursor.fetchall()

cursor.execute("""
    CREATE TABLE QcountlocalidadEnfR (
        id VARCHAR(20) PRIMARY KEY,
        localidad VARCHAR(20),
        cantidad INT,
        longitud FLOAT,
        latitud FLOAT,
        enf_resp VARCHAR(20)
    );
""")

for l in localidades:
    if l[2] == 'No':
        ide = str(l[0])  + 'N'
    else:
        ide = str(l[0]) + 'E'
    cursor.execute("""
        INSERT INTO QcountlocalidadEnfR VALUES (%s, %s, %s, %s, %s, %s);
    """, (ide, l[1], l[5], l[3], l[4], l[2]) )

# Confirmar los cambios en la base de datos
conn.commit()

# Cerrar la conexión
cursor.close()
conn.close()
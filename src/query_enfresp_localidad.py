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
    SELECT dl.idlocalidad, SUM(count)
    FROM tablahechos as th JOIN dimlocalidad as dl
                           ON th.idlocalidad = dl.idlocalidad
    GROUP BY dl.idlocalidad ORDER BY CAST(SUBSTRING(dl.idlocalidad, 2, 10) as int);
""")

localidades = cursor.fetchall()

cursor.execute("""
    CREATE TABLE Qcountlocalidad (
        id VARCHAR(20) PRIMARY KEY,
        cantidad INT
    );
""")

for localidad in localidades:
    cursor.execute("""
        INSERT INTO Qcountlocalidad VALUES (%s, %s);
    """, (localidad[0], localidad[1]))

# Confirmar los cambios en la base de datos
conn.commit()

# Cerrar la conexión
cursor.close()
conn.close()
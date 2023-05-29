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
    SELECT dl.idlocalidad, dl.localidad, SUM(count)
    FROM tablahechos as th JOIN dimlocalidad as dl
                           ON th.idlocalidad = dl.idlocalidad
    GROUP BY dl.idlocalidad, dl.localidad
    ORDER BY CAST(SUBSTRING(dl.idlocalidad, 2, 10) as int);
""")

localidades = cursor.fetchall()

cursor.execute("""
    CREATE TABLE Qcountlocalidad (
        id VARCHAR(20) PRIMARY KEY,
        localidad VARCHAR(20),
        cantidad INT
    );
""")

for localidad in localidades:
    cursor.execute("""
        INSERT INTO Qcountlocalidad VALUES (%s, %s, %s);
    """, (localidad[0], localidad[1], localidad[2]))

cursor.execute("""
    SELECT ql.id, dl.localidad, ql.cantidad, dl.longitud, dl.latitud
    FROM Qcountlocalidad as ql JOIN dimlocalidad as dl ON ql.id = dl.idlocalidad;
""")

juntos = cursor.fetchall()

cursor.execute("""
    CREATE TABLE Qcountlocalidad2 (
        id VARCHAR(20) PRIMARY KEY,
        localidad VARCHAR(20),
        cantidad INT,   
        longitud FLOAT,
        latitud FLOAT
    );
""")

for j in juntos:
    cursor.execute("""
        INSERT INTO Qcountlocalidad2 VALUES (%s, %s, %s, %s, %s);
    """, (j[0], j[1], j[2], j[3], j[4]))

# Confirmar los cambios en la base de datos
conn.commit()

# Cerrar la conexión
cursor.close()
conn.close()
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
    SELECT dh.idhumedad, dnv.idventilacion, dca.idcontaire, SUM(count)
    FROM (((tablahechos as th JOIN dimhumedad as dh
                           ON th.idhumedad = dh.idhumedad)
                           JOIN dimventilacion as dnv
                           ON th.idnivelventilacion = dnv.idventilacion)
                           JOIN dimcontaire as dca
                           ON th.idcontaire = dca.idcontaire)
                           JOIN dimenfrespiratorias as denf
                           ON th.idenfrespiratorias = denf.idenfermedad
    GROUP BY dh.idhumedad, dnv.idventilacion, dca.idcontaire, denf.idenfermedad
    HAVING denf.idenfermedad = 'ENF2';
""")

ambientes = cursor.fetchall()

cursor.execute("""
    CREATE TABLE Qcountambiente (
        idhumedad VARCHAR(20),
        idnivelventilacion VARCHAR(20),
        idcontaire VARCHAR(20),
        cantidad INT
    );
""")

for ambiente in ambientes:
    cursor.execute("""
        INSERT INTO Qcountambiente VALUES (%s, %s, %s, %s);
    """, (ambiente[0], ambiente[1], ambiente[2], ambiente[3]))

# Confirmar los cambios en la base de datos
conn.commit()

# Cerrar la conexión
cursor.close()
conn.close()
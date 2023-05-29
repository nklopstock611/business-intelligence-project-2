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
    SELECT ds.idsexo, de.idedad, SUM(count)
    FROM (tablahechos as th JOIN dimsexo as ds
                           ON th.idsexo = ds.idsexo)
                           JOIN dimedad as de
                           ON th.idedad = de.idedad
    GROUP BY ds.idsexo, de.idedad
    HAVING ds.idsexo = 'F' OR ds.idsexo = 'M';
""")

sexo_edades = cursor.fetchall()

cursor.execute("""
    CREATE TABLE Qcountsexoedad (
        idsexo VARCHAR(20),
        idedad VARCHAR(20),
        cantidad INT
    );
""")

for sexo_edad in sexo_edades:
    cursor.execute("""
        INSERT INTO Qcountsexoedad VALUES (%s, %s, %s);
    """, (sexo_edad[0], sexo_edad[1], sexo_edad[2]))

# Confirmar los cambios en la base de datos
conn.commit()

# Cerrar la conexión
cursor.close()
conn.close()
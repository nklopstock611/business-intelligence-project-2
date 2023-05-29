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
    SELECT de.idestrato, SUM(count)
    FROM tablahechos as th JOIN dimestrato as de
                           ON th.idestrato = de.idestrato
    GROUP BY de.idestrato ORDER BY de.idestrato;
""")

estratos = cursor.fetchall()

cursor.execute("""
    CREATE TABLE Qcountestrato (
        id VARCHAR(20) PRIMARY KEY,
        cantidad INT
    );
""")

for estrato in estratos:
    cursor.execute("""
        INSERT INTO Qcountestrato VALUES (%s, %s);
    """, (estrato[0], estrato[1]))

# Confirmar los cambios en la base de datos
conn.commit()

# Cerrar la conexión
cursor.close()
conn.close()
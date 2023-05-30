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
    SELECT de.estrato, denf.enf_resp, SUM(count)
    FROM tablahechos as th JOIN dimestrato as de
                           ON th.idestrato = de.idestrato
                           JOIN DimEnfRespiratorias as denf 
                           ON denf.idenfermedad = th.idenfrespiratorias
    GROUP BY de.idestrato,  denf.enf_resp
    ORDER BY de.idestrato;
""")

estratos = cursor.fetchall()

cursor.execute("""
    CREATE TABLE QcountestratoenfRespi (
        id VARCHAR(20) PRIMARY KEY,
        estrato INT,
        idenf VARCHAR(20),
        cantidad INT
    );
""")

for estrato in estratos:
    if estrato[1] == 'No':
        ide = str(estrato[0])  + 'N' 
    else:
        ide = str(estrato[0]) + 'E'
    cursor.execute("""
        INSERT INTO QcountestratoenfRespi VALUES (%s, %s, %s, %s);
    """, (ide, estrato[0], estrato[1], estrato[2],) )
# Confirmar los cambios en la base de datos
conn.commit()

# Cerrar la conexión
cursor.close()
conn.close()
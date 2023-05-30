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
    SELECT ds.idsexo, de.rango_edad, denf.enf_resp, SUM(count)
    FROM (tablahechos as th JOIN dimsexo as ds
                           ON th.idsexo = ds.idsexo)
                           JOIN dimedad as de
                           ON th.idedad = de.idedad
                           JOIN DimEnfRespiratorias as denf
                            ON denf.idenfermedad = th.idenfrespiratorias
    WHERE denf.enf_resp = 'Si'
    GROUP BY ds.idsexo, de.idedad, denf.enf_resp
    HAVING ds.idsexo = 'F' OR ds.idsexo = 'M' ;
""")

sexo_edades = cursor.fetchall()

cursor.execute("""
    CREATE TABLE QcountsexoedadEnfResp (
        idsexo VARCHAR(20),
        idedad VARCHAR(20),
        cantidad INT
    );
""")

for sexo_edad in sexo_edades:
    if sexo_edad[1] != "Menor de 18":
        cursor.execute("""
            INSERT INTO QcountsexoedadEnfResp VALUES (%s, %s, %s);
        """, (sexo_edad[0], sexo_edad[1], sexo_edad[3]))
    else:
        cursor.execute("""
            INSERT INTO QcountsexoedadEnfResp VALUES (%s, %s, %s);
        """, (sexo_edad[0], "0-18", sexo_edad[3]))

# Confirmar los cambios en la base de datos
conn.commit()

# Cerrar la conexión
cursor.close()
conn.close()
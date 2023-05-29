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

# gente con enfermedades respiratorias
cursor.execute("""
    SELECT SUM(count)
    FROM tablahechos
    WHERE idenfrespiratorias = 'ENF2';
""")

con_asma = cursor.fetchall()

# gente sin enfermedades respiratorias
cursor.execute("""
    SELECT SUM(count)
    FROM tablahechos
    WHERE idenfrespiratorias = 'ENF1';
""")

sin_asma = cursor.fetchall()

cursor.execute("""
    CREATE TABLE Qcountasma (
        id VARCHAR(20) PRIMARY KEY,
        cantidad INT
    );
""")

cursor.execute("""
    INSERT INTO Qcountasma VALUES ('SINASMA', %s);
""", sin_asma)

cursor.execute("""
    INSERT INTO Qcountasma VALUES ('ASMA', %s);
""", con_asma)

# Confirmar los cambios en la base de datos
conn.commit()

# Cerrar la conexión
cursor.close()
conn.close()
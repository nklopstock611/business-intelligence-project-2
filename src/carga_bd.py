import csv
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

# Crear tabla de dimensión Edad
cursor.execute("""
    CREATE TABLE DimEdad (
        rango_edad VARCHAR(20),
        idEdad VARCHAR(20) PRIMARY KEY
    );
""")

# Insertar datos en la tabla de dimensión Edad
with open('data/tabla_edad2021.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader)
    for row in reader:
        inser_query = "INSERT INTO DimEdad VALUES (%s, %s)"
        cursor.execute(inser_query, row)

# Crear tabla de dimensión Sexo
cursor.execute("""
    CREATE TABLE DimSexo (
        sexoBio VARCHAR(20),
        idSexo VARCHAR(20) PRIMARY KEY
    );
""")

# Insertar datos en la tabla de dimensión Sexo
with open('data/tabla_sexo2021.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader)
    for row in reader:
        inser_query = "INSERT INTO DimSexo VALUES (%s, %s)"
        cursor.execute(inser_query, row)

# Crear tabla de dimensión EnfRespiratorias
cursor.execute("""
    CREATE TABLE DimEnfRespiratorias (
        enf_resp VARCHAR(20),
        idEnfermedad VARCHAR(20) PRIMARY KEY
    );
""")

# Insertar datos en la tabla de dimensión EnfRespiratorias
with open('data/tabla_enfermedad2021.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader)
    for row in reader:
        inser_query = "INSERT INTO DimEnfRespiratorias VALUES (%s, %s)"
        cursor.execute(inser_query, row)

# Crear tabla de dimensión Localidad
cursor.execute("""
    CREATE TABLE DimLocalidad (
        localidad VARCHAR(20),
        idLocalidad VARCHAR(20) PRIMARY KEY,
        longitud FLOAT,
        latitud FLOAT
    );
""")

# Insertar datos en la tabla de dimensión Localidad
with open('data/tabla_localidad2021.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader)
    for row in reader:
        inser_query = "INSERT INTO DimLocalidad VALUES (%s, %s, %s, %s)"
        cursor.execute(inser_query, row)

# Crear tabla de dimensión Estrato
cursor.execute("""
    CREATE TABLE DimEstrato (
        idEstrato VARCHAR(20) PRIMARY KEY,
        estrato INT
    );
""")

# Insertar datos en la tabla de dimensión Estrato
with open('data/tabla_estrato2021.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader)
    for row in reader:
        inser_query = "INSERT INTO DimEstrato VALUES (%s, %s)"
        cursor.execute(inser_query, row)

# Crear tabla de dimensión Humedad
cursor.execute("""
    CREATE TABLE DimHumedad (
        humedad VARCHAR(20),
        idHumedad VARCHAR(20) PRIMARY KEY
    );
""")

# Insertar datos en la tabla de dimensión Humedad
with open('data/tabla_humedad2021.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader)
    for row in reader:
        inser_query = "INSERT INTO DimHumedad VALUES (%s, %s)"
        cursor.execute(inser_query, row)

# Crear tabla de dimensión Ventilación
cursor.execute("""
    CREATE TABLE DimVentilacion (
        ventilacion VARCHAR(20),
        idVentilacion VARCHAR(20) PRIMARY KEY
    );
""")

# Insertar datos en la tabla de dimensión Ventilación
with open('data/tabla_ventilacion2021.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader)
    for row in reader:
        inser_query = "INSERT INTO DimVentilacion VALUES (%s, %s)"
        cursor.execute(inser_query, row)

# Crear tabla de dimensión Contaminación Aire
cursor.execute("""
    CREATE TABLE DimContAire (
        contAire VARCHAR(20),
        idContAire VARCHAR(20) PRIMARY KEY
    );
""")

# Insertar datos en la tabla de dimensión Contaminación Aire
with open('data/tabla_contaire2021.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader)
    for row in reader:
        inser_query = "INSERT INTO DimContAire VALUES (%s, %s)"
        cursor.execute(inser_query, row)

# Crear tabla de hechos con FKs
cursor.execute("""
    CREATE TABLE TablaHechos (
        idLocalidad VARCHAR(20) REFERENCES DimLocalidad(idLocalidad),
        idEstrato VARCHAR(20) REFERENCES DimEstrato(idEstrato),
        idEdad VARCHAR(20) REFERENCES DimEdad(idEdad),
        idEnfRespiratorias VARCHAR(20) REFERENCES DimEnfRespiratorias(idEnfermedad),
        idHumedad VARCHAR(20) REFERENCES DimHumedad(idHumedad),
        idNivelVentilacion VARCHAR(20) REFERENCES DimVentilacion(idVentilacion),
        idContAire VARCHAR(20) REFERENCES DimContAire(idContAire),
        idSexo VARCHAR(20) REFERENCES DimSexo(idSexo),
        count FLOAT
    );
""")

# Insertar datos en la tabla de hechos
with open('data/tabla_hechos_metrica2021.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader)
    for row in reader:
        insert_query = "INSERT INTO TablaHechos (idLocalidad, idEstrato, idEdad, idEnfRespiratorias, idHumedad, idNivelVentilacion, idContAire, idSexo, count) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        cursor.execute(insert_query, row)

# Confirmar los cambios en la base de datos
conn.commit()

# Cerrar la conexión
cursor.close()
conn.close()

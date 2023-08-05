import requests
import pandas as pd
import json
import os
from dotenv import load_dotenv
import psycopg2

def run():
    # Cargar variables de entorno desde el archivo .env
    load_dotenv()

    # Obtener las credenciales de Redshift del entorno
    host = os.getenv("REDSHIFT_HOST")
    port = os.getenv("REDSHIFT_PORT")
    database = os.getenv("REDSHIFT_DATABASE")
    user = os.getenv("REDSHIFT_USER")
    password = os.getenv("REDSHIFT_PASSWORD")

    # Hacer la solicitud a la API de REST Countries
    url = 'https://restcountries.com/v3.1/all'
    response = requests.get(url)

    # Obtener el JSON de la respuesta
    json_data = response.json()

    # Convertir la lista en un DataFrame
    df = pd.json_normalize(json_data)

    # Verificar las columnas del DataFrame
    print(df.columns)

    # Imprimir DataFrame Completo
    pd.set_option('display.max_columns', None)  # Mostrar todas las columnas
    pd.set_option('display.expand_frame_repr', False)  # No envolver las filas para ajustarse a la pantalla
    df.head(5)

    # CREANDO UN NUEVO DATAFRAME FILTRANDO COLUMNAS CON LAS QUE TRABAJARÉ
    columnas_seleccionadas = ['name.official', 'name.common', 'continents','region','capital','timezones','population','area','independent','altSpellings','flag','latlng']
    df_or = df[columnas_seleccionadas]
    df_or['continents'] = df_or['continents'].apply(lambda x: x[0] if isinstance(x, list) else x)
    df_or['timezones'] = df_or['timezones'].apply(lambda x: x[0] if isinstance(x, list) else x)
    df_or['capital'] = df_or['capital'].fillna('No disponible')
    df_or['capital'] = df_or['capital'].apply(lambda x: x[0] if isinstance(x, list) else x)

    # Dividir la columna "latlng" en columnas separadas de latitud y longitud
    df_or[['latitud', 'longitud']] = pd.DataFrame(df_or['latlng'].tolist(), index=df.index)
    df_or.head(10)

    # Conexión a Amazon Redshift utilizando las variables de entorno
    conn = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )

    # Crear el cursor
    cursor = conn.cursor()
    print(cursor)

    # Crear la tabla en Amazon Redshift
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS countries_cris (
        name_official VARCHAR(255),
        name_common VARCHAR(255),
        continents VARCHAR(255),
        region VARCHAR(255),
        capital VARCHAR(255),
        timezones VARCHAR(255),
        population INT,
        area FLOAT,
        independent VARCHAR(255),
        altSpellings VARCHAR(255),
        flag VARCHAR(255),
        latlng VARCHAR(255),
        latitud FLOAT,
        longitud FLOAT
    );
    '''
    cursor.execute(create_table_query)
    conn.commit()

    # Insertar los datos en la tabla
    insert_query = '''
    INSERT INTO countries_cris (
        name_official,
        name_common,
        continents,
        region,
        capital,
        timezones,
        population,
        area,
        independent,
        altSpellings,
        flag,
        latlng,
        latitud,
        longitud
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    '''

    try:
        for index, row in df_or.iterrows():
            values = (
                str(row['name.official']),
                str(row['name.common']),
                str(row['continents']),
                str(row['region']),
                str(row['capital']),
                str(row['timezones']),
                int(row['population']),
                float(row['area']),
                str(row['independent']),
                str(row['altSpellings']),
                str(row['flag']),
                str(row['latlng']),
                float(row['latitud']),
                float(row['longitud'])
            )
            cursor.execute(insert_query, values)

        conn.commit()
        print("Inserción exitosa")

    except Exception as e:
        print("Error en la inserción:", e)
        conn.rollback()

    finally:
        cursor.close()
        conn.close()


run()
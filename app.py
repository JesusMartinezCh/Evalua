# -*- coding: utf-8 -*-
"""
Script para migrar datos desde una base de datos local de MySQL (XAMPP) a una
base de datos de MySQL alojada en Railway.
"""
import os
import mysql.connector
from dotenv import load_dotenv

# Cargar variables de entorno desde un archivo .env si existe
load_dotenv()

def conectar_a_bd_local():
    """
    Establece la conexión a la base de datos local de MySQL (XAMPP).
    """
    try:
        # Usar variables de entorno para una conexión segura
        db_user = os.getenv("XAMPP_DB_USER", "root")
        db_password = os.getenv("XAMPP_DB_PASS", "")
        db_host = os.getenv("XAMPP_DB_HOST", "localhost")
        db_name = os.getenv("XAMPP_DB_NAME")

        if not db_name:
            print("\n❌ Error: La variable de entorno 'XAMPP_DB_NAME' no está definida.")
            print("Por favor, configura el nombre de tu base de datos local en el archivo .env o en el entorno.")
            return None

        conn = mysql.connector.connect(
            host=db_host,
            user=db_user,
            password=db_password,
            database=db_name
        )
        print("\n✅ Conexión a la base de datos local (MySQL) exitosa.")
        return conn

    except mysql.connector.Error as e:
        print(f"\n❌ Error al conectar a la base de datos local (MySQL): {e}")
        print("Asegúrate de que tu servidor MySQL en XAMPP esté activo y los datos de conexión sean correctos.")
        return None

def conectar_a_bd_railway():
    """
    Establece la conexión a la base de datos de MySQL en Railway usando la URL de conexión.
    """
    railway_url = os.getenv("DATABASE_URL")
    if not railway_url:
        print("\n❌ Error: La variable de entorno 'DATABASE_URL' no está definida.")
        print("Por favor, configura la URL de conexión de tu base de datos de Railway (MySQL).")
        print("Ejemplo: export DATABASE_URL='mysql://user:password@host:port/database'")
        return None

    try:
        # Extraer los componentes de la URL de conexión
        from urllib.parse import urlparse
        url = urlparse(railway_url)

        conn = mysql.connector.connect(
            host=url.hostname,
            user=url.username,
            password=url.password,
            port=url.port,
            database=url.path[1:] # Eliminar el '/' inicial
        )
        print("\n✅ Conexión a la base de datos de Railway exitosa.")
        return conn

    except mysql.connector.Error as e:
        print(f"\n❌ Error al conectar a la base de datos de Railway: {e}")
        return None
    except Exception as e:
        print(f"\n❌ Error al procesar la URL de conexión de Railway: {e}")
        return None


def migrar_datos(conn_local, conn_railway):
    """
    Migra los datos desde la base de datos local (MySQL) a la de Railway (MySQL).
    """
    try:
        cursor_local = conn_local.cursor()
        cursor_railway = conn_railway.cursor()

        # --- Migrar datos de la tabla 'genero' ---
        print("\nMigrando la tabla 'genero'...")
        cursor_local.execute("SELECT id_genero, nom_genero FROM genero")
        generos = cursor_local.fetchall()

        if generos:
            for genero in generos:
                try:
                    # Usamos INSERT IGNORE para evitar errores si la clave primaria ya existe
                    cursor_railway.execute("""
                        INSERT IGNORE INTO genero (id_genero, nom_genero)
                        VALUES (%s, %s);
                    """, genero)
                except Exception as e:
                    print(f"Error al insertar género {genero}: {e}")
            conn_railway.commit()
            print(f"✅ Se han insertado o ignorado {len(generos)} registros en la tabla 'genero'.")
        else:
            print("❗ No se encontraron datos en la tabla 'genero' local para migrar.")

        # --- Migrar datos de la tabla 'peliculas' ---
        print("\nMigrando la tabla 'peliculas'...")
        cursor_local.execute("SELECT id_pelicula, nom_pelicula, dura_pelicula, doblado, comentario_pelicula, genero_id_genero FROM peliculas")
        peliculas = cursor_local.fetchall()

        if peliculas:
            for pelicula in peliculas:
                try:
                    # Usamos INSERT IGNORE para evitar errores si la clave primaria ya existe
                    cursor_railway.execute("""
                        INSERT IGNORE INTO peliculas (id_pelicula, nom_pelicula, dura_pelicula, doblado, comentario_pelicula, genero_id_genero)
                        VALUES (%s, %s, %s, %s, %s, %s);
                    """, pelicula)
                except Exception as e:
                    print(f"Error al insertar película {pelicula}: {e}")
            conn_railway.commit()
            print(f"✅ Se han insertado o ignorado {len(peliculas)} registros en la tabla 'peliculas'.")
        else:
            print("❗ No se encontraron datos en la tabla 'peliculas' local para migrar.")

    except mysql.connector.Error as e:
        print(f"Ha ocurrido un error durante la migración: {e}")
        return False
    finally:
        if 'cursor_local' in locals():
            cursor_local.close()
        if 'cursor_railway' in locals():
            cursor_railway.close()
    
    return True

if __name__ == "__main__":
    # 1. Conectar a la base de datos local (MySQL)
    conn_local = conectar_a_bd_local()
    if conn_local is None:
        exit()

    # 2. Conectar a la base de datos de Railway (MySQL)
    conn_railway = conectar_a_bd_railway()
    if conn_railway is None:
        conn_local.close()
        exit()
    
    # 3. Ejecutar la migración
    if migrar_datos(conn_local, conn_railway):
        print("\n🎉 ¡Migración de datos completada exitosamente!")
        print("Verifica los datos en tu base de datos de Railway para confirmar la migración.")
    
    # 4. Cerrar conexiones
    conn_local.close()
    conn_railway.close()
    print("\nConexiones a las bases de datos cerradas.")
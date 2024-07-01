import mysql.connector

# Configurar la conexión a MySQL
config = {
    'user': 'root',
    'password': '5N]S9-Rt',
    'host': 'localhost',    # Puedes cambiar esto según la ubicación de tu servidor MySQL
    'database': 'curso',  # Nombre de la base de datos a la que te quieres conectar
    'port': '3306'          # Puerto de conexión (por defecto es 3306 para MySQL)
}

try:
    # Crear la conexión
    conn = mysql.connector.connect(**config)

    if conn.is_connected():
        print('Conexión establecida a la base de datos MySQL')

        # Ejemplo de consulta
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM venta')

        # Mostrar resultados
        for row in cursor.fetchall():
            print(row)

except mysql.connector.Error as e:
    print(f'Error al conectar a MySQL: {e}')

finally:
    # Cerrar la conexión
    if 'conn' in locals() and conn.is_connected():
        cursor.close()
        conn.close()
        print('Conexión a MySQL cerrada')


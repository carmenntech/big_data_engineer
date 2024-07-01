#!/usr/bin/python3

import json
from pymongo import MongoClient

# Ruta para guardar el archivo JSON resultante
output_json_file_path = 'C:/sw/repos_big_data_engineer/mongodb_python/mongodb-env/ficheros/ventas_output.json'

# Conectar a MongoDB
client = MongoClient('mongodb://localhost:27017/')

try:
    db = client.testdb

    # Realizar la consulta en la colección "ventas"
    ventas = db.ventas.find()

    # Convertir el cursor de MongoDB a una lista de diccionarios
    ventas_list = list(ventas)

    # Remover el campo ObjectId de MongoDB, si existe
    for venta in ventas_list:
        venta.pop('_id', None)

    # Escribir los datos en un archivo JSON
    with open(output_json_file_path, 'w') as file:
        json.dump(ventas_list, file, indent=4)

    print(f'Data has been written to {output_json_file_path}')

finally:
    # Cerrar la conexión a MongoDB
    client.close()

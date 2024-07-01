#!/usr/bin/python3

import json
from pymongo import MongoClient

# Ruta al archivo JSON
json_file_path = 'C:/sw/repos_big_data_engineer/mongodb_python/mongodb-env/ficheros/ventas.json'

# Conectar a MongoDB
client = MongoClient('mongodb://localhost:27017/')

try:
    db = client.testdb

    # Cargar datos desde el archivo JSON
    with open(json_file_path, 'r') as file:
        data = json.load(file)

    # Insertar los datos en la colección "clientes"
    res = db.ventas.insert_many(data)
    print(f'Inserted {len(res.inserted_ids)} documents into the collection "clientes".')

finally:
    # Cerrar la conexión a MongoDB
    client.close()
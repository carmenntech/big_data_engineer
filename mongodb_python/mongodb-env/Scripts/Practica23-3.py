#!/usr/bin/python3

import json
from bson import json_util
from bson.json_util import dumps
from pymongo import MongoClient

# Ruta para guardar el archivo JSON resultante
output_json_file_path = 'C:/sw/repos_big_data_engineer/mongodb_python/mongodb-env/ficheros/ventas_clientes_output.json'

# Conectar a MongoDB
client = MongoClient('mongodb://localhost:27017/')

try:
    db = client.testdb

    # Pipeline de agregación para hacer el "join" entre ventas y clientes
    pipeline = [
        {
            "$lookup": {
                "from": "clientes",
                "localField": "id",
                "foreignField": "id_cliente",
                "as": "cliente_info"
            }
        },
        {
            "$unwind": "$cliente_info"
        }

    ]

    # Ejecutar la agregación
    ventas_clientes = db.ventas.aggregate(pipeline)

    # Convertir el cursor de MongoDB a una lista de diccionarios
    ventas_clientes_list = list(ventas_clientes)
    print(ventas_clientes_list)
    # Remover el campo ObjectId de MongoDB, si existe
    for venta_cliente in ventas_clientes_list:
        venta_cliente.pop('_id', None)

    # Escribir los datos en un archivo JSON
    with open(output_json_file_path, 'w') as file:
        #json.dump(ventas_clientes_list, file, indent=4)
        #json_util.dumps(ventas_clientes_list, file)
        json.dump(json.loads(dumps(ventas_clientes_list)), file)
    print(f'Data has been written to {output_json_file_path}')

finally:
    # Cerrar la conexión a MongoDB
    client.close()

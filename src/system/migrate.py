import ipaddress
import logging
import threading
import os

from src.modules import *
from src.modules.DATA_MODULE import *
from src.lib import *
from src.system.secret import *


def migrate(port="27017", user=None, password=None):
    logger.setLevel(logging.CRITICAL)
    # Ask for host and port
    print("Setup configuration of your database")

    print("Write the host of the database")
    host = input("|  Host: ")

    # Verificar si el host es una dirección IP válida
    try:
        ipaddress.ip_address(host)
    except ValueError:
        print("|  El host ingresado no es una dirección IP válida.")
        return  # Sale de la función si el host no es una IP válida

    check = input("You want to change default port = 27001 (y/n): ")
    if check == "y":
        port = input("|  Port: ")

    # Ask for user + pass
    check = input("Is there a user and password? (y/n): ")
    if check == "y":
        user = input("|  User: ")
        password = input("|  Password: ")

    # Ask for user + pass
    check = input("You want to use the same database name (y/n): ")
    if check == "n":
        DataBaseName = input("|  Write the name of the database: ")
    else:
        DataBaseName = DATABASE["dbname"]

    logger.info("Configurando la aplicación")
    conn = MongoDbFunctions(host, int(port), user, password, dbname=DataBaseName)
    logger.setLevel(logging.INFO)
    logger.info("Comienza la migración")
    db_collections = getDatabaseCollections()
    if not db_collections[0]:
        logger.error("An error occurred: %s", str(db_collections[1]))
        return
    db_collections = db_collections[1]

    logger.info("Migrando colecciones")
    for collection in db_collections:
        uploadDataFromCollections(collection, new_database=conn)


###############################
##### MIGRATION FUNCTIONS #####
###############################


def getDatabaseCollections():
    conn = None
    try:
        logger.info("Conectando a la base de datos")
        conn = MongoDbFunctions(
            DATABASE["host"],
            DATABASE["port"],
            DATABASE["username"],
            DATABASE["password"],
            DATABASE["dbname"],
        )
        logger.info("Conexión a la base de datos establecida")
        logger.info("Obteniendo colecciones de la base de datos")
        collections = conn.db.list_collection_names()
        logger.info("Colecciones obtenidas")
        logger.info("Cerrando conexión a la base de datos")
        conn.close()
        return (True, collections)
    except Exception as e:
        if conn:
            conn.close()
        logger.error("An error occurred: %s", str(e))
        return (False, e)


def uploadDataFromCollections(collection, new_database):
    print("")
    logger.info("Obteniendo todos los documentos de la colección %s", collection)

    conn = None
    try:
        logger.info("Conectando a la base de datos")
        conn = MongoDbFunctions(
            DATABASE["host"],
            DATABASE["port"],
            DATABASE["username"],
            DATABASE["password"],
            DATABASE["dbname"],
            collectionname=collection,
        )
        logger.info("Conexión a la base de datos establecida")

        all_ids = list(
            conn.findByMultipleFields(
                custom=True, fields={}, proyeccion={"_id": 1}, get_all=True
            )
        )
        logger.info(
            f"Se obtuvieron {len(all_ids)} documentos de la colección {collection}"
        )

        # Set collection on the new database
        new_database.changeCollection(collection)

        # Procesamos los datos en chucks de 100
        # Procesar la lista en grupos de 100 elementos
        tamaño_grupo = 100
        total_procesados = 0
        for i in range(0, len(all_ids), tamaño_grupo):
            start_time = time.time()
            grupo = all_ids[i : i + tamaño_grupo]
            grupo = [id["_id"] for id in grupo]

            # Aquí puedes procesar cada grupo
            # Por ejemplo, imprimir el grupo o realizar alguna operación
            data = conn.findByMultipleFields(
                custom=True,
                fields={"_id": {"$in": grupo}},
                get_all=True,
                proyeccion={"_id": 0},
            )

            # Insertamos los datos en la nueva base de datos
            new_database.insert_many(data)
            total_procesados += len(grupo)
            logger.info(
                f"{total_procesados}/{len(all_ids)} datos procesados en {round(time.time() - start_time,2)} segundos"
            )

        logger.info("Cerrando conexión a la base de datos")
        conn.close()
        return (True, "")
    except Exception as e:
        if conn:
            conn.close()
        logger.error("An error occurred: %s", str(e))
        return (False, e)

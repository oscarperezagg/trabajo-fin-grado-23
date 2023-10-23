# Import the required libraries
from src.system.logging_config import logger
from pymongo import MongoClient
import pymongo
from bson.objectid import ObjectId
from gridfs import GridFS
import sys
import bson


# Configure the logger


class MongoDbFunctions:
    """
    A utility class for interacting with MongoDB databases and collections.

    This class provides methods to connect to a MongoDB database, perform various database operations,
    and manage documents within collections.

    Args:
        host (str): The MongoDB host address.
        port (int): The MongoDB port number.
        username (str): The username for authentication.
        password (str): The password for authentication.
        dbname (str): The name of the MongoDB database to connect to.
        collectionname (str): The name of the collection within the database to interact with.

    Attributes:
        client: The MongoDB client object.
        db: The database object.
        collection: The collection object within the database.

    Usage:
        mongo = MongoDbFunctions(host, port, username, password, dbname, collectionname)
        # Use various methods to interact with the MongoDB collection
        mongo.insert(data)
        mongo.findByField("nombre", "Juan")
        mongo.updateByField("nombre", "Juan", {"edad": 31})
        mongo.deleteByField("nombre", "Juan")
        mongo.close()

    Methods:
        - changeCollection(collectionname): Change the reference to a different collection.
        - close(): Close the connection to the MongoDB client.
        - insert(data): Insert a single document into the collection.
        - insert_many(data): Insert multiple documents into the collection.
        - findById(id): Find a document by its ObjectId.
        - findByField(field, value, exact_match=True, get_all=False): Find documents by field and value.
        - findByMultipleFields(fields, exact_match=True, get_all=False): Find documents by multiple fields.
        - findByComplexQuery(query, get_all=False): Find documents by a complex query.
        - updateById(id, data): Update a document by its ObjectId.
        - updateByField(field, value, data, exact_match=True): Update documents by field and value.
        - updateByMultipleField(fields, data, exact_match=True): Update documents by multiple fields.
        - deleteByField(field, value, exact_match=True): Delete documents by field and value.
        - deleteByMultipleField(fields, exact_match=True): Delete documents by multiple fields.



    """

    def __init__(self, host, port, username, password, dbname, collectionname=None):
        if not username or not password:
            self.client = MongoClient(host, int(port))
        else:
            self.client = MongoClient(host, port, username=username, password=password)
        self.db = self.client[dbname]
        self.fs = GridFS(self.db)
        if collectionname:
            self.collection = self.db[collectionname]

    def changeCollection(self, collectionname):
        """
        Change the reference to a different collection within the same database.

        Args:
            collectionname (str): The name of the new collection.
        """
        self.collection = self.db[collectionname]
        logger.info(f"Changed collection to {collectionname}")

    def close(self):
        """
        Close the connection to the MongoDB client.
        """
        self.client.close()
        logger.info("Database connection closed")

    def insert(self, data, name="default"):
        """
        Insert a single document into the collection.

        Args:
            data (dict): The document to be inserted.
        """

        # Check if data size is greater than 16 MB (16777216 bytes)
        temporalData = bson.BSON.encode(data)
        data_size = len(temporalData)
        if data_size > 16000000:
            logger.warning(f"Data size is {data_size} bytes. Inserting with GridFS.")
            self.insertWithFS(data, name)
        else:
            self.collection.insert_one(data)
            logger.info("Inserted a document into the regular collection.")

    def insert_many(self, data):
        """
        Insert multiple documents into the collection.

        Args:
            data (list): A list of documents to be inserted.
        """
        self.collection.insert_many(data)

    def insertWithFS(self, data, name):
        file_id = self.fs.put(str(data).encode(), filename=f"{name}.json")
        logger.info(f"Inserted a large document with file_id: {file_id} into GridFS.")

    def findById(self, id):
        """
        Find a document in the collection by its ObjectId.

        Args:
            id (str): The ObjectId string of the document.

        Returns:
            dict or None: The matching document, or None if not found.
        """
        return self.collection.find_one(ObjectId(id))

    def findAll(
        self,
        sort=False,
        sortField=None,
        asc=True,
    ):
        """
        Find documents in the collection based on a field and value.

        Args:
            field (str): The field to search in.
            value (str): The value to search for.
            exact_match (bool, optional): Whether to perform an exact match or not. Default is True.
            get_all (bool, optional): Whether to retrieve all matching documents or just the first one.
                                    Default is False.

        Returns:
            list or dict: Depending on the 'get_all' parameter, either a list of matching documents or
                        the first matching document found. Returns None if no matches are found.
        """
        query = {}

        if sort:
            data = self.collection.find(query).sort(
                sortField, pymongo.ASCENDING if asc else pymongo.DESCENDING
            )
            return list(data)
        return list(self.collection.find(query))


    def findByField(
        self,
        field,
        value,
        exact_match=True,
        get_all=False,
        sort=False,
        sortField=None,
        asc=True,
    ):
        """
        Find documents in the collection based on a field and value.

        Args:
            field (str): The field to search in.
            value (str): The value to search for.
            exact_match (bool, optional): Whether to perform an exact match or not. Default is True.
            get_all (bool, optional): Whether to retrieve all matching documents or just the first one.
                                    Default is False.

        Returns:
            list or dict: Depending on the 'get_all' parameter, either a list of matching documents or
                        the first matching document found. Returns None if no matches are found.
        """
        if exact_match:
            query = {field: value}
        else:
            query = {field: {"$regex": value, "$options": "i"}}

        if get_all:
            if sort:
                data = self.collection.find(query).sort(
                    sortField, pymongo.ASCENDING if asc else pymongo.DESCENDING
                )
                return list(data)
            return list(self.collection.find(query))
        else:
            if sort:
                data = self.collection.find_one(query).sort(
                    sortField, pymongo.ASCENDING if asc else pymongo.DESCENDING
                )
                return data
            return self.collection.find_one(query)

    def findByMultipleFields(self, fields, exact_match=True, get_all=False,custom=False,proyeccion=None):
        """
        Find documents in the collection based on multiple fields and their values.

        Args:
            fields (dict): A dictionary where keys are field names and values are values to search for.
            exact_match (bool, optional): Whether to perform an exact match or not. Default is True.
            get_all (bool, optional): Whether to retrieve all matching documents or just the first one.
                                    Default is False.

        Returns:
            list or dict: Depending on the 'get_all' parameter, either a list of matching documents or
                        the first matching document found. Returns None if no matches are found.
        """
        if custom:
            query = fields
        else:
            query = {}
            for field, value in fields.items():
                if exact_match:
                    query[field] = value
                else:
                    query[field] = {"$regex": value, "$options": "i"}

        if get_all:
            if proyeccion:
                return list(self.collection.find(query,proyeccion))
            return list(self.collection.find(query))
        else:
            if proyeccion:
                return self.collection.find_one(query,proyeccion)
            return self.collection.find_one(query)

    def findByComplexQuery(self, query, get_all=False):
        """
        Find documents in the collection based on a complex query.

        Args:
            query (dict): A dictionary representing the query to filter documents.
            get_all (bool, optional): Whether to retrieve all matching documents. Default is False.

        Returns:
            dict or list: Depending on the 'get_all' parameter, either a list of matching documents or
                        the first matching document found. Returns None if no matches are found.
        """
        if get_all:
            return self.collection.find(query)
        else:
            return self.collection.find_one(query)

    def updateById(self, id, data):
        """
        Update a document in the collection based on its ObjectId.

        Args:
            id (str): The ObjectId string of the document to update.
            data (dict): The updated data to be applied.
        """
        logger.info(f"Updating document with id {id}")
        query = {"_id": ObjectId(id)}
        self.collection.update_one(query, {"$set": data})
        logger.info("Document updated")

    def updateByField(self, field, value, data, exact_match=True):
        """
        Update documents in the collection based on a field and value.

        Args:
            field (str): The field to search in.
            value (str): The value to search for.
            data (dict): The updated data to be applied.
            exact_match (bool, optional): Whether to perform an exact match or not. Default is True.
        """
        if exact_match:
            query = {field: value}
        else:
            query = {field: {"$regex": value, "$options": "i"}}
        if exact_match:
            self.collection.update_one(query, {"$set": data})
        else:
            self.collection.update_many(query, {"$set": data})

    def updateByMultipleField(self, field, value, data, exact_match=True):
        """
        Update documents in the collection based on a field and value.

        Args:
            field (str): The field to search in.
            value (str): The value to search for.
            data (dict): The updated data to be applied.
            exact_match (bool, optional): Whether to perform an exact match or not. Default is True.
        """
        query = {}
        for field, value in fields.items():
            if exact_match:
                query[field] = value
            else:
                query[field] = {"$regex": value, "$options": "i"}

        if exact_match:
            self.collection.update_one(query, {"$set": data})
        else:
            self.collection.update_many(query, {"$set": data})

    def deleteByField(self, field, value, exact_match=True):
        """
        Delete documents in the collection based on a field and value.

        Args:
            field (str): The field to search in.
            value (str): The value to search for.
            exact_match (bool, optional): Whether to perform an exact match or not. Default is True.
        """
        if exact_match:
            query = {field: value}
        else:
            query = {field: {"$regex": value, "$options": "i"}}
        if exact_match:
            self.collection.delete_one(query)
        else:
            self.collection.delete_many(query)

    def deleteByMultipleField(self, field=None, value=None, exact_match=True,custom=False,fields=None):
        """
        Delete documents in the collection based on multiple fields and their values.

        Args:
            fields (dict): A dictionary where keys are field names and values are values to search for.
            exact_match (bool, optional): Whether to perform an exact match or not. Default is True.
        """
        if custom:
            query = fields
        else:
            if exact_match:
                query = {field: value}
            else:
                query = {field: {"$regex": value, "$options": "i"}}
            
        if exact_match:
            logger.info(self.collection.delete_one(query))
        else:
            self.collection.delete_many(query)

    def deleteWithFS(self, filename):
        # Busca el archivo por nombre en GridFS
        file_info = self.fs.find_one({"filename": filename})

        if file_info:
            # Obtiene el ID del archivo

            # Elimina el archivo y sus fragmentos correspondientes
            self.fs.delete(file_info._id)
            logger.info(f"El archivo '{filename}' ha sido eliminado de GridFS.")
        else:
            logger.info(f"No se encontró el archivo '{filename}' en GridFS.")

    def doAgregate(self, pipeline):
        # Busca el archivo por nombre en GridFS
        return list(self.collection.aggregate(pipeline))


class MongoDbUtil:
    pass


if __name__ == "__main__":
    # Create a new instance of the MongoDbFunctions class
    mongo = MongoDbFunctions(
        "172.20.10.5",
        27017,
        "oscarperezarruti",
        "O28466371o#*",
        "WhealthGuardian",
        "preproduction",
    )

    logger.info(
        "Find one element in the collection with {'_id': ObjectId('64ecd7917e7233f660583a51')}"
    )
    element = mongo.findById("64ecdcdf67fe2726b022f500")
    logger.info(element["id"] if element else "No element found")

    logger.info(
        "Find one element in the collection with {'nombre': 'Juan'} with exact match"
    )
    element = mongo.findByField("nombre", "Juan")
    logger.info(element["nombre"] if element else "No element found")

    logger.info(
        "Find one element in the collection with {'nombre': 'Juan'} with no exact match"
    )
    element = mongo.findByField("nombre", "Juan", False)
    logger.info(element["nombre"] if element else "No element found")

    logger.info(
        "Find all elements in the collection with {'nombre': 'Juan'} with no exact match"
    )
    element = mongo.findByField("nombre", "Juan", False, True)
    logger.info(len(element) if element else "No element found")

    logger.info(
        "Find one document where nombre is Juan Pérez and ciudad contains Madrid"
    )
    fields = {"nombre": "Juan Pérez", "ciudad": "Madrid"}
    element = mongo.findByMultipleFields(fields)
    logger.info(
        f"{element['nombre']} | {element['ciudad']}" if element else "No element found"
    )

    logger.info("Find one document where nombre is Juan and ciudad contains Madrid")
    fields = {"nombre": "Juan Pérez", "ciudad": "Madrid"}
    element = mongo.findByMultipleFields(fields, exact_match=False)
    logger.info(
        f"{element['nombre']} | {element['ciudad']}" if element else "No element found"
    )

    logger.info("Find documents where nombre is Juan and ciudad contains Madrid")
    fields = {"nombre": "Juan", "ciudad": "Madrid"}
    elements = mongo.findByMultipleFields(fields, exact_match=False, get_all=True)
    for element in elements:
        logger.info(
            f"{element['nombre']} | {element['ciudad']}"
            if element
            else "No element found"
        )

    logger.info("Update one document where nombre is Juan Pérez")
    mongo.updateByField("nombre", "Juan Pérez", {"ciudad": "Barcelona"})
    mongo.updateByField("nombre", "Juan Pérez", {"ciudad": "Madrid"})

    logger.info("Update documents where nombre contains Juan")
    mongo.updateByField("nombre", "Juan", {"ciudad": "Barcelona"}, exact_match=False)
    mongo.updateByField("nombre", "Juan", {"ciudad": "Madrid"}, exact_match=False)

    logger.info("Delete documents where nombre contains Luis Rodríguez")
    input("Press enter to continue...")
    mongo.deleteByField("nombre", "Luis", exact_match=False)

    logger.info("Delete documents where nombre contains Juan")
    input("Press enter to continue...")
    mongo.deleteByField("nombre", "Juan", exact_match=False)

    # Find all the elements in the collection
    # element = mongo.find_all()
    # for i in element:
    #   print(i)

    # personas_data = [
    #     {
    #         "id": 12345,
    #         "nombre": "Juan Pérez",
    #         "edad": 30,
    #         "email": "juan@example.com",
    #         "ciudad": "Ciudad de México",
    #     },
    #     {
    #         "id": 67890,
    #         "nombre": "María García",
    #         "edad": 25,
    #         "email": "maria@example.com",
    #         "ciudad": "Madrid",
    #     },
    #     {
    #         "id": 98765,
    #         "nombre": "Luis Rodríguez",
    #         "edad": 28,
    #         "email": "luis@example.com",
    #         "ciudad": "Buenos Aires",
    #     },
    # ]

    # # Insertar los datos en la colección
    # mongo.insert_many(personas_data)

    # Cerrar la conexión
    mongo.close()

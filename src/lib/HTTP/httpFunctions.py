"""
Módulo de Funciones HTTP
=========================
Este módulo ofrece funciones de utilidad para realizar peticiones HTTP.
Dependencias:
    - json
    - requests
    - logger de src.system.logging_config
"""

import json
from src.system.logging_config import logger
import requests

# Configure the logger


class HttpFunctions:
    """
    Esta clase ofrece métodos estáticos para facilitar las peticiones HTTP GET y POST.
    """

    @staticmethod
    def httpRequest(method, url, payload=None, proxy=None, **parameters):
        """
        Envía una petición GET/POST a la URL especificada con datos opcionales y parámetros.

        Args:
           metodo (str): El método HTTP para la petición, ya sea "GET" o "POST".
           url (str): La URL a la cual enviar la petición.
           datos (dict, opcional): Los datos a incluir en la petición si el método es "POST".
           proxy (dict, opcional): Configuración del proxy para usar en la petición.
           **parametros: Parámetros adicionales para incluir en la URL.

        Devuelve:
              tuple: Una tupla que contiene un booleano (Verdadero si es exitoso, Falso en caso contrario) y el objeto de respuesta.
        """



        if parameters:
            parameters = HttpFunctions.buildParameterQuery(**parameters)
            url = url + parameters

        logger.debug("Request URL with parameters: %s", url)

        if method == "GET":
            response = requests.get(url, proxies=proxy,timeout=60)
        elif method == "POST":
            headers = {
                "Content-Type": "application/json"
            }  # Set the Content-Type header to JSON
            response = requests.post(
                url, data=json.dumps(payload), proxies=proxy, headers=headers,timeout=60
            )
        else:
            logger.error("Invalid HTTP method")
            return (False, "Invalid HTTP method")

        if response.status_code == 200:
            logger.debug("Request successful. Status code: %d", response.status_code)
            return (True, response)
        elif response.status_code == 201:
            logger.debug("Request successful. Status code: %d", response.status_code)
            return (True, response)
        elif response.status_code == 204:
            logger.debug("Request successful. Status code: %d", response.status_code)
            return (True, response)
        elif response.status_code == 301:
            logger.warning(
                "Resource moved permanently. Status code: %d", response.status_code
            )
            return (False, response)
        elif response.status_code == 302:
            logger.warning(
                "Resource found, temporary redirection. Status code: %d",
                response.status_code,
            )
            return (False, response)
        elif response.status_code == 400:
            logger.error("Bad request. Status code: %d", response.status_code)
            logger.error("ERROR: %s", response.text)
            return (False, response)
        elif response.status_code == 401:
            logger.error("Unauthorized access. Status code: %d", response.status_code)
            logger.error("ERROR: %s", response.text)

            return (False, response)
        elif response.status_code == 403:
            logger.error("Access forbidden. Status code: %d", response.status_code)
            logger.error("ERROR: %s", response.text)
            return (False, response)
        elif response.status_code == 404:
            logger.error("Resource not found. Status code: %d", response.status_code)
            logger.error("ERROR: %s", response.text)
            return (False, response)
        elif response.status_code == 500:
            logger.error("Internal server error. Status code: %d", response.status_code)
            logger.error("ERROR: %s", response.text)
            return (False, response)
        else:
            logger.error("Unknown error. Status code: %d", response.status_code)
            logger.error("ERROR: %s", response.text)
            return (False, response)

    @staticmethod
    def buildParameterQuery(**kwargs):
        """
        Construye una cadena de consulta URL a partir de los argumentos de palabras clave dados.

        Args:
            **kwargs: Pares de clave-valor para incluir en la cadena de consulta.

        Devuelve:
            str: La cadena de consulta construida empezando con '?'.
        """
        ...
        ...
        logger.debug("Building parameter query")
        params = []
        for key, value in kwargs.items():
            params.append("=".join([str(key), str(value)]))
            logger.debug("Parameter: %s", params[-1])
        return "?" + "&".join(params)

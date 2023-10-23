import json
from src.system.logging_config import logger
import requests

# Configure the logger


class HttpFunctions:
    @staticmethod
    def httpRequest(method, url, payload=None, proxy=None, **parameters):
        """
        Send a GET/POST request to the specified URL with optional payload and parameters.

        Args:
           method (str): The Method of the the request.
           url (str): The URL to send the request to.
           payload (dict, optional): The payload to include in the request.
           proxy (dict, optional): Proxy configuration to use for the request.
           **parameters: Additional parameters to include in the URL.

        Returns:
              tuple: A tuple containing a response message and the response object.
        """
        logger.debug("Sending GET request to URL: %s", url)

        if parameters:
            parameters = HttpFunctions.buildParameterQuery(**parameters)
            url = url + parameters

        logger.debug("Request URL with parameters: %s", url)

        if method == "GET":
            response = requests.get(url, proxies=proxy)
        elif method == "POST":
            headers = {
                "Content-Type": "application/json"
            }  # Set the Content-Type header to JSON
            response = requests.post(
                url, data=json.dumps(payload), proxies=proxy, headers=headers
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
            return (False, response)
        elif response.status_code == 401:
            logger.error("Unauthorized access. Status code: %d", response.status_code)
            return (False, response)
        elif response.status_code == 403:
            logger.error("Access forbidden. Status code: %d", response.status_code)
            return (False, response)
        elif response.status_code == 404:
            logger.error("Resource not found. Status code: %d", response.status_code)
            return (False, response)
        elif response.status_code == 500:
            logger.error("Internal server error. Status code: %d", response.status_code)
            return (False, response)
        else:
            logger.error("Unknown error. Status code: %d", response.status_code)
            return (False, response)

    @staticmethod
    def buildParameterQuery(**kwargs):
        logger.debug("Building parameter query")
        params = []
        for key, value in kwargs.items():
            params.append("=".join([str(key), str(value)]))
            logger.debug("Parameter: %s", params[-1])
        return "?" + "&".join(params)

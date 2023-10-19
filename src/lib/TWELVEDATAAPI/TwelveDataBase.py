from src.system.logging_config import logger
from ..HTTP import HttpFunctions

# Configure the logger



class TwelveDataBase:
    url = "https://api.twelvedata.com"

    @staticmethod
    def api_request(endpoint, requiered_parameters, optional_parameters, **parameters):
        required = requiered_parameters
        optional = optional_parameters

        # Check for missing required parameters
        for required_parameter in required:
            if required_parameter not in parameters:
                logger.error("Missing required parameter: %s", required_parameter)
                return (False,"Missing required parameter")

        # Check for invalid parameters
        for input_parameter in parameters:
            if input_parameter not in required and input_parameter not in optional:
                logger.error("Invalid parameter: %s", input_parameter)
                return  (False,"Invalid parameter")

        final_parameters = []

        for parameter_name in parameters:
            final_parameters.append(f"{parameter_name}={parameters[parameter_name]}")

        if final_parameters:
            final_url = TwelveDataBase.url + endpoint + "?" + "&".join(final_parameters)
        else:
            final_url = TwelveDataBase.url + endpoint
            
        logger.info("Performing request to %s", final_url)    
        
        return HttpFunctions.httpRequest("GET", final_url)

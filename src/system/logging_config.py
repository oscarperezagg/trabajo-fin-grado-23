import logging
from colorlog import ColoredFormatter

# Configura el logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # Establece el nivel de registro en INFO o inferior

# Crea un nuevo formatter que colorea las trazas de ERROR en rojo
formatter = ColoredFormatter(
    "%(log_color)s|     [%(asctime)s] [%(levelname)s] [%(filename)s] [Line %(lineno)d] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    log_colors={
        "DEBUG": "white",  # Change DEBUG to yellow
        "INFO": "cyan",  # Change INFO to cyan
        "WARNING": "yellow",  # Change WARNING to orange
        "ERROR": "red",  # Keep ERROR as red
        "CRITICAL": "red,bg_white",  # Keep CRITICAL as red with a white background
    },
)

# Configura el handler del logger con el formatter
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)

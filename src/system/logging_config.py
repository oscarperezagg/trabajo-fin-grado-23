import os
import logging
from colorlog import ColoredFormatter
from logging.handlers import TimedRotatingFileHandler
import datetime

# Configura el directorio para guardar los registros
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)

# Configura el logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # Establece el nivel de registro en INFO o inferior

# Crea un nuevo formatter que colorea las trazas de ERROR en rojo
formatter = ColoredFormatter(
    "%(log_color)s|     [%(asctime)s] [%(levelname)s] [%(filename)s] [Line %(lineno)d] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    log_colors={
        "DEBUG": "white",
        "INFO": "cyan",
        "WARNING": "yellow",
        "ERROR": "red",
        "CRITICAL": "red,bg_white",
    },
)

# Configura el handler para mostrar en pantalla
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Configura el handler para guardar en archivo rotativo diario
log_file = os.path.join(log_dir, "app.log")
file_handler = TimedRotatingFileHandler(log_file, when="midnight", interval=1, backupCount=7)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Ahora los registros se mostrarán por pantalla y se guardarán en un archivo nuevo para cada día en la carpeta "logs"

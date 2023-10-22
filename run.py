from src import iniciar_app
from src.system.logging_config import logger
if __name__ == "__main__":
    # Get system arguments
    import sys
    justStats = False
    if len(sys.argv) > 1:
        if sys.argv[1] == 'stats':
            justStats = True
    logger.info(f"justStats: {justStats}")
    iniciar_app(justStats)




import logging
from functools import wraps

def log_api_call(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        logging.info(f"➡️ Llamando {func.__name__} con args={args[1:]}, kwargs={kwargs}")
        try:
            result = func(*args, **kwargs)
            #logging.info(f"✅ Respuesta {func.__name__}: {result}")
            return result
        except Exception as e:
            logging.error(f"❌ Error en {func.__name__}: {e}", exc_info=True)
            raise
    return wrapper

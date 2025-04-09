from confluent_kafka import Consumer, KafkaException
import os
from dotenv import load_dotenv
import time
import logging
from typing import Optional

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_kafka_config() -> dict:    
    return {
        'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVER'),
        'group.id': os.getenv('KAFKA_GROUP_ID'),
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
        'max.poll.interval.ms': 300000
    }

def process_message(msg: Optional[bytes]) -> None:
    if msg is None:
        return
    
    try:
        decoded_message = msg.decode('utf-8')
        logger.info(f"Mensaje recibido: {decoded_message}")
        # Aquí puedes agregar la lógica de procesamiento específica
        time.sleep(5)  # Simulación de procesamiento
        logger.info("Procesamiento completado")
    except UnicodeDecodeError as e:
        logger.error(f"Error al decodificar el mensaje: {e}")
    except Exception as e:
        logger.error(f"Error al procesar el mensaje: {e}")

def handle_kafka_message(msg) -> None:
    if msg is None:
        return
        
    if msg.error():
        logger.error(f"Error en el mensaje: {msg.error()}")
        return
        
    process_message(msg.value())

def consumir_mensajes() -> None:
    config = get_kafka_config()
    topic = os.getenv('KAFKA_TOPIC')
    
    if not all([config['bootstrap.servers'], config['group.id'], topic]):
        logger.error("Faltan variables de entorno necesarias")
        return

    consumer = Consumer(config)
    consumer.subscribe([topic])
    
    logger.info(f"Iniciando consumo de mensajes del topic: {topic}")
    
    try:
        while True:
            msg = consumer.poll(5.0)
            handle_kafka_message(msg)
                
    except KafkaException as e:
        logger.error(f"Error de Kafka: {e}")
    except KeyboardInterrupt:
        logger.info("Consumo interrumpido por el usuario")
    except Exception as e:
        logger.error(f"Error inesperado: {e}")
    finally:
        consumer.close()
        logger.info("Consumidor cerrado")

if __name__ == "__main__":
    load_dotenv()
    consumir_mensajes()
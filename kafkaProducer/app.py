import json
import logging
import sys
from typing import Dict, Any, Optional
from confluent_kafka import Producer, KafkaError
import os
from dotenv import load_dotenv

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constantes
EXIT_COMMAND = 'exit'
DEFAULT_POLL_TIMEOUT = 0
MESSAGE_TEMPLATE = {
    'code': 'kafka message',
    'message': ''
}

class KafkaProducer:
    def __init__(self, bootstrap_servers: str, topic: str):
        self.topic = topic
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'python-producer',
            'acks': 'all',
            'retries': 3
        })
        logger.info(f"Productor Kafka inicializado para el tópico: {topic}")

    def delivery_report(self, err: Optional[KafkaError], msg: Any) -> None:
        if err is not None:
            logger.error(f"Error en la entrega: {err}")
        else:
            logger.info(f"Mensaje entregado a {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

    def send_message(self, message: str) -> None:
        try:
            message_data = MESSAGE_TEMPLATE.copy()
            message_data['message'] = message
            
            self.producer.produce(
                self.topic,
                json.dumps(message_data),
                callback=self.delivery_report
            )
            self.producer.poll(DEFAULT_POLL_TIMEOUT)
        except Exception as e:
            logger.error(f"Error al enviar mensaje: {e}")
            raise

    def close(self) -> None:
        try:
            self.producer.flush()
            logger.info("Productor Kafka cerrado correctamente")
        except Exception as e:
            logger.error(f"Error al cerrar el productor: {e}")
            raise

def load_config() -> Dict[str, str]:
    load_dotenv()
    required_vars = ['KAFKA_BOOTSTRAP_SERVER', 'KAFKA_TOPIC']
    
    config = {}
    for var in required_vars:
        value = os.getenv(var)
        if not value:
            raise ValueError(f"Variable de entorno {var} no encontrada")
        config[var] = value
    
    return config

def main() -> None:
    try:
        config = load_config()
        producer = KafkaProducer(
            bootstrap_servers=config['KAFKA_BOOTSTRAP_SERVER'],
            topic=config['KAFKA_TOPIC']
        )

        logger.info("Iniciando productor de mensajes. Escribe 'exit' para salir.")
        while True:
            try:
                message = input("Escribe un mensaje para enviar a Kafka: ")
                if message.lower() == EXIT_COMMAND:
                    break
                
                producer.send_message(message)
            except KeyboardInterrupt:
                logger.info("\nInterrumpido por el usuario")
                break
            except Exception as e:
                logger.error(f"Error al procesar mensaje: {e}")
                continue

    except Exception as e:
        logger.error(f"Error fatal: {e}")
        sys.exit(1)
    finally:
        if 'producer' in locals():
            producer.close()

if __name__ == "__main__":
    main()
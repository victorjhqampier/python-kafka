from confluent_kafka import Consumer, KafkaException
import os
from dotenv import load_dotenv

def consumir_mensajes():
    # Configurar el consumidor
    conf = {
        'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVER'),
        'group.id': os.getenv('KAFKA_GROUP_ID'),
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(conf)
    consumer.subscribe([os.getenv('KAFKA_TOPIC')])

    try:
        print("Esperando mensajes...")
        while True:
            msg = consumer.poll(5.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            print(f"Recibido: {msg.value().decode('utf-8')}")
    
    except KeyboardInterrupt:
        print("Interrumpido por el usuario")
    
    finally:
        # Asegurarse de cerrar el consumidor
        consumer.close()

if __name__ == "__main__":
    load_dotenv()
    consumir_mensajes()
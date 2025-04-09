import json
from confluent_kafka import Producer
import sys
import os
from dotenv import load_dotenv

def delivery_report(err, msg):    
    if err is not None:
        print(f"Error en la entrega: {err}")
    else:
        print(f"Mensaje entregado a {msg.topic()} [{msg.partition()}]")

def producir_mensajes():
    conf = {
        'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVER')
    }

    producer = Producer(conf)

    try:
        while True:
            mensaje = input("Escribe un mensaje en formato json (o 'exit' para salir): ")
            if mensaje.lower() == 'exit':
                break
            mess = {}
            # mess = {
            #     'code': 'kafka message',
            #     'message': mensaje
            # }
            try:
                mess = json.loads(mensaje)
            except json.JSONDecodeError as e:
                print("Error al convertir JSON:", str(e))
            print(json.dumps(mess))
            producer.produce(os.getenv('KAFKA_TOPIC'), json.dumps(mess), callback=delivery_report)
            producer.poll(0)
        producer.flush()

    except KeyboardInterrupt:
        print("\nInterrumpido por el usuario")

if __name__ == "__main__":
    load_dotenv()
    print(os.getenv('KAFKA_BOOTSTRAP_SERVER'),os.getenv('KAFKA_TOPIC'),os.getenv('KAFKA_GROUP_ID'))
    producir_mensajes()
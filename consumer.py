import paho.mqtt.client as mqtt
import json
import requests
import os
from dotenv import load_dotenv

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# --- Configuración ---
# Es una buena práctica usar variables de entorno para esto en un entorno de producción
MQTT_BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "localhost")
MQTT_BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", 1883))
MQTT_TOPIC = "/maquina/+/venta/dispensado"
# La URL correcta ahora se cargará desde el .env
API_ENDPOINT = os.getenv("API_ENDPOINT", "http://localhost:8000/api/v1/contenedores/dispensar")
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000/api/v1")

# --- Lógica del Consumidor ---

def on_connect(client, userdata, flags, rc):
    """Se ejecuta cuando el cliente se conecta al broker."""
    if rc == 0:
        print(f"Conectado exitosamente al Broker MQTT en {MQTT_BROKER_HOST}:{MQTT_BROKER_PORT}")
        # Suscribirse al tópico. El '+' es un comodín para cualquier ID de máquina.
        client.subscribe(MQTT_TOPIC)
        client.subscribe("sensores/temperatura")
        print(f"Suscrito al tópico: {MQTT_TOPIC}")
    else:
        print(f"Fallo al conectar, código de retorno: {rc}")

def on_message(client, userdata, msg):
    if msg.topic == "sensores/temperatura":
        payload = json.loads(msg.payload.decode())
        contenedor_id = payload['id_contenedor']  # CLAVE CORRECTA
        temperatura = payload['temperatura']
        
        # Actualizar temperatura via API
        api_url = f"{os.getenv('API_BASE_URL', 'http://localhost:8000/api/v1')}/contenedores/{contenedor_id}/temperatura"
        response = requests.put(api_url, json={"temperatura": temperatura})
        return
    
    """Se ejecuta cuando se recibe un mensaje del broker."""
    print(f"Mensaje recibido en el tópico {msg.topic}")
    
    try:
        # 1. Extraer el ID de la máquina del tópico
        topic_parts = msg.topic.split('/')
        # topic_parts será ['', 'maquina', 'ID_MAQUINA', 'venta', 'dispensado']
        if len(topic_parts) == 5 and topic_parts[1] == 'maquina' and topic_parts[3] == 'venta':
            id_maquina = int(topic_parts[2])
        else:
            print(f"Error: El tópico '{msg.topic}' no tiene el formato esperado.")
            return

        # 2. Decodificar el payload (mensaje)
        payload = json.loads(msg.payload.decode('utf-8'))
        id_producto = int(payload['id_producto'])
        cantidad_dispensada = float(payload['cantidad_dispensada'])

        print(f"Datos procesados: Máquina={id_maquina}, Producto={id_producto}, Cantidad={cantidad_dispensada}")

        # 3. Preparar los datos para la API
        api_data = {
            "id_maquina": id_maquina,
            "id_producto": id_producto,
            "cantidad_dispensada": cantidad_dispensada
        }

        # 4. Llamar al endpoint de la API usando la variable global
        print(f"Enviando petición POST a {API_ENDPOINT}...")
        response = requests.post(API_ENDPOINT, json=api_data)


        # 5. Procesar la respuesta de la API
        if response.status_code == 200:
            print("Éxito: La API procesó la solicitud correctamente.")
            print("Respuesta de la API:", response.json())
        else:
            print(f"Error: La API devolvió un estado {response.status_code}")
            print("Detalle del error:", response.text)

    except json.JSONDecodeError:
        print("Error: No se pudo decodificar el payload. No es un JSON válido.")
    except (KeyError, TypeError) as e:
        print(f"Error: El payload JSON no tiene el formato esperado. Falta la clave: {e}")
    except requests.exceptions.RequestException as e:
        print(f"Error de conexión: No se pudo contactar a la API en {API_ENDPOINT}. Detalle: {e}")
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")


# --- Inicialización del Cliente MQTT ---

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
client.on_connect = on_connect
client.on_message = on_message

# Conexión inicial
try:
    print("Intentando conectar al broker MQTT...")
    client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, 60)
except Exception as e:
    print(f"No se pudo establecer la conexión inicial con el broker: {e}")
    exit(1) # Salir si no se puede conectar al inicio

# Bucle principal para mantener el script corriendo y escuchando
client.loop_forever()
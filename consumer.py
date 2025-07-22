import paho.mqtt.client as mqtt
import json
import requests
import os
from dotenv import load_dotenv

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

MQTT_BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "50.19.13.195")
MQTT_BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", 1883))
MQTT_TOPIC_DISPENSADO = "/maquina/+/venta/dispensado"
MQTT_TOPIC_SENSORES = "/+/sensor/#"
MQTT_TOPIC_VALVULAS = "/maquina/+/valvula/+/confirmacion"

# La URL correcta ahora se cargará desde el .env
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000/api/v1")
API_ENDPOINT_DISPENSAR = f"{API_BASE_URL}/contenedores/dispensar"
API_ENDPOINT_SENSORES = f"{API_BASE_URL}/sensores"
API_ENDPOINT_VALVULAS = f"{API_BASE_URL}/valvulas/confirmar-dispensado"
# Agregar endpoint de ventas
API_ENDPOINT_VENTAS = f"{API_BASE_URL}/ventas"

def on_connect(client, userdata, flags, rc):
    """Se ejecuta cuando el cliente se conecta al broker."""
    if rc == 0:
        print(f"Conectado exitosamente al Broker MQTT en {MQTT_BROKER_HOST}:{MQTT_BROKER_PORT}")
        # Suscribirse a los tópicos
        client.subscribe(MQTT_TOPIC_DISPENSADO)
        client.subscribe(MQTT_TOPIC_SENSORES)
        client.subscribe(MQTT_TOPIC_VALVULAS)
        print(f"Suscrito a los tópicos: {MQTT_TOPIC_DISPENSADO}, {MQTT_TOPIC_SENSORES} y {MQTT_TOPIC_VALVULAS}")
    else:
        print(f"Fallo al conectar, código de retorno: {rc}")

def on_message(client, userdata, msg):
    """Se ejecuta cuando se recibe un mensaje del broker."""
    print(f"Mensaje recibido en el tópico {msg.topic}")
    
    try:
        topic_parts = msg.topic.strip('/').split('/')  
    
        # --- Lógica para Sensores (formato: machine_id/sensor/sensor_type) ---
        if len(topic_parts) >= 2 and topic_parts[1] == 'sensor':
            machine_id = topic_parts[0]  # raspberrypi
            sensor_type = "/".join(topic_parts[2:])  # temperatura
    
            # El payload es un valor simple, no un JSON. Lo procesamos directamente.
            try:
                value_str = msg.payload.decode('utf-8')
                value_numeric = float(value_str)
            except (UnicodeDecodeError, ValueError) as e:
                print(f"Error al procesar el payload del sensor: {e}")
                return

            # Determinar la unidad basándose en el tipo de sensor
            unit = "desconocida"
            if "temperatura" in sensor_type:
                unit = "°C"
            elif "humedad" in sensor_type:
                unit = "%"
            elif "ph" in sensor_type:
                unit = "pH"
            elif "ultrasonico" in sensor_type:
                unit = "cm"
            elif "moneda" in sensor_type:
                unit = "mxn"
            
            api_data = {
                "machine_id": machine_id,
                "sensor_type": sensor_type,
                "value_numeric": value_numeric,
                "unit": unit
            }

            print(f"Enviando datos de sensor a {API_ENDPOINT_SENSORES}: {api_data}")
            response = requests.post(API_ENDPOINT_SENSORES, json=api_data)

            if response.status_code == 201:
                print("Éxito: La API creó el registro del sensor correctamente.")
                print("Los datos se enviarán automáticamente por WebSocket desde la API.")
            else:
                print(f"Error al enviar datos del sensor: La API devolvió un estado {response.status_code}")
                print("Detalle del error:", response.text)
            return # Importante para no procesar como dispensado

        # --- Lógica para Dispensado ---
        if len(topic_parts) == 5 and topic_parts[1] == 'maquina' and topic_parts[3] == 'venta':
            id_maquina = int(topic_parts[2])
            payload = json.loads(msg.payload.decode('utf-8'))
            id_producto = int(payload['id_producto'])
            cantidad_dispensada = float(payload['cantidad_dispensada'])

            print(f"Datos de dispensado procesados: Máquina={id_maquina}, Producto={id_producto}, Cantidad={cantidad_dispensada}")

            api_data = {
                "id_maquina": id_maquina,
                "id_producto": id_producto,
                "cantidad_dispensada": cantidad_dispensada
            }

            print(f"Enviando petición POST a {API_ENDPOINT_DISPENSAR}...")
            response = requests.post(API_ENDPOINT_DISPENSAR, json=api_data)

            if response.status_code == 200:
                print("Éxito: La API procesó la solicitud de dispensado correctamente.")
                print("Respuesta de la API:", response.json())
            else:
                print(f"Error en dispensado: La API devolvió un estado {response.status_code}")
                print("Detalle del error:", response.text)
            return

        # --- Lógica para Válvulas (formato: /maquina/{id}/valvula/{pin}/confirmacion) ---
        if len(topic_parts) == 5 and topic_parts[0] == 'maquina' and topic_parts[2] == 'valvula' and topic_parts[4] == 'confirmacion':
            id_maquina = int(topic_parts[1])
            pin_valvula = int(topic_parts[3])
            payload = json.loads(msg.payload.decode('utf-8'))
            
            id_producto = int(payload['id_producto'])
            cantidad_dispensada = float(payload['cantidad_dispensada'])
            estado = payload.get('estado', 'completado')
            
            print(f"Confirmación de válvula recibida: Máquina={id_maquina}, Pin={pin_valvula}, Producto={id_producto}, Cantidad={cantidad_dispensada}")
            
            # Actualizar el contenedor con la cantidad dispensada
            api_data = {
                "id_maquina": id_maquina,
                "id_producto": id_producto,
                "cantidad_dispensada": cantidad_dispensada
            }
            
            print(f"Enviando confirmación de dispensado a {API_ENDPOINT_DISPENSAR}...")
            response = requests.post(API_ENDPOINT_DISPENSAR, json=api_data)
            
            # En la sección de confirmación de válvulas (línea ~120), después de actualizar el contenedor:
            if response.status_code == 200:
                print("Éxito: La API actualizó el contenedor después del dispensado de válvula.")
                print("Respuesta de la API:", response.json())
                
                # NUEVO: Registrar la venta
                venta_data = {
                    "id_maquina": id_maquina,
                    "id_producto": id_producto,
                    "cantidad_dispensada": cantidad_dispensada,
                    "pin_valvula": pin_valvula,
                    "metodo_dispensado": "valvula"
                }
                
                print(f"Registrando venta: {venta_data}")
                venta_response = requests.post(API_ENDPOINT_VENTAS, json=venta_data)
                
                if venta_response.status_code == 201:
                    print("✅ Venta registrada exitosamente")
                    print("Datos de la venta:", venta_response.json())
                else:
                    print(f"❌ Error al registrar venta: {venta_response.status_code}")
                    print("Detalle:", venta_response.text)
            else:
                print(f"Error al actualizar contenedor: La API devolvió un estado {response.status_code}")
                print("Detalle del error:", response.text)
            return
        else:
            print(f"Error: El tópico '{msg.topic}' no coincide con ningún patrón conocido.")

    except json.JSONDecodeError:
        print("Error: No se pudo decodificar el payload. No es un JSON válido.")
    except (KeyError, TypeError) as e:
        print(f"Error: El payload JSON no tiene el formato esperado. Falta la clave: {e}")
    except requests.exceptions.RequestException as e:
        print(f"Error de conexión: No se pudo contactar a la API. Detalle: {e}")
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")

# --- Inicialización del Cliente MQTT ---
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

# Conexión inicial
try:
    print("Intentando conectar al broker MQTT...")
    client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, 60)
except Exception as e:
    print(f"No se pudo establecer la conexión inicial con el broker: {e}")
    exit(1)

# Bucle principal para mantener el script corriendo y escuchando
print("Consumer MQTT iniciado. Presiona Ctrl+C para detener.")
client.loop_forever()
import socket
import threading
import json
import sys
import time
import os
from datetime import datetime
from typing import Dict, List, Optional
import csv


class LamportClock:
    """Reloj lógico de Lamport para mantener el orden causal de eventos."""

    def __init__(self):
        """Inicializa el reloj en 0."""
        self.value = 0
        self.lock = threading.Lock()

    def increment(self) -> int:
        """
        Incrementa el reloj en 1 y retorna el nuevo valor.
        Se usa al enviar un mensaje.
        """
        with self.lock:
            self.value += 1
            return self.value

    def update(self, received_timestamp: int) -> int:
        """
        Actualiza el reloj usando la fórmula de Lamport:
        local = max(local, recibido) + 1

        Args:
            received_timestamp: Timestamp recibido del otro nodo

        Returns:
            El nuevo valor del reloj
        """
        with self.lock:
            self.value = max(self.value, received_timestamp) + 1
            return self.value

    def get_value(self) -> int:
        """Obtiene el valor actual del reloj sin modificarlo."""
        with self.lock:
            return self.value


class TCPClient:
    """
    Cliente TCP que se conecta al servidor y maneja la comunicación
    con reloj lógico de Lamport.
    """

    def __init__(self, node_id: str, host: str = 'localhost', port: int = 5000):
        """
        Inicializa el cliente TCP.

        Args:
            node_id: Identificador único del nodo
            host: Dirección del servidor
            port: Puerto del servidor
        """
        self.node_id = node_id
        self.host = host
        self.port = port
        self.socket = None
        self.lamport_clock = LamportClock()
        self.running = False
        self.messages_history: List[Dict] = []
        self.messages_lock = threading.Lock()

    def connect(self) -> bool:
        """
        Conecta al servidor TCP.

        Returns:
            True si la conexión fue exitosa, False en caso contrario
        """
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
            self.running = True
            print(f"[{self.node_id}] Conectado al servidor {self.host}:{self.port}")

            # Iniciar thread para recibir mensajes
            receive_thread = threading.Thread(target=self._receive_messages, daemon=True)
            receive_thread.start()

            return True

        except ConnectionRefusedError:
            print(f"[{self.node_id}] ERROR: No se pudo conectar al servidor {self.host}:{self.port}")
            return False
        except Exception as e:
            print(f"[{self.node_id}] ERROR: {e}")
            return False

    def send_message(self, message: str) -> bool:
        """
        Envía un mensaje al servidor.

        Args:
            message: Contenido del mensaje

        Returns:
            True si se envió correctamente, False en caso contrario
        """
        try:
            # Incrementar reloj al enviar
            timestamp = self.lamport_clock.increment()

            # Crear mensaje JSON
            payload = {
                'sender': self.node_id,
                'timestamp': timestamp,
                'message': message
            }

            # Enviar
            self.socket.send(json.dumps(payload).encode('utf-8'))

            # Registrar en histórico
            with self.messages_lock:
                self.messages_history.append({
                    'type': 'sent',
                    'sender': self.node_id,
                    'timestamp': timestamp,
                    'message': message,
                    'datetime': datetime.now().isoformat()
                })

            print(f"[{self.node_id}] ENVIADO (Lamport: {timestamp}): {message}")
            return True

        except Exception as e:
            print(f"[{self.node_id}] ERROR al enviar: {e}")
            return False

    def _receive_messages(self):
        """
        Recibe mensajes del servidor en un thread secundario.
        Actualiza el reloj de Lamport según la fórmula.
        """
        try:
            while self.running:
                data = self.socket.recv(1024).decode('utf-8')

                if not data:
                    print(f"[{self.node_id}] Conexión cerrada por el servidor")
                    self.running = False
                    break

                try:
                    message = json.loads(data)

                    # Si contiene campo 'error', es un error del servidor
                    if 'error' in message:
                        print(f"[{self.node_id}] Error del servidor: {message['error']}")
                        continue

                    # Actualizar reloj de Lamport con el timestamp recibido
                    received_timestamp = int(message.get('timestamp', 0))
                    new_timestamp = self.lamport_clock.update(received_timestamp)

                    # Registrar en histórico
                    with self.messages_lock:
                        self.messages_history.append({
                            'type': 'received',
                            'sender': message.get('sender', 'unknown'),
                            'received_timestamp': received_timestamp,
                            'local_timestamp_after_update': new_timestamp,
                            'message': message.get('message', ''),
                            'datetime': datetime.now().isoformat()
                        })

                    print(f"[{self.node_id}] RECIBIDO de {message.get('sender')} "
                          f"(Lamport recibido: {received_timestamp}, local actualizado: {new_timestamp}): "
                          f"{message.get('message', '')}")

                except json.JSONDecodeError:
                    print(f"[{self.node_id}] ERROR: Formato JSON inválido recibido")

        except Exception as e:
            if self.running:
                print(f"[{self.node_id}] ERROR recibiendo mensajes: {e}")

    def disconnect(self):
        """Desconecta del servidor."""
        self.running = False
        if self.socket:
            try:
                self.socket.close()
            except OSError:
                pass
        print(f"[{self.node_id}] Desconectado")

    def auto_exchange_messages(self, num_exchanges: int = 20, delay: float = 1.0) -> List[Dict]:
        """
        Realiza automáticamente N intercambios de mensajes.
        Útil para pruebas.

        Args:
            num_exchanges: Número de mensajes a enviar
            delay: Tiempo de espera entre mensajes (en segundos)

        Returns:
            Lista con el histórico de mensajes
        """
        print(f"\n[{self.node_id}] Iniciando {num_exchanges} intercambios de mensajes...")

        for i in range(num_exchanges):
            message = f"Mensaje automático {i + 1} del nodo {self.node_id}"
            self.send_message(message)
            time.sleep(delay)

        print(f"[{self.node_id}] Intercambios completados")
        return self.messages_history

    def save_timestamps_to_json(self, output_dir: str = 'data') -> str:
        """
        Guarda el histórico de timestamps en un archivo JSON.

        Args:
            output_dir: Directorio donde guardar el archivo

        Returns:
            Ruta del archivo creado
        """
        # Crear directorio si no existe
        os.makedirs(output_dir, exist_ok=True)

        # Nombre del archivo con timestamp y ID del nodo
        filename = os.path.join(output_dir, f'timestamps_{self.node_id}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')

        # Preparar datos
        data = {
            'node_id': self.node_id,
            'lamport_clock_final': self.lamport_clock.get_value(),
            'messages': self.messages_history
        }

        # Guardar a JSON
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        print(f"[{self.node_id}] Timestamps guardados en: {filename}")
        return filename

    def measure_latency(self, num_sends: int = 100, output_dir: str = 'data') -> Dict:
        """
        Mide la latencia promedio de N envíos consecutivos.
        Usa time.perf_counter() para precisión.

        Args:
            num_sends: Número de envíos a realizar
            output_dir: Directorio donde guardar el CSV

        Returns:
            Diccionario con estadísticas de latencia
        """
        print(f"\n[{self.node_id}] Midiendo latencia de {num_sends} envíos...")

        latencies = []

        for i in range(num_sends):
            start = time.perf_counter()
            self.send_message(f"Prueba de latencia {i + 1}")
            end = time.perf_counter()

            latency_ms = (end - start) * 1000  # Convertir a milisegundos
            latencies.append(latency_ms)

            # Pequeño delay entre envíos
            time.sleep(0.01)

        # Calcular estadísticas
        avg_latency = sum(latencies) / len(latencies)
        min_latency = min(latencies)
        max_latency = max(latencies)

        stats = {
            'node_id': self.node_id,
            'num_sends': num_sends,
            'avg_latency_ms': avg_latency,
            'min_latency_ms': min_latency,
            'max_latency_ms': max_latency,
            'timestamp': datetime.now().isoformat()
        }

        # Crear directorio si no existe
        os.makedirs(output_dir, exist_ok=True)

        # Guardar en CSV
        csv_file = os.path.join(output_dir, 'latency_sockets.csv')
        file_exists = os.path.isfile(csv_file)

        with open(csv_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['node_id', 'num_sends', 'avg_latency_ms', 'min_latency_ms', 'max_latency_ms', 'timestamp'])
            if not file_exists:
                writer.writeheader()
            writer.writerow(stats)

        print(f"[{self.node_id}] Latencia promedio: {avg_latency:.4f} ms")
        print(f"[{self.node_id}] Rango: {min_latency:.4f} - {max_latency:.4f} ms")
        print(f"[{self.node_id}] Resultados guardados en: {csv_file}")

        return stats


def interactive_mode(client: TCPClient):
    """
    Modo interactivo donde el usuario puede enviar mensajes manualmente.

    Args:
        client: Instancia del cliente TCP
    """
    print(f"\n[{client.node_id}] Modo interactivo. Escribe 'quit' para salir.")
    print("Comandos disponibles:")
    print("  - quit: Salir")
    print("  - history: Ver histórico de mensajes")
    print("  - clock: Ver reloj de Lamport actual")
    print("  - save: Guardar timestamps a JSON")
    print("  - measure: Medir latencia")
    print("  - auto: Ejecutar 20 intercambios automáticos")
    print()

    while client.running:
        try:
            user_input = input(f"[{client.node_id}] > ").strip()

            if user_input.lower() == 'quit':
                break

            elif user_input.lower() == 'history':
                print(f"\nHistórico de {len(client.messages_history)} eventos:")
                for i, msg in enumerate(client.messages_history, 1):
                    print(f"  {i}. {msg}")

            elif user_input.lower() == 'clock':
                print(f"Reloj de Lamport actual: {client.lamport_clock.get_value()}")

            elif user_input.lower() == 'save':
                filename = client.save_timestamps_to_json()
                print(f"Guardado en: {filename}")

            elif user_input.lower() == 'measure':
                client.measure_latency(num_sends=100)

            elif user_input.lower() == 'auto':
                client.auto_exchange_messages(num_exchanges=20, delay=0.5)

            elif user_input:
                client.send_message(user_input)

        except KeyboardInterrupt:
            print("\nInterrupción detectada")
            break
        except Exception as e:
            print(f"ERROR: {e}")


def main():
    """Función principal del cliente."""
    # Obtener ID del nodo desde argumentos de línea de comandos
    if len(sys.argv) > 1:
        node_id = sys.argv[1]
    else:
        node_id = input("Ingresa el ID/nombre del nodo: ").strip()
        if not node_id:
            node_id = f"cliente_{int(time.time() * 1000) % 10000}"

    print(f"Iniciando cliente con ID: {node_id}")

    # Crear cliente
    client = TCPClient(node_id=node_id)

    # Conectar al servidor
    if not client.connect():
        return

    try:
        # Entrar en modo interactivo
        interactive_mode(client)

    except KeyboardInterrupt:
        print("\n[INTERRUPCIÓN] Presionado Ctrl+C")

    finally:
        # Guardar histórico antes de desconectar
        if client.messages_history:
            client.save_timestamps_to_json()

        client.disconnect()


if __name__ == '__main__':
    main()

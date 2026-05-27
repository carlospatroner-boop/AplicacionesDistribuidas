import sys
import os
import time
import json
import random
import queue
import threading
from datetime import datetime
from concurrent import futures

import grpc
from . import messaging_pb2
from . import messaging_pb2_grpc


class LamportClock:
    """Reloj lógico de Lamport para mantener orden causal."""

    def __init__(self):
        self.value = 0
        self.lock = threading.Lock()

    def increment(self) -> int:
        with self.lock:
            self.value += 1
            return self.value

    def update(self, received_timestamp: int) -> int:
        with self.lock:
            self.value = max(self.value, received_timestamp) + 1
            return self.value

    def get_value(self) -> int:
        with self.lock:
            return self.value


class GRPCClient:
    """Cliente gRPC que usa un reloj de Lamport y ofrece un menú interactivo."""

    def __init__(self, node_id: str, host: str = 'localhost', port: int = 5001):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.channel = None
        self.stub = None
        self.lamport_clock = LamportClock()
        self.running = False
        self.response_queue: queue.Queue = queue.Queue()
        self.messages_history = []
        self.history_lock = threading.Lock()
        self.response_thread: threading.Thread = None

    def connect(self) -> bool:
        try:
            self.channel = grpc.insecure_channel(f'{self.host}:{self.port}')
            self.stub = messaging_pb2_grpc.MessagingServiceStub(self.channel)
            self.running = True
            self.response_thread = threading.Thread(target=self._process_responses, daemon=True)
            self.response_thread.start()
            print(f'[{self.node_id}] Conectado al servidor gRPC {self.host}:{self.port}')
            return True
        except Exception as e:
            print(f'[{self.node_id}] ERROR al conectar: {e}')
            return False

    def _process_responses(self):
        while self.running:
            try:
                response, event = self.response_queue.get(timeout=0.5)
                if response is None:
                    break

                received_timestamp = int(response.timestamp)
                new_timestamp = self.lamport_clock.update(received_timestamp)

                with self.history_lock:
                    self.messages_history.append({
                        'type': 'received',
                        'sender': response.sender,
                        'received_timestamp': received_timestamp,
                        'local_timestamp_after_update': new_timestamp,
                        'message': response.message,
                        'datetime': datetime.now().isoformat()
                    })

                print(f"[{self.node_id}] RECIBIDO de {response.sender} "
                      f"(Lamport recibido: {received_timestamp}, local actualizado: {new_timestamp}): "
                      f"{response.message}")
                event.set()
            except queue.Empty:
                continue
            except Exception as e:
                print(f'[{self.node_id}] ERROR procesando respuesta: {e}')

    def send_message(self, message: str) -> bool:
        if not self.stub:
            print(f'[{self.node_id}] No conectado al servidor gRPC')
            return False

        timestamp = self.lamport_clock.increment()
        request = messaging_pb2.MessageRequest(
            sender=self.node_id,
            timestamp=timestamp,
            message=message
        )

        with self.history_lock:
            self.messages_history.append({
                'type': 'sent',
                'sender': self.node_id,
                'timestamp': timestamp,
                'message': message,
                'datetime': datetime.now().isoformat()
            })

        try:
            response = self.stub.SendMessage(request)
            event = threading.Event()
            self.response_queue.put((response, event))
            event.wait(timeout=5)
            print(f'[{self.node_id}] ENVIADO (Lamport: {timestamp}): {message}')
            return True
        except grpc.RpcError as e:
            print(f'[{self.node_id}] ERROR RPC: {e}')
            return False
        except Exception as e:
            print(f'[{self.node_id}] ERROR al enviar: {e}')
            return False

    def auto_exchange_messages(self, num_exchanges: int = 20) -> list:
        print(f'\n[{self.node_id}] Iniciando {num_exchanges} intercambios automáticos...')
        for i in range(num_exchanges):
            message = f'Mensaje automático {i + 1} del nodo {self.node_id}'
            self.send_message(message)
            time.sleep(random.uniform(0.1, 0.4))

        print(f'[{self.node_id}] Intercambios completados')
        return self.messages_history

    def save_timestamps_to_json(self, output_dir: str = 'data') -> str:
        os.makedirs(output_dir, exist_ok=True)
        filename = os.path.join(output_dir, f'timestamps_{self.node_id}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')

        data = {
            'node_id': self.node_id,
            'lamport_clock_final': self.lamport_clock.get_value(),
            'messages': self.messages_history
        }

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        print(f'[{self.node_id}] Timestamps guardados en: {filename}')
        return filename

    def measure_latency(self, num_sends: int = 100, output_dir: str = 'data') -> dict:
        print(f'\n[{self.node_id}] Midiendo latencia de {num_sends} envíos...')
        latencies = []

        for i in range(num_sends):
            start = time.perf_counter()
            self.send_message(f'Prueba de latencia {i + 1}')
            end = time.perf_counter()
            latency_ms = (end - start) * 1000
            latencies.append(latency_ms)
            time.sleep(0.01)

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

        os.makedirs(output_dir, exist_ok=True)
        csv_file = os.path.join(output_dir, 'latency_grpc.csv')
        file_exists = os.path.isfile(csv_file)

        with open(csv_file, 'a', newline='', encoding='utf-8') as f:
            import csv
            writer = csv.DictWriter(f, fieldnames=list(stats.keys()))
            if not file_exists:
                writer.writeheader()
            writer.writerow(stats)

        print(f'[{self.node_id}] Latencia promedio: {avg_latency:.4f} ms')
        print(f'[{self.node_id}] Resultados guardados en: {csv_file}')
        return stats

    def disconnect(self):
        self.running = False
        if self.channel:
            try:
                self.channel.close()
            except Exception:
                pass
        self.response_queue.put((None, None))
        print(f'[{self.node_id}] Desconectado')


def interactive_mode(client: GRPCClient):
    print(f"\n[{client.node_id}] Modo interactivo. Escribe 'quit' para salir.")
    print('Comandos disponibles:')
    print('  - quit: Salir')
    print('  - history: Ver histórico de mensajes')
    print('  - clock: Ver reloj de Lamport actual')
    print('  - save: Guardar timestamps a JSON')
    print('  - measure: Medir latencia')
    print('  - auto: Ejecutar 20 intercambios automáticos')
    print()

    while client.running:
        try:
            user_input = input(f'[{client.node_id}] > ').strip()
            if user_input.lower() == 'quit':
                break
            elif user_input.lower() == 'history':
                print(f"\nHistórico de {len(client.messages_history)} eventos:")
                for i, msg in enumerate(client.messages_history, 1):
                    print(f'  {i}. {msg}')
            elif user_input.lower() == 'clock':
                print(f'Reloj de Lamport actual: {client.lamport_clock.get_value()}')
            elif user_input.lower() == 'save':
                filename = client.save_timestamps_to_json()
                print(f'Guardado en: {filename}')
            elif user_input.lower() == 'measure':
                client.measure_latency(num_sends=100)
            elif user_input.lower() == 'auto':
                client.auto_exchange_messages(num_exchanges=20)
            elif user_input:
                client.send_message(user_input)
        except KeyboardInterrupt:
            print('\nInterrupción detectada')
            break
        except Exception as e:
            print(f'ERROR: {e}')


def main():
    if len(sys.argv) > 1:
        node_id = sys.argv[1]
    else:
        node_id = input('Ingresa el ID/nombre del nodo: ').strip()
        if not node_id:
            node_id = f'cliente_{int(time.time() * 1000) % 10000}'

    print(f'Iniciando cliente gRPC con ID: {node_id}')
    client = GRPCClient(node_id=node_id)

    if not client.connect():
        return

    try:
        interactive_mode(client)
    except KeyboardInterrupt:
        print('\n[INTERRUPCIÓN] Presionado Ctrl+C')
    finally:
        if client.messages_history:
            client.save_timestamps_to_json()
        client.disconnect()


if __name__ == '__main__':
    main()

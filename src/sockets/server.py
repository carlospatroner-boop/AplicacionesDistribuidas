import socket
import threading
import json
from datetime import datetime
from typing import Dict, List


class TCPServer:
    """
    Servidor TCP que maneja múltiples clientes concurrentes.
    Retransmite (broadcast) mensajes en formato JSON a todos los clientes conectados.
    """

    def __init__(self, host: str = 'localhost', port: int = 5000):
        """
        Inicializa el servidor TCP.

        Args:
            host: Dirección IP o hostname donde escuchar (por defecto localhost)
            port: Puerto en el que escuchar (por defecto 5000)
        """
        self.host = host
        self.port = port
        self.server_socket = None
        self.clients: List[socket.socket] = []
        self.clients_lock = threading.Lock()
        self.running = False

    def start(self):
        """Inicia el servidor TCP."""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        try:
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(5)
            self.running = True
            print(f"[SERVIDOR] Escuchando en {self.host}:{self.port}")

            # Thread para aceptar conexiones de clientes
            accept_thread = threading.Thread(target=self._accept_connections, daemon=True)
            accept_thread.start()

            # Mantener el servidor en ejecución
            accept_thread.join()

        except OSError as e:
            print(f"[ERROR] No se pudo iniciar el servidor: {e}")
            self.stop()

    def _accept_connections(self):
        """Acepta conexiones de clientes en un loop infinito."""
        try:
            while self.running:
                client_socket, client_address = self.server_socket.accept()
                print(f"[CONEXIÓN] Cliente conectado desde {client_address}")

                # Agregar cliente a la lista
                with self.clients_lock:
                    self.clients.append(client_socket)

                # Crear thread para manejar el cliente
                client_thread = threading.Thread(
                    target=self._handle_client,
                    args=(client_socket, client_address),
                    daemon=True
                )
                client_thread.start()

        except OSError as e:
            if self.running:
                print(f"[ERROR] Error aceptando conexiones: {e}")

    def _handle_client(self, client_socket: socket.socket, client_address: tuple):
        """
        Maneja la comunicación con un cliente específico.

        Args:
            client_socket: Socket del cliente
            client_address: Tupla (host, puerto) del cliente
        """
        sender_name = f"{client_address[0]}:{client_address[1]}"

        try:
            while self.running:
                # Recibir datos del cliente
                data = client_socket.recv(1024).decode('utf-8')

                if not data:
                    print(f"[DESCONEXIÓN] {sender_name} se desconectó")
                    break

                try:
                    # Parsear mensaje JSON recibido
                    message_data = json.loads(data)

                    # Crear mensaje con formato estándar
                    broadcast_message = {
                        'sender': sender_name,
                        'timestamp': datetime.now().isoformat(),
                        'message': message_data.get('message', '')
                    }

                    print(f"[MENSAJE] De {sender_name}: {message_data.get('message', '')}")

                    # Retransmitir a todos los demás clientes
                    self._broadcast(broadcast_message, client_socket)

                except json.JSONDecodeError:
                    print(f"[ERROR] Formato JSON inválido de {sender_name}")
                    error_response = {
                        'error': 'Formato JSON inválido',
                        'timestamp': datetime.now().isoformat()
                    }
                    client_socket.send(json.dumps(error_response).encode('utf-8'))

        except Exception as e:
            print(f"[ERROR] Error manejando cliente {sender_name}: {e}")

        finally:
            # Remover cliente de la lista
            with self.clients_lock:
                if client_socket in self.clients:
                    self.clients.remove(client_socket)

            try:
                client_socket.close()
            except OSError:
                pass

            print(f"[INFO] Cliente {sender_name} removido. Clientes conectados: {len(self.clients)}")

    def _broadcast(self, message: Dict, sender_socket: socket.socket = None):
        """
        Retransmite un mensaje a todos los clientes excepto el remitente.

        Args:
            message: Diccionario con el mensaje a retransmitir
            sender_socket: Socket del cliente remitente (se excluye de la retransmisión)
        """
        message_json = json.dumps(message).encode('utf-8')

        with self.clients_lock:
            for client_socket in self.clients:
                # No enviar el mensaje al remitente
                if client_socket != sender_socket:
                    try:
                        client_socket.send(message_json)
                    except OSError as e:
                        print(f"[ERROR] No se pudo enviar mensaje a cliente: {e}")

    def stop(self):
        """Detiene el servidor y cierra todas las conexiones."""
        print("[SERVIDOR] Deteniendo servidor...")
        self.running = False

        # Cerrar todas las conexiones de clientes
        with self.clients_lock:
            for client_socket in self.clients:
                try:
                    client_socket.close()
                except OSError:
                    pass
            self.clients.clear()

        # Cerrar socket del servidor
        if self.server_socket:
            try:
                self.server_socket.close()
            except OSError:
                pass

        print("[SERVIDOR] Servidor detenido")


if __name__ == '__main__':
    server = TCPServer(host='localhost', port=5000)
    try:
        server.start()
    except KeyboardInterrupt:
        print("\n[INTERRUPCIÓN] Presionado Ctrl+C")
        server.stop()

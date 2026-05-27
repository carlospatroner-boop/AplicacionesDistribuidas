import json
from concurrent import futures
import grpc

from . import messaging_pb2
from . import messaging_pb2_grpc


class MessagingServiceServicer(messaging_pb2_grpc.MessagingServiceServicer):
    def SendMessage(self, request, context):
        message_data = {
            'sender': request.sender,
            'timestamp': request.timestamp,
            'message': request.message
        }
        print(json.dumps(message_data, ensure_ascii=False))

        return messaging_pb2.MessageResponse(
            sender=request.sender,
            timestamp=request.timestamp,
            message=request.message
        )


def serve(host: str = '127.0.0.1', port: int = 5001):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    messaging_pb2_grpc.add_MessagingServiceServicer_to_server(MessagingServiceServicer(), server)
    bind_address = '127.0.0.1' if host == 'localhost' else host
    server.add_insecure_port(f'{bind_address}:{port}')
    server.start()
    print(f'GRPC server listening on {bind_address}:{port}')
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print('\nShutting down gRPC server...')
        server.stop(0)


if __name__ == '__main__':
    serve()

import pika
from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareCloseError,
)

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name
        self._consumer_running = False
        self.consumer_tag = None

        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=self.host,
                    heartbeat=0,
                    blocked_connection_timeout=300
                )
            )
            self.channel = self.connection.channel()

            self.channel.queue_declare(queue=self.queue_name, durable=True)
            self.channel.basic_qos(prefetch_count=1)

        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareDisconnectedError(str(e))

    def _check_connection(self):
        if not self.connection.is_open or not self.channel.is_open:
            raise MessageMiddlewareDisconnectedError("Conexión cerrada")

    def send(self, message):
        try:
            self._check_connection()

            if isinstance(message, str):
                message = message.encode()

            self.channel.basic_publish(
                exchange='',
                routing_key=self.queue_name,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2
                )
            )
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(str(e))

    def reserve_receiver_resources(self, on_message_callback):
        """Registra el callback sin bloquear (permite múltiples consumers en el mismo channel)"""
        def callback(ch, method, properties, body):
            try:
                ack = lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
                nack = lambda: ch.basic_nack(
                    delivery_tag=method.delivery_tag,
                    requeue=True
                )

                on_message_callback(body, ack, nack)

            except Exception as e:
                try:
                    ch.basic_nack(
                        delivery_tag=method.delivery_tag,
                        requeue=True
                    )
                except:
                    pass
                raise MessageMiddlewareMessageError(str(e))

        try:
            self._check_connection()
            self.consumer_tag = self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=callback,
                auto_ack=False
            )
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareDisconnectedError(str(e))

    def start_consuming(self, on_message_callback):

        def callback(ch, method, properties, body):
            try:
                ack = lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
                nack = lambda: ch.basic_nack(
                    delivery_tag=method.delivery_tag,
                    requeue=True
                )

                on_message_callback(body, ack, nack)

            except Exception as e:
                try:
                    ch.basic_nack(
                        delivery_tag=method.delivery_tag,
                        requeue=True
                    )
                except:
                    pass
                raise MessageMiddlewareMessageError(str(e))

        try:
            self._check_connection()

            self._consumer_running = True

            self.consumer_tag = self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=callback,
                auto_ack=False
            )

            self.channel.start_consuming()

        except pika.exceptions.AMQPError as e:
            self._consumer_running = False
            raise MessageMiddlewareDisconnectedError(str(e))

    def stop_consuming(self):
        if self._consumer_running:
            try:
                self._consumer_running = False

                if self.consumer_tag:
                    self.channel.basic_cancel(self.consumer_tag)

                self.channel.stop_consuming()

            except pika.exceptions.AMQPError as e:
                raise MessageMiddlewareDisconnectedError(str(e))

    def close(self):
        try:
            try:
                if self._consumer_running:
                    self.stop_consuming()
            except:
                pass

            if self.channel.is_open:
                self.channel.close()

            if self.connection.is_open:
                self.connection.close()

        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareCloseError(str(e))


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):

    def __init__(self, host, exchange_name, routing_keys, channel=None):
        self.host = host
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self._consumer_running = False
        self.consumer_tag = None
        self._owns_connection = channel is None

        try:
            if channel is None:
                # Crear nueva conexión
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=self.host,
                        heartbeat=0,
                        blocked_connection_timeout=300
                    )
                )
                self.channel = self.connection.channel()
            else:
                # Reutilizar channel existente
                self.channel = channel
                self.connection = getattr(channel, 'connection', None)

            self.channel.exchange_declare(
                exchange=self.exchange_name,
                exchange_type="topic",
                durable=True
            )

            self.channel.basic_qos(prefetch_count=1)

            result = self.channel.queue_declare(queue="", exclusive=True)
            self.queue_name = result.method.queue

            keys = routing_keys if isinstance(routing_keys, list) else [routing_keys]

            for key in keys:
                self.channel.queue_bind(
                    exchange=self.exchange_name,
                    queue=self.queue_name,
                    routing_key=key
                )

        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareDisconnectedError(str(e))

    def _check_connection(self):
        if not self.channel.is_open:
            raise MessageMiddlewareDisconnectedError("Conexión cerrada")
        if self.connection and not self.connection.is_open:
            raise MessageMiddlewareDisconnectedError("Conexión cerrada")

    def send(self, message, routing_key=None):
        try:
            self._check_connection()

            if isinstance(message, str):
                message = message.encode()

            if routing_key is None:
                routing_key = self.routing_keys[0] if self.routing_keys else ""

            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=routing_key,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2
                )
            )

        except (pika.exceptions.AMQPError, IndexError) as e:
            raise MessageMiddlewareMessageError(str(e))

    def reserve_receiver_resources(self, on_message_callback):
        """Registra el callback sin bloquear (permite múltiples consumers en el mismo channel)"""
        def callback(ch, method, properties, body):
            try:
                ack = lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
                nack = lambda: ch.basic_nack(
                    delivery_tag=method.delivery_tag,
                    requeue=True
                )

                # Detectar si es un método (tiene __self__) o función
                is_method = hasattr(on_message_callback, '__self__')
                argcount = on_message_callback.__code__.co_argcount
                
                if (is_method and argcount == 4) or (not is_method and argcount == 3):
                    on_message_callback(body, ack, nack)
                else:
                    on_message_callback(body, method.routing_key, ack, nack)

            except Exception as e:
                try:
                    ch.basic_nack(
                        delivery_tag=method.delivery_tag,
                        requeue=True
                    )
                except:
                    pass
                raise MessageMiddlewareMessageError(str(e))

        try:
            self._check_connection()

            self.consumer_tag = self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=callback,
                auto_ack=False
            )

        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareDisconnectedError(str(e))

    def start_consuming(self, on_message_callback):

        def callback(ch, method, properties, body):
            try:
                ack = lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
                nack = lambda: ch.basic_nack(
                    delivery_tag=method.delivery_tag,
                    requeue=True
                )

                # Detectar si es un método (tiene __self__) o función
                # Métodos tienen co_argcount = 4 (self + 3 parámetros)
                # Funciones tienen co_argcount = 3 (3 parámetros)
                is_method = hasattr(on_message_callback, '__self__')
                argcount = on_message_callback.__code__.co_argcount
                
                if (is_method and argcount == 4) or (not is_method and argcount == 3):
                    on_message_callback(body, ack, nack)
                else:
                    on_message_callback(body, method.routing_key, ack, nack)

            except Exception as e:
                try:
                    ch.basic_nack(
                        delivery_tag=method.delivery_tag,
                        requeue=True
                    )
                except:
                    pass
                raise MessageMiddlewareMessageError(str(e))

        try:
            self._check_connection()

            self._consumer_running = True

            self.consumer_tag = self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=callback,
                auto_ack=False
            )

            self.channel.start_consuming()

        except pika.exceptions.AMQPError as e:
            self._consumer_running = False
            raise MessageMiddlewareDisconnectedError(str(e))

    def stop_consuming(self):
        if self._consumer_running:
            try:
                self._consumer_running = False

                if self.consumer_tag:
                    self.channel.basic_cancel(self.consumer_tag)

                self.channel.stop_consuming()

            except pika.exceptions.AMQPError as e:
                raise MessageMiddlewareDisconnectedError(str(e))

    def close(self):
        try:
            try:
                if self._consumer_running:
                    self.stop_consuming()
            except:
                pass

            # Solo cerrar conexión si la creamos nosotros
            if self._owns_connection:
                if self.channel and self.channel.is_open:
                    self.channel.close()

                if self.connection and self.connection.is_open:
                    self.connection.close()

        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareCloseError(str(e))
import os
import logging
import signal
import heapq

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.closed = False
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self.handle_sigterm)
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruits_by_client = {}
        self.eof_count_by_client_id = {}
        self.completed_clients = set()

    def handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self.close()
        if self._prev_sigterm_handler:
            self._prev_sigterm_handler(signum, frame)

    def _process_data(self, client_id, fruit, amount):
        
        if client_id not in self.fruits_by_client:
            self.fruits_by_client[client_id] = {}
        
        client_fruits = self.fruits_by_client[client_id]
        if fruit in client_fruits:
            client_fruits[fruit] = client_fruits[fruit] + fruit_item.FruitItem(fruit, amount)
        else:
            client_fruits[fruit] = fruit_item.FruitItem(fruit, amount)

    def _process_eof(self, client_id):


        if client_id in self.completed_clients:
            logging.info(f"Ignoring duplicate EOF for completed client {client_id}")
            return
        
        received_eof_count = self.eof_count_by_client_id.get(client_id, 0) + 1
        self.eof_count_by_client_id[client_id] = received_eof_count
        
        logging.info(f"EOF recibido: cliente {client_id} ({received_eof_count}/{SUM_AMOUNT})")
                
        if received_eof_count < SUM_AMOUNT:
            return
        
        logging.info(f"Top completado para cliente {client_id}, enviando a Join")
        
        client_fruits = self.fruits_by_client.get(client_id, {})
        fruit_heap = list(client_fruits.values())
        heapq.heapify(fruit_heap)
        fruit_top_items = heapq.nlargest(TOP_SIZE, fruit_heap)
        
        fruit_top = [client_id] + list(
            map(lambda f: (f.fruit, f.amount), fruit_top_items)
        )
        
        self.output_queue.send(message_protocol.internal.serialize(fruit_top))
        self.output_queue.send(message_protocol.internal.serialize([client_id]))
        
        self.completed_clients.add(client_id)
        if client_id in self.eof_count_by_client_id:
            del self.eof_count_by_client_id[client_id]
        if client_id in self.fruits_by_client:
            del self.fruits_by_client[client_id]

    def process_messsage(self, message, ack, nack):
        if self.closed:
            ack()
            return
        try:
            fields = message_protocol.internal.deserialize(message)
            client_id = fields[0]
            
            if len(fields) == 3:
                self._process_data(client_id, fields[1], fields[2])
            else:
                self._process_eof(client_id)
            ack()
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            nack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)

    def close(self):
        """Cierra todas las conexiones y canales"""
        try:
            self.closed = True
            self.input_exchange.stop_consuming()
            if self.input_exchange:
                self.input_exchange.close()
            if self.output_queue:
                self.output_queue.close()
        except Exception as e:
            logging.error(f"Error closing resources: {e}")


def main():
    logging.getLogger("pika").setLevel(logging.WARNING)
    logging.getLogger("pika.adapters").setLevel(logging.WARNING)
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    try:
        aggregation_filter.start()
    except Exception as e:
        logging.error(f"Error in aggregation_filter: {e}")
        return 1
    finally:
        aggregation_filter.close()
    return 0


if __name__ == "__main__":
    main()

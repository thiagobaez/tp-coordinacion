import os
import logging
import signal
import bisect

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.closed = False
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self.handle_sigterm)
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.tops_by_client = {}
        self.eof_count_by_client_id = {}
        self.completed_client_ids = set()

    def handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self.close()
        if self._prev_sigterm_handler:
            self._prev_sigterm_handler(signum, frame)

    def _process_top(self, client_id, fruit_top_data):
        
        if client_id not in self.tops_by_client:
            self.tops_by_client[client_id] = []
        
        for fruit, amount in fruit_top_data:
            found = False
            for i in range(len(self.tops_by_client[client_id])):
                if self.tops_by_client[client_id][i].fruit == fruit:
                    self.tops_by_client[client_id][i] = self.tops_by_client[client_id][i] + fruit_item.FruitItem(
                        fruit, amount
                    )
                    found = True
                    break
            
            if not found:
                bisect.insort(self.tops_by_client[client_id], fruit_item.FruitItem(fruit, amount))

    def _process_eof(self, client_id):
        if client_id in self.completed_client_ids:
            logging.info(f"Ignoring duplicate EOF for completed client {client_id}")
            return
        
        received_eof_count = self.eof_count_by_client_id.get(client_id, 0) + 1
        self.eof_count_by_client_id[client_id] = received_eof_count
        
        logging.info(f"EOF recibido: cliente {client_id} ({received_eof_count}/{AGGREGATION_AMOUNT})")
        
        if received_eof_count < AGGREGATION_AMOUNT:
            return
        
        if client_id not in self.tops_by_client:
            logging.warning(f"No tops found for client {client_id}")
            self.completed_client_ids.add(client_id)
            if client_id in self.eof_count_by_client_id:
                del self.eof_count_by_client_id[client_id]
            return
        
        fruit_chunk = list(self.tops_by_client[client_id][-TOP_SIZE:])
        fruit_chunk.reverse()
        
        fruit_top = [client_id] + list(
            map(
                lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                fruit_chunk,
            )
        )
        
        result_str = ", ".join([f"{fruit}: {amount}" for fruit, amount in fruit_top[1:]])
        logging.info(f"Resultado enviado al cliente {client_id}: {result_str}")
        
        self.output_queue.send(message_protocol.internal.serialize(fruit_top))
        
        self.completed_client_ids.add(client_id)
        if client_id in self.eof_count_by_client_id:
            del self.eof_count_by_client_id[client_id]
        if client_id in self.tops_by_client:
            del self.tops_by_client[client_id]

    def process_messsage(self, message, ack, nack):
        if self.closed:
            ack()
            return
        try:
            fields = message_protocol.internal.deserialize(message)
            client_id = fields[0]
            
            if len(fields) > 1:
                fruit_top_data = fields[1:]
                self._process_top(client_id, fruit_top_data)
            else:
                self._process_eof(client_id)
            
            ack()
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            nack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)

    def close(self):
        try:
            self.closed = True
            self.input_queue.stop_consuming()
            if self.input_queue:
                self.input_queue.close()
            if self.output_queue:
                self.output_queue.close()
        except Exception as e:
            logging.error(f"Error closing resources: {e}")


def main():
    logging.getLogger("pika").setLevel(logging.WARNING)
    logging.getLogger("pika.adapters").setLevel(logging.WARNING)
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    try:
        join_filter.start()
    except Exception as e:
        logging.error(f"Error in join_filter: {e}")
        return 1
    finally:
        join_filter.close()
    return 0


if __name__ == "__main__":
    main()

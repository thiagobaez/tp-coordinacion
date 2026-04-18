import os
import logging
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
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.tops_by_client = {}

    def _process_top(self, client_id, fruit_top_data):
        """Agrega frutas del top parcial de un aggregator"""
        logging.info(f"Processing top from client {client_id}")
        
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
        """Genera el top-3 final del cliente y envía al gateway"""
        logging.info(f"Received EOF for client {client_id}")
        
        if client_id not in self.tops_by_client:
            logging.warning(f"No tops found for client {client_id}")
            return
        
        # Obtener los últimos TOP_SIZE elementos (top-3)
        fruit_chunk = list(self.tops_by_client[client_id][-TOP_SIZE:])
        fruit_chunk.reverse()

        print(f"Final top for client {client_id}: {[str(f) for f in fruit_chunk]}")
        
        fruit_top = [client_id] + list(
            map(
                lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                fruit_chunk,
            )
        )
        
        self.output_queue.send(message_protocol.internal.serialize(fruit_top))
        del self.tops_by_client[client_id]

    def process_messsage(self, message, ack, nack):
        logging.info("Received message")
        fields = message_protocol.internal.deserialize(message)
        client_id = fields[0]
        
        if len(fields) > 1:
            fruit_top_data = fields[1:]
            self._process_top(client_id, fruit_top_data)
        else:
            self._process_eof(client_id)
        
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()

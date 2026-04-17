import os
import logging
import threading

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.control_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [f"sum.{ID}"]
        )
        self.amount_by_client = {}
        self.processed_clients = set()

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data from client {client_id}")
        
        if client_id not in self.amount_by_client:
            self.amount_by_client[client_id] = {}
        
        self.amount_by_client[client_id][fruit] = self.amount_by_client[client_id].get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _get_aggregator_index(self, client_id, fruit):
        """Determina el índice del aggregator usando sharding por client_id y fruta"""
        return (client_id + hash(fruit)) % AGGREGATION_AMOUNT

    def _process_eof(self, client_id):
        logging.info(f"Broadcasting data messages for client {client_id}")
        aggregators_used = set()
        
        for final_fruit_item in self.amount_by_client[client_id].values():
            aggregator_index = self._get_aggregator_index(client_id, final_fruit_item.fruit)
            aggregators_used.add(aggregator_index)
            data_output_exchange = self.data_output_exchanges[aggregator_index]
            
            data_output_exchange.send(
                message_protocol.internal.serialize(
                    [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                )
            )

        logging.info(f"Broadcasting EOF message for client {client_id}")
        for aggregator_index in aggregators_used:
            self.data_output_exchanges[aggregator_index].send(
                message_protocol.internal.serialize([client_id])
            )
        
        del self.amount_by_client[client_id]
        
        # Notificar a otros Sums que ya procesamos este cliente
        logging.info(f"Publishing EOF control notification for client {client_id}")
        self.control_exchange.send(
            message_protocol.internal.serialize([client_id])
        )


    def _on_eof_broadcast(self, message, ack, nack):
        """Maneja notificaciones de EOF desde otros Sums"""
        fields = message_protocol.internal.deserialize(message)
        client_id = fields[0]
        
        logging.info(f"Received EOF broadcast for client {client_id}")
        
        # Solo procesar si tengo datos y no lo hice ya
        if client_id not in self.processed_clients and client_id in self.amount_by_client:
            logging.info(f"Processing EOF for client {client_id} from broadcast")
            self._process_eof(client_id)
            self.processed_clients.add(client_id)
        
        ack()

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        client_id = fields[0]
        
        if len(fields) == 3:
            self._process_data(client_id, fields[1], fields[2])
        else:
            self._process_eof(client_id)
            self.processed_clients.add(client_id)
        ack()

    def start(self):
        # Thread 1: Consume datos de INPUT_QUEUE
        t_data = threading.Thread(target=self.input_queue.start_consuming, args=(self.process_data_messsage,))
        # Thread 2: Consume notificaciones de control desde otros Sums
        t_control = threading.Thread(target=self.control_exchange.start_consuming, args=(self._on_eof_broadcast,))
        
        t_data.start()
        t_control.start()
        
        t_data.join()
        t_control.join()

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()

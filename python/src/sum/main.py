import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
SUM_CONTROL_ROUTING_KEY = f"{SUM_CONTROL_EXCHANGE}_ALL"

class SumFilter:
    def __init__(self):
        self.closed = False
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self.handle_sigterm)
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)

        self.control_exchange = None
        if SUM_AMOUNT > 1:
            self.control_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_CONTROL_ROUTING_KEY], 
                channel=self.input_queue.channel
            )

        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)

        self.amount_by_client = {}
        self.processed_clients = set() 

    def handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self.close()
        if self._prev_sigterm_handler:
            self._prev_sigterm_handler(signum, frame)

    def _process_data(self, client_id, fruit, amount):        
        if client_id not in self.amount_by_client:
            self.amount_by_client[client_id] = {}
        
        self.amount_by_client[client_id][fruit] = self.amount_by_client[client_id].get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _get_aggregator_index(self, client_id, fruit):
        client_hash = sum((i + 1) * ord(c) for i, c in enumerate(str(client_id)))
        fruit_hash = sum(ord(c) for c in fruit)
        return (client_hash + fruit_hash) % AGGREGATION_AMOUNT

    def _send_to_aggregators(self, client_id):        
        if client_id not in self.amount_by_client:
            return
        
        logging.info(f"Enviando datos a agregadores: cliente {client_id}")
        for final_fruit_item in self.amount_by_client[client_id].values():
            aggregator_index = self._get_aggregator_index(client_id, final_fruit_item.fruit)
            data_output_exchange = self.data_output_exchanges[aggregator_index]
            
            data_output_exchange.send(
                message_protocol.internal.serialize(
                    [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                )
            )
        
        for aggregator_index in range(AGGREGATION_AMOUNT):
            self.data_output_exchanges[aggregator_index].send(
                message_protocol.internal.serialize([client_id])
            )
        
        del self.amount_by_client[client_id]

    def _process_eof(self, client_id):
        if client_id in self.processed_clients:
            return
        
        self.processed_clients.add(client_id)
        self._send_to_aggregators(client_id)      
        if SUM_AMOUNT > 1:
            self.control_exchange.send(message_protocol.internal.serialize([client_id]))


    def _on_eof_broadcast(self, message, ack, nack):
        """Recibe notificación de EOF desde otros Sums: envía datos acumulados"""
        if self.closed:
            ack()
            return
        try:
            fields = message_protocol.internal.deserialize(message)
            client_id = fields[0]
            
            logging.info(f"EOF broadcast recibido de otro Sum: cliente {client_id}")
                    
            if client_id in self.processed_clients:
                ack()
                return
            
            self.processed_clients.add(client_id)
            self._send_to_aggregators(client_id)
            
            ack()
        except Exception as e:
            logging.error(f"Error processing EOF broadcast: {e}")
            nack()

    def process_data_messsage(self, message, ack, nack):
        if self.closed:
            ack()
            return
        fields = message_protocol.internal.deserialize(message)
        client_id = fields[0]
        
        if len(fields) == 3:
            self._process_data(client_id, fields[1], fields[2])
        else:
            logging.info(f"EOF recibido: cliente {client_id}")
            self._process_eof(client_id)
        ack()

    def start(self):
        self.input_queue.reserve_receiver_resources(self.process_data_messsage)
        if SUM_AMOUNT > 1:
            self.control_exchange.reserve_receiver_resources(self._on_eof_broadcast)
        self.input_queue.channel.start_consuming()

    def close(self):
        """Cierra todas las conexiones y canales"""
        try:
            self.closed = True
            self.input_queue.stop_consuming()
            if SUM_AMOUNT > 1 and self.control_exchange:
                self.control_exchange.close()
            for exchange in self.data_output_exchanges:
                if exchange:
                    exchange.close()
            if self.input_queue:
                self.input_queue.close()
        except Exception as e:
            logging.error(f"Error closing resources: {e}")

def main():
    logging.getLogger("pika").setLevel(logging.WARNING)
    logging.getLogger("pika.adapters").setLevel(logging.WARNING)
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    try:
        sum_filter.start()
    except Exception as e:
        logging.error(f"Error in sum_filter: {e}")
        return 1
    finally:
        sum_filter.close()
    return 0


if __name__ == "__main__":
    main()

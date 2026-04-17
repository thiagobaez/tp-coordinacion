import threading
from common import message_protocol


class MessageHandler:
    _client_id_counter = 0
    _lock = threading.Lock()

    def __init__(self):
        with MessageHandler._lock:
            MessageHandler._client_id_counter += 1
            self.client_id = MessageHandler._client_id_counter
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize([self.client_id, fruit, amount])

    def serialize_eof_message(self, client_id=None):
        return message_protocol.internal.serialize([self.client_id])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) > 0 and fields[0] == self.client_id:
            return fields[1:]
        return None

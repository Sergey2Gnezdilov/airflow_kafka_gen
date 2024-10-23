import json
from datetime import datetime, timedelta

from confluent_kafka import Consumer, KafkaError


class KafkaConsumer:
    def __init__(self, kafka_config: dict, topic: str, timeout: int = 30, batch_size: int = 100_000) -> None:
        self.default_kafka_config = {
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
        self.kafka_config = {**self.default_kafka_config, **kafka_config}
        self.consumer = Consumer(self.kafka_config)
        self.consumer.subscribe([v for v in topic.split(",")])
        self.timeout = timeout
        self.batch_size = batch_size

    def consume_batch(self) -> tuple[list[dict], set]:
        finish_time = datetime.utcnow() + timedelta(seconds=self.timeout)
        result, log = [], set()
        while self.batch_size > len(result) and finish_time > datetime.utcnow():
            message = self.consumer.poll(timeout=1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() != KafkaError._PARTITION_EOF:
                    log.add(message.error())
                continue

            row = json.loads(message.value().decode("utf-8"))
            if isinstance(row, list):
                for each in row:
                    self.__update_row_metadata(each, message)
                    self.__validate_row(each)
                    result.append(each)
            else:
                self.__update_row_metadata(row, message)
                self.__validate_row(row)
                result.append(row)

        return result, log

    def __update_row_metadata(self, row: dict, message) -> None:
        row["timestamp"] = message.timestamp()[1]
        row["partition"] = message.partition()
        row["offset"] = message.offset()

    def __validate_row(self, data: dict) -> None:
        for key, value in data.items():
            if not isinstance(value, (int, float, str, bool)):
                value = json.dumps(value)
                data[key] = value
            
            if isinstance(value, str) and value.lower() == "null":
                data[key] = None

    def close_consumer(self) -> None:
        self.consumer.close()

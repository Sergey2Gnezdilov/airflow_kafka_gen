import sys
from datetime import datetime

import objgraph
from kafka import KafkaConsumer
from logger import ClickHouseLogger, LogLevel
from s3 import S3Writer


class KafkaToS3Processor:
    def __init__(self, kafka_consumer: KafkaConsumer, s3_writer: S3Writer, logger: ClickHouseLogger) -> None:
        self.kafka_consumer = kafka_consumer
        self.s3_writer = s3_writer
        self.logger = logger
        self.start_date = datetime.utcnow().date()

    def __lifetime_exceeded(self, current_date: datetime) -> bool:
        if self.start_date != current_date.date():
            self.kafka_consumer.close_consumer()
            return True
        return False

    def process(self) -> None:
        while True:
            dt = datetime.utcnow()
            if self.__lifetime_exceeded(current_date=dt):
                return

            data, log = self.kafka_consumer.consume_batch()
            if len(data) > 0:
                self.s3_writer.save(data=data, dt=dt)
                self.kafka_consumer.consumer.commit(asynchronous=False)

            log.add(f"consumed_messages: {len(data)}")
            log.add(f"size: {sys.getsizeof(data)}")
            for v in objgraph.most_common_types(limit=3):
                log.add(f"sizeof_{v[0]}: {v[1]}")
            self.logger.log(str(log), LogLevel.Information, dt=dt)

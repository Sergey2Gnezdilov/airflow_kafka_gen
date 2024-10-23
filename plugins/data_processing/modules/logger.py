import enum
from datetime import datetime

import clickhouse_connect


@enum.unique
class LogLevel(enum.Enum):
    Fatal = 1
    Critical = 2
    Error = 3
    Warning = 4
    Notice = 5
    Information = 6
    Debug = 7
    Trace = 8
    Test = 9


class ClickHouseLogger:
    def __init__(self, click_house_config: dict, source: str) -> None:
        self.source = source
        self.database = "raw"
        self.table = "logs"
        self.client = clickhouse_connect.get_client(**click_house_config)
        self.__log_table_init()

    def __log_table_init(self) -> None:
        try:
            self.client.command(
                f"""
            CREATE TABLE IF NOT EXISTS {self.database}.{self.table}
            (
                `dt` DateTime DEFAULT now(),
                `src` String,
                `lvl` Enum8({",".join(f"'{loglevel.name}' = {loglevel.value}" for loglevel in LogLevel)}),
                `msg` String
            )
            ENGINE = MergeTree
            PARTITION BY toYYYYMMDD(dt)
            ORDER BY (dt, src, lvl)
            """
            )
        except Exception as e:
            print(f"Error creating ClickHouse table: {e}")

    def log(self, message: dict, level: LogLevel, dt: datetime) -> None:
        column_names = ["dt", "src", "lvl", "msg"]
        data = [dt, self.source, level.value, str(message)]
        try:
            self.client.insert(database=self.database, table=self.table, data=[data], column_names=column_names)
        except Exception as e:
            print(f"Error logging to ClickHouse: {e}")

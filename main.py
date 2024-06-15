import config
import os
import re
import psycopg2
from datetime import datetime, date


class Log:
    def __init__(self):
        self.ip = 'Нет данных'
        self.date = date.today()
        self.method = 'Нет данных'
        self.url = 'Нет данных'
        self.status = 'Нет данных'
        self.user_agent = 'Нет данных'

    def __repr__(self):
        return f'ip: {self.ip}, date: {self.date}, method: {self.method}, url: {self.url}, status: {self.status}, user_agent: {self.user_agent}'

    def to_tuple(self):
        return (self.ip, self.date, self.method, self.url, self.status, self.user_agent)


data_patterns = {
    '%h': r'\b(?:\d{1,3}\.){3}\d{1,3}\b',
    '%t': r'\[\d{2}\/[A-Za-z]{3}\/\d{4}:\d{2}:\d{2}:\d{2} \+\d{4}\]',
    '%r': r'"([^ ]*) ([^ ]*) HTTP/1\.[01]"',
    '%>s': r'\b\d{3}\b',
    '%b': r'\b\d+\b'
}


class LogManager:
    def __init__(self, files, data_patterns):
        self.files = files
        self.data_patterns = data_patterns

    def read_logs(self):
        logs = []
        for file_path, file_format in self.files:
            if not os.path.exists(file_path):
                print(f'File not found: {file_path}')
                continue

            with open(file_path, 'r', encoding='utf-8') as file:
                for line in file:
                    log = self.parse_log(line, file_format)
                    if log:
                        logs.append(log)
        return logs

    def read_and_upload_logs(self, db_manager):
        logs = self.read_logs()
        log_tuples = [log.to_tuple() for log in logs]
        db_manager.insert_data(log_tuples)

    def get_logs(self, db_manager, query_input):
        query, columns = self.construct_query(query_input)
        result = db_manager.execute_query(query)
        logs = []
        for row in result:
            log = {columns[i]: row[i] for i in range(len(columns))}
            logs.append(log)
        return logs

    def construct_query(self, query_input):
        parts = query_input.split()
        if parts[0].lower() != 'select':
            raise ValueError("Invalid command format. Expected 'select'.")

        columns = []
        i = 1
        while i < len(parts) and parts[i].lower() != 'from':
            columns.append(parts[i])
            i += 1

        if not columns:
            raise ValueError("No columns specified for selection.")

        if i < len(parts) - 1:
            table = parts[i + 1]
            conditions = ' '.join(parts[i + 2:])
        else:
            raise ValueError("Invalid command format. Expected 'from' and table name.")

        query = f"SELECT {', '.join(columns)} FROM {table}"
        if conditions:
            query += f" WHERE {conditions}"
        query += ";"

        return query, columns

    def parse_log(self, line, file_format):
        log = Log()
        for pattern in file_format:
            if pattern in self.data_patterns:
                match = re.search(self.data_patterns[pattern], line)
                if not match:
                    continue
                if pattern == '%h':
                    log.ip = match.group()
                elif pattern == '%t':
                    date_str = match.group().strip('[]')
                    log.date = datetime.strptime(date_str, '%d/%b/%Y:%H:%M:%S %z').date()
                elif pattern == '%r':
                    log.method, log.url = match.groups()
                elif pattern == '%>s':
                    log.status = match.group()
                elif pattern == '%b':
                    log.user_agent = match.group()
        return log if any(getattr(log, attr) != 'Нет данных' for attr in log.__dict__) else None


class DatabaseManager:
    def __init__(self, db_info):
        self.connection = None
        self.db_info = db_info

    def connect(self):
        self.connection = self.initialize_connection()

    def execute_query(self, query):
        if not self.connection:
            self.connect()
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()

    def insert_data(self, data):
        if not self.connection:
            self.connect()
        with self.connection.cursor() as cursor:
            for log_tuple in data:
                if len(log_tuple) != 6 or not self.validate_log_tuple(log_tuple):
                    print(f"Invalid tuple: {log_tuple}. Skipping...")
                    continue
                truncated_tuple = self.truncate_log_tuple(log_tuple)
                cursor.execute(
                    "INSERT INTO logs (ip, timestamp, method, url, status, user_agent) VALUES (%s, %s, %s, %s, %s, %s)",
                    truncated_tuple
                )
        self.connection.commit()

    def validate_log_tuple(self, log_tuple):
        ip, timestamp, method, url, status, user_agent = log_tuple
        return (
            isinstance(ip, str) and
            isinstance(timestamp, date) and
            isinstance(method, str) and
            isinstance(url, str) and
            isinstance(status, str) and
            isinstance(user_agent, str)
        )

    def truncate_log_tuple(self, log_tuple):
        max_lengths = {
            'ip': 255,
            'method': 255,
            'url': 255,
            'status': 255,
            'user_agent': 255
        }
        ip, timestamp, method, url, status, user_agent = log_tuple
        return (
            ip[:max_lengths['ip']],
            timestamp,
            method[:max_lengths['method']],
            url[:max_lengths['url']],
            status[:max_lengths['status']],
            user_agent[:max_lengths['user_agent']]
        )

    def initialize_connection(self):
        try:
            return psycopg2.connect(**self.db_info)
        except Exception as e:
            print(f"Database connection failed: {e}")
            return None


def main():
    db_manager = DatabaseManager(config.db_info)
    log_manager = LogManager(config.file, data_patterns)
    while True:
        user_input = input('# ')
        if 'check_logs' in user_input:
            log_manager.read_and_upload_logs(db_manager)
        elif 'select' in user_input:
            try:
                logs = log_manager.get_logs(db_manager, user_input)
                for log in logs:
                    print(log)
            except ValueError as e:
                print(e)
        else:
            print('Unknown command')


if __name__ == '__main__':
    main()

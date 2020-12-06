import mysql.connector
from queries import *


class DB:
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super(DB, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        self.connection = self.connect()
        self.cur = self.connection.cursor()
        self.create_tables()
        self.create_index()
        self.connection.close()

    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                        host = 'localhost',
                        user = 'root',
                        passwd = 'root',
                        database = 'rooms_db',
                        auth_plugin='mysql_native_password'
                    )
        except mysql.connector.errors.OperationalError:
            raise Exception('Couldn`t connect to MySQL')
        return self.connection

    def create_tables(self):
        self.cur.execute('''CREATE TABLE IF NOT EXISTS rooms(id SMALLINT UNSIGNED NOT NULL,
                                                             name VARCHAR(20) NOT NULL,
                                                             PRIMARY KEY(id))''')
        self.cur.execute('''CREATE TABLE IF NOT EXISTS students(id INT PRIMARY KEY NOT NULL,
                                                                name VARCHAR(80) NOT NULL,
                                                                room SMALLINT UNSIGNED NOT NULL,
                                                                sex CHAR(1) NOT NULL,
                                                                birthday DATETIME NOT NULL,
                                                                FOREIGN KEY (room) REFERENCES rooms(id) ON DELETE CASCADE)''')

    def insert_rooms(self, rooms):
        self.connection = self.connect()
        try:
            with self.connection.cursor() as cur:
                sql = '''INSERT INTO rooms (id, name) VALUES (%s, %s)'''
                values = [(room['id'], room['name']) for room in rooms]
                cur.executemany(sql, values)
                self.connection.commit()
        finally:
            self.connection.close()

    def insert_students(self, students):
        self.connection = self.connect()
        try:
            with self.connection.cursor() as cur:
                sql = '''INSERT INTO students(id, name, room, sex, birthday) VALUES (%s, %s, %s, %s, %s)'''
                values = [(student['id'], student['name'], student['room'],
                           student['sex'], student['birthday']) for student in students]
                cur.executemany(sql, values)
                self.connection.commit()
        finally:
            self.connection.close()

    def create_index(self):
        sql = '''CREATE INDEX ID_rooms ON rooms(id)'''
        self.cur.execute(sql)

    def select_requests(self, list_fun_queries: list):
        for query in list_fun_queries:
            query()



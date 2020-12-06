import mysql.connector

# decorator to ensure that connection closes after each session
def safe_session(method):
    def wrapper(ref, data_to_insert=None):
        ref.connection = ref._connect()
        try:
            if data_to_insert is not None:
                method(ref, data_to_insert)
            else:
                method(ref)
        finally:
            ref.connection.close()

    return wrapper


class DB:
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super(DB, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        self.connection = self._connect()
        self.cur = self.connection.cursor()
        self._create_tables()
        self._create_index()
        self.connection.close()

    def _connect(self):
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

    def _create_tables(self):
        self.cur.execute('''CREATE TABLE IF NOT EXISTS rooms(id SMALLINT UNSIGNED NOT NULL,
                                                             name VARCHAR(20) NOT NULL,
                                                             PRIMARY KEY(id))''')
        self.cur.execute('''CREATE TABLE IF NOT EXISTS students(id INT PRIMARY KEY NOT NULL,
                                                                name VARCHAR(80) NOT NULL,
                                                                room SMALLINT UNSIGNED NOT NULL,
                                                                sex CHAR(1) NOT NULL,
                                                                birthday DATETIME NOT NULL,
                                                                FOREIGN KEY (room) REFERENCES rooms(id) ON DELETE CASCADE)''')


    @safe_session
    def _insert_rooms(self, rooms):
        with self.connection.cursor() as cur:
            sql = '''INSERT INTO rooms (id, name) VALUES (%s, %s)'''
            values = [(room['id'], room['name']) for room in rooms]
            cur.executemany(sql, values)
            self.connection.commit()


    @safe_session
    def _insert_students(self, students) -> None:
        with self.connection.cursor() as cur:
            sql = '''INSERT INTO students(id, name, room, sex, birthday) VALUES (%s, %s, %s, %s, %s)'''
            values = [(student['id'], student['name'], student['room'],
                       student['sex'], student['birthday']) for student in students]
            cur.executemany(sql, values)
            self.connection.commit()


    def _create_index(self) -> None:
        sql = '''CREATE INDEX idx_rooms ON rooms(id)'''
        self.cur.execute(sql)

    @safe_session
    def _rooms_and_students_amount_query(self) -> list:
        with self.connection.cursor(buffered=True) as cur:
            sql = '''SELECT rooms.id AS rooms_id, rooms.name AS room_name,
                     COUNT(students.id) as amount 
                     FROM rooms LEFT JOIN students ON rooms.id = students.room 
                     GROUP BY rooms.id'''
            cur.execute(sql)
        return cur.fetchall()

    @safe_session
    def _top5_with_smallest_avg_age_query(self) -> list:
        with self.connection.cursor(buffered=True) as cur:
            sql = '''SELECT rooms.id AS room_id, rooms.name AS room_name, 
                     AVG((YEAR(NOW()) - YEAR(students.birthday)) - (RIGHT(NOW(), 5) < RIGHT(students.birthday, 5))) as avg_age
                     FROM rooms LEFT JOIN students ON rooms.id=students.room
                     GROUP BY rooms.id
                     ORDER BY avg_age
                     LIMIT 5'''
            cur.execute(sql)
        return cur.fetchall()

    @safe_session
    def _top5_with_biggest_age_diff_query(self) -> list:
        with self.connection.cursor(buffered=True) as cur:
            sql = '''SELECT rooms.id AS room_id, (MAX(TIMESTAMPDIFF(YEAR, NOW(), students.birthday)) -
                     MIN(TIMESTAMPDIFF(YEAR, NOW(), students.birthday))) AS age_diff
                     FROM rooms LEFT JOIN students ON rooms.id=students.room
                     GROUP BY rooms.id
                     ORDER BY age_diff DESC
                     LIMIT 5'''
            cur.execute(sql)
        return cur.fetchall()

    @safe_session
    def _rooms_with_diff_sex_students_query(self) -> list:
        with self.connection.cursor(buffered=True) as cur:
            sql = '''SELECT rooms.id as room_id, rooms.name as room_name
                     FROM rooms JOIN Students ON rooms.id = students.room
                     GROUP BY rooms.id
                     HAVING COUNT(DISTINCT students.sex) > 1'''
            cur.execute(sql)
        return cur.fetchall()









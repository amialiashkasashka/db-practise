
def rooms_and_students_amount_query():
    sql = '''SELECT rooms.id as rooms_id, rooms.name as room_name,
             COUNT(students.id) as amount 
             FROM rooms LEFT JOIN students ON rooms.id = students.room 
             GROUP BY rooms.id'''

def top5_with_smallest_avg_age_query():
    sql = '''SELECT rooms.id as room_id, rooms.name as room_name,
    CAST(AVG(TIMESTAMPDIFF(YEAR,Students.birthday,NOW())) as float) as avg_age
    FROM rooms LEFT JOIN students on rooms.id = students.id
    GROUP BY rooms.id
    ORDER BY avg_age
    LIMIT 5'''

def top5_with_biggest_age_diff_query():
    sql = '''SELECT rooms.id as room_id, rooms.name as room_name,
             TIMESTAMPDIFF(YEAR,MIN(students.birthday),MAX(students.birthday)) as age_diff
             FROM rooms JOIN students ON rooms.id = students.room
             GROUP BY rooms.id
             ORDER BY age_diff DESC
             LIMIT 5'''

def rooms_with_diff_sex_students_query():
    sql = '''SELECT rooms.id as room_id, rooms.name as room_name
             FROM rooms JOIN Students ON rooms.id = students.room
             GROUP BY rooms.id
             HAVING COUNT(DISTINCT students.sex) > 1'''
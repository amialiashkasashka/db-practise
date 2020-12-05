import argparse
from data_loader import FileLoader
from typing import Tuple, List, Dict
from serializers import XMLSerializer
from data_writer import SaverXML, SaverJSON
from db_manager import DB
import time


class Solution:

    def _merge_rooms_students(self, students, rooms) -> List[Dict]:
        for room in rooms:
            room['students'] = []
        for student in students:
            student_room_num = student['room']
            rooms[student_room_num]['students'].append(student)

        return rooms

    def dict_to_tuple(self):
        pass


    def args_parser(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('path_students', type=str, help='path to students.json')
        parser.add_argument('path_rooms', type=str, help='path to rooms.json')
        parser.add_argument('output_format', type=str, choices=['json', 'xml'], help='preferred output format')
        args = parser.parse_args()

        return args


def run():

    file_loader = FileLoader()
    students = file_loader.load(args.path_students)
    rooms = file_loader.load(args.path_rooms)
    data_merged = solution._merge_rooms_students(students, rooms)

    if args.output_format == 'json':
        saver = SaverJSON()
        saver.save(data=data_merged, path='output', output_format='json')

    elif args.output_format == 'xml':
        xml_serializer = XMLSerializer()
        xml_serialized_data = xml_serializer.serialize(data_merged)
        saver = SaverXML()
        saver.save(data=xml_serialized_data, path='output', output_format='xml')

    db = DB()
    db.insert_rooms(rooms)
    db.insert_students(students)

def import_db():
    pass





if __name__ == '__main__':
    start = time.time()
    solution = Solution()
    args = solution.args_parser()

    run()
    end = time.time()
    print(end - start)








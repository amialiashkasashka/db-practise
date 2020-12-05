from xml.etree import ElementTree as ET
from xml.dom.minidom import parseString


class XMLSerializer:
    
    def serialize(self, data_to_serialize):
        root = ET.Element('rooms')
        for room in data_to_serialize:
            each_room = ET.SubElement(root, 'room')
            ET.SubElement(each_room, 'id').text = str(room.get('id'))
            ET.SubElement(each_room, 'name').text = each_room.get('name')
            students_field = ET.SubElement(each_room, 'students')

            for student in room.get('students'):
                ET.SubElement(students_field, 'student').text = str(student)
        tree = ET.ElementTree(root)

        return parseString(ET.tostring(root)).toprettyxml(indent='   ')



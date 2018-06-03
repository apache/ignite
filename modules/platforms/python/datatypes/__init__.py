import ctypes
import socket

from datatypes.class_configs import simple_type_config


class BaseDataType(ctypes.LittleEndianStructure):

    @staticmethod
    def __new__(cls, connection: socket.socket, *args, **kwargs) -> object:
        if cls is BaseDataType:
            type_code = int.from_bytes(
                connection.recv(1),
                byteorder='little',
            )
            class_name, ctypes_type = simple_type_config[type_code]
            data_class = type(
                class_name,
                (BaseDataType,),
                {
                    '_fields_': [
                        ('type_code', ctypes.c_byte),
                        ('value', ctypes_type)
                    ],
                    '_type_code': type_code,
                },
            )
            return data_class.__new__(data_class, connection)
        return super().__new__(cls, *args, **kwargs)

    def __init__(self, connection):
        super().__init__()
        self.type_code = self._type_code
        self.value = self.value.from_bytes(
            connection.recv(ctypes.sizeof(simple_type_config[self.type_code][1])),
            byteorder='little',
        )

import ctypes

from .type_codes import *


simple_type_config = {
    TC_BYTE: ('Byte', ctypes.c_byte),
    TC_SHORT: ('Short', ctypes.c_short),
    TC_INT: ('Int', ctypes.c_int),
    TC_LONG: ('Long', ctypes.c_long),
    TC_FLOAT: ('Float', ctypes.c_float),
    TC_DOUBLE: ('Double', ctypes.c_double),
}

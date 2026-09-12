# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import asyncio
import ctypes
import decimal
import inspect
import warnings

from functools import wraps
from typing import Any, Optional, Type, Tuple, Union

from pyignite.datatypes.base import IgniteDataType
from .constants import *

FALLBACK = False

try:
    from pyignite import _cutils
except ImportError:
    FALLBACK = True


LONG_MASK = 0xffffffff
DIGITS_PER_INT = 9


def is_pow2(value: int) -> bool:
    """ Check if value is power of two. """
    return value > 0 and ((value & (value - 1)) == 0)


def is_iterable(value: Any) -> bool:
    """ Check if value is iterable. """
    try:
        iter(value)
        return True
    except TypeError:
        return False


def is_binary(value):
    """
    Check if a value is a pythonic representation of a Complex object.
    """
    return all([
        hasattr(value, 'type_name'),
        hasattr(value, 'type_id'),
        hasattr(value, 'schema'),
        hasattr(value, 'schema_id'),
    ])


def is_hinted(value):
    """
    Check if a value is a tuple of data item and its type hint.
    """
    return isinstance(value, tuple) and len(value) == 2 and inspect.isclass(value[1]) and \
        issubclass(value[1], IgniteDataType)


def int_overflow(value: int) -> int:
    """
    Simulates 32bit integer overflow.
    """
    return ((value ^ 0x80000000) & 0xffffffff) - 0x80000000


def hashcode(data: Union[str, bytes, bytearray, memoryview]) -> int:
    """
    Calculate hash code used for identifying objects in Ignite binary API.

    :param data: UTF-8-encoded string identifier of binary buffer or byte array
    :return: hash code.
    """
    if FALLBACK:
        return __hashcode_fallback(data)

    return _cutils.hashcode(data)


def __hashcode_fallback(data: Union[str, bytes, bytearray, memoryview]) -> int:
    if data is None:
        return 0

    if isinstance(data, str):
        """
        For strings we iterate over code point which are of the int type
        and can take up to 4 bytes and can only be positive.
        """
        result = 0
        for char in data:
            try:
                char_val = ord(char)
                result = int_overflow(31 * result + char_val)
            except TypeError:
                pass
    else:
        """
        For byte array we iterate over bytes which only take 1 byte. But
        according to protocol, bytes during hashing should be treated as signed
        integer numbers 8 bits long. On other hand elements in Python's `bytes`
        are unsigned. For this reason we use ctypes.c_byte() to make them
        signed.
        """
        result = 1
        for byte in data:
            byte = ctypes.c_byte(byte).value
            result = int_overflow(31 * result + byte)
    return result


def cache_id(cache: Union[str, int]) -> int:
    """
    Create a cache ID from cache name.

    :param cache: cache name or ID,
    :return: cache ID.
    """
    return cache if type(cache) is int else hashcode(cache)


def entity_id(cache: Union[str, int]) -> Optional[int]:
    """
    Create a type ID from type name or field ID from field name.

    :param cache: entity name or ID,
    :return: entity ID.
    """
    if cache is None:
        return None
    return cache if type(cache) is int else hashcode(cache.lower())


def schema_id(schema: Union[int, dict]) -> int:
    """
    Calculate Complex Object schema ID.

    :param schema: a dict of field names: field types,
    :return: schema ID.
    """
    if FALLBACK:
        return __schema_id_fallback(schema)
    return _cutils.schema_id(schema)


def __schema_id_fallback(schema: Union[int, dict]) -> int:
    if isinstance(schema, int):
        return schema

    if schema is None:
        return 0

    s_id = FNV1_OFFSET_BASIS if schema else 0
    for field_name in schema.keys():
        field_id = __hashcode_fallback(field_name.lower())
        s_id ^= (field_id & 0xff)
        s_id = int_overflow(s_id * FNV1_PRIME)
        s_id ^= ((field_id >> 8) & 0xff)
        s_id = int_overflow(s_id * FNV1_PRIME)
        s_id ^= ((field_id >> 16) & 0xff)
        s_id = int_overflow(s_id * FNV1_PRIME)
        s_id ^= ((field_id >> 24) & 0xff)
        s_id = int_overflow(s_id * FNV1_PRIME)
    return s_id


def decimal_hashcode(value: decimal.Decimal) -> int:
    """
    This is a translation of `java.math.BigDecimal` class `hashCode()` method
    to Python.

    :param value: pythonic decimal value,
    :return: hashcode.
    """
    sign, digits, scale = value.normalize().as_tuple()
    sign = -1 if sign else 1
    value = int(''.join([str(d) for d in digits]))

    if value < MAX_LONG:
        # this is the case when Java BigDecimal digits are stored
        # compactly, in the internal 64-bit integer field
        int_hash = (
            (unsigned(value, ctypes.c_ulonglong) >> 32) * 31 + (value & LONG_MASK)
        ) & LONG_MASK
    else:
        # digits are not fit in the 64-bit long, so they get split internally
        # to an array of values within 32-bit integer range each (it is really
        # a part of `java.math.BigInteger` class internals)
        magnitude = []
        order = 0
        while True:
            elem = value >> order
            if elem > 1:
                magnitude.insert(0, ctypes.c_int(elem).value)
                order += 32
            else:
                break

        int_hash = 0
        for v in magnitude:
            int_hash = (31 * int_hash + (v & LONG_MASK)) & LONG_MASK

    return ctypes.c_int(31 * int_hash * sign - scale).value


def datetime_hashcode(value: int) -> int:
    """
    Calculates hashcode from UNIX epoch.

    :param value: UNIX time,
    :return: Java hashcode.
    """
    return (value & LONG_MASK) ^ (unsigned(value, ctypes.c_ulonglong) >> 32)


def status_to_exception(exc: Type[Exception], ignore_timeout=False):
    """
    Converts erroneous status code with error message to an exception with type of the given class. Supports coroutines.
    Also, support `timeout` argument for decorated async function.

    :param exc: the class of exception to raise,
    :param ignore_timeout: If set, ignore `timeout` argument.
    :return: decorated function.
    """
    def process_result(result):
        if result.status != 0:
            raise exc(result.message)
        return result.value

    def ste_decorator(fn):
        if inspect.iscoroutinefunction(fn):
            @wraps(fn)
            async def ste_wrapper_async(*args, **kwargs):
                timeout = kwargs.pop('timeout', 0)
                if timeout and not ignore_timeout:
                    result = await asyncio.wait_for(fn(*args, **kwargs), timeout)
                else:
                    result = await fn(*args, **kwargs)

                return process_result(result)

            return ste_wrapper_async
        else:
            @wraps(fn)
            def ste_wrapper(*args, **kwargs):
                return process_result(fn(*args, **kwargs))
            return ste_wrapper
    return ste_decorator


def get_field_by_id(obj: 'GenericObjectMeta', field_id: int) -> Tuple[Any, IgniteDataType]:
    """
    Returns a complex object's field value, given the field's entity ID.

    :param obj: complex object,
    :param field_id: field ID,
    :return: complex object field's value and type.
    """
    for fname, ftype in obj._schema.items():
        if entity_id(fname) == field_id:
            return getattr(obj, fname, getattr(ftype, 'default')), ftype


def unsigned(value: int, c_type: ctypes._SimpleCData = ctypes.c_uint) -> int:
    """ Convert signed integer value to unsigned. """
    return c_type(value).value


def capitalize(string: str) -> str:
    """
    Capitalizing the string, assuming the first character is a letter.
    Does not touch any other character, unlike the `string.capitalize()`.
    """
    return string[:1].upper() + string[1:]


def process_delimiter(name: str, delimiter: str) -> str:
    """
    Splits the name by delimiter, capitalize each part, merge.
    """
    return ''.join([capitalize(x) for x in name.split(delimiter)])


def deprecated(version, reason):
    def decorator_deprecated(fn):
        @wraps(fn)
        def wrapper_deprecated(*args, **kwds):
            warnings.warn(f'Deprecated since {version}. The reason: {reason}', category=DeprecationWarning)
            return fn(*args, **kwds)
        return wrapper_deprecated
    return decorator_deprecated

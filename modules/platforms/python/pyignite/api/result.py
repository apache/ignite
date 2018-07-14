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

from pyignite.datatypes import String
from pyignite.queries import Response
from pyignite.utils import int_overflow


def hashcode(string: str) -> int:
    """
    Calculate hash code used for referencing cache bucket name
    in Ignite binary API.

    :param string: bucket name (or other string identifier),
    :return: hash code.
    """
    result = 0
    for char in string:
        try:
            char = ord(char)
        except TypeError:
            pass
        result = int_overflow(31 * result + char)
    return result


class APIResult:
    """
    Dataclass which represents the result of API request.

    Fields are:

    * status: request status code. 0 if successful,
    * message: 'Success' if status == 0, verbatim error description
      otherwise,
    * value: return value or None.
    """

    message = 'Success'
    value = None

    def __init__(self, response: Response):
        self.status = response.status_code
        self.query_id = response.query_id
        if hasattr(response, 'error_message'):
            self.message = String.to_python(response.error_message)

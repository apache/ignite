#
# Copyright 2019 GridGain Systems, Inc. and Contributors.
#
# Licensed under the GridGain Community Edition License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from pygridgain.queries.op_codes import OP_SUCCESS
from pygridgain.datatypes import String


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

    def __init__(self, response: 'Response'):
        self.status = getattr(response, 'status_code', OP_SUCCESS)
        self.query_id = response.query_id
        if hasattr(response, 'error_message'):
            self.message = String.to_python(response.error_message)

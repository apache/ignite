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


def is_iterable(value):
    """ Check if value is iterable. """
    try:
        iter(value)
        return True
    except TypeError:
        return False


def is_hinted(value):
    """
    Check if a value is a tuple of data item and its type hint.
    """
    return (
        isinstance(value, tuple)
        and len(value) == 2
        and isinstance(value[1], object)
    )

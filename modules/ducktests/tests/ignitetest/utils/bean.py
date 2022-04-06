# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License

class Bean:
    def __init__(self, class_name, **kwargs):
        self.class_name = class_name
        self.properties = kwargs

    def __eq__(self, other):
        return self.class_name == other.class_name

    def __hash__(self):
        return self.class_name.__hash__()

    def __repr__(self):
        return self.class_name

    class_name: str
    properties: {}

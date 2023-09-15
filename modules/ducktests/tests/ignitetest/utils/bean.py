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
    """
    Helper class to store bean class name, optional constructor parameters and optional set of
    name/value pairs for properties.

    Serves as a parameter for the 'bean' jinja2 template macro used to generate lists of beans in the XML
    ignite node configuration (see the ignitetest/services/utils/templates/misc_macro.j2::bean).

    Hashable to be stored in a set.
    """
    def __init__(self, class_name, constructor_args=None, **kwargs):
        """
        :param class_name: bean class name
        :param constructor_args: optional list of constructor parameters
        :param kwargs: properties name / value pairs
        """
        if constructor_args is None:
            self.constructor_args = []
        else:
            self.constructor_args = constructor_args

        self.class_name = class_name
        self.properties = kwargs

    def __eq__(self, other):
        return self.class_name == other.class_name

    def __hash__(self):
        return hash(self.class_name)

    def __repr__(self):
        return self.class_name

    class_name: str
    properties: {}


class BeanRef:
    """
    Helper class to represent property which is a bean reference.
    """
    def __init__(self, ref):
        self.ref = ref

    ref: str

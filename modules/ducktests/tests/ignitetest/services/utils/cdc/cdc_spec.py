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

import os

from ignitetest.services.utils.config_template import TEMPLATE_PATHES


def get_cdc_spec(base, service):
    """
    :param base: either IgniteNodeSpec or ISENodeSpec
    :param service IgniteService
    :return: Spec for CDC applications
    """

    def add_template_dir():
        tmpl_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")
        if tmpl_dir not in TEMPLATE_PATHES:
            TEMPLATE_PATHES.append(tmpl_dir)

    class CdcSpec(base):
        def __init__(self, service, jvm_opts=None, merge_with_default=True):
            super().__init__(service, jvm_opts if jvm_opts else [], merge_with_default)

            add_template_dir()

    return CdcSpec(service, service.spec.jvm_opts, merge_with_default=True)

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
# limitations under the License.

"""
This module contains basic ignite test.
"""
from time import monotonic

from ducktape.utils.local_filesystem_utils import mkdir_p
from ducktape.tests.test import Test, TestContext
from ignitetest.services.ignite import IgniteService


# pylint: disable=W0223
class IgniteTest(Test):
    """
    Basic ignite test.
    """
    def __init__(self, test_context):
        super().__init__(test_context=test_context)

    @staticmethod
    def monotonic():
        """
        monotonic() -> float

        :return:
            The value (in fractional seconds) of a monotonic clock, i.e. a clock that cannot go backwards.
            The clock is not affected by system clock updates. The reference point of the returned value is undefined,
            so that only the difference between the results of consecutive calls is valid.
        """
        return monotonic()

    def copy_ignite_work_dir(self):
        """
        Copying work directory from service nodes to the results directory.
        """
        for service in self.test_context.services:
            if not isinstance(service, IgniteService):
                self.logger.debug("Won't collect service workdir from %s." % service.service_id)
                continue

            if service.config.data_storage and service.config.data_storage.default.persistent:
                # Try to copy the root directory
                self.logger.debug("Copying persistence dir...")
                try:
                    for node in service.nodes:
                        dest = os.path.join(
                            TestContext.results_dir(self.test_context, self.test_context.test_index),
                            service.service_id, node.account.hostname)
                        self.logger.debug("Dest dir " + dest)
                        if not os.path.isdir(dest):
                            mkdir_p(dest)

                        tgz_work = f'{service.WORK_DIR}.tgz'

                        node.account.ssh(f'cd {service.WORK_DIR} ; tar czf "{tgz_work}" *;')
                        node.account.copy_from(tgz_work, dest)
                except Exception as ex:  # pylint: disable=W0703
                    self.logger.warn(
                        "Error copying persistence dir from %(source)s to %(dest)s. \
                        service %(service)s: %(message)s" %
                        {'source': service.WORK_DIR,
                         'dest': dest,
                         'service': service,
                         'message': ex})

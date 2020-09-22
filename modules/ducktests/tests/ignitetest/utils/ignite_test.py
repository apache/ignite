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
import os
from time import monotonic

from ducktape.utils.local_filesystem_utils import mkdir_p
from ducktape.tests.test import Test, TestContext

# pylint: disable=C0103,W0223,W0703
from ignitetest.services.ignite import IgniteService
from ignitetest.services.utils.ignite_persistence import PersistenceAware, IgnitePersistenceAware


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

    def copy_ignite_workdir(self):
        """
        Copy work dir from service nodes to the results directory.

        If the test passed, only the default set will be collected. If the the test failed, all logs will be collected.
        """
        self.logger.info("copy_ignite_workdir")
        for service in self.test_context.services:
            if not isinstance(service, IgniteService):
                self.test_context.logger.debug("Won't collect service workdir from %s." %
                                               service.service_id)
                continue

            if service.config.data_storage and service.config.data_storage.default.persistent:
                # Try to copy the service logs
                self.test_context.logger.debug("Copying persistence dir...")
                try:
                    for node in service.nodes:
                        dest = os.path.join(
                            TestContext.results_dir(self.test_context, self.test_context.test_index),
                            service.service_id, node.account.hostname)
                        self.test_context.logger.debug("Dest dir " + dest)
                        if not os.path.isdir(dest):
                            mkdir_p(dest)

                        tgz_work = '%s.tgz' % PersistenceAware.PERSISTENT_ROOT

                        node.account.ssh(compress_cmd(PersistenceAware.PERSISTENT_ROOT, tgz_work))
                        node.account.copy_from(tgz_work, dest)
                except Exception as e:
                    self.test_context.logger.warn(
                        "Error copying persistence dir from %(source)s to %(dest)s. \
                        service %(service)s: %(message)s" %
                        {'source': IgnitePersistenceAware.WORK_DIR,
                         'dest': dest,
                         'service': service,
                         'message': e})


def compress_cmd(src_path, dest_tgz):
    """Return bash command which compresses the given path to a tarball."""

    compres_cmd = f'cd {src_path} ; tar czf "{dest_tgz}" *;'

    return compres_cmd

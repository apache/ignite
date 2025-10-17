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

from ducktape.mark import defaults

from ignitetest.services.utils import IgniteServiceType
from ignitetest.services.utils.cdc.ignite_to_ignite_cdc_helper import IgniteToIgniteCdcHelper
from ignitetest.services.utils.cdc.ignite_to_kafka_cdc_helper import KafkaCdcParams, \
    IgniteToKafkaCdcHelper
from ignitetest.services.utils.cdc.ignite_to_ignite_client_cdc_helper import IgniteToIgniteClientCdcHelper
from ignitetest.tests.cdc.cdc_replication_abstract_test import CdcReplicationAbstractTest
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.version import DEV_BRANCH

WAL_FORCE_ARCHIVE_TIMEOUT_MS = 100


class CdcReplicationTest(CdcReplicationAbstractTest):
    """
    CDC replication tests.
    """
    @cluster(num_nodes=6)
    @ignite_versions(str(DEV_BRANCH))
    @defaults(pds=[True, False], mode=["active-active", "active-passive"])
    def ignite_to_ignite_test(self, ignite_version, pds, mode):
        self.pds = pds

        return self.run(ignite_version, mode, IgniteToIgniteCdcHelper())

    @cluster(num_nodes=6)
    @ignite_versions(str(DEV_BRANCH))
    @defaults(pds=[True, False], mode=["active-active", "active-passive"])
    def ignite_to_ignite_client_test(self, ignite_version, pds, mode):
        self.pds = pds

        return self.run(ignite_version, mode, IgniteToIgniteClientCdcHelper())

    @cluster(num_nodes=10)
    @ignite_versions(str(DEV_BRANCH))
    @defaults(pds=[True, False], mode=["active-active", "active-passive"])
    def ignite_to_kafka_to_ignite_test(self, ignite_version, pds, mode):
        self.pds = pds

        zk, kafka = self.start_kafka(kafka_nodes=1)

        res = self.run(ignite_version, mode, IgniteToKafkaCdcHelper(), KafkaCdcParams(kafka=kafka))

        self.stop_kafka(zk, kafka)

        return res

    @cluster(num_nodes=10)
    @ignite_versions(str(DEV_BRANCH))
    @defaults(pds=[True, False], mode=["active-active", "active-passive"])
    def ignite_to_kafka_to_ignite_client_test(self, ignite_version, pds, mode):
        self.pds = pds

        zk, kafka = self.start_kafka(kafka_nodes=1)

        res = self.run(ignite_version, mode, IgniteToKafkaCdcHelper(), KafkaCdcParams(
            kafka=kafka,
            kafka_to_ignite_client_type=IgniteServiceType.THIN_CLIENT
        ))

        self.stop_kafka(zk, kafka)

        return res

    def ignite_config(self, ignite_version, ignite_instance_name):
        cfg = super().ignite_config(ignite_version, ignite_instance_name)

        cfg = cfg._replace(
            data_storage=cfg.data_storage._replace(
                wal_force_archive_timeout=WAL_FORCE_ARCHIVE_TIMEOUT_MS
            )
        )

        return cfg

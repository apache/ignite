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
from ignitetest.services.utils.cdc.ignite_to_ignite_cdc_configurer import IgniteToIgniteCdcConfigurer
from ignitetest.services.utils.cdc.ignite_to_kafka_cdc_configurer import KafkaCdcParams, \
    IgniteToKafkaCdcConfigurer
from ignitetest.services.utils.cdc.ignite_to_ignite_client_cdc_configurer import IgniteToIgniteClientCdcConfigurer
from ignitetest.tests.cdc.cdc_replication_abstract_test import CdcReplicationAbstractTest
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.version import DEV_BRANCH, LATEST

WAL_FORCE_ARCHIVE_TIMEOUT_MS = 100

class CdcReplicationTest(CdcReplicationAbstractTest):
    """
    CDC replication tests.
    """
    @cluster(num_nodes=6)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @defaults(pds=[True, False], mode=["active-active", "active-passive"])
    def ignite_to_ignite_test(self, ignite_version, pds, mode):
        return self.run(ignite_version, pds, mode, IgniteToIgniteCdcConfigurer())

    @cluster(num_nodes=6)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @defaults(pds=[True, False], mode=["active-active", "active-passive"])
    def ignite_to_ignite_client_test(self, ignite_version, pds, mode):
        return self.run(ignite_version, pds, mode, IgniteToIgniteClientCdcConfigurer())

    @cluster(num_nodes=10)
    @ignite_versions(str(DEV_BRANCH))
    @defaults(pds=[True, False], mode=["active-active", "active-passive"])
    def ignite_to_kafka_to_ignite_test(self, ignite_version, pds, mode):
        zk, kafka = self.start_kafka(kafka_nodes=1)

        res = self.run(ignite_version, pds, mode, IgniteToKafkaCdcConfigurer(), KafkaCdcParams(kafka=kafka))

        self.stop_kafka(zk, kafka)

        return res

    @cluster(num_nodes=10)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @defaults(pds=[True, False], mode=["active-active", "active-passive"])
    def ignite_to_kafka_to_ignite_client_test(self, ignite_version, pds, mode):
        zk, kafka = self.start_kafka(kafka_nodes=1)

        res = self.run(ignite_version, pds, mode, IgniteToKafkaCdcConfigurer(), KafkaCdcParams(
            kafka=kafka,
            kafka_to_ignite_client_type=IgniteServiceType.THIN_CLIENT
        ))

        self.stop_kafka(zk, kafka)

        return res

    def ignite_config(self, ignite_version, pds, ignite_instance_name):
        cfg = super().ignite_config(ignite_version, pds, ignite_instance_name)

        cfg = cfg._replace(
            data_storage=cfg.data_storage._replace(
                wal_force_archive_timeout=WAL_FORCE_ARCHIVE_TIMEOUT_MS
            )
        )

        return cfg

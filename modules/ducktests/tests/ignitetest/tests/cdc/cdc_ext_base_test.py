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
import difflib

from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.utils.ignite_test import IgniteTest


class CdcExtBaseTest(IgniteTest):
    def check_partitions_are_same(self, source_cluster, target_cluster):
        """
        Compare partitions on source and target clusters.

        @:return True if there is no any divergence between partitions on source and target clusters.
        """
        source_cluster_dump = self.dump_partitions(source_cluster)

        target_cluster_dump = self.dump_partitions(target_cluster)

        if source_cluster_dump != target_cluster_dump:
            def diff(source, target):
                return "".join(difflib.unified_diff(
                    source.splitlines(True),
                    target.splitlines(True),
                    "source",
                    "target",
                    n=1)
                )

            self.logger.debug("Partitions are different in source and target clusters:\n"
                              f"{diff(source_cluster_dump, target_cluster_dump)}")
            return False
        else:
            return True

    @staticmethod
    def dump_partitions(ignite):
        """
        Dump partitions info skipping the cluster-specific fields.

        Saves original dump file in the service log directory.
        """
        dump_filename = ControlUtility(ignite).idle_verify_dump(ignite.nodes[0])

        orig_dump_filename = os.path.join(ignite.log_dir, 'idle_verify_dump_orig.txt')

        ignite.nodes[0].account.ssh(f"mv {dump_filename} {orig_dump_filename}")

        processed_dump_filename = os.path.join(ignite.log_dir, 'idle_verify_dump.txt')

        ignite.nodes[0].account.ssh(
            f"cat {orig_dump_filename} | "
            f"sed -E 's/, partVerHash=[-0-9]+]/]/g' | "
            f"sed -E 's/ consistentId=[^,]+,//g' > "
            f"{processed_dump_filename}")

        return ignite.nodes[0].account.ssh_output(f"cat {processed_dump_filename}").decode("utf-8")

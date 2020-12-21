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
import os

from ignitetest.services.utils.path import PathAware


class ZookeeperPathAware(PathAware):
    def init_persistent(self, node):
        super().init_persistent(node)

        setattr(self, 'logs', {
            "zookeeper_log": {
                "path": self.log_path,
                "collect_default": True
            }
        })

    @property
    def log_path(self):
        return os.path.join(self.persistent_root, "zookeeper.log")

    @property
    def data_dir(self):
        return os.path.join(self.persistent_root, "data")

    @property
    def log4j_config_file(self):
        return os.path.join(self.persistent_root, "log4j.properties")

    @property
    def config_file(self):
        return os.path.join(self.persistent_root, "zookeeper.properties")

    @property
    def home(self):
        return os.path.join(self.install_root, f"{self.project}-{self.version}")

    @property
    def project(self):
        return "zookeeper"

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

"""
This module contains classes and utilities for SslContextFactory.
"""
import os

DEFAULT_PASSWORD = "123456"
DEFAULT_KEYSTORE = "server.jks"
DEFAULT_TRUSTSTORE = "truststore.jks"


class SslContextFactory:
    """
    Ignite SslContextFactory.
    """
    # pylint: disable=W0622, R0913
    def __init__(self, globals,
                 key_store_jks: str = DEFAULT_KEYSTORE, key_store_password: str = DEFAULT_PASSWORD,
                 trust_store_jks: str = DEFAULT_TRUSTSTORE, trust_store_password: str = DEFAULT_PASSWORD):
        self.globals = globals
        self.key_store_path = self.globals.get("key_store_path", self.jks_path(key_store_jks))
        self.key_store_password = self.globals.get("key_store_password", key_store_password)
        self.trust_store_path = self.globals.get("trust_store_path", self.jks_path(trust_store_jks))
        self.trust_store_password = self.globals.get("trust_store_password", trust_store_password)

    @property
    def certificate_dir(self):
        """
        :return: path to certificate directory
        """
        return os.path.join(self.globals.get("install_root", "/opt"),
                            "ignite-dev", "modules", "ducktests", "tests", "certs")

    def jks_path(self, jks_name: str):
        """
        :return Path to jks file.
        """
        return os.path.join(self.certificate_dir, jks_name)

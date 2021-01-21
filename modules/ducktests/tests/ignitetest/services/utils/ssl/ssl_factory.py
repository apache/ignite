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
DEFAULT_SERVER_KEYSTORE = "server.jks"
DEFAULT_CLIENT_KEYSTORE = "client.jks"
DEFAULT_ADMIN_KEYSTORE = "admin.jks"
DEFAULT_TRUSTSTORE = "truststore.jks"


class SslContextFactory:
    """
    Ignite SslContextFactory.
    """
    # pylint: disable=R0913
    def __init__(self, root_dir: str = "/opt",
                 key_store_jks: str = DEFAULT_SERVER_KEYSTORE, key_store_password: str = DEFAULT_PASSWORD,
                 trust_store_jks: str = DEFAULT_TRUSTSTORE, trust_store_password: str = DEFAULT_PASSWORD):

        certificate_dir = os.path.join(root_dir, "ignite-dev", "modules", "ducktests", "tests", "certs")

        self.key_store_path = os.path.join(certificate_dir, key_store_jks)
        self.key_store_password = key_store_password
        self.trust_store_path = os.path.join(certificate_dir, trust_store_jks)
        self.trust_store_password = trust_store_password

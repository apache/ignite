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


class SslContextFactory:
    """
    Ignite SslContextFactory.
    """
    def __init__(self, cluster, key_store_jks: str = None, key_store_pwd: str = "123456",
                 trust_store_jks: str = "truststore.jks", trust_store_pwd: str = "123456"):

        self.key_store_path = cluster.get_cert_path(key_store_jks)
        self.key_store_pwd = key_store_pwd
        self.trust_store_path = cluster.get_cert_path(trust_store_jks)
        self.trust_store_pwd = trust_store_pwd

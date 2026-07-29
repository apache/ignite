# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# the "License"); you may not use this file except in compliance with
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
This module contains tests for JMX Prometheus Exporter metrics collection.
"""

import requests
import time
from ignitetest.services.ignite import IgniteService
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.utils import cluster
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion


class JmxMetricsTest(IgniteTest):
    """
    Tests JMX Prometheus Exporter metrics collection
    """

    @cluster(num_nodes=1)
    def test_jmx_metrics_collection(self):
        """
        Test that JMX Exporter collects JVM memory and GC metrics
        """
        ignite = IgniteService(
            self.test_context, 
            IgniteConfiguration(version=IgniteVersion(DEV_BRANCH)),
            num_nodes=1
        )
        ignite.start()

        node = ignite.nodes[0]
        
        # Get the node's IP address
        node_ip = node.account.externally_routable_ip

        # JMX Exporter exposes metrics on port 8083
        metrics_url = f"http://{node_ip}:8083/metrics"

        self.logger.info(f"Fetching metrics from {metrics_url}")

        # Wait for JMX Exporter to be ready (status 200)
        metrics_text = self._wait_for_metrics(metrics_url, timeout=60)
        self.logger.info(f"JMX Exporter is ready. Received metrics (first 2000 chars):\n{metrics_text[:2000]}")

        # Check for expected JVM memory metrics (in default JMX Exporter format)
        assert "jvm_memory_used_bytes" in metrics_text, f"Missing jvm_memory_used_bytes metric"
        assert "jvm_memory_committed_bytes" in metrics_text, f"Missing jvm_memory_committed_bytes metric"

        # Check for GC metrics (in default JMX Exporter format)
        assert "jvm_gc_collection_seconds" in metrics_text, f"Missing jvm_gc_collection_seconds metric"

        self.logger.info("All expected JMX metrics are present!")
        
        ignite.stop()

    def _wait_for_metrics(self, url, timeout=60):
        """
        Wait for metrics endpoint to return status 200.
        :param url: URL to metrics endpoint
        :param timeout: Maximum wait time in seconds
        :return: Metrics text
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                response = requests.get(url, timeout=5)
                if response.status_code == 200:
                    return response.text
            except requests.exceptions.RequestException as e:
                self.logger.debug(f"Waiting for metrics: {e}")
            
            time.sleep(1)
        
        raise TimeoutError(f"Failed to get metrics from {url} within {timeout} seconds")

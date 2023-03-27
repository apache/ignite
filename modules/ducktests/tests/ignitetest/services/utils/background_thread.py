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
Background thread service.
"""

import threading
import traceback
from abc import ABCMeta, abstractmethod

from ignitetest.services.utils.ducktests_service import DucktestsService


class BackgroundThreadService(DucktestsService, metaclass=ABCMeta):
    """BackgroundThreadService allow to start nodes simultaneously using pool of threads."""

    def __init__(self, context, num_nodes=None, cluster_spec=None, **kwargs):
        super().__init__(context, num_nodes, cluster_spec, **kwargs)
        self.worker_threads = {}
        self.worker_errors = {}
        self.errors = ''
        self.lock = threading.RLock()

    def _protected_worker(self, idx, node, **kwargs):
        """Protected worker captures exceptions and makes them available to the main thread.

        This gives us the ability to propagate exceptions thrown in background threads, if desired.
        """
        try:
            self.worker(idx, node, **kwargs)
        except BaseException:
            with self.lock:
                self.logger.info("BackgroundThreadService threw exception: ")
                trace_fmt = traceback.format_exc()
                self.logger.info(trace_fmt)
                self.worker_errors[threading.currentThread().name] = trace_fmt
                if self.errors:
                    self.errors += "\n"
                self.errors += "%s: %s" % (threading.currentThread().name, trace_fmt)

            raise

    @abstractmethod
    def worker(self, idx, node, **kwargs):
        """
        :param idx: Node index
        :param node: Cluster node instance
        """

    def start_node(self, node, **kwargs):
        idx = self.idx(node)

        if idx in self.worker_threads and self.worker_threads[idx].is_alive():
            raise RuntimeError("Cannot restart node since previous thread is still alive")

        self.logger.info("Running %s node %d on %s", self.service_id, idx, node.account.hostname)
        worker = threading.Thread(
            name=self.service_id + "-worker-" + str(idx),
            target=self._protected_worker,
            args=(idx, node),
            kwargs=kwargs
        )
        worker.daemon = True
        worker.start()
        self.worker_threads[idx] = worker

    def wait(self, timeout_sec=600):
        """Wait no more than timeout_sec for all worker threads to finish.

        raise TimeoutException if all worker threads do not finish within timeout_sec
        """
        super().wait(timeout_sec)

        self._propagate_exceptions()

    def stop(self, force_stop=False, **kwargs):
        alive_workers = sum(1 for worker in self.worker_threads.values() if worker.is_alive())
        if alive_workers > 0:
            self.logger.debug(
                "Called stop with at least one worker thread is still running: " + str(alive_workers))

            self.logger.debug("%s" % str(self.worker_threads))

        super().stop(force_stop, **kwargs)

        self._propagate_exceptions()

    def wait_node(self, node, timeout_sec=600):
        idx = self.idx(node)
        worker_thread = self.worker_threads[idx]
        worker_thread.join(timeout_sec)
        return not worker_thread.is_alive()

    def _propagate_exceptions(self):
        """
        Propagate exceptions thrown in background threads
        """
        with self.lock:
            if len(self.worker_errors) > 0:
                raise Exception(self.errors)

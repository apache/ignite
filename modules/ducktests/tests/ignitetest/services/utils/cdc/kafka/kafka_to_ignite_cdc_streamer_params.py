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

from typing import NamedTuple

from ignitetest.utils.bean import Bean


class KafkaToIgniteCdcStreamerParams(NamedTuple):
    caches: list = None
    kafka_request_timeout: int = None
    max_batch_size: int = None
    metadata_consumer_group: str = None
    metadata_topic: str = "ignite-metadata"
    thread_count: int = None
    topic: str = "ignite"

    def to_bean(self, **kwargs):
        filtered = {k: v for k, v in self._asdict().items() if v is not None}

        return Bean("org.apache.ignite.cdc.kafka.KafkaToIgniteCdcStreamerConfiguration", **filtered, **kwargs)

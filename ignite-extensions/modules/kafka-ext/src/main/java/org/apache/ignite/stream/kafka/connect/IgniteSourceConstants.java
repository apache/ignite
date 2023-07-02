/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.stream.kafka.connect;

/**
 * Sink configuration strings.
 */
public class IgniteSourceConstants {
    /** Ignite configuration file path. */
    public static final String CACHE_CFG_PATH = "igniteCfg";

    /** Cache name. */
    public static final String CACHE_NAME = "cacheName";

    /** Events to be listened to. Names corresponds to {@link IgniteSourceTask.CacheEvt}. */
    public static final String CACHE_EVENTS = "cacheEvts";

    /** Internal buffer size. */
    public static final String INTL_BUF_SIZE = "evtBufferSize";

    /** Size of one chunk drained from the internal buffer. */
    public static final String INTL_BATCH_SIZE = "evtBatchSize";

    /** User-defined filter class. */
    public static final String CACHE_FILTER_CLASS = "cacheFilterCls";

    /** Kafka topic. */
    public static final String TOPIC_NAMES = "topicNames";
}

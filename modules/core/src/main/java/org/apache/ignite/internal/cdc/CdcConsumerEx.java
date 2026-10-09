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

package org.apache.ignite.internal.cdc;

import java.util.Iterator;

import org.apache.ignite.cdc.CdcCacheEvent;
import org.apache.ignite.cdc.CdcConsumer;
import org.apache.ignite.metric.MetricRegistry;

/**
 * Extended CdcConsumer interface which provides overloaded {@link CdcConsumerEx#start(MetricRegistry, Iterator)} method
 * required for CDC regex filters.
 */
public interface CdcConsumerEx extends CdcConsumer {
    /**
     * Starts the consumer.
     * @param mreg Metric registry for consumer specific metrics.
     * @param cacheEvents The iterator contains previously handled {@link CdcCacheEvent}s that represent the actual
     * caches at the time the consumer started. Note that changes which occurred while the application was down (creates,
     * destroys, edits) are not included. Such changes will be delivered via the regular notifications after this method
     * returns.
     */
    void start(MetricRegistry mreg, Iterator<CdcCacheEvent> cacheEvents);
}

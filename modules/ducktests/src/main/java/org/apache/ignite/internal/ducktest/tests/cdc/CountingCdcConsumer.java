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

package org.apache.ignite.internal.ducktest.tests.cdc;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cdc.CdcCacheEvent;
import org.apache.ignite.cdc.CdcConsumer;
import org.apache.ignite.cdc.CdcEvent;
import org.apache.ignite.cdc.TypeMapping;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.resources.LoggerResource;

/**
 * Simple CDC consumer.
 *
 * Just counts the total number of the objects consumed.
 */
public class CountingCdcConsumer implements CdcConsumer {
    /** Logger. */
    @LoggerResource
    protected IgniteLogger log;

    /** Total count of the object consumed. */
    private final AtomicLong objectsConsumed = new AtomicLong();

    /** {@inheritDoc} */
    @Override public void start(MetricRegistry mreg) {
        log.info("CountingCdcConsumer started");
    }

    /** {@inheritDoc} */
    @Override public boolean onEvents(Iterator<CdcEvent> events) {
        events.forEachRemaining(this::consume);

        return true;
    }

    /** {@inheritDoc} */
    @Override public void onTypes(Iterator<BinaryType> types) {
        types.forEachRemaining(this::consume);
    }

    /** {@inheritDoc} */
    @Override public void onMappings(Iterator<TypeMapping> mappings) {
        mappings.forEachRemaining(this::consume);
    }

    /** {@inheritDoc} */
    @Override public void onCacheChange(Iterator<CdcCacheEvent> cacheEvents) {
        cacheEvents.forEachRemaining(this::consume);
    }

    /** {@inheritDoc} */
    @Override public void onCacheDestroy(Iterator<Integer> caches) {
        caches.forEachRemaining(this::consume);
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        log.info("CountingCdcConsumer stopped, objectsConsumed: " + objectsConsumed.get());
    }

    /**
     * Consume object by counting it.
     *
     * @param object object passed from the CDC engine.
     */
    private void consume(Object object) {
        objectsConsumed.incrementAndGet();

        log.info("Consumed: [object=" + object + "]");
    }
}

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

package org.apache.ignite.cdc;

import org.apache.ignite.Ignition;
import org.apache.ignite.cdc.conflictresolve.CacheVersionConflictResolverImpl;
import org.apache.ignite.cdc.kafka.KafkaToIgniteCdcStreamer;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteExperimental;

/**
 * Change Data Consumer that streams all data changes to provided {@link #dest} Ignite cluster.
 * Consumer will just fail in case of any error during write. Fail of consumer will lead to the fail of {@code ignite-cdc} application.
 * It expected that {@code ignite-cdc} will be configured for automatic restarts with the OS tool to failover temporary errors
 * such as Kafka unavailability or network issues.
 *
 * If you have plans to apply written messages to the other Ignite cluster in active-active manner,
 * e.g. concurrent updates of the same entry in other cluster is possible,
 * please, be aware of {@link CacheVersionConflictResolverImpl} conflict resolved.
 * Configuration of {@link CacheVersionConflictResolverImpl} can be found in {@link KafkaToIgniteCdcStreamer} documentation.
 *
 * @see CdcMain
 * @see CacheVersionConflictResolverImpl
 */
@IgniteExperimental
public class IgniteToIgniteCdcStreamer extends AbstractIgniteCdcStreamer {
    /** Destination cluster client configuration. */
    private IgniteConfiguration destIgniteCfg;

    /** Destination Ignite cluster client */
    private IgniteEx dest;

    /** {@inheritDoc} */
    @Override public void start(MetricRegistry mreg) {
        super.start(mreg);

        if (log.isInfoEnabled())
            log.info("Ignite To Ignite Streamer [cacheIds=" + cachesIds + ']');

        A.notNull(destIgniteCfg, "Destination Ignite configuration.");

        dest = (IgniteEx)Ignition.start(destIgniteCfg);

        applier = new CdcEventsIgniteApplier(dest, maxBatchSize, log);
    }

    /** {@inheritDoc} */
    @Override protected BinaryContext binaryContext() {
        return ((CacheObjectBinaryProcessorImpl)dest.context().cacheObjects()).binaryContext();
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        dest.close();
    }

    /**
     * Sets Ignite client node configuration that will connect to destination cluster.
     * @param destIgniteCfg Ignite client node configuration that will connect to destination cluster.
     * @return {@code this} for chaining.
     */
    public IgniteToIgniteCdcStreamer setDestinationIgniteConfiguration(IgniteConfiguration destIgniteCfg) {
        this.destIgniteCfg = destIgniteCfg;

        return this;
    }
}

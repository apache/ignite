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

package org.apache.ignite.cdc.thin;

import org.apache.ignite.Ignition;
import org.apache.ignite.cdc.AbstractIgniteCdcStreamer;
import org.apache.ignite.cdc.conflictresolve.CacheVersionConflictResolverImpl;
import org.apache.ignite.cdc.kafka.KafkaToIgniteCdcStreamer;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.ignite.internal.client.thin.ClientBinary;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * Change Data Consumer that streams all data changes to destination cluster through Ignite thin client.
 * <p/>
 * Consumer will just fail in case of any error during write. Fail of consumer will lead to the fail of {@code ignite-cdc} application.
 * It expected that {@code ignite-cdc} will be configured for automatic restarts with the OS tool to failover temporary errors
 * such as destination cluster unavailability or other issues.
 * <p/>
 * If you have plans to apply written messages to the other Ignite cluster in active-active manner,
 * e.g. concurrent updates of the same entry in other cluster is possible,
 * please, be aware of {@link CacheVersionConflictResolverImpl} conflict resolved.
 * Configuration of {@link CacheVersionConflictResolverImpl} can be found in {@link KafkaToIgniteCdcStreamer} documentation.
 *
 * @see IgniteClient
 * @see CdcMain
 * @see CacheVersionConflictResolverImpl
 */
public class IgniteToIgniteClientCdcStreamer extends AbstractIgniteCdcStreamer {
    /** Ignite thin client configuration. */
    private ClientConfiguration destClientCfg;

    /** Ignite thin client. */
    private IgniteClient dest;

    /** {@inheritDoc} */
    @Override public void start(MetricRegistry mreg) {
        super.start(mreg);

        if (log.isInfoEnabled())
            log.info("Ignite To Ignite Client Streamer [cacheIds=" + cachesIds + ']');

        A.notNull(destClientCfg, "Destination thin client configuration");

        dest = Ignition.startClient(destClientCfg);

        applier = new CdcEventsIgniteClientApplier(dest, maxBatchSize, log);
    }

    /** {@inheritDoc} */
    @Override protected BinaryContext binaryContext() {
        return ((ClientBinary)dest.binary()).binaryContext();
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        dest.close();
    }

    /**
     * Sets Ignite thin client configuration that will connect to destination cluster.
     *
     * @param destClientCfg Ignite thin client configuration that will connect to destination cluster.
     * @return {@code this} for chaining.
     */
    public IgniteToIgniteClientCdcStreamer setDestinationClientConfiguration(ClientConfiguration destClientCfg) {
        this.destClientCfg = destClientCfg;

        return this;
    }
}

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

package org.apache.ignite.cdc.kafka;

import java.util.Collection;
import java.util.Objects;
import java.util.Properties;
import org.apache.ignite.Ignition;
import org.apache.ignite.cdc.AbstractCdcEventsApplier;
import org.apache.ignite.cdc.CdcEvent;
import org.apache.ignite.cdc.CdcEventsIgniteApplier;
import org.apache.ignite.cdc.conflictresolve.CacheConflictResolutionManagerImpl;
import org.apache.ignite.cdc.conflictresolve.CacheVersionConflictResolverImpl;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteExperimental;

/**
 * Main class of Kafka to Ignite application.
 * This application is counterpart of {@link IgniteToKafkaCdcStreamer} Change Data Capture consumer.
 * Application runs several {@link KafkaToIgniteCdcStreamerApplier} thread to read Kafka topic partitions
 * and apply {@link CdcEvent} to Ignite through client node.
 * <p>
 * In case of any error during read applier just fail. Fail of any applier will lead to the fail of whole application.
 * It expected that application will be configured for automatic restarts with the OS tool to failover temporary errors
 * such as Kafka or Ignite unavailability.
 * <p>
 * To resolve possible update conflicts (in case of concurrent update in source and destination Ignite clusters)
 * real-world deployments should use some conflict resolver, for example {@link CacheVersionConflictResolverImpl}.
 * Example of Ignite configuration with the conflict resolver:
 * <pre>
 * {@code
 * CacheVersionConflictResolverCachePluginProvider conflictPlugin = new CacheVersionConflictResolverCachePluginProvider();
 *
 * conflictPlugin.setClusterId(clusterId); // Cluster id.
 * conflictPlugin.setCaches(new HashSet<>(Arrays.asList("my-cache", "some-other-cache"))); // Caches to replicate.
 *
 * IgniteConfiguration cfg = ...;
 *
 * cfg.setPluginProviders(conflictPlugin);
 * }
 * </pre>
 * Please, see {@link CacheConflictResolutionManagerImpl} for additional information.
 *
 * @see CdcMain
 * @see IgniteToKafkaCdcStreamer
 * @see CdcEvent
 * @see KafkaToIgniteCdcStreamerApplier
 * @see CacheConflictResolutionManagerImpl
 */
@IgniteExperimental
public class KafkaToIgniteCdcStreamer extends AbstractKafkaToIgniteCdcStreamer {
    /** Ignite configuration. */
    private final IgniteConfiguration igniteCfg;

    /** Ignite client node. */
    private IgniteEx ign;

    /**
     * @param igniteCfg Ignite configuration.
     * @param kafkaProps Kafka properties.
     * @param streamerCfg Streamer configuration.
     */
    public KafkaToIgniteCdcStreamer(
        IgniteConfiguration igniteCfg,
        Properties kafkaProps,
        KafkaToIgniteCdcStreamerConfiguration streamerCfg
    ) {
        super(kafkaProps, streamerCfg);

        A.notNull(igniteCfg, "Destination Ignite configuration.");

        this.igniteCfg = igniteCfg;
    }

    /** {@inheritDoc} */
    @Override protected void initLogger() throws Exception {
        U.initWorkDir(igniteCfg);

        log = U.initLogger(igniteCfg, "kafka-ignite-streamer");

        igniteCfg.setGridLogger(log);
    }

    /** {@inheritDoc} */
    @Override protected void runx() throws Exception {
        try (IgniteEx ign = (IgniteEx)Ignition.start(igniteCfg)) {
            this.ign = ign;

            runAppliers();
        }
    }

    /** {@inheritDoc} */
    @Override protected AbstractCdcEventsApplier eventsApplier() {
        U.setCurrentIgniteName(ign.name());

        return new CdcEventsIgniteApplier(ign, streamerCfg.getMaxBatchSize(), log);
    }

    /** {@inheritDoc} */
    @Override protected BinaryContext binaryContext() {
        return ((CacheObjectBinaryProcessorImpl)ign.context().cacheObjects()).binaryContext();
    }

    /** {@inheritDoc} */
    @Override protected void checkCaches(Collection<String> caches) {
        caches.forEach(name -> Objects.requireNonNull(ign.cache(name), name + " not exists!"));
    }
}

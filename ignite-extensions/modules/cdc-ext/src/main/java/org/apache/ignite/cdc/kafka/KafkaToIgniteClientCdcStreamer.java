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
import java.util.Properties;
import java.util.UUID;
import org.apache.ignite.Ignition;
import org.apache.ignite.cdc.AbstractCdcEventsApplier;
import org.apache.ignite.cdc.CdcEvent;
import org.apache.ignite.cdc.conflictresolve.CacheConflictResolutionManagerImpl;
import org.apache.ignite.cdc.conflictresolve.CacheVersionConflictResolverImpl;
import org.apache.ignite.cdc.thin.CdcEventsIgniteClientApplier;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.ignite.internal.client.thin.ClientBinary;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteExperimental;

/**
 * Main class of Kafka to Ignite application.
 * This application is counterpart of {@link IgniteToKafkaCdcStreamer} Change Data Capture consumer.
 * Application runs several {@link KafkaToIgniteCdcStreamerApplier} thread to read Kafka topic partitions
 * and apply {@link CdcEvent} to Ignite through thin client.
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
 * @see IgniteClient
 * @see KafkaToIgniteCdcStreamerApplier
 * @see CacheConflictResolutionManagerImpl
 */
@IgniteExperimental
public class KafkaToIgniteClientCdcStreamer extends AbstractKafkaToIgniteCdcStreamer {
    /** Ignite thin client configuration. */
    private final ClientConfiguration clientCfg;

    /** Ignite thin client. */
    private IgniteClient client;

    /**
     * @param clientCfg Ignite thin client configuration.
     * @param kafkaProps Kafka properties.
     * @param streamerCfg Streamer configuration.
     */
    public KafkaToIgniteClientCdcStreamer(
        ClientConfiguration clientCfg,
        Properties kafkaProps,
        KafkaToIgniteCdcStreamerConfiguration streamerCfg
    ) {
        super(kafkaProps, streamerCfg);

        A.notNull(clientCfg, "Destination thin client configuration.");

        this.clientCfg = clientCfg;
    }

    /** {@inheritDoc} */
    @Override protected void initLogger() throws Exception {
        log = U.initLogger(null, "kafka-ignite-streamer", UUID.randomUUID(), U.defaultWorkDirectory());
    }

    /** {@inheritDoc} */
    @Override protected void runx() throws Exception {
        try (IgniteClient client = Ignition.startClient(clientCfg)) {
            this.client = client;

            runAppliers();
        }
    }

    /** {@inheritDoc} */
    @Override protected AbstractCdcEventsApplier eventsApplier() {
        GridBinaryMarshaller.popContext(binaryContext());

        return new CdcEventsIgniteClientApplier(client, streamerCfg.getMaxBatchSize(), log);
    }

    /** {@inheritDoc} */
    @Override protected BinaryContext binaryContext() {
        return ((ClientBinary)client.binary()).binaryContext();
    }

    /** {@inheritDoc} */
    @Override protected void checkCaches(Collection<String> caches) {
        Collection<String> clusterCaches = client.cacheNames();

        caches.forEach(name -> A.ensure(clusterCaches.contains(name), name + " not exists!"));
    }
}

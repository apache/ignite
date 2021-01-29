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

import java.net.URL;
import java.time.Duration;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.resource.GridSpringResourceContext;
import org.apache.ignite.internal.util.spring.IgniteSpringHelper;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import static org.apache.ignite.cdc.CDCIgniteToKafka.IGNITE_TO_KAFKA_TOPIC;
import static org.apache.ignite.cdc.KafkaUtils.TIMEOUT_MIN;
import static org.apache.ignite.cdc.Utils.fromSystemOrProperty;
import static org.apache.ignite.internal.IgniteComponentType.SPRING;

/** CDC data from Kafka to Ignite application. */
public class KafkaToIgnite implements Runnable {
    /** */
    public static final String KAFKA_TO_IGNITE_THREAD_COUNT = "kafka.to.ignite.thread.count";

    /** Error message. */
    public static final String ERR_MSG =
        "Please, provide path to the plugin properties file in \"propertiesPath\" property.";

    /** */
    private final IgniteEx ign;

    /** Properties. */
    private final Properties props;

    /** Topic name. */
    private final String topic;

    /** Map to cache ids. */
    private ConcurrentMap<Long, IgniteCache<BinaryObject, BinaryObject>> caches = new ConcurrentHashMap<>();

    /**
     * @param props Properties.
     * @param ign
     */
    public KafkaToIgnite(Properties props, IgniteEx ign) {
        this.props = props;
        this.topic = fromSystemOrProperty(IGNITE_TO_KAFKA_TOPIC, props);
        this.ign = ign;
    }

    /**
     * @param args CMD arguments.
     * @throws Exception In case of error.
     */
    public static void main(String[] args) throws Exception {
        new KafkaToIgnite(
            Utils.properties(args[0], ERR_MSG),
            (IgniteEx)Ignition.start(igniteCfg(args[1]))
        ).run();
    }

    /** {@inheritDoc} */
    @Override public void run() {
        try {
            startx();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** */
    public void startx() throws Exception {
        KafkaUtils.initTopic(topic, props);

        int thNum = Integer.parseInt(fromSystemOrProperty(KAFKA_TO_IGNITE_THREAD_COUNT, props));

        ExecutorService execSvc = Executors.newFixedThreadPool(thNum);

        for (int i = 0; i < thNum; i++)
            execSvc.submit(new KafkaToIgniteWorker());

        execSvc.shutdown();

        execSvc.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
    }

    /** Kafka to Ignite worker. */
    private class KafkaToIgniteWorker implements Runnable {
        public static final int RETRY_CNT = 3;

        /** */
        private boolean stoped;

        /** */
        private int retryCnt = RETRY_CNT;

        /** {@inheritDoc} */
        @Override public void run() {
            KafkaConsumer<BinaryObject, EntryEvent<BinaryObject, BinaryObject>> consumer = new KafkaConsumer<>(props);

            while (retryCnt > 0 && !stoped) {
                ConsumerRecords<BinaryObject, EntryEvent<BinaryObject, BinaryObject>> recs;

                try {
                    recs = consumer.poll(Duration.ofMinutes(TIMEOUT_MIN));

                    retryCnt = RETRY_CNT;
                }
                catch (Exception e) {
                    retryCnt--;

                    e.printStackTrace();

                    try {
                        Thread.sleep(Duration.ofMillis(TIMEOUT_MIN).toMillis());
                    }
                    catch (InterruptedException err) {
                        throw new RuntimeException(err);
                    }

                    continue;
                }

                for (ConsumerRecord<BinaryObject, EntryEvent<BinaryObject, BinaryObject>> rec : recs)
                    apply(rec.value());
            }
        }

        /** Applies single event to Ignite. */
        private void apply(EntryEvent<BinaryObject, BinaryObject> evt) {
            IgniteCache<BinaryObject, BinaryObject> cache = caches.computeIfAbsent(evt.cacheId(), cacheId -> {
                for (String cacheName : ign.cacheNames()) {
                    if (CU.cacheId(cacheName) == cacheId)
                        return ign.cache(cacheName).withKeepBinary();
                }

                throw new IllegalStateException("Cache with id not found[cacheId=" + cacheId + ']');
            });

            CacheEntry<BinaryObject, BinaryObject> entry = cache.getEntry(evt.key());

            EntryEventOrder ord = evt.order();

            GridCacheVersion kafkaVer = new GridCacheVersion(ord.topVer(), ord.nodeOrderDrId(), ord.order());

            if (entry.version().compareTo(kafkaVer) < 0) {

            }
        }

        private boolean isGreater(EntryEventOrder kafkaOrd, GridCacheVersion curVer) {
            return true;
        }
    }

    /**
     * @param path Path to the Ignite configuration file.
     * @return Ignite configuration from the file.
     * @throws IgniteCheckedException In case of load error.
     */
    private static IgniteConfiguration igniteCfg(String path) throws IgniteCheckedException {
        URL cfgUrl = U.resolveSpringUrl(path);

        IgniteSpringHelper spring = SPRING.create(false);

        IgniteBiTuple<Collection<IgniteConfiguration>, ? extends GridSpringResourceContext> cfgTuple =
            spring.loadConfigurations(cfgUrl);

        if (cfgTuple.get1().size() > 1)
            throw new IllegalArgumentException("Found " + cfgTuple.get1().size() + " configurations. Can use only 1");

        return cfgTuple.get1().iterator().next();
    }
}
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

package org.apache.ignite;

import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jsr166.LongAdder8;

/**
 * Copy
 * scp -i ../.ssh/yzhdanov_key ignite_core_jar/ignite-core.jar yzhdanov@172.25.1.26:
 * Client.
 * java -DCHEAT_CACHE=1 -DIGNITE_QUIET=false -DIGNITE_OVERRIDE_MCAST_GRP=228.1.2.6 -server -Xmx3g -Xms3g -cp ignite-core.jar org.apache.ignite.BenchAtomic true 128 512
 *
 * Server
 * java -DCHEAT_CACHE=1 -DIGNITE_QUIET=false -DIGNITE_OVERRIDE_MCAST_GRP=228.1.2.6 -server -Xmx3g -Xms3g -cp ignite-core.jar org.apache.ignite.BenchAtomic false
 *
 * -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintHeapAtGC -XX:+PrintTenuringDistribution -Xloggc:~/`date +%Y%m%d%H%M%S`gc.log
 */
public class BenchAtomic {
    public static final int KEYS = 100_000;

    public static void main(String[] args) throws InterruptedException {
        boolean client = Boolean.parseBoolean(args[0]);
        int threadCnt = args.length > 1 ? Integer.parseInt(args[1]) : 64;
        final int payLoad = args.length > 2 ? Integer.parseInt(args[2]) : 512;
        final CacheWriteSynchronizationMode writeSync = args.length > 3 ?
            CacheWriteSynchronizationMode.valueOf(args[3]) : CacheWriteSynchronizationMode.FULL_SYNC;

        String locHost = System.getProperty("LOC_HOST");
        int msgQLim = Integer.getInteger(
            "MSG_Q_LIM",
            1024);
        final boolean ioTest = Boolean.getBoolean("IO_TEST");
        final boolean ioTestNio = Boolean.getBoolean("IO_TEST_NIO");
        final int connPairs = Integer.getInteger("CONN_PAIRS", 1);
        final int selectors = Integer.getInteger("SELECTORS", 2);
        final boolean async = Boolean.getBoolean("ASYNC");

        System.out.println("Params [client=" + client +
            ", threads=" + threadCnt +
            ", payLoad=" + payLoad +
            ", writeSync=" + writeSync +
            ", locHost=" + locHost +
            ", msgQLim=" + msgQLim +
            ", ioTest=" + ioTest +
            ", ioTestNio=" + ioTestNio +
            ", selectors=" + selectors +
            ", async=" + async +
            ']');

        final Ignite ignite = Ignition.start(
            config(
                null,
                client,
                msgQLim,
                connPairs,
                locHost,
                selectors));

        if (!client)
            return;

        final LongAdder8 cnt = new LongAdder8();

        new Thread(
            new Runnable() {
                @Override public void run() {
                    long avg = 0;
                    int i = 0;

                    for (;;) {
                        try {
                            Thread.sleep(1000);
                        }
                        catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        long c;

                        System.out.println("TPS [cnt=" + (c = cnt.sumThenReset()) + ']');

                        avg += c;

                        i++;

                        if (i == 30) {
                            i = 0;

                            System.out.println("30 sec avg: " + avg * 1.0 / 30);

                            avg = 0;
                        }
                    }
                }
            }
        ).start();

        final IgniteCache<Integer, byte[]> cache0 = ignite.getOrCreateCache(
            BenchAtomic.<Integer, byte[]>cacheConfig(writeSync));

        final IgniteCache<Integer, byte[]> asyncCache = cache0.withAsync();

        final Semaphore sem = new Semaphore(2048);

        final IgniteInClosure<IgniteFuture<Object>> lsnr = new IgniteInClosure<IgniteFuture<Object>>() {
            @Override public void apply(IgniteFuture<Object> future) {
                sem.release();
            }
        };

        final ClusterNode[] servers = ignite.cluster().forServers().nodes().toArray(new ClusterNode[0]);

        for (int i = 0; i < (async ? 32 : threadCnt); i++) {
            new Thread(
                    new Runnable() {
                    @Override public void run() {
                        IgniteKernal kernal = (IgniteKernal)ignite;

                        for (;;) {
                            cnt.increment();

                            byte[] val = new byte[ThreadLocalRandom.current().nextInt(
                                64,
                                payLoad)];

                            if (ioTest) {
                                try {
                                    kernal.sendIoTest(
                                        servers[ThreadLocalRandom.current().nextInt(servers.length)],
                                        val,
                                        ioTestNio
                                    ).get();
                                }
                                catch (IgniteCheckedException e) {
                                    e.printStackTrace();

                                    return;
                                }

                                continue;
                            }

                            int key = ThreadLocalRandom.current().nextInt(KEYS);

                            if (async) {
                                sem.acquireUninterruptibly();

                                asyncCache.put(key, val);

                                IgniteFuture<Object> f = asyncCache.future();

                                f.listen(lsnr);

                                continue;
                            }

                            boolean startTx = cache0.getConfiguration(CacheConfiguration.class).getAtomicityMode() ==
                                CacheAtomicityMode.TRANSACTIONAL;

                            try {
                                Transaction tx = startTx ?
                                    ignite.transactions().txStart(TransactionConcurrency.PESSIMISTIC,
                                        TransactionIsolation.REPEATABLE_READ) :
                                    null;

                                cache0.put(key,
                                    val);

                                if (tx != null)
                                    tx.commit();
                            }
                            catch (Exception e) {
                                if (!X.hasCause(e,
                                    NodeStoppingException.class))
                                    e.printStackTrace();
                                else
                                    U.debug("Process stopping.");

                                break;
                            }

                        }
                    }
                }
            ).start();
        }
    }

    /**
     * @return Cache config.
     * @param writeSync
     */
    private static <K, V> CacheConfiguration<K, V> cacheConfig(CacheWriteSynchronizationMode writeSync) {
        return new CacheConfiguration<K, V>()
            .setName("1")
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
//            .setAtomicWriteOrderMode(CacheAtomicWriteOrderMode.PRIMARY)
           // .setAffinity(new FairAffinityFunction(1024))
            .setBackups(0)
            .setRebalanceMode(CacheRebalanceMode.SYNC)
            .setCopyOnRead(false)
//            .setMemoryMode(CacheMemoryMode.OFFHEAP_TIERED)
            .setWriteSynchronizationMode(writeSync);
    }

    private static IgniteConfiguration config(
        String name,
        boolean client,
        Integer msgQLim,
        int connPairs,
        String locHost,
        int selectors
    ) {
        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();

        commSpi.setSharedMemoryPort(-1);
        commSpi.setMessageQueueLimit(msgQLim);
        commSpi.setConnectionsPerNode(connPairs);
        commSpi.setSelectorsCount(selectors);

        IgniteConfiguration cfg = new IgniteConfiguration()
            .setGridName(name)
            .setClientMode(client)
            .setLocalHost(locHost)
            .setCommunicationSpi(commSpi)
            .setMetricsLogFrequency(15000)
            .setConnectorConfiguration(null);


        if (U.isMacOs())
            cfg.setLocalHost("127.0.0.1");

        return cfg;
    }
}

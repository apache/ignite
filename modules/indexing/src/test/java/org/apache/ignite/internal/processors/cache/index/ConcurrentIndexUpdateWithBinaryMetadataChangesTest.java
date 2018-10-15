package org.apache.ignite.internal.processors.cache.index;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Random;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryTypeImpl;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.managers.discovery.CustomMessageWrapper;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiClosure;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * <p> The <code>ConcurrentIndexUpdateWithBinaryMetadataChangesTest</code> </p>
 *
 * @author Alexei Scherbakov
 */
public class ConcurrentIndexUpdateWithBinaryMetadataChangesTest extends GridCommonAbstractTest {
    /** */
    private static final int GRIDS = 6;

    /** */
    public static final int CLIENTS = 10;

    /** */
    private static final int FIELDS = 100;

    /** */
    private static final int MB = 1024 * 1024;

    /** */
    private static final TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        cfg.setIncludeEventTypes(EventType.EVTS_DISCOVERY);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        cfg.setDiscoverySpi(spi.setIpFinder(ipFinder));

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setClientMode(igniteInstanceName.startsWith("client"));

        cfg.setMetricsUpdateFrequency(600_000);
        cfg.setClientFailureDetectionTimeout(600_000);
        cfg.setFailureDetectionTimeout(600_000);

//        cfg.setDataStorageConfiguration(new DataStorageConfiguration().
//            setWalMode(LOG_ONLY).setPageSize(1024).setWalSegmentSize(16 * MB).
//            setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true).setMaxSize(50 * MB)));

        QueryEntity qryEntity = new QueryEntity("java.lang.Integer", "Value");

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        Collection<QueryIndex> indexes = new ArrayList<QueryIndex>(FIELDS);

        for (int i = 0; i < FIELDS; i++) {
            String name = "s" + i;

            fields.put(name, "java.lang.String");

            indexes.add(new QueryIndex(name, QueryIndexType.SORTED));
        }

        qryEntity.setFields(fields);

        qryEntity.setIndexes(indexes);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(
            new DataRegionConfiguration().setMaxSize(50 * 1024 * 1024)));

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME).
            setBackups(2).
            setQueryEntities(Collections.singleton(qryEntity)).
            setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL).
            setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC).
            setCacheMode(CacheMode.PARTITIONED));

        return cfg;
    }

    public void testConcurrentUpdate() throws Exception {
        try {
            Ignite crd = startGridsMultiThreaded(GRIDS);

            AtomicInteger cnt = new AtomicInteger();

            multithreaded(new Runnable() {
                @Override public void run() {
                    try {
                        startGrid("client" + cnt.getAndIncrement());
                    }
                    catch (Exception e) {
                        log.error("Cannot start client", e);

                        fail(e.getMessage());
                    }
                }
            }, CLIENTS);

            int iters = 5000;

            long seed = System.nanoTime();

            log.info("Seed: " + seed);

            Random r = new Random(seed);

            for (int i = 0; i < iters; i++) {
                cnt.set(0);

                AtomicInteger keyRef = new AtomicInteger();
                AtomicInteger clientRef = new AtomicInteger();
                AtomicReference<int[]> fieldsRef = new AtomicReference<>();

                CyclicBarrier b = new CyclicBarrier(CLIENTS, new Runnable() {
                    @Override public void run() {
                        keyRef.set(r.nextInt(100));

                        clientRef.set(r.nextInt(CLIENTS));

                        fieldsRef.set(r.ints(r.nextInt(FIELDS - 1) + 1, 0, FIELDS).toArray());
                    }
                });

                multithreaded(new Runnable() {
                    @Override public void run() {
                        try {
                            b.await();
                        }
                        catch (Exception e) {
                            throw new RuntimeException(e);
                        }

                        IgniteEx client = grid("client" + r.nextInt(CLIENTS));

                        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                            int key = keyRef.get();

                            BinaryObject obj = build(client, "val" + r.nextInt(100000), fieldsRef.get());

                            client.cache(DEFAULT_CACHE_NAME).put(key, obj);

                            tx.commit();
                        }
                    }
                }, CLIENTS);
            }


        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param ignite Ignite.
     * @param prefix Prefix.
     * @param fields Fields.
     */
    protected BinaryObject build(Ignite ignite, String prefix, int... fields) {
        BinaryObjectBuilder builder = ignite.binary().builder("Value");

        for (int i = 0; i < fields.length; i++) {
            int field = fields[i];

            builder.setField("i" + field, field);
            builder.setField("s" + field, prefix + field);
        }

        return builder.build();
    }

    private BinaryTypeImpl localMetadata(IgniteEx ignite, int typeId) {
        BinaryMarshaller marshaller = (BinaryMarshaller)ignite.context().cache().context().marshaller();

        GridBinaryMarshaller impl = U.field(marshaller, "impl");

        return (BinaryTypeImpl)impl.context().metadata(typeId);
    }

    /**
     * Discovery SPI which can simulate network split.
     */
    protected class CustomTcpDiscoverySpi extends TcpDiscoverySpi {
        /** Block predicate. */
        private volatile IgniteBiClosure<ClusterNode, DiscoveryCustomMessage, Void> clo;

        /**
         * @param clo Closure.
         */
        public void setClosure(IgniteBiClosure<ClusterNode, DiscoveryCustomMessage, Void> clo) {
            this.clo = clo;
        }

        /**
         * @param addr Address.
         * @param msg Message.
         */
        private synchronized void apply(ClusterNode addr, TcpDiscoveryAbstractMessage msg) {
            if (!(msg instanceof TcpDiscoveryCustomEventMessage))
                return;

            TcpDiscoveryCustomEventMessage cm = (TcpDiscoveryCustomEventMessage)msg;

            DiscoveryCustomMessage delegate;

            try {
                DiscoverySpiCustomMessage custMsg = cm.message(marshaller(), U.resolveClassLoader(ignite().configuration()));

                delegate = ((CustomMessageWrapper)custMsg).delegate();

            }
            catch (Throwable throwable) {
                throw new RuntimeException(throwable);
            }

            if (clo != null)
                clo.apply(addr, delegate);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(
            Socket sock,
            TcpDiscoveryAbstractMessage msg,
            byte[] data,
            long timeout
        ) throws IOException {
            if (spiCtx != null)
                apply(spiCtx.localNode(), msg);

            super.writeToSocket(sock, msg, data, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock,
            OutputStream out,
            TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            if (spiCtx != null)
                apply(spiCtx.localNode(), msg);

            //doSleep(500);

            super.writeToSocket(sock, out, msg, timeout);
        }
    }
}

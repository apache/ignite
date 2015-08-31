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

package org.apache.ignite.stream.socket;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.stream.StreamTupleExtractor;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;

/**
 * Tests {@link SocketStreamer}.
 */
public class SocketStreamerSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Grid count. */
    private final static int GRID_CNT = 3;

    /** Count. */
    private static final int CNT = 500;

    /** Delimiter. */
    private static final byte[] DELIM = new byte[] {0, 1, 2, 3, 4, 5, 4, 3, 2, 1, 0};

    /** Port. */
    private static int port;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg = defaultCacheConfiguration();

        cfg.setCacheConfiguration(ccfg);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }


    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(GRID_CNT);

        try (ServerSocket sock = new ServerSocket(0)) {
            port = sock.getLocalPort();
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testSizeBasedDefaultConverter() throws Exception {
        test(null, null, new Runnable() {
            @Override public void run() {
                try (Socket sock = new Socket(InetAddress.getLocalHost(), port);
                     OutputStream os = new BufferedOutputStream(sock.getOutputStream())) {
                    Marshaller marsh = new JdkMarshaller();

                    for (int i = 0; i < CNT; i++) {
                        byte[] msg = marsh.marshal(new Tuple(i));

                        os.write(msg.length >>> 24);
                        os.write(msg.length >>> 16);
                        os.write(msg.length >>> 8);
                        os.write(msg.length);

                        os.write(msg);
                    }
                }
                catch (IOException | IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testSizeBasedCustomConverter() throws Exception {
        SocketMessageConverter<Tuple> converter = new SocketMessageConverter<Tuple>() {
            @Override public Tuple convert(byte[] msg) {
                int i = (msg[0] & 0xFF) << 24;
                i |= (msg[1] & 0xFF) << 16;
                i |= (msg[2] & 0xFF) << 8;
                i |= msg[3] & 0xFF;

                return new Tuple(i);
            }
        };

        test(converter, null, new Runnable() {
            @Override public void run() {
                try(Socket sock = new Socket(InetAddress.getLocalHost(), port);
                    OutputStream os = new BufferedOutputStream(sock.getOutputStream())) {

                    for (int i = 0; i < CNT; i++) {
                        os.write(0);
                        os.write(0);
                        os.write(0);
                        os.write(4);

                        os.write(i >>> 24);
                        os.write(i >>> 16);
                        os.write(i >>> 8);
                        os.write(i);
                    }
                }
                catch (IOException e) {
                    throw new IgniteException(e);
                }
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testDelimiterBasedDefaultConverter() throws Exception {
        test(null, DELIM, new Runnable() {
            @Override public void run() {
                try(Socket sock = new Socket(InetAddress.getLocalHost(), port);
                    OutputStream os = new BufferedOutputStream(sock.getOutputStream())) {
                    Marshaller marsh = new JdkMarshaller();

                    for (int i = 0; i < CNT; i++) {
                        byte[] msg = marsh.marshal(new Tuple(i));

                        os.write(msg);
                        os.write(DELIM);
                    }
                }
                catch (IOException | IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }
        });

    }

    /**
     * @throws Exception If failed.
     */
    public void testDelimiterBasedCustomConverter() throws Exception {
        SocketMessageConverter<Tuple> converter = new SocketMessageConverter<Tuple>() {
            @Override public Tuple convert(byte[] msg) {
                int i = (msg[0] & 0xFF) << 24;
                i |= (msg[1] & 0xFF) << 16;
                i |= (msg[2] & 0xFF) << 8;
                i |= msg[3] & 0xFF;

                return new Tuple(i);
            }
        };

        test(converter, DELIM, new Runnable() {
            @Override public void run() {
                try(Socket sock = new Socket(InetAddress.getLocalHost(), port);
                    OutputStream os = new BufferedOutputStream(sock.getOutputStream())) {

                    for (int i = 0; i < CNT; i++) {
                        os.write(i >>> 24);
                        os.write(i >>> 16);
                        os.write(i >>> 8);
                        os.write(i);

                        os.write(DELIM);
                    }
                }
                catch (IOException e) {
                    throw new IgniteException(e);
                }
            }
        });
    }

    /**
     * @param converter Converter.
     * @param r Runnable..
     */
    private void test(@Nullable SocketMessageConverter<Tuple> converter, @Nullable byte[] delim, Runnable r) throws Exception
    {
        SocketStreamer<Tuple, Integer, String> sockStmr = null;

        Ignite ignite = grid(0);

        IgniteCache<Integer, String> cache = ignite.cache(null);

        cache.clear();

        try (IgniteDataStreamer<Integer, String> stmr = ignite.dataStreamer(null)) {

            stmr.allowOverwrite(true);
            stmr.autoFlushFrequency(10);

            sockStmr = new SocketStreamer<>();

            sockStmr.setIgnite(ignite);

            sockStmr.setStreamer(stmr);

            sockStmr.setPort(port);

            sockStmr.setDelimiter(delim);

            sockStmr.setTupleExtractor(new StreamTupleExtractor<Tuple, Integer, String>() {
                @Override public Map.Entry<Integer, String> extract(Tuple msg) {
                    return new IgniteBiTuple<>(msg.key, msg.val);
                }
            });

            if (converter != null)
                sockStmr.setConverter(converter);

            final CountDownLatch latch = new CountDownLatch(CNT);

            IgniteBiPredicate<UUID, CacheEvent> locLsnr = new IgniteBiPredicate<UUID, CacheEvent>() {
                @Override public boolean apply(UUID uuid, CacheEvent evt) {
                    latch.countDown();

                    return true;
                }
            };

            ignite.events(ignite.cluster().forCacheNodes(null)).remoteListen(locLsnr, null, EVT_CACHE_OBJECT_PUT);

            sockStmr.start();

            r.run();

            latch.await();

            for (int i = 0; i < CNT; i++)
                assertEquals(Integer.toString(i), cache.get(i));

            assertEquals(CNT, cache.size(CachePeekMode.PRIMARY));
        }
        finally {
            if (sockStmr != null)
                sockStmr.stop();
        }

    }

    /**
     * Tuple.
     */
    private static class Tuple implements Serializable {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Key. */
        private final int key;

        /** Value. */
        private final String val;

        /**
         * @param key Key.
         */
        Tuple(int key) {
            this.key = key;
            this.val = Integer.toString(key);
        }
    }
}
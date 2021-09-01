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
import java.util.HashMap;
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
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.stream.StreamMultipleTupleExtractor;
import org.apache.ignite.stream.StreamSingleTupleExtractor;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;

/**
 * Tests {@link SocketStreamer}.
 */
public class SocketStreamerSelfTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRID_CNT = 3;

    /** Count. */
    private static final int CNT = 500;

    /** Delimiter. */
    private static final byte[] DELIM = new byte[] {0, 1, 2, 3, 4, 5, 4, 3, 2, 1, 0};

    /** Port. */
    private static int port;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setIncludeEventTypes(EventType.EVTS_ALL);

        CacheConfiguration ccfg = defaultCacheConfiguration();

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }


    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(GRID_CNT);

        try (ServerSocket sock = new ServerSocket(0)) {
            port = sock.getLocalPort();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSizeBasedDefaultConverter() throws Exception {
        test(null, null, new Runnable() {
            @Override public void run() {
                try (Socket sock = new Socket(InetAddress.getLocalHost(), port);
                     OutputStream os = new BufferedOutputStream(sock.getOutputStream())) {
                    Marshaller marsh = new JdkMarshaller();

                    for (int i = 0; i < CNT; i++) {
                        byte[] msg = marsh.marshal(new Message(i));

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
        }, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleEntriesFromOneMessage() throws Exception {
        test(null, null, new Runnable() {
            @Override public void run() {
                try (Socket sock = new Socket(InetAddress.getLocalHost(), port);
                     OutputStream os = new BufferedOutputStream(sock.getOutputStream())) {
                    Marshaller marsh = new JdkMarshaller();

                    int[] values = new int[CNT];
                    for (int i = 0; i < CNT; i++)
                        values[i] = i;

                    byte[] msg = marsh.marshal(new Message(values));

                    os.write(msg.length >>> 24);
                    os.write(msg.length >>> 16);
                    os.write(msg.length >>> 8);
                    os.write(msg.length);

                    os.write(msg);
                }
                catch (IOException | IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }
        }, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSizeBasedCustomConverter() throws Exception {
        SocketMessageConverter<Message> converter = new SocketMessageConverter<Message>() {
            @Override public Message convert(byte[] msg) {
                int i = (msg[0] & 0xFF) << 24;
                i |= (msg[1] & 0xFF) << 16;
                i |= (msg[2] & 0xFF) << 8;
                i |= msg[3] & 0xFF;

                return new Message(i);
            }
        };

        test(converter, null, new Runnable() {
            @Override public void run() {
                try (Socket sock = new Socket(InetAddress.getLocalHost(), port);
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
        }, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDelimiterBasedDefaultConverter() throws Exception {
        test(null, DELIM, new Runnable() {
            @Override public void run() {
                try (Socket sock = new Socket(InetAddress.getLocalHost(), port);
                    OutputStream os = new BufferedOutputStream(sock.getOutputStream())) {
                    Marshaller marsh = new JdkMarshaller();

                    for (int i = 0; i < CNT; i++) {
                        byte[] msg = marsh.marshal(new Message(i));

                        os.write(msg);
                        os.write(DELIM);
                    }
                }
                catch (IOException | IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }
        }, true);

    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDelimiterBasedCustomConverter() throws Exception {
        SocketMessageConverter<Message> converter = new SocketMessageConverter<Message>() {
            @Override public Message convert(byte[] msg) {
                int i = (msg[0] & 0xFF) << 24;
                i |= (msg[1] & 0xFF) << 16;
                i |= (msg[2] & 0xFF) << 8;
                i |= msg[3] & 0xFF;

                return new Message(i);
            }
        };

        test(converter, DELIM, new Runnable() {
            @Override public void run() {
                try (Socket sock = new Socket(InetAddress.getLocalHost(), port);
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
        }, true);
    }

    /**
     * @param converter Converter.
     * @param r Runnable..
     */
    private void test(@Nullable SocketMessageConverter<Message> converter,
        @Nullable byte[] delim,
        Runnable r,
        boolean oneMessagePerTuple) throws Exception {
        SocketStreamer<Message, Integer, String> sockStmr = null;

        Ignite ignite = grid(0);

        IgniteCache<Integer, String> cache = ignite.cache(DEFAULT_CACHE_NAME);

        cache.clear();

        try (IgniteDataStreamer<Integer, String> stmr = ignite.dataStreamer(DEFAULT_CACHE_NAME)) {
            stmr.allowOverwrite(true);
            stmr.autoFlushFrequency(10);

            sockStmr = new SocketStreamer<>();

            sockStmr.setIgnite(ignite);

            sockStmr.setStreamer(stmr);

            sockStmr.setPort(port);

            sockStmr.setDelimiter(delim);

            if (oneMessagePerTuple) {
                sockStmr.setSingleTupleExtractor(new StreamSingleTupleExtractor<Message, Integer, String>() {
                    @Override public Map.Entry<Integer, String> extract(Message msg) {
                        return new IgniteBiTuple<>(msg.key, msg.val);
                    }
                });
            }
            else {
                sockStmr.setMultipleTupleExtractor(new StreamMultipleTupleExtractor<Message, Integer, String>() {
                    @Override public Map<Integer, String> extract(Message msg) {
                        Map<Integer, String> answer = new HashMap<>();
                        for (int value : msg.values) {
                            answer.put(value, Integer.toString(value));
                        }
                        return answer;
                    }
                });
            }

            if (converter != null)
                sockStmr.setConverter(converter);

            final CountDownLatch latch = new CountDownLatch(CNT);

            final GridConcurrentHashSet<CacheEvent> evts = new GridConcurrentHashSet<>();

            IgniteBiPredicate<UUID, CacheEvent> locLsnr = new IgniteBiPredicate<UUID, CacheEvent>() {
                @Override public boolean apply(UUID uuid, CacheEvent evt) {
                    evts.add(evt);

                    latch.countDown();

                    return true;
                }
            };

            ignite.events(ignite.cluster().forCacheNodes(DEFAULT_CACHE_NAME)).remoteListen(locLsnr, null, EVT_CACHE_OBJECT_PUT);

            sockStmr.start();

            r.run();

            latch.await();

            for (int i = 0; i < CNT; i++) {
                Object val = cache.get(i);
                String exp = Integer.toString(i);

                if (!exp.equals(val))
                    log.error("Unexpected cache value [key=" + i +
                        ", exp=" + exp +
                        ", val=" + val +
                        ", evts=" + evts + ']');

                assertEquals(exp, val);
            }

            assertEquals(CNT, cache.size(CachePeekMode.PRIMARY));
        }
        finally {
            if (sockStmr != null)
                sockStmr.stop();
        }

    }

    /**
     * Message.
     */
    private static class Message implements Serializable {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Key. */
        private final int key;

        /** Value. */
        private final String val;

        /** Multiple values. */
        private final int[] values;

        /**
         * @param key Key.
         */
        Message(int key) {
            this.key = key;
            this.val = Integer.toString(key);
            this.values = new int[0];
        }

        /**
         * @param values Multiple values.
         */
        Message(int[] values) {
            this.key = -1;
            this.val = null;
            this.values = values;
        }
    }
}

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

package org.apache.ignite.examples.streaming.wordcount.socket;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.util.Map;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.AffinityUuid;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.examples.ExamplesUtils;
import org.apache.ignite.examples.streaming.wordcount.CacheConfig;
import org.apache.ignite.examples.streaming.wordcount.QueryWords;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.stream.StreamSingleTupleExtractor;
import org.apache.ignite.stream.socket.SocketMessageConverter;
import org.apache.ignite.stream.socket.SocketStreamer;

/**
 * Example demonstrates streaming of data from external components into Ignite cache.
 * <p>
 * {@code WordsSocketStreamerServer} is simple socket streaming server implementation that
 * receives words from socket using {@link SocketStreamer} and message delimiter based protocol
 * and streams them into Ignite cache. Example illustrates usage of TCP socket streamer in case of non-Java clients.
 * In this example words are zero-terminated strings.
 * <p>
 * To start the example, you should:
 * <ul>
 *     <li>Start a few nodes using {@link ExampleNodeStartup}.</li>
 *     <li>Start socket server using {@link WordsSocketStreamerServer}.</li>
 *     <li>Start a few socket clients using {@link WordsSocketStreamerClient}.</li>
 *     <li>Start querying popular words using {@link QueryWords}.</li>
 * </ul>
 */
public class WordsSocketStreamerServer {
    /** Port. */
    private static final int PORT = 5555;

    /** Delimiter. */
    private static final byte[] DELIM = new byte[] {0};

    /**
     * Starts socket streaming server.
     *
     * @param args Command line arguments (none required).
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        // Mark this cluster member as client.
        Ignition.setClientMode(true);

        CacheConfiguration<AffinityUuid, String> cfg = CacheConfig.wordCache();

        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {

            if (!ExamplesUtils.hasServerNodes(ignite))
                return;

            // The cache is configured with sliding window holding 1 second of the streaming data.
            try (IgniteCache<AffinityUuid, String> stmCache = ignite.getOrCreateCache(cfg)) {

                IgniteDataStreamer<AffinityUuid, String> stmr = ignite.dataStreamer(stmCache.getName());

                InetAddress addr = InetAddress.getLocalHost();

                // Configure socket streamer
                SocketStreamer<String, AffinityUuid, String> sockStmr = new SocketStreamer<>();

                sockStmr.setAddr(addr);

                sockStmr.setPort(PORT);

                sockStmr.setDelimiter(DELIM);

                sockStmr.setIgnite(ignite);

                sockStmr.setStreamer(stmr);

                // Converter from zero-terminated string to Java strings.
                sockStmr.setConverter(new SocketMessageConverter<String>() {
                    @Override public String convert(byte[] msg) {
                        try {
                            return new String(msg, "ASCII");
                        }
                        catch (UnsupportedEncodingException e) {
                            throw new IgniteException(e);
                        }
                    }
                });

                sockStmr.setSingleTupleExtractor(new StreamSingleTupleExtractor<String, AffinityUuid, String>() {
                    @Override public Map.Entry<AffinityUuid, String> extract(String word) {
                        // By using AffinityUuid we ensure that identical
                        // words are processed on the same cluster node.
                        return new IgniteBiTuple<>(new AffinityUuid(word), word);
                    }
                });

                sockStmr.start();
            }
            catch (IgniteException e) {
                System.err.println("Streaming server didn't start due to an error: ");

                e.printStackTrace();
            }
            finally {
                // Distributed cache could be removed from cluster only by #destroyCache() call.
                ignite.destroyCache(cfg.getName());
            }
        }
    }
}
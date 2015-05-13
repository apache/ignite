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

package org.apache.ignite.examples.streaming.socket;

import org.apache.ignite.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.examples.*;
import org.apache.ignite.examples.streaming.wordcount.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.stream.*;
import org.apache.ignite.stream.socket.*;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * Receives words through socket using {@link SocketStreamer} and message delimiter based protocol
 * and streams them into Ignite cache. Example illustrates usage of TCP socket streamer in case of non-Java clients.
 * In this example words are zero-terminated strings.
 * <p>
 * To start the example, you should:
 * <ul>
 *     <li>Start a few nodes using {@link ExampleNodeStartup} or by starting remote nodes as specified below.</li>
 *     <li>Start socket server using {@link ZWordsSocketStreamerServer}.</li>
 *     <li>Start a few socket clients using {@link ZWordsSocketStreamerClient}.</li>
 *     <li>Start querying popular words using {@link QueryWords}.</li>
 * </ul>
 * <p>
 * You should start remote nodes by running {@link ExampleNodeStartup} in another JVM.
 */
public class ZWordsSocketStreamerServer {
    /** Port. */
    private static final int PORT = 5555;

    /** Delimiter. */
    private static final byte[] DELIM = new byte[] {0};

    /**
     * @param args Args.
     */
    public static void main(String[] args) throws InterruptedException, IOException {
        // Mark this cluster member as client.
        Ignition.setClientMode(true);

        Ignite ignite = Ignition.start("examples/config/example-ignite.xml");

        if (!ExamplesUtils.hasServerNodes(ignite)) {
            ignite.close();

            return;
        }

        // The cache is configured with sliding window holding 1 second of the streaming data.
        IgniteCache<AffinityUuid, String> stmCache = ignite.getOrCreateCache(CacheConfig.wordCache());

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

        sockStmr.setTupleExtractor(new StreamTupleExtractor<String, AffinityUuid, String>() {
            @Override public Map.Entry<AffinityUuid, String> extract(String word) {
                // By using AffinityUuid we ensure that identical
                // words are processed on the same cluster node.
                return new IgniteBiTuple<>(new AffinityUuid(word), word);
            }
        });

        sockStmr.start();
    }
}

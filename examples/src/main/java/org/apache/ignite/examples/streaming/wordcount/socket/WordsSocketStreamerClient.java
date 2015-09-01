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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.examples.streaming.wordcount.QueryWords;
import org.apache.ignite.stream.socket.SocketStreamer;

/**
 * Example demonstrates streaming of data from external components into Ignite cache.
 * <p>
 * {@code WordsSocketStreamerClient} is simple socket streaming client implementation that sends words to socket server
 * based on {@link SocketStreamer} using message delimiter based protocol. Example illustrates usage of TCP socket
 * streamer in case of non-Java clients. In this example words are zero-terminated strings.
 * <p>
 * To start the example, you should:
 * <ul>
 *     <li>Start a few nodes using {@link ExampleNodeStartup}.</li>
 *     <li>Start socket server using {@link WordsSocketStreamerServer}.</li>
 *     <li>Start a few socket clients using {@link WordsSocketStreamerClient}.</li>
 *     <li>Start querying popular words using {@link QueryWords}.</li>
 * </ul>
 */
public class WordsSocketStreamerClient {
    /** Port. */
    private static final int PORT = 5555;

    /** Delimiter. */
    private static final byte[] DELIM = new byte[] {0};

    /**
     * @param args Args.
     */
    public static void main(String[] args) throws IOException {
        InetAddress addr = InetAddress.getLocalHost();

        try (
            Socket sock = new Socket(addr, PORT);
            OutputStream oos = new BufferedOutputStream(sock.getOutputStream())
        ) {
            System.out.println("Words streaming started.");

            while (true) {
                try (InputStream in = WordsSocketStreamerClient.class.getResourceAsStream("../alice-in-wonderland.txt");
                     LineNumberReader rdr = new LineNumberReader(new InputStreamReader(in))) {
                    for (String line = rdr.readLine(); line != null; line = rdr.readLine()) {
                        for (String word : line.split(" ")) {
                            if (!word.isEmpty()) {
                                // Stream words into Ignite through socket.
                                byte[] arr = word.getBytes("ASCII");

                                // Write message
                                oos.write(arr);

                                // Write message delimiter
                                oos.write(DELIM);
                            }
                        }
                    }
                }
            }
        }
    }
}
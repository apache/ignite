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

import org.apache.ignite.examples.*;
import org.apache.ignite.examples.streaming.wordcount.*;
import org.apache.ignite.stream.socket.*;

import java.io.*;
import java.net.*;

/**
 * Sends words to socket server based on {@link SocketStreamer} using message size based protocol.
 * <p>
 * To start the example, you should:
 * <ul>
 *     <li>Start a few nodes using {@link ExampleNodeStartup} or by starting remote nodes as specified below.</li>
 *     <li>Start socket server using {@link WordsSocketStreamerServer}.</li>
 *     <li>Start a few socket clients using {@link WordsSocketStreamerClient}.</li>
 *     <li>Start querying popular words using {@link QueryWords}.</li>
 * </ul>
 * <p>
 * You should start remote nodes by running {@link ExampleNodeStartup} in another JVM.
 */
public class WordsSocketStreamerClient {
    /** Port. */
    private static final int PORT = 5555;

    /**
     * @param args Args.
     */
    public static void main(String[] args) throws IOException {
        InetAddress addr = InetAddress.getLocalHost();

        try (Socket sock = new Socket(addr, PORT);
             OutputStream oos = new BufferedOutputStream(sock.getOutputStream())) {

            System.out.println("Words streaming started.");

            while (true) {
                try (InputStream in = StreamWords.class.getResourceAsStream("../wordcount/alice-in-wonderland.txt");
                     LineNumberReader rdr = new LineNumberReader(new InputStreamReader(in))) {
                    for (String line = rdr.readLine(); line != null; line = rdr.readLine()) {
                        for (String word : line.split(" ")) {
                            if (!word.isEmpty()) {
                                // Stream words into Ignite through socket.
                                try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                                     ObjectOutputStream out = new ObjectOutputStream(bos)) {

                                    // Write message
                                    out.writeObject(word);

                                    byte[] arr = bos.toByteArray();

                                    // Write message length
                                    oos.write(arr.length >>> 24);
                                    oos.write(arr.length >>> 16);
                                    oos.write(arr.length >>> 8);
                                    oos.write(arr.length);

                                    oos.write(arr);
                                }
                            }
                        }
                    }
                }
            }
        }

    }
}

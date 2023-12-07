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

package org.apache.ignite.internal.util.nio;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

/**
 * Unit tests for {@link GridNioServer}.
 */
@RunWith(MockitoJUnitRunner.class)
public class GridNioServerTest {
    /***/
    private final List<String> logMessages = new CopyOnWriteArrayList<>();

    /***/
    private static final int PORT = 5555;

    /***/
    @Mock
    private GridNioServerListener<Object> noOpListener;

    /***/
    @Test
    public void shouldNotLogWarningsOnKeyClose() throws Exception {
        GridNioServer<Object> server = startServerCollectingLogMessages();

        try (Socket ignored = openSocketTo(server)) {
            server.stop();
        }

        assertThat(logMessages, not(hasItem(containsString("Failed to shutdown socket"))));
        assertThat(logMessages, not(hasItem(containsString("ClosedChannelException"))));
    }

    /***/
    private GridNioServer<Object> startServerCollectingLogMessages() throws IgniteCheckedException,
        UnknownHostException {
        GridNioServer<Object> server = GridNioServer.builder()
            .address(InetAddress.getLocalHost())
            .port(PORT)
            .selectorCount(1)
            .listener(noOpListener)
            .logger(logMessagesCollector())
            .build();

        server.start();

        return server;
    }

    /***/
    private ListeningTestLogger logMessagesCollector() {
        ListeningTestLogger log = new ListeningTestLogger();

        log.registerListener(logMessages::add);

        return log;
    }

    /***/
    private Socket openSocketTo(GridNioServer<Object> server) throws IOException {
        return new Socket(server.localAddress().getAddress(), server.port());
    }
}

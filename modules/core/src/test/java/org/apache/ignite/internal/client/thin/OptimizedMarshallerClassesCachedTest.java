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

package org.apache.ignite.internal.client.thin;

import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class OptimizedMarshallerClassesCachedTest extends GridCommonAbstractTest {
    /** */
    private final AtomicInteger cnt = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName);
    }

    /** Test check that meta for classes serialized by {@link OptimizedMarshaller} are cached on client. */
    @Test
    public void testLocalDateTimeMetaCached() throws Exception {
        try (Ignite srv = startGrid(0)) {
            srv.getOrCreateCache(Config.DEFAULT_CACHE_NAME).put(1, LocalDateTime.now());

            IgniteClient cli = new TcpIgniteClient((cfg0, hnd) -> new TcpClientChannel(cfg0, hnd) {
                @Override public <T> T service(
                    ClientOperation op,
                    Consumer<PayloadOutputChannel> payloadWriter,
                    Function<PayloadInputChannel, T> payloadReader
                ) throws ClientException {
                    if (op == ClientOperation.GET_BINARY_TYPE_NAME)
                        cnt.incrementAndGet();

                    return super.service(op, payloadWriter, payloadReader);
                }

                @Override public <T> CompletableFuture<T> serviceAsync(
                    ClientOperation op,
                    Consumer<PayloadOutputChannel> payloadWriter,
                    Function<PayloadInputChannel, T> payloadReader
                ) {
                    if (op == ClientOperation.GET_BINARY_TYPE_NAME)
                        cnt.incrementAndGet();

                    return super.serviceAsync(op, payloadWriter, payloadReader);
                }
            }, new ClientConfiguration().setAddresses(Config.SERVER));

            try {
                cli.cache(Config.DEFAULT_CACHE_NAME).get(1);
                cli.cache(Config.DEFAULT_CACHE_NAME).get(1);
            }
            finally {
                cli.close();
            }

            assertEquals(1, cnt.get());
        }
    }
}

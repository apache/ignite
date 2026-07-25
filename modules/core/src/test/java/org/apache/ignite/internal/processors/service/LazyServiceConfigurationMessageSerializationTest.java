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

package org.apache.ignite.internal.processors.service;

import java.io.Externalizable;
import java.io.Serializable;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.ignite.internal.CoreMessagesProvider;
import org.apache.ignite.internal.direct.DirectMessageReader;
import org.apache.ignite.internal.direct.DirectMessageWriter;
import org.apache.ignite.internal.managers.communication.IgniteMessageFactoryImpl;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.util.CommonUtils.gridClassLoader;
import static org.apache.ignite.internal.util.CommonUtils.makeMessageType;
import static org.apache.ignite.marshaller.Marshallers.jdk;
import static org.junit.Assert.assertArrayEquals;

/** Test for serialization round-trip of {@link LazyServiceConfigurationMessage}. */
public class LazyServiceConfigurationMessageSerializationTest extends GridCommonAbstractTest {
    /** Error suffix. */
    public static final String ERROR_SUFFIX = " count is not equal to the expected fields count. " +
        "Has the number of fields in the `LazyServiceConfiguration` class changed?";

    /** */
    private final MessageFactory msgFactory = new IgniteMessageFactoryImpl(
        new MessageFactoryProvider[] {new CoreMessagesProvider(jdk(), jdk(), gridClassLoader())});

    /**
     * ServiceConfiguration declares {@code svc}, {@code nodeFilter}, {@code interceptors} as non-transient,
     * so serializableFieldsCount includes them. Yet they are not serialized via MessageSerializer —
     * LazyServiceConfiguration replaces them with byte[] counterparts (srvcBytes, nodeFilterBytes, interceptorsBytes).
     */
    private static final long SHADOWED_FIELD_COUNT = 3;

    /** */
    @Test
    public void testLazyServiceConfiguration() {
        LazyServiceConfiguration cfg = lazyServiceConfiguration();

        assertEquals(cfg, serializeAndDeserialize(cfg));
    }

    /** */
    @Test
    public void testLazyServiceConfigurationWithBytes() {
        LazyServiceConfiguration cfg = lazyServiceConfigurationWithBytes();

        LazyServiceConfiguration res = serializeAndDeserialize(cfg);

        assertEquals(cfg, res);

        // Explicitly verify byte[] and array fields survived round-trip
        // (equalsIgnoreNodeFilter doesn't cover platformMtdNames, srvcClsName).
        assertArrayEquals(cfg.serviceBytes(), res.serviceBytes());
        assertArrayEquals(cfg.nodeFilterBytes(), res.nodeFilterBytes());
        assertArrayEquals(cfg.interceptorBytes(), res.interceptorBytes());
        assertArrayEquals(cfg.platformMtdNames(), res.platformMtdNames());
        assertEquals(cfg.serviceClassName(), res.serviceClassName());
    }

    /**
     * @param src Source configuration.
     *
     * @return Configuration read during a full serde round-trip.
     */
    private LazyServiceConfiguration serializeAndDeserialize(LazyServiceConfiguration src) {
        long expReadsWritesCnt = serializableFieldsCount(LazyServiceConfiguration.class) - SHADOWED_FIELD_COUNT;

        return writeAndReadBack(new LazyServiceConfigurationMessage(src), expReadsWritesCnt).toConfiguration();
    }

    /** @param cls Class of an object. */
    private long serializableFieldsCount(Class<?> cls) {
        if (cls == Object.class)
            return 0;

        assertTrue("Not a serializable class: " + cls, Serializable.class.isAssignableFrom(cls));
        assertFalse("Should not be Externalizable:" + cls, Externalizable.class.isAssignableFrom(cls));

        return serializableFieldsCount(cls.getSuperclass()) + Arrays.stream(cls.getDeclaredFields())
            .filter(f -> !Modifier.isStatic(f.getModifiers()) && !Modifier.isTransient(f.getModifiers()))
            .count();
    }

    /**
     * @param msg Message to write and read back through {@link DirectMessageWriter}/{@link DirectMessageReader}.
     * @param expReadsWritesCnt Expected count of field reads and writes.
     * @param <T> Type of Message.
     *
     * @return Restored message.
     */
    private <T extends Message> T writeAndReadBack(T msg, long expReadsWritesCnt) {
        ByteBuffer buf = ByteBuffer.allocate(64 * 1024);

        MessageSerializer<T> serde = (MessageSerializer<T>)msgFactory.serializer(msg.directType());

        DirectMessageWriter writer = new DirectMessageWriter(msgFactory);
        writer.setBuffer(buf);

        assertTrue(serde.writeTo(msg, writer));
        assertEquals("Writes" + ERROR_SUFFIX, expReadsWritesCnt, writer.state());

        buf.flip();

        DirectMessageReader reader = new DirectMessageReader(msgFactory, null);
        reader.setBuffer(buf);

        T res = (T)msgFactory.create(makeMessageType(buf.get(), buf.get()));

        assertTrue(serde.readFrom(res, reader));
        assertEquals("Reads" + ERROR_SUFFIX, expReadsWritesCnt, reader.state());

        return res;
    }

    /** @return Lazy service configuration with scalar fields only (null byte arrays). */
    private LazyServiceConfiguration lazyServiceConfiguration() {
        return (LazyServiceConfiguration)new LazyServiceConfiguration()
            .setName("testService")
            .setTotalCount(5)
            .setMaxPerNodeCount(1)
            .setCacheName("testCache")
            .setAffinityKey("affKey")
            .setStatisticsEnabled(true)
            .setLocalStartOrder(10);
    }

    /** @return Lazy service configuration with all fields populated, including serialized byte arrays. */
    private LazyServiceConfiguration lazyServiceConfigurationWithBytes() {
        LazyServiceConfiguration cfg = (LazyServiceConfiguration)new LazyServiceConfiguration()
            .setName("testService")
            .setTotalCount(5)
            .setMaxPerNodeCount(1)
            .setCacheName("testCache")
            .setAffinityKey("affKey")
            .setStatisticsEnabled(true)
            .setLocalStartOrder(10);

        return cfg.serviceClassName("org.apache.ignite.TestService")
            .serviceBytes("service_bytes".getBytes())
            .nodeFilterBytes("nodeFilter".getBytes())
            .interceptorBytes("interceptors".getBytes())
            .platformMtdNames(new String[]{"method1", "method2"});
    }
}

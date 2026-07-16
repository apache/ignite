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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.ignite.internal.CoreMessagesProvider;
import org.apache.ignite.internal.direct.DirectMessageReader;
import org.apache.ignite.internal.direct.DirectMessageWriter;
import org.apache.ignite.internal.managers.communication.IgniteMessageFactoryImpl;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.util.CommonUtils.makeMessageType;
import static org.apache.ignite.marshaller.Marshallers.jdk;
import static org.junit.Assert.assertArrayEquals;

/** Test for serialization round-trip of {@link LazyServiceConfigurationMessage}. */
public class LazyServiceConfigurationMessageSerializationTest extends GridCommonAbstractTest {
    /** Error suffix. */
    public static final String ERROR_SUFFIX = " count is not equal to the expected fields count. " +
        "Has the number of fields in the `LazyServiceConfiguration` class changed?";

    /** */
    private final Marshaller marsh = jdk();

    /** */
    private final MessageFactory msgFactory = new IgniteMessageFactoryImpl(
        new MessageFactoryProvider[] {new CoreMessagesProvider(marsh, marsh, U.gridClassLoader())});

    /**
     * LazyServiceConfiguration marks {@code svc}, {@code nodeFilter}, {@code interceptors} as transient,
     * so they are not serialized via MessageSerializer (replaced by byte[] counterparts).
     */
    private static final long TRANSIENT_FIELD_PENALTY = 3;

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
        long expReadsWritesCnt = serializableFieldsCount(LazyServiceConfiguration.class) - TRANSIENT_FIELD_PENALTY;

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
        assertEquals("Writes" + ERROR_SUFFIX,
            expReadsWritesCnt, writer.state());

        buf.flip();

        DirectMessageReader reader = new DirectMessageReader(msgFactory, null);
        reader.setBuffer(buf);

        T res = (T)msgFactory.create(makeMessageType(buf.get(), buf.get()));

        assertTrue(serde.readFrom(res, reader));
        assertEquals("Reads" + ERROR_SUFFIX,
            expReadsWritesCnt, reader.state());

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
        return new LazyServiceConfiguration(
            baseServiceConfiguration(),
            "org.apache.ignite.TestService",
            "service_bytes".getBytes(StandardCharsets.UTF_8),
            "nodeFilter".getBytes(StandardCharsets.UTF_8),
            "interceptors".getBytes(StandardCharsets.UTF_8),
            new String[]{"method1", "method2"}
        );
    }

    /** @return Base service configuration shared by both factory methods. */
    private ServiceConfiguration baseServiceConfiguration() {
        return new ServiceConfiguration()
            .setName("testService")
            .setTotalCount(5)
            .setMaxPerNodeCount(1)
            .setCacheName("testCache")
            .setAffinityKey("affKey")
            .setStatisticsEnabled(true)
            .setLocalStartOrder(10);
    }
}

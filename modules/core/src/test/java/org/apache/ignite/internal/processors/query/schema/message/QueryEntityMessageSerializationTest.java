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

package org.apache.ignite.internal.processors.query.schema.message;

import java.io.Externalizable;
import java.io.Serializable;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.internal.CoreMessagesProvider;
import org.apache.ignite.internal.direct.DirectMessageReader;
import org.apache.ignite.internal.direct.DirectMessageWriter;
import org.apache.ignite.internal.managers.communication.IgniteMessageFactoryImpl;
import org.apache.ignite.internal.processors.query.QueryEntityEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.marshaller.Marshallers.jdk;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.makeMessageType;

/** Test for serialization round-trip of {@link QueryEntityMessage} and {@link QueryEntityExMessage}. */
public class QueryEntityMessageSerializationTest extends GridCommonAbstractTest {
    /** Error suffix. */
    public static final String ERROR_SUFFIX = " count is not equal to the expected fields count. " +
        "Has the number of fields in the `QueryEntity` or `QueryEntityEx` classes changed?";

    /** */
    private final Marshaller marsh = jdk();

    /** */
    private final MessageFactory msgFactory = new IgniteMessageFactoryImpl(
        new MessageFactoryProvider[] {new CoreMessagesProvider(marsh, marsh, U.gridClassLoader())});

    /** */
    @Test
    public void testQueryEntity() throws Exception {
        QueryEntity entity = queryEntity();

        assertEquals(entity, serializeAndDeserialize(entity, serializableFieldsCount(QueryEntity.class)));
    }

    /** */
    @Test
    public void testQueryEntityEx() throws Exception {
        QueryEntityEx entity = queryEntityEx();

        long expReadWriteCnt = serializableFieldsCount(QueryEntityEx.class) -
            /* QueryEntityEx duplicates 'notNullFields', but 'QueryEntityExMessage' doesn't */ 1;

        QueryEntity res = serializeAndDeserialize(entity, expReadWriteCnt);

        assertTrue(res instanceof QueryEntityEx);
        assertEquals(entity, res);

        // Not part of QueryEntityEx.equals(), so assert it explicitly.
        assertEquals(entity.fillAbsentPKsWithDefaults(), ((QueryEntityEx)res).fillAbsentPKsWithDefaults());
    }

    /**
     * @param src Source entity.
     * @param expReadsWritesCnt Expected count of field reads and writes.
     *
     * @return Entity read during a full serde round-trip.
     */
    private QueryEntity serializeAndDeserialize(QueryEntity src, long expReadsWritesCnt) {
        QueryEntityMessage msg = src instanceof QueryEntityEx
            ? new QueryEntityExMessage((QueryEntityEx)src) : new QueryEntityMessage(src);

        return writeAndReadBack(msg, expReadsWritesCnt).toEntity();
    }

    /**
     * @param cls Class of an object.
     */
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

    /** @return Query entity with every field populated, including non-empty default field values. */
    private QueryEntity queryEntity() {
        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("id", Integer.class.getName());
        fields.put("name", String.class.getName());
        fields.put("price", BigDecimal.class.getName());

        return new QueryEntity()
            .setKeyType(Integer.class.getName())
            .setValueType("org.apache.ignite.Person")
            .setKeyFieldName("id")
            .setValueFieldName("name")
            .setTableName("PERSON")
            .setFields(fields)
            .setKeyFields(Set.of("id"))
            .setAliases(Map.of("name", "NAME_ALIAS"))
            .setIndexes(List.of(new QueryIndex("name", QueryIndexType.SORTED).setInlineSize(32)))
            .setNotNullFields(Set.of("id", "name"))
            .setDefaultFieldValues(Map.of(
                "name", "unknown",
                "price", new BigDecimal("9.99"),
                "id", 42))
            .setFieldsPrecision(Map.of("name", 64))
            .setFieldsScale(Map.of("price", 2));
    }

    /** @return Extended query entity with all base and extended fields populated. */
    private QueryEntityEx queryEntityEx() {
        QueryEntity base = queryEntity();
        base.setNotNullFields(null);

        QueryEntityEx entity = new QueryEntityEx(base);

        // Natural QueryEntityEx: notNullFields lives only in the Ex field, base shadow stays null.
        entity.setNotNullFields(Set.of("id", "name"));
        entity.setPreserveKeysOrder(true);
        entity.implicitPk(true);
        entity.fillAbsentPKsWithDefaults(true);
        entity.sql(true);
        entity.setPrimaryKeyInlineSize(16);
        entity.setAffinityKeyInlineSize(24);

        return entity;
    }
}

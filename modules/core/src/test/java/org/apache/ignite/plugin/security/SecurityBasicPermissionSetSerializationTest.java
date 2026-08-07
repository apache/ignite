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

package org.apache.ignite.plugin.security;

import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.CoreMessagesProvider;
import org.apache.ignite.internal.direct.DirectMessageReader;
import org.apache.ignite.internal.direct.DirectMessageWriter;
import org.apache.ignite.internal.managers.communication.IgniteMessageFactoryImpl;
import org.apache.ignite.internal.util.nio.MessageSerialization;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.util.CommonUtils.makeMessageType;
import static org.apache.ignite.marshaller.Marshallers.jdk;
import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_CACHE;
import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_QUERY;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_CREATE;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_PUT;
import static org.apache.ignite.plugin.security.SecurityPermission.SERVICE_CANCEL;
import static org.apache.ignite.plugin.security.SecurityPermission.SERVICE_INVOKE;
import static org.apache.ignite.plugin.security.SecurityPermission.TASK_CANCEL;
import static org.apache.ignite.plugin.security.SecurityPermission.TASK_EXECUTE;

/** Test for serialization round-trip of {@link SecurityBasicPermissionSet}. */
public class SecurityBasicPermissionSetSerializationTest extends GridCommonAbstractTest {
    /** */
    private final MessageFactory<? extends Message> msgFactory = new IgniteMessageFactoryImpl<>(
        new MessageFactoryProvider[] {new CoreMessagesProvider(jdk(), jdk())});

    /** */
    @Test
    public void testWriteAndReadBack() throws Exception {
        SecurityBasicPermissionSet src = new SecurityBasicPermissionSet();

        src.setDefaultAllowAll(true);
        src.setSystemPermissions(EnumSet.of(ADMIN_CACHE, ADMIN_QUERY));
        src.setTaskPermissions(Map.of("task", EnumSet.of(TASK_EXECUTE, TASK_CANCEL)));
        src.setServicePermissions(Map.of("service", EnumSet.of(SERVICE_INVOKE, SERVICE_CANCEL)));
        src.setCachePermissions(Map.of("cache", EnumSet.of(CACHE_CREATE, CACHE_PUT)));

        SecurityBasicPermissionSet res = writeAndReadBack(src);

        assertEquals("Permission sets are not equal", src, res);
        assertEquals("Hashes of permission sets are not equal [src=" + src + ", res=" + res + "]",
            src.hashCode(), res.hashCode());
    }

    /** */
    @Test
    public void testWithEmptyPermissions() throws Exception {
        SecurityBasicPermissionSet src = new SecurityBasicPermissionSet();
        src.setDefaultAllowAll(true);

        EnumSet<SecurityPermission> emptyPerms = EnumSet.noneOf(SecurityPermission.class);

        src.setSystemPermissions(emptyPerms);

        HashMap<String, EnumSet<SecurityPermission>> taskPerms = new HashMap<>();
        taskPerms.put("task1", emptyPerms);
        taskPerms.put("task2", EnumSet.of(TASK_EXECUTE));

        src.setTaskPermissions(taskPerms);

        SecurityBasicPermissionSet res = writeAndReadBack(src);

        assertEquals("Permission sets are not equal", src, res);
        assertEquals("Hashes of permission sets are not equal [src=" + src + ", res=" + res + "]",
            src.hashCode(), res.hashCode());

        // Explicitly test 'null' for system permissions.
        src.setSystemPermissions(null);

        res = writeAndReadBack(src);

        assertEquals("Permission sets are not equal", src, res);
        assertEquals("Hashes of permission sets are not equal [src=" + src + ", res=" + res + "]",
            src.hashCode(), res.hashCode());
    }

    /**
     * @param msg Message to write and read back through {@link DirectMessageWriter}/{@link DirectMessageReader}.
     * @param <T> Type of Message.
     *
     * @return Restored message.
     */
    private <T extends Message> T writeAndReadBack(T msg) throws IgniteCheckedException {
        GridTestKernalContext kctx = newContext();

        GridTestUtils.setFieldValue(kctx.grid(), "msgFactory", msgFactory);

        ByteBuffer buf = ByteBuffer.allocate(64 * 1024);

        DirectMessageWriter writer = new DirectMessageWriter(msgFactory);
        writer.setBuffer(buf);

        assertTrue(MessageSerialization.writeTo(msgFactory, msg, writer));

        buf.flip();

        DirectMessageReader reader = new DirectMessageReader(msgFactory, null);
        reader.setBuffer(buf);

        T res = (T)msgFactory.create(makeMessageType(buf.get(), buf.get()));

        assertTrue(MessageSerialization.readFrom(msgFactory, res, reader));

        return res;
    }
}

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
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.CoreMessagesProvider;
import org.apache.ignite.internal.direct.DirectMessageReader;
import org.apache.ignite.internal.direct.DirectMessageWriter;
import org.apache.ignite.internal.managers.communication.IgniteMessageFactoryImpl;
import org.apache.ignite.internal.util.nio.MessageSerialization;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
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
        src.setSystemPermissions(Set.of(ADMIN_CACHE, ADMIN_QUERY));
        src.setTaskPermissions(Map.of("task", Set.of(TASK_EXECUTE, TASK_CANCEL)));
        src.setServicePermissions(Map.of("service", Set.of(SERVICE_INVOKE, SERVICE_CANCEL)));
        src.setCachePermissions(Map.of("cache", Set.of(CACHE_CREATE, CACHE_PUT)));

        src.setCachePermissions(Map.of("cache", Set.of(CACHE_CREATE, CACHE_PUT)));
        SecurityBasicPermissionSet res = writeAndReadBack(src);

        assertTrue("Permission sets are not equal [src=" + src + ", res=" + res + "]", deepEquals(src, res));
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

    /**
     * Perfroms deep equals of permission sets.
     *
     * @param lhs First permissions set for equality check.
     * @param rhs Second permissions set for equality check.
     * @return Whether specified permission sets are equal.
     */
    public static boolean deepEquals(SecurityPermissionSet lhs, SecurityPermissionSet rhs) {
        if (lhs == rhs)
            return true;

        return lhs != null
            && rhs != null
            && lhs.defaultAllowAll() == rhs.defaultAllowAll()
            && (F.isEmpty(rhs.systemPermissions()) && F.isEmpty(rhs.systemPermissions())
            || F.eqNotOrdered(rhs.systemPermissions(), lhs.systemPermissions()))
            && eqNotOrdered(rhs.taskPermissions(), lhs.taskPermissions())
            && eqNotOrdered(rhs.servicePermissions(), lhs.servicePermissions())
            && eqNotOrdered(rhs.cachePermissions(), lhs.cachePermissions());
    }

    /**
     * @param m1 First map to check.
     * @param m2 Second map to check
     * @return {@code True} is maps are equal, {@code False} otherwise.
     */
    public static boolean eqNotOrdered(
        @Nullable Map<String, Collection<SecurityPermission>> m1,
        @Nullable Map<String, Collection<SecurityPermission>> m2) {
        if (m1 == m2)
            return true;

        if (m1 == null || m2 == null)
            return false;

        if (m1.size() != m2.size())
            return false;

        for (Map.Entry<String, Collection<SecurityPermission>> e : m1.entrySet()) {
            Collection<SecurityPermission> v1 = e.getValue();
            Collection<SecurityPermission> v2 = m2.get(e.getKey());

            if (v1 == v2)
                continue;

            if (v1 == null || v2 == null)
                return false;

            if (!F.eqNotOrdered(v1, v2))
                return false;
        }

        return true;
    }
}

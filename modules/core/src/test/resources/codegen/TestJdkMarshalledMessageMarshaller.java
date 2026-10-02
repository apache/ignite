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

package org.apache.ignite.internal;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.TestJdkMarshalledMessage;
import org.apache.ignite.internal.managers.communication.MessageMarshalling;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.MessageMarshaller;

/**
 * This class is generated automatically.
 *
 * @see org.apache.ignite.internal.MessageProcessor
 */
public final class TestJdkMarshalledMessageMarshaller implements MessageMarshaller<TestJdkMarshalledMessage> {
    /** */
    @Override public void marshal(TestJdkMarshalledMessage msg, Marshaller marsh, GridKernalContext kctx, CacheObjectContext cacheObjCtx) throws IgniteCheckedException {
        Marshaller jdkMarsh = kctx.marshallerContext().jdkMarshaller();

        CacheObjectContext ctx = cacheObjCtx;

        if (msg.data != null && msg.dataBytes == null)
            msg.dataBytes = U.marshal(jdkMarsh, msg.data);

        if (msg.nested != null)
            MessageMarshalling.marshal(msg.nested, jdkMarsh, kctx, ctx);

        if (msg.nioMsg != null)
            MessageMarshalling.marshal(msg.nioMsg, jdkMarsh, kctx, ctx);
    }

    /** */
    @Override public void unmarshal(TestJdkMarshalledMessage msg, Marshaller marsh, GridKernalContext kctx, CacheObjectContext cacheObjCtx, ClassLoader clsLdr) throws IgniteCheckedException {
        Marshaller jdkMarsh = kctx.marshallerContext().jdkMarshaller();

        CacheObjectContext ctx = cacheObjCtx;

        if (msg.nested != null)
            MessageMarshalling.unmarshal(msg.nested, jdkMarsh, kctx, ctx, clsLdr);

        if (msg.dataBytes != null) {
            msg.data = U.unmarshal(jdkMarsh, msg.dataBytes, clsLdr);

            msg.dataBytes = null;
        }
    }

    /** */
    @Override public void unmarshalNio(TestJdkMarshalledMessage msg, Marshaller marsh, GridKernalContext kctx) throws IgniteCheckedException {
        Marshaller jdkMarsh = kctx.marshallerContext().jdkMarshaller();

        if (msg.nioMsg != null)
            MessageMarshalling.unmarshal(msg.nioMsg, jdkMarsh, kctx);
    }
}

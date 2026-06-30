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
import org.apache.ignite.internal.TestMarshallableMessage;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.MessageMarshaller;

/**
 * This class is generated automatically.
 *
 * @see org.apache.ignite.internal.MessageProcessor
 */
public class TestMarshallableMessageMarshaller implements MessageMarshaller<TestMarshallableMessage> {
    /** */
    private final Marshaller marshaller;

    /** */
    public TestMarshallableMessageMarshaller(Marshaller marshaller) {
        this.marshaller = marshaller;
    }

    /** */
    @Override public void marshal(TestMarshallableMessage msg, GridKernalContext kctx, CacheObjectContext nested) throws IgniteCheckedException {
        msg.marshal(marshaller);
    }

    /** */
    @Override public void unmarshal(TestMarshallableMessage msg, GridKernalContext kctx, CacheObjectContext nested, ClassLoader clsLdr) throws IgniteCheckedException {
        msg.unmarshal(marshaller, clsLdr);
    }

    /** */
    @Override public void unmarshal(TestMarshallableMessage msg, GridKernalContext kctx) throws IgniteCheckedException {
        msg.unmarshal(marshaller, U.resolveClassLoader(kctx.config()));
    }
}
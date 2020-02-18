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

package org.apache.ignite.internal.processors.query.h2;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.h2.api.JavaObjectSerializer;
import org.jetbrains.annotations.NotNull;

/**
 * Ignite java object serializer implementation for H2.
 */
class H2JavaObjectSerializer implements JavaObjectSerializer {
    /** Class loader. */
    private final ClassLoader clsLdr;

    /** Marshaller. */
    private final Marshaller marshaller;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    H2JavaObjectSerializer(@NotNull GridKernalContext ctx) {
        marshaller = ctx.config().getMarshaller();
        clsLdr = U.resolveClassLoader(ctx.config());
    }

    /** {@inheritDoc} */
    @Override public byte[] serialize(Object obj) throws Exception {
        return U.marshal(marshaller, obj);
    }

    /** {@inheritDoc} */
    @Override public Object deserialize(byte[] bytes) throws Exception {
        return U.unmarshal(marshaller, bytes, clsLdr);
    }
}

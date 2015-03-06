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

import org.apache.ignite.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.util.concurrent.*;

/**
 * Marshaller context implementation.
 */
public class MarshallerContextImpl extends MarshallerContextAdapter {
    /** */
    private final CountDownLatch latch = new CountDownLatch(1);

    /** */
    private volatile GridCacheAdapter<Integer, String> cache;

    /**
     * @param ctx Kernal context.
     */
    public void onMarshallerCacheReady(GridKernalContext ctx) {
        assert ctx != null;

        cache = ctx.cache().marshallerCache();

        latch.countDown();
    }

    /** {@inheritDoc} */
    @Override public void registerClass(int id, Class cls) {
        if (!clsNameById.containsKey(id)) {
            U.debug("REG: " + cls.getName());

            try {
                if (cache == null)
                    U.awaitQuiet(latch);

                String old = cache.putIfAbsent(id, cls.getName());

                if (old != null && !old.equals(cls.getName()))
                    throw new IgniteException("Type ID collision occurred in OptimizedMarshaller. Use " +
                        "OptimizedMarshallerIdMapper to resolve it [id=" + id + ", clsName1=" + cls.getName() +
                        "clsName2=" + old + ']');

                clsNameById.putIfAbsent(id, cls.getName());
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public Class className(int id, ClassLoader ldr) throws ClassNotFoundException {
        String clsName = clsNameById.get(id);

        if (clsName == null) {
            try {
                if (cache == null)
                    U.awaitQuiet(latch);

                clsName = cache.get(id);

                assert clsName != null : id;

                String old = clsNameById.putIfAbsent(id, clsName);

                if (old != null)
                    clsName = old;
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
        }

        return Class.forName(clsName, false, ldr);
    }
}

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
    @Override protected boolean registerClassName(int id, String clsName) {
        try {
            if (cache == null)
                return false;

            String old = cache.putIfAbsent(id, clsName);

            if (old != null && !old.equals(clsName))
                throw new IgniteException("Type ID collision occurred in OptimizedMarshaller. Use " +
                    "OptimizedMarshallerIdMapper to resolve it [id=" + id + ", clsName1=" + clsName +
                    "clsName2=" + old + ']');
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override protected String className(int id) {
        try {
            if (cache == null)
                U.awaitQuiet(latch);

            return cache.get(id);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }
}

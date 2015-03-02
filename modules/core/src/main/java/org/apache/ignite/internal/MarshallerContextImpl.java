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
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.*;
import org.jdk8.backport.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Marshaller context implementation.
 */
public class MarshallerContextImpl implements MarshallerContext {
    /** */
    private static final String CLS_NAMES_FILE = "org/apache/ignite/marshaller/optimized/classnames.properties";

    /** */
    private final ConcurrentMap<Integer, IgniteBiTuple<Class, Boolean>> clsById = new ConcurrentHashMap8<>(256);

    /** */
    private final CountDownLatch latch = new CountDownLatch(1);

    /** */
    private volatile GridCacheAdapter<Integer, String> cache;

    MarshallerContextImpl() {
        try {
            ClassLoader ldr = getClass().getClassLoader();

            BufferedReader rdr = new BufferedReader(new InputStreamReader(ldr.getResourceAsStream(CLS_NAMES_FILE)));

            String clsName;

            while ((clsName = rdr.readLine()) != null) {
                Class cls = U.forName(clsName, ldr);

                clsById.put(cls.getName().hashCode(), F.t(cls, true));
            }
        }
        catch (IOException | ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * @param ctx Kernal context.
     */
    public void onMarshallerCacheReady(GridKernalContext ctx) {
        assert ctx != null;

        cache = ctx.cache().marshallerCache();

        latch.countDown();
    }

    /**
     * @param ldr Undeployed class loader.
     */
    public void onUndeployed(ClassLoader ldr) {
        for (Map.Entry<Integer, IgniteBiTuple<Class, Boolean>> e : clsById.entrySet()) {
            if (!e.getValue().get2() && ldr.equals(e.getValue().get1().getClassLoader()))
                clsById.remove(e.getKey());
        }
    }

    /**
     * Clears cached classes.
     */
    public void clear() {
        for (Map.Entry<Integer, IgniteBiTuple<Class, Boolean>> e : clsById.entrySet()) {
            if (!e.getValue().get2())
                clsById.remove(e.getKey());
        }
    }

    /** {@inheritDoc} */
    @Override public void registerClass(int id, Class cls) {
        if (cache == null)
            U.awaitQuiet(latch);

        if (clsById.putIfAbsent(id, F.t(cls, false)) == null) {
            try {
                String old = cache.putIfAbsent(id, cls.getName());

                if (old != null && !old.equals(cls.getName()))
                    throw new IgniteException("Type ID collision occurred in OptimizedMarshaller. Use " +
                        "OptimizedMarshallerIdMapper to resolve it [id=" + id + ", clsName1=" + cls.getName() +
                        "clsName2=" + old + ']');
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public Class className(int id, ClassLoader ldr) throws ClassNotFoundException {
        if (cache == null)
            U.awaitQuiet(latch);

        IgniteBiTuple<Class, Boolean> t = clsById.get(id);

        if (t == null) {
            String clsName;

            try {
                clsName = cache.get(id);
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }

            assert clsName != null : id;

            Class cls = U.forName(clsName, ldr);

            IgniteBiTuple<Class, Boolean> old = clsById.putIfAbsent(id, t = F.t(cls, false));

            if (old != null)
                t = old;
        }

        return t.get1();
    }
}

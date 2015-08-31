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

package org.apache.ignite.internal.util;

import java.lang.annotation.Annotation;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.util.lang.GridTuple;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

/**
 * Caches class loaders for classes.
 */
public final class GridAnnotationsCache {
    /** Annotation cache. */
    private static final ConcurrentMap<Class<?>,
        ConcurrentMap<Class<? extends Annotation>, GridTuple<Annotation>>> anns =
        new ConcurrentHashMap8<>();

    /**
     * @param cls Class.
     * @param annCls Annotation class.
     * @return Annotation (or {@code null}).
     */
    @Nullable public static <T extends Annotation> T getAnnotation(Class<?> cls, Class<T> annCls) {
        ConcurrentMap<Class<? extends Annotation>, GridTuple<Annotation>> clsAnns = anns.get(cls);

        if (clsAnns == null) {
            ConcurrentMap<Class<? extends Annotation>, GridTuple<Annotation>> old = anns.putIfAbsent(cls,
                clsAnns = new ConcurrentHashMap8<>());

            if (old != null)
                clsAnns = old;
        }

        GridTuple<T> ann = (GridTuple<T>)clsAnns.get(annCls);

        if (ann == null) {
            ann = F.t(U.getAnnotation(cls, annCls));

            clsAnns.putIfAbsent(annCls, (GridTuple<Annotation>)ann);
        }

        return ann.get();
    }


    /**
     * @param ldr Undeployed class loader.
     */
    public static void onUndeployed(ClassLoader ldr) {
        assert ldr != null;

        for (Class<?> cls : anns.keySet()) {
            if (ldr.equals(cls.getClassLoader()))
                anns.remove(cls);
        }
    }

    /**
     * Ensure singleton.
     */
    private GridAnnotationsCache() {
        // No-op.
    }
}
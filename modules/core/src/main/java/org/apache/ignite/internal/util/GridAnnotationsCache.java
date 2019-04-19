/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util;

import java.lang.annotation.Annotation;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.util.lang.GridTuple;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Caches class loaders for classes.
 */
public final class GridAnnotationsCache {
    /** Annotation cache. */
    private static final ConcurrentMap<Class<?>,
        ConcurrentMap<Class<? extends Annotation>, GridTuple<Annotation>>> anns =
        new ConcurrentHashMap<>();

    /**
     * @param cls Class.
     * @param annCls Annotation class.
     * @return Annotation (or {@code null}).
     */
    @Nullable public static <T extends Annotation> T getAnnotation(Class<?> cls, Class<T> annCls) {
        ConcurrentMap<Class<? extends Annotation>, GridTuple<Annotation>> clsAnns = anns.get(cls);

        if (clsAnns == null) {
            ConcurrentMap<Class<? extends Annotation>, GridTuple<Annotation>> old = anns.putIfAbsent(cls,
                clsAnns = new ConcurrentHashMap<>());

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
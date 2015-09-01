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

package org.apache.ignite.internal.processors.cache;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.affinity.AffinityKeyMapper;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.GridArgumentCheck;
import org.apache.ignite.internal.util.GridReflectionCache;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;

/**
 * Default key affinity mapper. If key class has annotation {@link AffinityKeyMapped},
 * then the value of annotated method or field will be used to get affinity value instead
 * of the key itself. If there is no annotation, then the key is used as is.
 * <p>
 * Convenience affinity key adapter, {@link AffinityKey} can be used in
 * conjunction with this mapper to automatically provide custom affinity keys for cache keys.
 * <p>
 * If non-default affinity mapper is used, is should be provided via
 * {@link CacheConfiguration#getAffinityMapper()} configuration property.
 */
public class GridCacheDefaultAffinityKeyMapper implements AffinityKeyMapper {
    /** */
    private static final long serialVersionUID = 0L;

    /** Injected ignite instance. */
    @IgniteInstanceResource
    protected transient Ignite ignite;

    /** Reflection cache. */
    private GridReflectionCache reflectCache = new GridReflectionCache(
        new P1<Field>() {
            @Override public boolean apply(Field f) {
                // Account for anonymous inner classes.
                return f.getAnnotation(AffinityKeyMapped.class) != null;
            }
        },
        new P1<Method>() {
            @Override public boolean apply(Method m) {
                // Account for anonymous inner classes.
                Annotation ann = m.getAnnotation(AffinityKeyMapped.class);

                if (ann != null) {
                    if (!F.isEmpty(m.getParameterTypes()))
                        throw new IllegalStateException("Method annotated with @AffinityKeyMapped annotation " +
                            "cannot have parameters: " + m);

                    return true;
                }

                return false;
            }
        }
    );

    /** Logger. */
    @LoggerResource
    protected transient IgniteLogger log;

    /**
     * If key class has annotation {@link AffinityKeyMapped},
     * then the value of annotated method or field will be used to get affinity value instead
     * of the key itself. If there is no annotation, then the key is returned as is.
     *
     * @param key Key to get affinity key for.
     * @return Affinity key for given key.
     */
    @Override public Object affinityKey(Object key) {
        GridArgumentCheck.notNull(key, "key");

        try {
            Object o = reflectCache.firstFieldValue(key);

            if (o != null)
                return o;
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to access affinity field for key [field=" +
                reflectCache.firstField(key.getClass()) + ", key=" + key + ']', e);
        }

        try {
            Object o = reflectCache.firstMethodValue(key);

            if (o != null)
                return o;
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to invoke affinity method for key [mtd=" +
                reflectCache.firstMethod(key.getClass()) + ", key=" + key + ']', e);
        }

        return key;
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        // No-op.
    }
}
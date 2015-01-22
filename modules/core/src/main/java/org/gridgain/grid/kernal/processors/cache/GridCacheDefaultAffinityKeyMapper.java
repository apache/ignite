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

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.portables.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.lang.annotation.*;
import java.lang.reflect.*;

/**
 * Default key affinity mapper. If key class has annotation {@link GridCacheAffinityKeyMapped},
 * then the value of annotated method or field will be used to get affinity value instead
 * of the key itself. If there is no annotation, then the key is used as is.
 * <p>
 * Convenience affinity key adapter, {@link GridCacheAffinityKey} can be used in
 * conjunction with this mapper to automatically provide custom affinity keys for cache keys.
 * <p>
 * If non-default affinity mapper is used, is should be provided via
 * {@link CacheConfiguration#getAffinityMapper()} configuration property.
 */
public class GridCacheDefaultAffinityKeyMapper implements GridCacheAffinityKeyMapper {
    /** */
    private static final long serialVersionUID = 0L;

    /** Reflection cache. */
    private GridReflectionCache reflectCache = new GridReflectionCache(
        new P1<Field>() {
            @Override public boolean apply(Field f) {
                // Account for anonymous inner classes.
                return f.getAnnotation(GridCacheAffinityKeyMapped.class) != null;
            }
        },
        new P1<Method>() {
            @Override public boolean apply(Method m) {
                // Account for anonymous inner classes.
                Annotation ann = m.getAnnotation(GridCacheAffinityKeyMapped.class);

                if (ann != null) {
                    if (!F.isEmpty(m.getParameterTypes()))
                        throw new IllegalStateException("Method annotated with @GridCacheAffinityKey annotation " +
                            "cannot have parameters: " + m);

                    return true;
                }

                return false;
            }
        }
    );

    /** Logger. */
    @IgniteLoggerResource
    private transient IgniteLogger log;

    /**
     * If key class has annotation {@link GridCacheAffinityKeyMapped},
     * then the value of annotated method or field will be used to get affinity value instead
     * of the key itself. If there is no annotation, then the key is returned as is.
     *
     * @param key Key to get affinity key for.
     * @return Affinity key for given key.
     */
    @Override public Object affinityKey(Object key) {
        GridArgumentCheck.notNull(key, "key");

        if (key instanceof PortableObject) {
            PortableObject po = (PortableObject)key;

            try {
                PortableMetadata meta = po.metaData();

                if (meta != null) {
                    String affKeyFieldName = meta.affinityKeyFieldName();

                    if (affKeyFieldName != null)
                        return po.field(affKeyFieldName);
                }
            }
            catch (PortableException e) {
                U.error(log, "Failed to get affinity field from portable object: " + key, e);
            }
        }
        else {
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
        }

        return key;
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        // No-op.
    }
}

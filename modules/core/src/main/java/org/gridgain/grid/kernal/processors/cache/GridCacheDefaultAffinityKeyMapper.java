/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.portables.*;
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
 * {@link GridCacheConfiguration#getAffinityMapper()} configuration property.
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
    @GridLoggerResource
    private transient GridLogger log;

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

        if (key instanceof GridPortableObject) {
            GridPortableObject po = (GridPortableObject)key;

            try {
                GridPortableMetadata meta = po.metaData();

                if (meta != null) {
                    String affKeyFieldName = meta.affinityKeyFieldName();

                    if (affKeyFieldName != null)
                        return po.field(affKeyFieldName);
                }
            }
            catch (GridPortableException e) {
                U.error(log, "Failed to get affinity field from portable object: " + key, e);
            }
        }
        else {
            try {
                Object o = reflectCache.firstFieldValue(key);

                if (o != null)
                    return o;
            }
            catch (GridException e) {
                U.error(log, "Failed to access affinity field for key [field=" +
                    reflectCache.firstField(key.getClass()) + ", key=" + key + ']', e);
            }

            try {
                Object o = reflectCache.firstMethodValue(key);

                if (o != null)
                    return o;
            }
            catch (GridException e) {
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

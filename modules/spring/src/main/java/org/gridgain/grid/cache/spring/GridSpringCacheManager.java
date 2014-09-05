/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.spring;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.springframework.beans.factory.*;
import org.springframework.cache.*;
import org.springframework.cache.support.*;

import java.io.*;
import java.util.*;

/**
 * Cache manager implementation.
 */
public class GridSpringCacheManager implements InitializingBean, CacheManager {
    /** Grid configuration file path. */
    private String cfgPath;

    /** Grid configuration. */
    private GridConfiguration cfg;

    /** Grid name. */
    private String gridName;

    /** Grid instance. */
    private Grid grid;

    /**
     * @return Grid configuration file path.
     */
    public String getConfigurationPath() {
        return cfgPath;
    }

    /**
     * @param cfgPath Grid configuration file path.
     */
    public void setConfigurationPath(String cfgPath) {
        this.cfgPath = cfgPath;
    }

    /**
     * @return Grid configuration.
     */
    public GridConfiguration getConfiguration() {
        return cfg;
    }

    /**
     * @param cfg Grid configuration.
     */
    public void setConfiguration(GridConfiguration cfg) {
        this.cfg = cfg;
    }

    /**
     * @return Grid name.
     */
    public String getGridName() {
        return gridName;
    }

    /**
     * @param gridName Grid name.
     */
    public void setGridName(String gridName) {
        this.gridName = gridName;
    }

    /** {@inheritDoc} */
    @Override public void afterPropertiesSet() throws Exception {
        assert grid == null;

        grid = GridGain.grid(gridName);
    }

    /** {@inheritDoc} */
    @Override public Cache getCache(String name) {
        assert grid != null;

        return new SpringCache(grid.cache(name));
    }

    /** {@inheritDoc} */
    @Override public Collection<String> getCacheNames() {
        assert grid != null;

        return F.viewReadOnly(grid.caches(), new GridClosure<GridCache<?, ?>, String>() {
            @Override public String apply(GridCache<?, ?> c) {
                return c.name();
            }
        });
    }

    /**
     * Cache implementation.
     */
    private static class SpringCache implements Cache {
        /** */
        private final GridCache<Object, Object> cache;

        /**
         * @param cache Cache.
         */
        SpringCache(GridCache<Object, Object> cache) {
            assert cache != null;

            this.cache = cache;
        }

        /** {@inheritDoc} */
        @Override public String getName() {
            return cache.name();
        }

        /** {@inheritDoc} */
        @Override public Object getNativeCache() {
            return cache;
        }

        /** {@inheritDoc} */
        @Override public ValueWrapper get(Object key) {
            try {
                Object val = cache.get(key);

                return val != null ? new SimpleValueWrapper(val) : null;
            }
            catch (GridException e) {
                throw new GridRuntimeException("Failed to get value from cache [cacheName=" + cache.name() +
                    ", key=" + key + ']', e);
            }
        }

        /** {@inheritDoc} */
        @Override public void put(Object key, Object val) {
            try {
                cache.putx(key, val);
            }
            catch (GridException e) {
                throw new GridRuntimeException("Failed to put value to cache [cacheName=" + cache.name() +
                    ", key=" + key + ", val=" + val + ']', e);
            }
        }

        /** {@inheritDoc} */
        @Override public void evict(Object key) {
            try {
                cache.removex(key);
            }
            catch (GridException e) {
                throw new GridRuntimeException("Failed to remove value from cache [cacheName=" + cache.name() +
                    ", key=" + key + ']', e);
            }
        }

        /** {@inheritDoc} */
        @Override public void clear() {
            try {
                cache.gridProjection().compute().broadcast(new ClearClosure(cache.name())).get();
            }
            catch (GridException e) {
                throw new GridRuntimeException("Failed to clear cache [cacheName=" + cache.name() + ']', e);
            }
        }
    }

    /**
     * Closure that removes all entries from cache.
     */
    private static class ClearClosure extends CAX implements Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Cache name. */
        private String cacheName;

        /** Injected grid instance. */
        @GridInstanceResource
        private Grid grid;

        /**
         * For {@link Externalizable}.
         */
        public ClearClosure() {
            // No-op.
        }

        /**
         * @param cacheName Cache name.
         */
        private ClearClosure(String cacheName) {
            this.cacheName = cacheName;
        }

        /** {@inheritDoc} */
        @Override public void applyx() throws GridException {
            GridCache<Object, Object> cache = grid.cache(cacheName);

            if (cache != null)
                cache.removeAll();
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeString(out, cacheName);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            cacheName = U.readString(in);
        }
    }
}

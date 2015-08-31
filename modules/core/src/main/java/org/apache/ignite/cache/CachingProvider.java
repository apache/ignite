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

package org.apache.ignite.cache;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.WeakHashMap;
import javax.cache.CacheException;
import javax.cache.configuration.OptionalFeature;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of JSR-107 {@link javax.cache.spi.CachingProvider}.
 */
public class CachingProvider implements javax.cache.spi.CachingProvider {
    /** */
    private static final URI DEFAULT_URI;

    /**
     *
     */
    static {
        URI uri = null;

        try {
            URL dfltCfgURL = U.resolveIgniteUrl(IgnitionEx.DFLT_CFG);

            if (dfltCfgURL != null)
                uri = dfltCfgURL.toURI();
        }
        catch (URISyntaxException ignored) {
            // No-op.
        }

        if (uri == null)
            uri = URI.create("ignite://default");

        DEFAULT_URI = uri;
    }

    /** */
    public static final Properties DFLT_PROPS = new Properties();

    /** */
    private final Map<ClassLoader, Map<URI, GridFutureAdapter<CacheManager>>> cacheManagers = new WeakHashMap<>();

    /** {@inheritDoc} */
    @Override public javax.cache.CacheManager getCacheManager(@Nullable URI uri, ClassLoader clsLdr, Properties props)
        throws CacheException {
        if (uri == null)
            uri = getDefaultURI();

        if (clsLdr == null)
            clsLdr = getDefaultClassLoader();

        GridFutureAdapter<CacheManager> fut;

        boolean needStartMgr = false;

        Map<URI, GridFutureAdapter<CacheManager>> uriMap;

        synchronized (cacheManagers) {
            uriMap = cacheManagers.get(clsLdr);

            if (uriMap == null) {
                uriMap = new HashMap<>();

                cacheManagers.put(clsLdr, uriMap);
            }

            fut = uriMap.get(uri);

            if (fut == null) {
                needStartMgr = true;

                fut = new GridFutureAdapter<>();

                uriMap.put(uri, fut);
            }
        }

        if (needStartMgr) {
            try {
                CacheManager mgr = new CacheManager(uri, this, clsLdr, props);

                fut.onDone(mgr);

                return mgr;
            }
            catch (Throwable e) {
                synchronized (cacheManagers) {
                    uriMap.remove(uri);
                }

                fut.onDone(e);

                if (e instanceof Error)
                    throw (Error)e;

                throw CU.convertToCacheException(U.cast(e));
            }
        }
        else {
            try {
                return fut.get();
            }
            catch (IgniteCheckedException e) {
                throw CU.convertToCacheException(e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public ClassLoader getDefaultClassLoader() {
        return getClass().getClassLoader();
    }

    /** {@inheritDoc} */
    @Override public URI getDefaultURI() {
        return DEFAULT_URI;
    }

    /** {@inheritDoc} */
    @Override public Properties getDefaultProperties() {
        return DFLT_PROPS;
    }

    /** {@inheritDoc} */
    @Override public javax.cache.CacheManager getCacheManager(URI uri, ClassLoader clsLdr) {
        return getCacheManager(uri, clsLdr, getDefaultProperties());
    }

    /** {@inheritDoc} */
    @Override public javax.cache.CacheManager getCacheManager() {
        return getCacheManager(getDefaultURI(), getDefaultClassLoader());
    }

    /**
     * @param ignite Ignite.
     */
    public javax.cache.CacheManager findManager(Ignite ignite) {
        synchronized (cacheManagers) {
            for (Map<URI, GridFutureAdapter<CacheManager>> map : cacheManagers.values()) {
                for (GridFutureAdapter<CacheManager> fut : map.values()) {
                    if (fut.isDone()) {
                        assert !fut.isFailed();

                        try {
                            CacheManager mgr = fut.get();

                            if (mgr.unwrap(Ignite.class) == ignite)
                                return mgr;
                        }
                        catch (IgniteCheckedException e) {
                            throw CU.convertToCacheException(e);
                        }
                    }
                }
            }
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        Collection<GridFutureAdapter<CacheManager>> futs = new ArrayList<>();

        synchronized (cacheManagers) {
            for (Map<URI, GridFutureAdapter<CacheManager>> uriMap : cacheManagers.values())
                futs.addAll(uriMap.values());

            cacheManagers.clear();
        }

        closeManagers(futs);
    }

    /** {@inheritDoc} */
    @Override public void close(ClassLoader clsLdr) {
        Map<URI, GridFutureAdapter<CacheManager>> uriMap;

        synchronized (cacheManagers) {
            uriMap = cacheManagers.remove(clsLdr);
        }

        if (uriMap == null)
            return;

        closeManagers(uriMap.values());
    }

    /**
     * @param futs Futs.
     */
    private void closeManagers(Collection<GridFutureAdapter<CacheManager>> futs) {
        for (GridFutureAdapter<CacheManager> fut : futs) {
            try {
                CacheManager mgr = fut.get();

                mgr.close();
            }
            catch (Exception ignored) {
                // No-op.
            }
        }
    }

    /**
     * @param mgr Manager.
     */
    protected void removeClosedManager(CacheManager mgr) {
        synchronized (cacheManagers) {
            Map<URI, GridFutureAdapter<CacheManager>> uriMap = cacheManagers.get(mgr.getClassLoader());

            GridFutureAdapter<CacheManager> fut = uriMap.get(mgr.getURI());

            if (fut != null && fut.isDone() && !fut.isFailed()) {
                try {
                    CacheManager cachedManager = fut.get();

                    if (cachedManager == mgr)
                        uriMap.remove(mgr.getURI());
                }
                catch (IgniteCheckedException e) {
                    throw CU.convertToCacheException(e);
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void close(URI uri, ClassLoader clsLdr) {
        GridFutureAdapter<CacheManager> fut;

        synchronized (cacheManagers) {
            Map<URI, GridFutureAdapter<CacheManager>> uriMap = cacheManagers.get(clsLdr);

            if (uriMap == null)
                return;

            fut = uriMap.remove(uri);
        }

        if (fut != null) {
            CacheManager mgr;

            try {
                mgr = fut.get();
            }
            catch (Exception ignored) {
                return;
            }

            mgr.close();
        }

    }

    /** {@inheritDoc} */
    @Override public boolean isSupported(OptionalFeature optionalFeature) {
        return false;
    }
}
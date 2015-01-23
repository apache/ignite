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

package org.apache.ignite;

import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.configuration.*;
import javax.cache.spi.*;
import java.net.*;
import java.util.*;

/**
 *
 */
public class IgniteCachingProvider implements CachingProvider {
    /** */
    private static final URI DEFAULT_URI;

    /**
     *
     */
    static {
        URI uri = null;

        try {
            URL dfltCfgURL = U.resolveGridGainUrl(GridGainEx.DFLT_CFG);
            if (dfltCfgURL != null)
                uri = dfltCfgURL.toURI();
        }
        catch (URISyntaxException ignored) {

        }

        if (uri == null)
            uri = URI.create("ignite://default");

        DEFAULT_URI = uri;
    }

    /** */
    public static final Properties DFLT_PROPS = new Properties();

    /** */
    private final Map<ClassLoader, Map<URI, IgniteCacheManager>> cacheManagers = new WeakHashMap<>();

    /** {@inheritDoc} */
    @Override public CacheManager getCacheManager(@Nullable URI uri, ClassLoader clsLdr, Properties props) {
        if (uri == null)
            uri = getDefaultURI();

        if (clsLdr == null)
            clsLdr = getDefaultClassLoader();

        synchronized (cacheManagers) {
            Map<URI, IgniteCacheManager> uriMap = cacheManagers.get(clsLdr);

            if (uriMap == null) {
                uriMap = new HashMap<>();

                cacheManagers.put(clsLdr, uriMap);
            }

            IgniteCacheManager mgr = uriMap.get(uri);

            if (mgr == null || mgr.isClosed()) {
                mgr = new IgniteCacheManager(uri, this, clsLdr, props);

                uriMap.put(uri, mgr);
            }

            return mgr;
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
    @Override public CacheManager getCacheManager(URI uri, ClassLoader clsLdr) {
        return getCacheManager(uri, clsLdr, getDefaultProperties());
    }

    /** {@inheritDoc} */
    @Override public CacheManager getCacheManager() {
        return getCacheManager(getDefaultURI(), getDefaultClassLoader());
    }

    /**
     * @param cache Cache.
     */
    public CacheManager findManager(IgniteCache<?,?> cache) {
        Ignite ignite = cache.unwrap(Ignite.class);

        synchronized (cacheManagers) {
            for (Map<URI, IgniteCacheManager> map : cacheManagers.values()) {
                for (IgniteCacheManager manager : map.values()) {
                    if (manager.isManagedIgnite(ignite))
                        return manager;
                }
            }
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        Collection<IgniteCacheManager> mgrs = new ArrayList<>();

        synchronized (cacheManagers) {
            for (Map<URI, IgniteCacheManager> uriMap : cacheManagers.values())
                mgrs.addAll(uriMap.values());

            cacheManagers.clear();
        }

        for (IgniteCacheManager mgr : mgrs)
            mgr.close();
    }

    /** {@inheritDoc} */
    @Override public void close(ClassLoader clsLdr) {
        Collection<IgniteCacheManager> mgrs;

        synchronized (cacheManagers) {
            Map<URI, IgniteCacheManager> uriMap = cacheManagers.remove(clsLdr);

            if (uriMap == null)
                return;

            mgrs = uriMap.values();
        }

        for (IgniteCacheManager mgr : mgrs)
            mgr.close();
    }

    /** {@inheritDoc} */
    @Override public void close(URI uri, ClassLoader clsLdr) {
        IgniteCacheManager mgr;

        synchronized (cacheManagers) {
            Map<URI, IgniteCacheManager> uriMap = cacheManagers.get(clsLdr);

            if (uriMap == null)
                return;

            mgr = uriMap.remove(uri);
        }

        if (mgr != null)
            mgr.close();
    }

    /** {@inheritDoc} */
    @Override public boolean isSupported(OptionalFeature optionalFeature) {
        return false;
    }
}

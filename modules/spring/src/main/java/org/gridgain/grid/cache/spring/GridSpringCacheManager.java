/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.spring;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.springframework.beans.factory.*;
import org.springframework.cache.*;
import org.springframework.cache.support.*;

import java.io.*;
import java.util.*;

/**
 * Implementation of Spring cache abstraction based on GridGain cache.
 * <h1>Overview</h1>
 * Spring cache abstraction allows to enable caching for Java methods
 * so that the result of a method execution is stored in some storage. If
 * later the same method is called with the same set of parameters,
 * the result will be retrieved from that storage instead of actually
 * executing the method. For more information, refer to
 * <a href="http://docs.spring.io/spring/docs/current/spring-framework-reference/html/cache.html">
 * Spring Cache Abstraction documentation</a>.
 * <h1 class="header">How To Enable Caching</h1>
 * To enable caching based on GridGain cache in your Spring application,
 * you will need to do the following:
 * <ul>
 *     <li>
 *         Start a GridGain node with configured cache in the same JVM
 *         where you application is running.
 *     </li>
 *     <li>
 *         Configure {@code GridSpringCacheManager} as a cache provider
 *         in Spring application context.
 *     </li>
 * </ul>
 * {@code GridSpringCacheManager} can start a node itself on its startup
 * based on provided GridGain configuration. You can provide path to a
 * Spring configuration XML file, like below (path can be absolute or
 * relative to {@code GRIDGAIN_HOME}):
 * <pre name="code" class="xml">
 * &lt;beans xmlns="http://www.springframework.org/schema/beans"
 *        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 *        xmlns:cache="http://www.springframework.org/schema/cache"
 *        xsi:schemaLocation="
 *         http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd
 *         http://www.springframework.org/schema/cache http://www.springframework.org/schema/cache/spring-cache.xsd"&gt;
 *     &lt;-- Provide configuration file path --&gt;
 *     &lt;bean id="cacheManager" class="org.gridgain.grid.cache.spring.GridSpringCacheManager"&gt;
 *         &lt;property name="configurationPath" value="examples/config/spring-cache.xml"/&gt;
 *     &lt;/bean>
 *
 *     &lt;-- Use annotation-driven caching configuration. --&gt;
 *     &lt;cache:annotation-driven/&gt;
 * &lt;/beans&gt;
 * </pre>
 * Or you can provide a {@link org.apache.ignite.configuration.IgniteConfiguration} bean, like below:
 * <pre name="code" class="xml">
 * &lt;beans xmlns="http://www.springframework.org/schema/beans"
 *        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 *        xmlns:cache="http://www.springframework.org/schema/cache"
 *        xsi:schemaLocation="
 *         http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd
 *         http://www.springframework.org/schema/cache http://www.springframework.org/schema/cache/spring-cache.xsd"&gt;
 *     &lt;-- Provide configuration bean. --&gt;
 *     &lt;bean id="cacheManager" class="org.gridgain.grid.cache.spring.GridSpringCacheManager"&gt;
 *         &lt;property name="configuration"&gt;
 *             &lt;bean id="gridCfg" class="org.gridgain.grid.GridConfiguration"&gt;
 *                 ...
 *             &lt;/bean&gt;
 *         &lt;/property&gt;
 *     &lt;/bean&gt;
 *
 *     &lt;-- Use annotation-driven caching configuration. --&gt;
 *     &lt;cache:annotation-driven/&gt;
 * &lt;/beans&gt;
 * </pre>
 * Note that providing both configuration path and configuration bean is illegal
 * and results in {@link IllegalArgumentException}.
 * <p>
 * If you already have GridGain node running within your application,
 * simply provide correct Grid name, like below (if there is no Grid
 * instance with such name, exception will be thrown):
 * <pre name="code" class="xml">
 * &lt;beans xmlns="http://www.springframework.org/schema/beans"
 *        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 *        xmlns:cache="http://www.springframework.org/schema/cache"
 *        xsi:schemaLocation="
 *         http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd
 *         http://www.springframework.org/schema/cache http://www.springframework.org/schema/cache/spring-cache.xsd"&gt;
 *     &lt;-- Provide configuration file path --&gt;
 *     &lt;bean id="cacheManager" class="org.gridgain.grid.cache.spring.GridSpringCacheManager"&gt;
 *         &lt;property name="gridName" value="myGrid"/&gt;
 *     &lt;/bean>
 *
 *     &lt;-- Use annotation-driven caching configuration. --&gt;
 *     &lt;cache:annotation-driven/&gt;
 * &lt;/beans&gt;
 * </pre>
 * This can be used, for example, when you are running your application
 * in a J2EE Web container and use {@gglink org.gridgain.grid.startup.servlet.GridServletContextListenerStartup}
 * for node startup.
 * <p>
 * If neither {@link #setConfigurationPath(String) configurationPath},
 * {@link #setConfiguration(org.apache.ignite.configuration.IgniteConfiguration) configuration}, nor
 * {@link #setGridName(String) gridName} are provided, cache manager
 * will try to use default Grid instance (the one with the {@code null}
 * name). If it doesn't exist, exception will be thrown.
 * <h1>Starting Remote Nodes</h1>
 * Remember that the node started inside your application is an entry point
 * to the whole topology it connects to. You can start as many remote standalone
 * nodes as you need using {@code bin/ggstart.{sh|bat}} scripts provided in
 * GridGain distribution, and all these nodes will participate
 * in caching data.
 */
public class GridSpringCacheManager implements InitializingBean, CacheManager {
    /** Grid configuration file path. */
    private String cfgPath;

    /** Grid configuration. */
    private IgniteConfiguration cfg;

    /** Grid name. */
    private String gridName;

    /** Grid instance. */
    private Ignite ignite;

    /**
     * Gets configuration file path.
     *
     * @return Grid configuration file path.
     */
    public String getConfigurationPath() {
        return cfgPath;
    }

    /**
     * Sets configuration file path.
     *
     * @param cfgPath Grid configuration file path.
     */
    public void setConfigurationPath(String cfgPath) {
        this.cfgPath = cfgPath;
    }

    /**
     * Gets configuration bean.
     *
     * @return Grid configuration bean.
     */
    public IgniteConfiguration getConfiguration() {
        return cfg;
    }

    /**
     * Sets configuration bean.
     *
     * @param cfg Grid configuration bean.
     */
    public void setConfiguration(IgniteConfiguration cfg) {
        this.cfg = cfg;
    }

    /**
     * Gets grid name.
     *
     * @return Grid name.
     */
    public String getGridName() {
        return gridName;
    }

    /**
     * Sets grid name.
     *
     * @param gridName Grid name.
     */
    public void setGridName(String gridName) {
        this.gridName = gridName;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("IfMayBeConditional")
    @Override public void afterPropertiesSet() throws Exception {
        assert ignite == null;

        if (cfgPath != null && cfg != null) {
            throw new IllegalArgumentException("Both 'configurationPath' and 'configuration' are " +
                "provided. Set only one of these properties if you need to start a GridGain node inside of " +
                "GridSpringCacheManager. If you already have a node running, omit both of them and set" +
                "'gridName' property.");
        }

        if (cfgPath != null)
            ignite = Ignition.start(cfgPath);
        else if (cfg != null)
            ignite = Ignition.start(cfg);
        else
            ignite = Ignition.ignite(gridName);
    }

    /** {@inheritDoc} */
    @Override public Cache getCache(String name) {
        assert ignite != null;

        return new SpringCache(ignite.cache(name));
    }

    /** {@inheritDoc} */
    @Override public Collection<String> getCacheNames() {
        assert ignite != null;

        return F.viewReadOnly(ignite.caches(), new IgniteClosure<GridCache<?, ?>, String>() {
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
        @Override public <T> T get(Object key, Class<T> type) {
            try {
                Object val = cache.get(key);

                if (val != null && type != null && !type.isInstance(val))
                    throw new IllegalStateException("Cached value is not of required type [cacheName=" + cache.name() +
                        ", key=" + key + ", val=" + val + ", requiredType=" + type + ']');

                return (T)val;
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
        @Override public ValueWrapper putIfAbsent(Object key, Object val) {
            try {
                Object old = cache.putIfAbsent(key, val);

                return old != null ? new SimpleValueWrapper(old) : null;
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
                Ignite ignite = cache.gridProjection().ignite();

                ignite.compute(cache.gridProjection()).broadcast(new ClearClosure(cache.name()));
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
        @IgniteInstanceResource
        private Ignite ignite;

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
            GridCache<Object, Object> cache = ignite.cache(cacheName);

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
